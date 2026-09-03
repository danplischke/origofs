//! One co-editing undo stack per (path, actor) across workers (#146).
//!
//! `coedit_undo_multiworker.rs` measured why this is needed: two workers each
//! holding a stack for the same actor can pop items that touch the same content,
//! and because origofs's author stamp is written in the same undo step as the
//! insert it describes, one worker's undo strips a stamp the other's restore
//! needs — leaving text present but **unattributed**, which the next checkpoint
//! credits to the checkpointer. The claim removes that precondition rather than
//! patching the symptom.
//!
//! These drive the **store** directly rather than going through `Fs`, on the same
//! reasoning `posix_lock_sim.rs` gives: the decision can be right while the SQL
//! translation is not — an upsert arm that fires when it should not, an expiry
//! comparison off by one. Three properties matter. The claim is genuinely
//! exclusive (two workers must never both be told yes), a dead worker's claim is
//! reclaimable (or one crash denies an actor undo until somebody edits the
//! database by hand), and a live worker keeps its claim across renewals.

#![cfg(feature = "coedit")]

use origofs_core::{MetadataStore, SqliteMetadataStore};
use std::sync::Arc;

/// Wall-clock stands still in these tests: every time is passed explicitly, so a
/// lease test asserts the comparison rather than sleeping through it.
const NOW: i64 = 1_000_000;
const LEASE: i64 = 3600;

async fn store() -> Arc<SqliteMetadataStore> {
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    meta.init().await.unwrap();
    Arc::new(meta)
}

/// A claim with an hour of lease left, as of [`NOW`].
async fn claim(m: &SqliteMetadataStore, path: &str, actor: i64, holder: &str) -> bool {
    m.claim_undo_stack(path, actor, holder, NOW + LEASE, NOW)
        .await
        .unwrap()
}

/// A claim whose lease ran out before [`NOW`] — the shape a crashed worker leaves.
async fn stale_claim(m: &SqliteMetadataStore, path: &str, actor: i64, holder: &str) {
    assert!(
        m.claim_undo_stack(path, actor, holder, NOW - 1, NOW - 2)
            .await
            .unwrap()
    );
}

/// The exclusion, which is the whole point.
#[tokio::test]
async fn only_one_worker_holds_an_actors_stack() {
    let m = store().await;

    assert!(claim(&m, "/doc.md", 7, "worker-a").await);
    assert!(
        !claim(&m, "/doc.md", 7, "worker-b").await,
        "two workers were both granted the same actor's undo stack — the \
         attribution defect this exists to prevent is reachable again"
    );

    // The holder may re-claim freely; that is how a rejoin renews rather than
    // locking itself out of a stack it already owns.
    assert!(claim(&m, "/doc.md", 7, "worker-a").await);
}

/// Per (path, actor), not per path or per actor: two people editing one document,
/// or one person editing two documents, must not block each other.
#[tokio::test]
async fn claims_do_not_collide_across_actors_or_paths() {
    let m = store().await;

    assert!(claim(&m, "/doc.md", 7, "worker-a").await);
    assert!(claim(&m, "/doc.md", 8, "worker-b").await); // another actor, same room
    assert!(claim(&m, "/other.md", 7, "worker-b").await); // same actor, another doc
}

/// Releasing hands the actor straight to the next worker rather than making them
/// wait out a lease nobody is renewing.
#[tokio::test]
async fn releasing_frees_the_actor_immediately() {
    let m = store().await;

    assert!(claim(&m, "/doc.md", 7, "worker-a").await);
    assert!(!claim(&m, "/doc.md", 7, "worker-b").await);

    assert!(
        m.release_undo_stack("/doc.md", 7, "worker-a")
            .await
            .unwrap()
    );
    assert!(claim(&m, "/doc.md", 7, "worker-b").await);
}

/// A release names its holder, so a worker whose lease already lapsed cannot
/// delete the claim its successor has since taken.
#[tokio::test]
async fn a_release_cannot_drop_someone_elses_claim() {
    let m = store().await;

    assert!(claim(&m, "/doc.md", 7, "worker-a").await);
    assert!(
        !m.release_undo_stack("/doc.md", 7, "worker-b")
            .await
            .unwrap(),
        "a stale worker deleted the claim its successor holds"
    );
    assert!(
        !claim(&m, "/doc.md", 7, "worker-b").await,
        "still worker-a's"
    );
}

/// A clean shutdown drops everything at once, so a redeploy does not leave every
/// actor it was serving without undo until their leases run out.
#[tokio::test]
async fn a_shutdown_releases_every_claim_a_worker_holds() {
    let m = store().await;

    for (path, actor) in [("/a.md", 1), ("/b.md", 1), ("/a.md", 2)] {
        assert!(claim(&m, path, actor, "worker-a").await);
    }
    // Another worker's claim must survive this one's shutdown.
    assert!(claim(&m, "/c.md", 3, "worker-b").await);

    assert_eq!(
        m.release_undo_claims_for_holder("worker-a").await.unwrap(),
        3
    );
    for (path, actor) in [("/a.md", 1), ("/b.md", 1), ("/a.md", 2)] {
        assert!(claim(&m, path, actor, "worker-b").await);
    }
    assert!(
        !claim(&m, "/c.md", 3, "worker-a").await,
        "worker-b's unrelated claim was collected by worker-a's shutdown"
    );
}

/// A worker that is OOM-killed cannot release anything, so the lease is what
/// stops one crash denying an actor undo permanently.
#[tokio::test]
async fn an_expired_claim_is_taken_over() {
    let m = store().await;

    stale_claim(&m, "/doc.md", 7, "crashed").await;
    assert!(
        claim(&m, "/doc.md", 7, "worker-b").await,
        "a crashed worker's claim outlived its lease and denied the actor undo"
    );
    assert!(
        !claim(&m, "/doc.md", 7, "worker-c").await,
        "and now it really is worker-b's"
    );
}

/// Renewal keeps a live worker's claim and leaves everyone else's alone — the
/// property that makes the lease safe to keep short.
#[tokio::test]
async fn renewal_extends_only_the_holders_claims() {
    let m = store().await;

    stale_claim(&m, "/doc.md", 7, "worker-a").await;
    stale_claim(&m, "/doc.md", 8, "worker-b").await;

    // Only worker-a is alive to renew.
    assert_eq!(
        m.renew_undo_claims("worker-a", NOW + LEASE).await.unwrap(),
        1
    );

    assert!(
        !claim(&m, "/doc.md", 7, "worker-c").await,
        "worker-a's claim is live again and must not be takeable"
    );
    assert!(
        claim(&m, "/doc.md", 8, "worker-c").await,
        "worker-b's is still expired and must be takeable"
    );
}

/// Concurrent claimants: exactly one wins. The decision is read-decide-write, so
/// it has to be one atomic statement — two workers must never both be told yes,
/// and a test that only ever claims sequentially would never notice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_produce_exactly_one_winner() {
    let m = store().await;

    let mut tasks = Vec::new();
    for i in 0..16 {
        let m = m.clone();
        tasks.push(tokio::spawn(async move {
            m.claim_undo_stack("/doc.md", 7, &format!("worker-{i}"), NOW + LEASE, NOW)
                .await
                .unwrap()
        }));
    }
    let mut winners = 0;
    for t in tasks {
        if t.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(
        winners, 1,
        "{winners} workers were granted the same actor's undo stack; the claim is \
         not atomic"
    );
}
