//! The ACL cache is **exact, not stale** (issue #124, phase 0).
//!
//! `effective_perms` was up to three round trips — `list_acl`, which returns every
//! grant the actor holds, plus the default-deny switch, plus the actor's policy —
//! with a prefix match linear over the result. Measured against the read it would
//! guard, that cost 16% of a read at one grant and 228% at 201: more than twice the
//! read it protects, growing with exactly the per-project grants a multi-tenant
//! deployment accumulates.
//!
//! The obvious fix is a time-to-live, and it is the wrong one. Every write check in
//! this engine is exact today; a TTL would leave a revoked actor writing on another
//! worker until the window closed, trading a correctness property for a
//! performance one. So the cache is keyed on a generation counter in the store,
//! bumped by every change that can alter an answer. These tests are mostly about
//! that: a cache that is fast and wrong is worse than the slow version.

use origofs_core::{Fs, MemStore, MetadataStore, Perms, SqliteMetadataStore, WritePolicy};
use std::sync::Arc;
use std::time::Instant;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

async fn fixture() -> (TestFs, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    (fs, agent)
}

/// Two handles over **one store**, which is how a cache goes stale in production:
/// worker A revokes, worker B must not keep serving the old answer.
async fn two_handles() -> (TestFs, TestFs, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let content = Arc::new(MemStore::new());
    let a = Fs::new(meta.clone(), content.clone());
    a.init().await.unwrap();
    let agent = a.create_agent("claude", "opus", None).await.unwrap();
    let b = Fs::new(meta, content);
    (a, b, agent)
}

// --- invalidation ------------------------------------------------------------

#[tokio::test]
async fn a_grant_is_visible_to_the_very_next_check() {
    let (fs, agent) = fixture().await;
    // Warm the cache on the pre-grant answer.
    assert!(
        fs.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
    fs.grant(agent, "/x", Perms::NONE, None).await.unwrap();
    assert_eq!(fs.effective_perms(agent, "/x").await.unwrap(), Perms::NONE);
}

#[tokio::test]
async fn a_revoke_is_visible_to_the_very_next_check() {
    let (fs, agent) = fixture().await;
    fs.grant(agent, "/x", Perms::NONE, None).await.unwrap();
    assert_eq!(fs.effective_perms(agent, "/x").await.unwrap(), Perms::NONE);
    assert!(fs.revoke(agent, "/x", None).await.unwrap());
    // Back to the write-policy fallback.
    assert!(
        fs.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
}

#[tokio::test]
async fn flipping_default_deny_is_visible_at_once() {
    let (fs, agent) = fixture().await;
    assert!(
        fs.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
    fs.set_acl_default_deny(true).await.unwrap();
    assert_eq!(fs.effective_perms(agent, "/x").await.unwrap(), Perms::NONE);
    fs.set_acl_default_deny(false).await.unwrap();
    assert!(
        fs.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
}

#[tokio::test]
async fn changing_the_write_policy_is_visible_at_once() {
    // The policy is the fallback the cache also holds, so it has to invalidate
    // too — easy to miss, because it lives in a different module from the ACLs.
    let (fs, agent) = fixture().await;
    assert!(
        fs.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
    fs.set_write_policy(agent, WritePolicy::Propose)
        .await
        .unwrap();
    let perms = fs.effective_perms(agent, "/x").await.unwrap();
    assert!(!perms.contains(Perms::WRITE), "{perms}");
    assert!(perms.contains(Perms::PROPOSE), "{perms}");
}

// --- the cross-worker case ---------------------------------------------------

#[tokio::test]
async fn a_grant_on_one_handle_is_seen_by_another_on_the_same_store() {
    let (a, b, agent) = two_handles().await;
    // Warm B's cache on the pre-grant answer, so a stale hit would be visible.
    assert!(
        b.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
    a.grant(agent, "/x", Perms::NONE, None).await.unwrap();
    assert_eq!(
        b.effective_perms(agent, "/x").await.unwrap(),
        Perms::NONE,
        "handle B served a cached answer after handle A changed the grant"
    );
}

#[tokio::test]
async fn a_revoke_on_one_handle_is_seen_by_another_on_the_same_store() {
    // The direction that matters: a stale cache here keeps a revoked actor
    // writing, which is the failure a TTL would have shipped.
    let (a, b, agent) = two_handles().await;
    a.grant(agent, "/x", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    assert!(
        b.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );

    a.grant(agent, "/x", Perms::NONE, None).await.unwrap();
    assert_eq!(
        b.effective_perms(agent, "/x").await.unwrap(),
        Perms::NONE,
        "handle B kept granting write after handle A revoked it"
    );
}

#[tokio::test]
async fn default_deny_flipped_on_one_handle_is_seen_by_another() {
    let (a, b, agent) = two_handles().await;
    assert!(
        b.effective_perms(agent, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
    a.set_acl_default_deny(true).await.unwrap();
    assert_eq!(b.effective_perms(agent, "/x").await.unwrap(), Perms::NONE);
}

// --- the cache must not blur actors, paths, or workspaces --------------------

#[tokio::test]
async fn one_actors_cached_grants_do_not_answer_for_another() {
    let (fs, alice) = fixture().await;
    let bob = fs.create_agent("bob", "opus", None).await.unwrap();
    fs.grant(alice, "/x", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    fs.grant(bob, "/x", Perms::NONE, None).await.unwrap();

    assert!(
        fs.effective_perms(alice, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
    assert_eq!(fs.effective_perms(bob, "/x").await.unwrap(), Perms::NONE);
    // ...and again, now that both are cached, in the other order.
    assert_eq!(fs.effective_perms(bob, "/x").await.unwrap(), Perms::NONE);
    assert!(
        fs.effective_perms(alice, "/x")
            .await
            .unwrap()
            .contains(Perms::WRITE)
    );
}

#[tokio::test]
async fn longest_prefix_still_wins_from_cache() {
    let (fs, agent) = fixture().await;
    fs.grant(agent, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    fs.grant(agent, "/locked", Perms::NONE, None).await.unwrap();
    for _ in 0..3 {
        assert!(
            fs.effective_perms(agent, "/open/f")
                .await
                .unwrap()
                .contains(Perms::WRITE)
        );
        assert_eq!(
            fs.effective_perms(agent, "/locked/f").await.unwrap(),
            Perms::NONE
        );
    }
}

#[tokio::test]
async fn a_second_workspace_does_not_inherit_the_first_ones_cache() {
    // `for_workspace` builds an `Fs` over a *scoped* metadata store, so its grants
    // are different rows. Sharing a cache across the two would answer one
    // workspace's question with the other's data.
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let root = Fs::new(meta, Arc::new(MemStore::new()));
    root.init().await.unwrap();
    let agent = root.create_agent("claude", "opus", None).await.unwrap();

    root.grant(agent, "/", Perms::NONE, None).await.unwrap();
    assert_eq!(
        root.effective_perms(agent, "/x").await.unwrap(),
        Perms::NONE
    );

    let (id, other_root) = root
        .backends()
        .meta
        .create_workspace("other")
        .await
        .unwrap();
    let other: TestFs = root.rebind(root.backends().meta.with_workspace(id), other_root);
    let perms = other.effective_perms(agent, "/x").await.unwrap();
    assert!(
        perms.contains(Perms::WRITE),
        "the other workspace inherited a grant it has no row for: {perms}"
    );
}

// --- the acceptance test the scope named -------------------------------------

#[tokio::test]
async fn the_check_is_constant_in_the_number_of_grants() {
    // The whole point of phase 0. Uncached, cost grew with the actor's grant
    // count: 26µs at one grant, 366µs at 201. This pins the shape, not a wall
    // -clock number — a debug build on a shared runner cannot promise microseconds,
    // but it can promise that 200 extra grants do not multiply the cost.
    let (fs, agent) = fixture().await;
    fs.grant(agent, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();

    const N: u32 = 400;
    async fn bench(fs: &TestFs, agent: i64, n: u32) -> std::time::Duration {
        let t = Instant::now();
        for _ in 0..n {
            fs.effective_perms(agent, "/deep/path/file.md")
                .await
                .unwrap();
        }
        t.elapsed()
    }

    let few = bench(&fs, agent, N).await;
    for i in 0..200 {
        fs.grant(agent, &format!("/dir{i}"), Perms::READ, None)
            .await
            .unwrap();
    }
    let many = bench(&fs, agent, N).await;

    let ratio = many.as_secs_f64() / few.as_secs_f64();
    println!("1 grant: {few:?} for {N}  |  201 grants: {many:?}  |  ratio {ratio:.2}x");
    // 2.0 rather than something tighter because this is a ratio of two timed
    // loops on whatever runner CI gives us. It is still a real bound: every
    // implementation that scaled with grant count measured well above it —
    // uncached 14x, cached-but-reparsing 8.5x, indexed-but-cloning 2.2x. The
    // current shape measures ~1.15x.
    assert!(
        ratio < 2.0,
        "cost scales with grant count: {few:?} -> {many:?} ({ratio:.2}x)"
    );
}

#[tokio::test]
async fn a_warm_cache_beats_a_cold_one() {
    // Directly: the same query, before and after the first load.
    let (fs, agent) = fixture().await;
    for i in 0..200 {
        fs.grant(agent, &format!("/dir{i}"), Perms::READ, None)
            .await
            .unwrap();
    }
    let t = Instant::now();
    fs.effective_perms(agent, "/dir0/f").await.unwrap();
    let cold = t.elapsed();

    const N: u32 = 200;
    let t = Instant::now();
    for _ in 0..N {
        fs.effective_perms(agent, "/dir0/f").await.unwrap();
    }
    let warm = t.elapsed() / N;
    println!("cold {cold:?} | warm {warm:?}");
    assert!(
        warm < cold,
        "warm ({warm:?}) was not faster than cold ({cold:?})"
    );
}

// --- the indexed lookup must be the old scan, exactly ------------------------

/// Differential test: the prefix **index** answers every case the linear
/// `Scope::contains` scan did.
///
/// The optimization rests on a claim — that a grant prefix covers a path exactly
/// when it is one of that path's ancestors — and if the claim is wrong in some
/// corner, the failure mode is a grant matching a path it should not. So rather
/// than trust it, this replays the original algorithm next to the engine's answer
/// over a deliberately nasty set of prefixes and paths: adjacent names that share
/// a leading substring (`/tenant-a` vs `/tenant-ab`), trailing slashes, the root
/// grant, padded paths, and deep nesting.
#[tokio::test]
async fn the_prefix_index_agrees_with_a_linear_scan() {
    use origofs_core::Scope;

    let prefixes = [
        "/",
        "/a",
        "/a/b",
        "/a/b/c",
        "/tenant-a",
        "/tenant-ab",
        "/x y",
        "/deep/nest/ed/path",
    ];
    let paths = [
        "/",
        "/a",
        "/a/",
        "/a/b",
        "/a/bb",
        "/a/b/",
        "/a/b/c",
        "/a/b/c/d",
        "/ab",
        "/tenant-a",
        "/tenant-a/f",
        "/tenant-ab",
        "/tenant-abc",
        "/tenant-ab/f",
        "/x y",
        "/x y/z",
        " /a",
        "/a ",
        "/deep/nest/ed/path/f",
        "/deep/nest",
        "/zzz",
        "",
    ];

    for (i, prefix) in prefixes.iter().enumerate() {
        // A fresh workspace per prefix, so exactly one grant is in play and the
        // engine's answer is unambiguous.
        let (fs, agent) = fixture().await;
        // NONE is the tell: distinguishable from every fallback, so "the grant
        // matched" and "the grant did not match" can never be confused.
        fs.grant(agent, prefix, Perms::NONE, None).await.unwrap();
        let scope = Scope::at(prefix).unwrap();

        for path in paths {
            // The original algorithm, verbatim.
            let expected_match = scope.contains(Some(path));
            let got = fs.effective_perms(agent, path).await.unwrap();
            let got_match = got == Perms::NONE;
            assert_eq!(
                got_match, expected_match,
                "prefix {prefix:?} vs path {path:?} (case {i}): \
                 scan said match={expected_match}, index said match={got_match}"
            );
        }
    }
}

/// The neighbour case on its own, because it is the one a `starts_with`
/// implementation gets wrong and the one that leaks across tenants.
#[tokio::test]
async fn an_adjacent_tenant_is_not_covered() {
    let (fs, agent) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    fs.grant(agent, "/tenant-a", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();

    assert!(
        fs.effective_perms(agent, "/tenant-a/f")
            .await
            .unwrap()
            .contains(Perms::READ)
    );
    for neighbour in ["/tenant-ab", "/tenant-ab/f", "/tenant-abc/secret"] {
        assert_eq!(
            fs.effective_perms(agent, neighbour).await.unwrap(),
            Perms::NONE,
            "a grant on /tenant-a reached {neighbour}"
        );
    }
}

/// A padded path must not borrow a grant. Trimming in the lookup would widen
/// every grant for a caller that passed whitespace.
#[tokio::test]
async fn a_padded_path_does_not_match_a_trimmed_prefix() {
    use origofs_core::Scope;
    let (fs, agent) = fixture().await;
    fs.set_acl_default_deny(true).await.unwrap();
    fs.grant(agent, "/a", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();

    let scope = Scope::at("/a").unwrap();
    for padded in [" /a", "/a ", "\t/a"] {
        assert!(!scope.contains(Some(padded)), "premise: {padded:?}");
        assert_eq!(
            fs.effective_perms(agent, padded).await.unwrap(),
            Perms::NONE,
            "padded path {padded:?} matched the /a grant"
        );
    }
}
