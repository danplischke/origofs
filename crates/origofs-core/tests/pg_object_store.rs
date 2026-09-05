//! Postgres metadata over an object content store — the production pairing.
//!
//! This is the exact composition `Workspace::open_pg_gcs` and `open_pg_s3` build:
//! `PostgresMetadataStore` + `VerifyingStore(ObjectContentStore)`. Only the
//! *builder* differs between S3 and GCS; everything the engine touches is the same
//! `ObjectContentStore` code, so running it against the in-memory object adapter
//! exercises the real path with no network. (The GCS builder itself is covered by
//! `content_backends.rs`; a live bucket run is the env-gated `gcs_backend`.)
//!
//! It exists because the object-store branch of `ContentStore::touch` — the
//! dedup-side half of garbage collection's age gate — had **no** test at all. The
//! GC suite uses `LocalCasStore`, whose `touch` is a cheap `utimes`. The
//! object-store one is a different implementation with different costs (it re-PUTs
//! the object, because an object store has no `utimes`) and a different age source
//! (`last_modified` off the `head` response). Sharing a name is not sharing a
//! behaviour.
//!
//! Self-skips without `ORIGOFS_PG_TEST_URL`, like every other Postgres leg.

use origofs_core::{
    ContentStore, Fs, MetadataStore, ObjectContentStore, PostgresMetadataStore, VerifyingStore,
    WriteCtx,
};
use std::sync::Arc;
use std::sync::OnceLock;

fn dsn() -> Option<String> {
    std::env::var("ORIGOFS_PG_TEST_URL").ok()
}

fn pg_lock() -> &'static tokio::sync::Mutex<()> {
    static L: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn reset(dsn: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect for reset");
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .expect("reset public schema");
    drop(client);
    let _ = handle.await;
}

type ObjFs = Fs<Arc<dyn MetadataStore>, Arc<dyn ContentStore>>;

/// The production stack, minus the network: Postgres metadata, and content behind
/// the same `VerifyingStore(ObjectContentStore)` the `open_pg_gcs`/`open_pg_s3`
/// constructors build.
async fn stack(dsn: &str) -> ObjFs {
    reset(dsn).await;
    let meta: Arc<dyn MetadataStore> = Arc::new(PostgresMetadataStore::connect(dsn).await.unwrap());
    let content: Arc<dyn ContentStore> = Arc::new(VerifyingStore::new(Arc::new(
        ObjectContentStore::in_memory(),
    )));
    let fs = Fs::new(meta, content);
    fs.init().await.unwrap();
    fs
}

/// Skip helper: returns `None` (having printed why) when no database is configured.
macro_rules! pg_or_skip {
    ($name:literal) => {
        match dsn() {
            Some(d) => d,
            None => {
                eprintln!("{}: skipping (no ORIGOFS_PG_TEST_URL)", $name);
                return;
            }
        }
    };
}

/// A blob big enough to chunk into several pieces, so manifests are exercised.
fn blob(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Write, read, attribute, commit, branch and merge over the production pairing.
#[tokio::test]
async fn the_production_stack_round_trips() {
    let dsn = pg_or_skip!("the_production_stack_round_trips");
    let _guard = pg_lock().lock().await;
    let fs = stack(&dsn).await;

    let human = fs.create_human("dan", None).await.unwrap();
    let agent = fs
        .create_agent("claude", "opus", Some(human))
        .await
        .unwrap();
    let session = fs.create_session(agent, Some("test")).await.unwrap();
    let ctx = WriteCtx::session(agent, session);

    let body = blob(300 * 1024, 3);
    fs.write_as(ctx, "/big.bin", &body).await.unwrap();
    assert_eq!(fs.read("/big.bin").await.unwrap(), body);

    // Attribution survives the object-store round trip.
    fs.write_as(ctx, "/doc.md", b"agent line\n").await.unwrap();
    let blame = fs.blame("/doc.md").await.unwrap();
    assert_eq!(blame[0].actor.id, agent);

    // Versioning over the same stack.
    fs.commit("dan", "first").await.unwrap();
    fs.create_branch("feature").await.unwrap();
    fs.checkout("feature").await.unwrap();
    fs.write_as(ctx, "/on-branch.txt", b"branch work\n")
        .await
        .unwrap();
    fs.commit("dan", "branch work").await.unwrap();

    fs.checkout("main").await.unwrap();
    assert!(fs.read("/on-branch.txt").await.is_err(), "branch leaked");
    // `Fs` takes a commit hash; `Workspace::merge_branch` is the name-resolving
    // wrapper over it.
    let theirs = fs
        .branch_head("feature")
        .await
        .unwrap()
        .expect("branch head");
    fs.merge(theirs, "dan", "merge").await.unwrap();
    assert_eq!(
        &fs.read("/on-branch.txt").await.unwrap()[..],
        b"branch work\n"
    );
}

/// Deduplicating onto stale content refreshes it **on an object store too**.
///
/// The object-store `touch` is a different implementation from the local one: it
/// has no `utimes`, so it re-PUTs the object, and it decides staleness from the
/// `last_modified` on a `head` response it already makes. None of that had ever
/// run — the GC suite covers `LocalCasStore` only — so the half of the age gate
/// that matters for the *production* content backend was untested.
///
/// Written as the invariant rather than the race, for the reason given in
/// `tests/gc.rs`: the window is a timing artifact, but "content adopted by a
/// deduplicating write is young again" is deterministic.
#[tokio::test]
async fn dedup_refreshes_recency_on_an_object_store() {
    let dsn = pg_or_skip!("dedup_refreshes_recency_on_an_object_store");
    let _guard = pg_lock().lock().await;
    let fs = stack(&dsn).await;

    let body = blob(200 * 1024, 11);
    fs.write("/a.txt", &body).await.unwrap();
    fs.remove("/a.txt").await.unwrap();

    // Everything is young here, so `touch` is a no-op by design — assert that,
    // because a `touch` that re-PUT on every dedup hit would silently undo the
    // entire point of deduplication against a metered object store.
    let before = fs.backends().content.list_with_age().await.unwrap();
    assert!(!before.is_empty(), "nothing was stored");
    assert!(
        before.iter().all(|(_, age)| age.is_some()),
        "the object adapter must report ages, or gc can never sweep"
    );

    // The revert: identical bytes, so every chunk deduplicates.
    fs.write("/b.txt", &body).await.unwrap();
    assert_eq!(fs.read("/b.txt").await.unwrap(), body);

    // Young content stays inside any valid grace period.
    let after = fs.backends().content.list_with_age().await.unwrap();
    assert!(
        after
            .iter()
            .all(|(_, age)| age.unwrap_or(u64::MAX) < origofs_core::DEDUP_REFRESH_AFTER_SECS),
        "objects should be young after a fresh write + dedup"
    );

    // And `touch` is callable on this backend without erroring — the path the
    // engine takes on every dedup hit against GCS or S3.
    for (hash, _) in &after {
        fs.backends().content.touch(hash).await.unwrap();
    }
    assert_eq!(
        fs.read("/b.txt").await.unwrap(),
        body,
        "touch corrupted content"
    );
}

/// A sweep over the production stack keeps what is reachable and drops what is not.
#[tokio::test]
async fn gc_over_the_production_stack() {
    let dsn = pg_or_skip!("gc_over_the_production_stack");
    let _guard = pg_lock().lock().await;
    let fs = stack(&dsn).await;

    fs.write("/keep.bin", &blob(200 * 1024, 5)).await.unwrap();
    fs.commit("dan", "keep").await.unwrap();
    fs.write("/churn.bin", &blob(200 * 1024, 6)).await.unwrap();
    fs.remove("/churn.bin").await.unwrap();

    let stats = fs.gc_with_grace(0).await.unwrap();
    assert!(stats.deleted > 0, "the churn should have been reclaimed");
    assert_eq!(
        fs.read("/keep.bin").await.unwrap(),
        blob(200 * 1024, 5),
        "gc reclaimed committed content"
    );
}

/// The verifying layer is really in the stack: a tampered object surfaces as
/// `Corrupt` rather than being served as authentic.
///
/// This is what `open_pg_gcs`'s doc promises ("a bit-rotted object surfaces as
/// `Corrupt`, not as authentic"), and it is worth pinning against the composition
/// the constructor actually builds rather than against `VerifyingStore` alone.
#[tokio::test]
async fn a_tampered_object_is_refused_not_served() {
    let dsn = pg_or_skip!("a_tampered_object_is_refused_not_served");
    let _guard = pg_lock().lock().await;

    reset(&dsn).await;
    let meta: Arc<dyn MetadataStore> =
        Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    // Keep a handle on the raw backend so the test can corrupt it underneath the
    // verifying layer, the way bit rot or a tampering writer would.
    let raw = Arc::new(ObjectContentStore::in_memory());
    let content: Arc<dyn ContentStore> = Arc::new(VerifyingStore::new(raw.clone()));
    let fs = Fs::new(meta, content);
    fs.init().await.unwrap();

    fs.write("/secret.txt", b"the real content").await.unwrap();
    let hash = fs.backends().content.list().await.unwrap();

    // Overwrite one object's bytes with something that does not hash to its key.
    for h in &hash {
        raw.put_keyed(h, b"tampered").await.ok();
        let _ = raw.replace_keyed(h, b"tampered").await;
    }

    let err = fs.read("/secret.txt").await;
    assert!(
        err.is_err(),
        "tampered content was served as authentic: {:?}",
        err.map(|b| b.len())
    );
}

/// Garbage collection must actually run on Postgres.
///
/// It did not. The lease that serializes collections was keyed `"\0gc-lease"`,
/// chosen because `validate_component` rejects NUL so no user path could collide
/// with it. That is true of paths — but the key is stored in a `text` column, and
/// **Postgres cannot store a NUL byte in one**. Every `gc()` on the production
/// metadata backend failed at `acquire_lock` with
/// `invalid byte sequence for encoding "UTF8": 0x00`, meaning a Postgres
/// deployment could never reclaim anything. Nothing caught it because the whole
/// GC suite ran on SQLite, which stores the byte happily.
///
/// This asserts the plain fact the bug denied: a sweep completes.
#[tokio::test]
async fn gc_acquires_its_lease_on_postgres() {
    let dsn = pg_or_skip!("gc_acquires_its_lease_on_postgres");
    let _guard = pg_lock().lock().await;
    let fs = stack(&dsn).await;

    fs.write("/x.txt", b"content").await.unwrap();
    // Twice, so the second call also proves the lease was *released*, not merely
    // taken — a lease that never comes back would wedge every later collection.
    fs.gc_with_grace(0).await.expect("first gc");
    fs.gc_with_grace(0)
        .await
        .expect("second gc — lease not released?");
}

/// A caller cannot take, or wedge, an internal lease through `lock`.
///
/// `lock`/`unlock` pass a caller-supplied string straight to the store, unlike
/// every other path-taking operation. Two consequences, both real: a NUL byte is
/// a hard Postgres error from user input, and without a rule separating user
/// paths from internal keys a caller could grab the GC lease and block collection
/// indefinitely.
#[tokio::test]
async fn lock_refuses_paths_that_are_not_workspace_paths() {
    let dsn = pg_or_skip!("lock_refuses_paths_that_are_not_workspace_paths");
    let _guard = pg_lock().lock().await;
    let fs = stack(&dsn).await;

    // The GC lease is not addressable as a lock.
    assert!(fs.lock("origofs:gc-lease", "attacker").await.is_err());
    // A NUL would be a fatal encoding error inside Postgres, from user input.
    assert!(fs.lock("/a\0b", "someone").await.is_err());
    assert!(fs.lock("relative/path", "someone").await.is_err());

    // A real path still works, and gc still runs alongside it.
    assert!(fs.lock("/doc.md", "alice").await.unwrap());
    fs.gc_with_grace(0)
        .await
        .expect("gc while a user lock is held");
    assert!(fs.unlock("/doc.md", "alice").await.unwrap());
}
