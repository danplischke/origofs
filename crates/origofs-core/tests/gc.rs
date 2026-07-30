//! Garbage collection: orphaned content (uncommitted churn, superseded bodies)
//! is reclaimed, while everything reachable from a ref or the live working tree
//! survives — verified against both the in-memory and on-disk content stores.

//! These call `gc_with_grace(0)`, which disables the age gate. That is the right
//! model here — each test quiesces the store before collecting — and it also makes
//! the *preservation* assertions stronger: content survives because it is
//! reachable, not merely because it is young. The one test that races GC against a
//! live writer deliberately keeps the production default, since the grace is
//! exactly what makes that safe.

use origofs_core::{ContentStore, Fs, LocalCasStore, MemStore, SqliteMetadataStore};
use std::sync::Arc;

async fn mem_fixture() -> (Fs<SqliteMetadataStore, Arc<MemStore>>, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

/// A multi-chunk body; distinct seeds share no chunks.
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

#[tokio::test]
async fn reclaims_uncommitted_churn_keeps_live_body() {
    let (fs, store) = mem_fixture().await;
    let v1 = blob(200 * 1024, 1);
    let v2 = blob(200 * 1024, 2);

    fs.write("/a.bin", &v1).await.unwrap();
    let after_v1 = store.len();
    fs.write("/a.bin", &v2).await.unwrap(); // overwrite; v1 now orphaned
    let after_v2 = store.len();
    assert!(after_v2 > after_v1, "v2 added distinct objects");

    let stats = fs.gc_with_grace(0).await.unwrap();

    // Every v1 object was unreachable and reclaimed; only v2's remain.
    assert_eq!(stats.deleted, after_v1, "all of v1 collected");
    assert_eq!(store.len(), after_v2 - after_v1, "only v2 survives");
    assert_eq!(stats.reachable, store.len());
    assert!(stats.bytes_freed >= 200 * 1024, "freed at least v1's bytes");

    // The live body still reads back intact.
    assert_eq!(&fs.read("/a.bin").await.unwrap()[..], &v2[..]);

    // GC is idempotent: a second pass finds nothing to reclaim.
    let again = fs.gc_with_grace(0).await.unwrap();
    assert_eq!(again.deleted, 0);
}

#[tokio::test]
async fn committed_content_survives_working_tree_moving_on() {
    let (fs, _store) = mem_fixture().await;
    let x = blob(128 * 1024, 10);
    let y = blob(128 * 1024, 20);
    let z = blob(128 * 1024, 30);

    fs.write("/a.bin", &x).await.unwrap();
    fs.commit("alice", "keep x").await.unwrap(); // x reachable via commit

    fs.write("/a.bin", &y).await.unwrap(); // uncommitted
    fs.write("/a.bin", &z).await.unwrap(); // orphans y; z is the live body

    let stats = fs.gc_with_grace(0).await.unwrap();
    assert!(stats.deleted > 0, "the orphaned y body was reclaimed");

    // Working tree is untouched.
    assert_eq!(&fs.read("/a.bin").await.unwrap()[..], &z[..]);

    // And the committed x survived GC: checking out the branch restores it.
    fs.checkout("main").await.unwrap();
    assert_eq!(&fs.read("/a.bin").await.unwrap()[..], &x[..]);
}

#[tokio::test]
async fn gc_never_touches_a_clean_committed_workspace() {
    let (fs, store) = mem_fixture().await;
    fs.mkdir_p("/dir").await.unwrap();
    fs.write("/dir/a.txt", b"alpha").await.unwrap();
    fs.write("/b.txt", b"beta").await.unwrap();
    fs.commit("alice", "snapshot").await.unwrap();

    let before = store.len();
    let stats = fs.gc_with_grace(0).await.unwrap();
    assert_eq!(stats.deleted, 0, "nothing reachable was collected");
    assert_eq!(store.len(), before);
    assert_eq!(&fs.read("/dir/a.txt").await.unwrap()[..], b"alpha");
}

#[tokio::test]
async fn reclaims_on_the_local_on_disk_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalCasStore::open(dir.path().join("cas")).await.unwrap());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();

    let v1 = blob(300 * 1024, 100);
    let v2 = blob(300 * 1024, 200);
    fs.write("/big.bin", &v1).await.unwrap();
    fs.write("/big.bin", &v2).await.unwrap();

    let before = store.list().await.unwrap().len();
    let stats = fs.gc_with_grace(0).await.unwrap();
    assert!(stats.deleted > 0);
    assert!(stats.bytes_freed >= 300 * 1024);
    assert_eq!(store.list().await.unwrap().len(), before - stats.deleted);

    // Body still intact, and a re-run is a no-op.
    assert_eq!(&fs.read("/big.bin").await.unwrap()[..], &v2[..]);
    assert_eq!(fs.gc_with_grace(0).await.unwrap().deleted, 0);
}

// A3 (issue #70): GC is documented as unsafe alongside active writers — a freshly
// `put` chunk is briefly unreferenced, so an in-flight *new* write can lose a race
// with the sweep. That known hazard aside, the guarantee users actually depend on
// must hold even under concurrent churn: content reachable from a **ref that
// exists throughout the pass** is never reclaimed. This runs a real GC
// concurrently with a writer hammering a *different, uncommitted* path (maximal
// orphan + working-tree pressure) and asserts the committed body — pinned by the
// `main` ref and never touched by the writer — always survives. The assertion is
// on content committed *before* GC starts, so it is deterministic despite the race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_never_reclaims_committed_refs_under_concurrent_writers() {
    for round in 0..40u64 {
        let (fs, _store) = mem_fixture().await;
        let fs = Arc::new(fs);

        // Commit a body; it is now reachable via the `main` branch ref.
        let keep = blob(200 * 1024, round * 2 + 1);
        fs.write("/a.bin", &keep).await.unwrap();
        fs.commit("gc-race", "keep a.bin").await.unwrap();

        // A writer churning a DIFFERENT, uncommitted path — it never touches
        // /a.bin and never moves `main`, so it can't legitimately affect `keep`.
        let writer = {
            let fs = Arc::clone(&fs);
            tokio::spawn(async move {
                for i in 0..25u64 {
                    let scratch = blob(48 * 1024, 9000 + round * 100 + i);
                    let _ = fs.write("/scratch.bin", &scratch).await;
                }
            })
        };
        // GC runs concurrently with that churn.
        let sweeper = {
            let fs = Arc::clone(&fs);
            tokio::spawn(async move { fs.gc().await })
        };

        writer.await.unwrap();
        sweeper.await.unwrap().unwrap();

        // The committed, still-referenced body survived the concurrent sweep.
        assert_eq!(
            &fs.read("/a.bin").await.unwrap()[..],
            &keep[..],
            "round {round}: GC reclaimed content still reachable from the `main` ref"
        );
    }
}

/// GC must be safe to run while writers are working.
///
/// Content is written *before* the metadata that references it — that ordering is
/// the durability barrier — so every write has a window in which its chunks are
/// stored and nothing points at them. A sweep that trusts reachability alone
/// deletes exactly that. Measured on the unfixed code, this loop produced:
///
///     writer result: Some("content missing for hash 0aee3f9d…")
///     gc errors: 46
///     unreadable files after the race: 1/5
///
/// — the writer failing on content it had itself just stored, and committed files
/// destroyed. Nothing here is exotic: a periodic collector plus an agent writing
/// is the ordinary state of a shared workspace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_is_safe_to_run_while_writers_are_working() {
    use origofs_core::{LocalCasStore, WriteCtx};

    // Several rounds: the race needs a store with objects already in it, so the
    // first round on an empty store is often clean. On the unfixed code rounds
    // after the first reported ~59 writer errors and 1/5 files destroyed.
    for round in 0..3u64 {
        let dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(Fs::new(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(LocalCasStore::open(dir.path()).await.unwrap()),
        ));
        fs.init().await.unwrap();
        let actor = fs.create_human("dan", None).await.unwrap();

        let writer = {
            let fs = Arc::clone(&fs);
            tokio::spawn(async move {
                for i in 0..300u64 {
                    let body = format!("body {i} {}", "x".repeat(200));
                    fs.write_as(
                        WriteCtx::actor(actor),
                        &format!("/f{}.txt", i % 5),
                        body.as_bytes(),
                    )
                    .await?;
                }
                Ok::<_, origofs_core::OrigoFSError>(())
            })
        };
        let sweeper = {
            let fs = Arc::clone(&fs);
            tokio::spawn(async move {
                // The production default: the grace is what makes this safe.
                for _ in 0..60 {
                    fs.gc().await?;
                }
                Ok::<_, origofs_core::OrigoFSError>(())
            })
        };

        writer.await.unwrap().unwrap_or_else(|e| {
            panic!("round {round}: a writer lost content to a concurrent collection: {e}")
        });
        sweeper.await.unwrap().unwrap_or_else(|e| {
            panic!("round {round}: a collection failed against a live writer: {e}")
        });

        // Every file the writer produced is still readable.
        for i in 0..5 {
            let path = format!("/f{i}.txt");
            fs.read(&path).await.unwrap_or_else(|e| {
                panic!("round {round}: {path} was destroyed by a concurrent collection: {e}")
            });
        }
    }
}

/// Two collections must not overlap: a sweep is destructive, and a second one
/// marking against the first one's deletions gains nothing and doubles the
/// exposure. Nothing guarded this before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_collections_are_serialized() {
    let (fs, _store) = mem_fixture().await;
    let fs = Arc::new(fs);
    fs.write("/a.txt", b"hello").await.unwrap();
    fs.commit("dan", "base").await.unwrap();

    let a = {
        let fs = Arc::clone(&fs);
        tokio::spawn(async move { fs.gc().await })
    };
    let b = {
        let fs = Arc::clone(&fs);
        tokio::spawn(async move { fs.gc().await })
    };
    let (ra, rb) = (a.await.unwrap(), b.await.unwrap());

    // Either both ran (serialized by the lease) or the loser was told so — never
    // two sweeps interleaved.
    assert!(
        ra.is_ok() || rb.is_ok(),
        "at least one collection should succeed"
    );
    for r in [&ra, &rb] {
        if let Err(e) = r {
            assert!(
                matches!(e, origofs_core::OrigoFSError::Conflict(_)),
                "a refused collection must say why, got {e:?}"
            );
        }
    }
    // The lease is released, so a later collection still works.
    fs.gc().await.unwrap();
    assert_eq!(&fs.read("/a.txt").await.unwrap()[..], b"hello");
}
