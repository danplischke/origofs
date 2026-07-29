//! Concurrency-tier simulation: the real `Fs` engine under **genuinely
//! concurrent** tokio tasks (multi-threaded runtime, a shared `Arc`-backed
//! store), asserting the safety invariants that single-threaded tests can't
//! reach — content CAS, change-feed exactly-once, write atomicity, and ref-CAS
//! under contention.
//!
//! Honest scope: unlike `simulation.rs` (deterministic, seed-replayable), this
//! tier is **randomized-schedule stress** — real threads race, so a failure is a
//! real bug but is not bit-for-bit reproducible from a seed (true madsim-style
//! deterministic replay is impractical over rusqlite's blocking C FFI). Each test
//! runs many rounds to shake out interleavings; a violation prints its round and
//! the observed state.

use origofs_core::{
    EventInit, FileKind, Fs, MemStore, OrigoFSError, SqliteMetadataStore, WriteCtx,
};
use std::collections::HashSet;
use std::sync::Arc;

type CFs = Fs<Arc<SqliteMetadataStore>, Arc<MemStore>>;

/// A shared, `Arc`-backed workspace: cloning the `Arc` hands every task the
/// *same* underlying metadata + content store, so their writes genuinely race.
async fn shared() -> Arc<CFs> {
    let fs = Fs::new(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    Arc::new(fs)
}

/// Content compare-and-set: when many writers race `write_as_expecting` against
/// the *same* base, **exactly one** must win and the rest get `Conflict` — the
/// lost-update guarantee. The winner's content is what survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_cas_has_exactly_one_winner() {
    for round in 0..150u64 {
        let fs = shared().await;
        let actor = fs.create_human("a", None).await.unwrap();
        fs.write_as(WriteCtx::actor(actor), "/f", b"base")
            .await
            .unwrap();
        let base = fs.stat("/f").await.unwrap().content;

        let n = 6 + (round % 6) as usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let fs = Arc::clone(&fs);
            handles.push(tokio::spawn(async move {
                let data = format!("winner-{i}");
                fs.write_as_expecting(WriteCtx::actor(actor), "/f", data.as_bytes(), base)
                    .await
            }));
        }

        let (mut oks, mut conflicts, mut other) = (0, 0, 0);
        for h in handles {
            match h.await.unwrap() {
                Ok(()) => oks += 1,
                Err(OrigoFSError::Conflict(_)) => conflicts += 1,
                Err(_) => other += 1,
            }
        }
        assert_eq!(
            oks, 1,
            "round {round}: expected exactly one CAS winner, got oks={oks} conflicts={conflicts} other={other}"
        );
        assert_eq!(other, 0, "round {round}: unexpected non-conflict errors");
        assert!(
            fs.read("/f").await.unwrap().starts_with(b"winner-"),
            "round {round}: surviving content is not a winner's"
        );
    }
}

/// The change feed assigns a **monotonic, gap-free, duplicate-free** `seq` even
/// when many writers append at once (exactly-once, H6).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_events_are_exactly_once_and_monotonic() {
    for round in 0..80u64 {
        let fs = shared().await;
        let (tasks, per) = (8usize, 8usize);
        let mut handles = Vec::new();
        for t in 0..tasks {
            let fs = Arc::clone(&fs);
            handles.push(tokio::spawn(async move {
                for k in 0..per {
                    fs.record_event(EventInit {
                        actor_id: Some(1),
                        session_id: None,
                        kind: "t".to_string(),
                        path: format!("/{t}/{k}"),
                        detail: None,
                        branch: None,
                    })
                    .await
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let events = fs.events_since(0, 100_000).await.unwrap();
        assert_eq!(
            events.len(),
            tasks * per,
            "round {round}: feed lost or duplicated events (got {})",
            events.len()
        );
        // events_since is oldest-first: seqs must be strictly increasing (hence
        // distinct — no two appends collided on a seq).
        let seqs: Vec<i64> = events.iter().map(|e| e.seq).collect();
        for w in seqs.windows(2) {
            assert!(
                w[0] < w[1],
                "round {round}: seq not strictly increasing ({} then {})",
                w[0],
                w[1]
            );
        }
        let distinct: HashSet<i64> = seqs.iter().copied().collect();
        assert_eq!(distinct.len(), seqs.len(), "round {round}: duplicate seq");
    }
}

/// An attributed write is atomic: when writers race the *same* path, the file
/// ends as **exactly one** writer's content (never interleaved) and its blame
/// credits that same writer (content and blame commit together — no mismatch).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_never_tear_content_or_blame() {
    for round in 0..100u64 {
        let fs = shared().await;
        let n = 5;
        let mut actors = Vec::new();
        for i in 0..n {
            actors.push(fs.create_human(&format!("a{i}"), None).await.unwrap());
        }

        // Each writer's content is that actor's id repeated per line, so the file
        // self-identifies its author and blame is unambiguous.
        let mut expected: Vec<Vec<u8>> = Vec::new();
        let mut handles = Vec::new();
        for &actor in &actors {
            let body = format!("{actor}\n").repeat(12).into_bytes();
            expected.push(body.clone());
            let fs = Arc::clone(&fs);
            handles.push(tokio::spawn(async move {
                fs.write_as(WriteCtx::actor(actor), "/shared", &body).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        let final_c = fs.read("/shared").await.unwrap().to_vec();
        assert!(
            expected.contains(&final_c),
            "round {round}: torn/interleaved content survived a concurrent write"
        );
        let winner: i64 = std::str::from_utf8(&final_c)
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        for r in &fs.blame("/shared").await.unwrap() {
            assert_eq!(
                r.actor.id, winner,
                "round {round}: content is actor {winner}'s but blame credits {}",
                r.actor.id
            );
        }
    }
}

/// Concurrent commits linearize through the branch-ref CAS: every commit that
/// reports success is reachable from the final head — none is orphaned or lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_commits_never_lose_a_commit() {
    for round in 0..60u64 {
        let fs = shared().await;
        fs.write("/f", b"0").await.unwrap();
        fs.commit("x", "base").await.unwrap();

        let n = 6;
        let mut handles = Vec::new();
        for i in 0..n {
            let fs = Arc::clone(&fs);
            handles.push(tokio::spawn(async move {
                for attempt in 0..1000 {
                    fs.write(&format!("/f{i}"), format!("v{i}-{attempt}").as_bytes())
                        .await
                        .unwrap();
                    match fs.commit("x", &format!("c{i}")).await {
                        Ok(h) => return h,
                        // The branch moved under us — retry against the new head.
                        Err(OrigoFSError::Metadata(_)) => continue,
                        // A transient backend failure (serialization/deadlock/
                        // contention) is likewise just a signal to retry.
                        Err(e) if e.retryable() => continue,
                        Err(e) => panic!("round {round}: unexpected commit error: {e}"),
                    }
                }
                panic!("round {round}: commit never succeeded after retries");
            }));
        }

        let mut committed = Vec::new();
        for h in handles {
            committed.push(h.await.unwrap());
        }

        let head = fs.head_commit().await.unwrap().unwrap();
        for c in &committed {
            assert!(
                *c == head || fs.is_ancestor(*c, head).await.unwrap(),
                "round {round}: a successfully-committed commit is not in history (lost/orphaned)"
            );
        }
    }
}

/// A1 (issue #70) — `mkdir_p` is idempotent under concurrency (see the engine's
/// `mkdir_p` docstring, C1/M6): when many tasks race to create the SAME deep
/// path, each missing segment is created **exactly once** — a loser hits the
/// dentry unique index, rolls back its just-created inode, and adopts the
/// winner's directory rather than orphaning a second inode or forking the tree.
/// So every racer must succeed and agree on the leaf inode, every component must
/// resolve to a single directory, and no name may be duplicated in its parent (a
/// botched rollback would leave two dentries).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_mkdir_p_is_idempotent_and_orphan_free() {
    for round in 0..120u64 {
        let fs = shared().await;
        let path = "/x/y/z/w";
        let n = 8;
        let mut handles = Vec::new();
        for _ in 0..n {
            let fs = Arc::clone(&fs);
            handles.push(tokio::spawn(async move { fs.mkdir_p(path).await }));
        }

        // Every racer succeeds and returns the SAME leaf inode (the tree never forks).
        let mut leaves = Vec::new();
        for h in handles {
            leaves.push(
                h.await
                    .unwrap()
                    .unwrap_or_else(|e| panic!("round {round}: mkdir_p failed under a race: {e}")),
            );
        }
        let leaf = leaves[0];
        for got in &leaves {
            assert_eq!(
                *got, leaf,
                "round {round}: racers disagree on the leaf inode ({got} vs {leaf}) — tree forked"
            );
        }

        // Each component resolves to a single directory, and no name is duplicated in
        // its parent (the unique-index conflict + rollback left no second dentry).
        for (parent, name) in [("/", "x"), ("/x", "y"), ("/x/y", "z"), ("/x/y/z", "w")] {
            let dupes = fs
                .ls(parent)
                .await
                .unwrap()
                .into_iter()
                .filter(|e| e.name == name)
                .count();
            assert_eq!(
                dupes, 1,
                "round {round}: '{name}' under {parent} appears {dupes}× (dup dentry / orphan)"
            );
        }
        let leaf_stat = fs.stat(path).await.unwrap();
        assert_eq!(
            leaf_stat.kind,
            FileKind::Dir,
            "round {round}: leaf is not a directory"
        );
        assert_eq!(
            leaf_stat.ino, leaf,
            "round {round}: leaf inode unstable after the race"
        );
    }
}
