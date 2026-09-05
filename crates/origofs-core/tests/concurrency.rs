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

use origofs_core::posixlock::{self, LockAnswer, LockKind, LockRequest};
use origofs_core::{
    EventInit, FileKind, Fs, MemStore, MetadataStore, OrigoFSError, SqliteMetadataStore, WriteCtx,
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

/// An attributed write is atomic *and* optimistic: when writers race the same
/// path, exactly one lands, the rest are told, and nothing is invented.
///
/// The write was previously unconditional. Every racer "succeeded", the last one
/// to commit replaced the others' bytes with no error and no signal to anyone,
/// and — worse for a system whose thesis is attribution — the winner's blame had
/// been derived against a version that no longer existed, so the `edit_op` log
/// claimed a `pre_hash` that was never the thing overwritten. A human and an
/// agent editing one file at the same moment is what this system is *for*.
///
/// Three things are asserted, and the first is the one the old behaviour passed:
/// content is never interleaved, blame credits the actor whose bytes survived,
/// and the losers see `Conflict` rather than a lie.
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
        let mut landed = 0usize;
        for h in handles {
            match h.await.unwrap() {
                Ok(()) => landed += 1,
                // The only failure a racer may see. Anything else — a torn
                // transaction surfacing as a backend error, say — is a bug.
                Err(OrigoFSError::Conflict(_)) => {}
                Err(e) => panic!("round {round}: unexpected error {e:?}"),
            }
        }
        assert!(
            landed >= 1,
            "round {round}: every writer was refused; at least one must make progress"
        );

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

        // The op-log records edits that *happened*. A refused write rolls its
        // whole transaction back, op-log entry included, so a reader of the
        // ground truth never sees an edit whose bytes were never stored.
        let mut logged = 0usize;
        for &actor in &actors {
            for op in fs.edit_ops(actor, None).await.unwrap() {
                assert_eq!(op.path, "/shared");
                logged += 1;
            }
        }
        assert_eq!(
            logged, landed,
            "round {round}: the op-log must show exactly the writes that landed"
        );
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

/// Concurrent identical `put`s against a **real on-disk** store must each return
/// an object whose bytes hash to the address they were given.
///
/// Every other test in this file runs over `MemStore`, which is a `HashMap` and
/// so immune by construction — but `LocalCasStore` writes a temp sibling and
/// renames. When that temp name was derived from the object's path it was shared
/// by every writer of the same content, which is precisely what dedup produces:
/// two agents writing the same chunk, `mirror_refs`, a `store_body` retry.
/// `File::create` truncates, so one writer could zero another's partially-written
/// file, and the first would then fsync and rename a zero-filled hole into place —
/// returning `Ok(hash)` for bytes that don't hash to `hash`. The loser of the
/// rename race would see a spurious `ENOENT` for a write that had succeeded.
///
/// The body is large enough that a write spans several syscalls, which is what
/// gives the interleaving room to happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_puts_to_a_real_cas_never_corrupt() {
    use origofs_core::{ContentStore, Hash, LocalCasStore};

    for round in 0..8u64 {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalCasStore::open(dir.path()).await.unwrap());

        // Distinct per round so no round can be satisfied by another's leftovers.
        let body: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|i| (i as u64 ^ round).to_le_bytes()[0])
            .collect();
        let want = Hash::of(&body);

        let mut handles = Vec::new();
        for _ in 0..16 {
            let store = Arc::clone(&store);
            let body = body.clone();
            handles.push(tokio::spawn(async move { store.put(&body).await }));
        }
        for h in handles {
            let got = h
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("round {round}: a racing put failed: {e}"));
            assert_eq!(got, want, "round {round}: put returned the wrong address");
        }

        // The decisive check: what is actually on disk must hash to its address.
        let stored = store.get(&want).await.unwrap();
        assert_eq!(
            Hash::of(&stored),
            want,
            "round {round}: stored bytes do not hash to their address ({} bytes)",
            stored.len()
        );
        assert_eq!(&stored[..], &body[..], "round {round}: stored bytes differ");

        // No temp files left behind.
        let leftovers = walk_tmp(dir.path());
        assert!(
            leftovers.is_empty(),
            "round {round}: temp files left behind: {leftovers:?}"
        );
    }
}

/// Every `*.tmp` under `root`, recursively.
fn walk_tmp(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "tmp") {
                out.push(p);
            }
        }
    }
    out
}

/// A deterministic version of the race above, because the randomized one cannot
/// tell the two behaviours apart: with a large enough gap between attempts every
/// writer lands under *either* rule.
///
/// Here the second writer is held inside its prior-content read until the first
/// writer has committed, so its blame is provably derived from a version that no
/// longer exists. It must be refused, and it must leave nothing behind.
struct Gated {
    inner: Arc<MemStore>,
    /// Fires on the first `get` after arming, then disarms.
    reached: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Held until the test lets the read finish.
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl origofs_core::ContentStore for Gated {
    async fn put(&self, b: &[u8]) -> origofs_core::Result<origofs_core::Hash> {
        self.inner.put(b).await
    }
    async fn put_keyed(&self, k: &origofs_core::Hash, b: &[u8]) -> origofs_core::Result<()> {
        self.inner.put_keyed(k, b).await
    }
    async fn get(&self, h: &origofs_core::Hash) -> origofs_core::Result<bytes::Bytes> {
        let armed = self.reached.lock().unwrap().take();
        if let Some(tx) = armed {
            let rx = self.release.lock().unwrap().take();
            let _ = tx.send(());
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        }
        self.inner.get(h).await
    }
    async fn get_range(
        &self,
        h: &origofs_core::Hash,
        o: u64,
        l: u64,
    ) -> origofs_core::Result<bytes::Bytes> {
        self.inner.get_range(h, o, l).await
    }
    async fn has(&self, h: &origofs_core::Hash) -> origofs_core::Result<bool> {
        self.inner.has(h).await
    }
    async fn list(&self) -> origofs_core::Result<Vec<origofs_core::Hash>> {
        self.inner.list().await
    }
    async fn list_with_age(&self) -> origofs_core::Result<Vec<(origofs_core::Hash, Option<u64>)>> {
        self.inner.list_with_age().await
    }
    async fn delete(&self, h: &origofs_core::Hash) -> origofs_core::Result<u64> {
        self.inner.delete(h).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_derived_from_a_replaced_version_is_refused() {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let content = Arc::new(Gated {
        inner: Arc::new(MemStore::new()),
        reached: std::sync::Mutex::new(None),
        release: std::sync::Mutex::new(Some(release_rx)),
    });
    let fs = Arc::new(Fs::new(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        content.clone(),
    ));
    fs.init().await.unwrap();
    let ann = fs.create_human("ann", None).await.unwrap();
    let bot = fs.create_agent("bot", "opus", None).await.unwrap();
    fs.write_as(WriteCtx::actor(ann), "/doc.md", b"base\n")
        .await
        .unwrap();

    // Bot starts a write based on "base\n" and is parked reading it.
    *content.reached.lock().unwrap() = Some(reached_tx);
    let slow = {
        let fs = Arc::clone(&fs);
        tokio::spawn(async move {
            fs.write_as(WriteCtx::actor(bot), "/doc.md", b"base\nbot\n")
                .await
        })
    };
    reached_rx.await.unwrap();

    // Ann replaces the file entirely while bot is parked. The gate has disarmed,
    // so this write runs to completion.
    fs.write_as(WriteCtx::actor(ann), "/doc.md", b"ann only\n")
        .await
        .unwrap();

    release_tx.send(()).unwrap();
    let err = slow
        .await
        .unwrap()
        .expect_err("a write derived from a replaced version must not land");
    assert!(
        matches!(err, OrigoFSError::Conflict(_)),
        "expected a Conflict, got {err:?}"
    );

    // Ann's content stands, and bot left nothing behind — no bytes, no blame, and
    // no op-log entry claiming an edit that never happened.
    assert_eq!(&fs.read("/doc.md").await.unwrap()[..], b"ann only\n");
    for r in fs.blame("/doc.md").await.unwrap() {
        assert_eq!(r.actor.id, ann, "bot's authorship must not appear");
    }
    assert!(
        fs.edit_ops(bot, None).await.unwrap().is_empty(),
        "a refused write must not appear in the attribution ground truth"
    );
}

/// `rmdir` must not leave a dentry parented to an inode it deleted.
///
/// Emptiness used to be read *before* the transaction and then trusted, so a
/// `mkdir` that committed in between produced a directory entry whose parent no
/// longer exists: a row nothing can reach, invisible to `ls`, to `build_tree`,
/// and to the GC mark — the same shape of loss as `rename`-into-itself. The
/// check now runs as part of the delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rmdir_racing_mkdir_never_orphans_a_dentry() {
    for round in 0..80u64 {
        let fs = shared().await;
        fs.mkdir_p("/dir").await.unwrap();
        let dir = fs
            .backends()
            .meta
            .lookup(1, "dir")
            .await
            .unwrap()
            .expect("/dir exists");

        let (a, b) = (Arc::clone(&fs), Arc::clone(&fs));
        let remove = tokio::spawn(async move { a.remove("/dir").await });
        let create = tokio::spawn(async move { b.mkdir_p("/dir/child").await });
        let removed = remove.await.unwrap();
        let created = create.await.unwrap();

        // Whatever the interleaving, the two outcomes must agree: a surviving
        // child implies a surviving parent.
        let children = fs.backends().meta.child_count(dir).await.unwrap();
        let parent_gone = fs.backends().meta.get_inode(dir).await.unwrap().is_none();
        assert!(
            !(parent_gone && children > 0),
            "round {round}: /dir deleted with {children} orphaned child dentr(ies) \
             (remove: {removed:?}, mkdir: {created:?})"
        );
    }
}

// --- POSIX advisory locks under contention (issue #119) ---------------------
//
// Honest scope, because this tier is easy to over-claim. The correctness
// argument for `apply_posix_lock` is that read-decide-write is *serialized* —
// SQLite by `BEGIN IMMEDIATE`, Postgres by a per-inode advisory lock — or two
// callers both find no conflict and both insert. What follows exercises that
// under genuinely concurrent tasks, but `SqliteMetadataStore` also holds a single
// process-wide connection mutex, which would mask a missing `BEGIN IMMEDIATE` in
// this process. So a pass here is evidence the *invariant* survives contention,
// not proof that the transaction is correctly scoped across processes. The
// cross-process claim is only really exercised by the Postgres path, which
// self-skips unless `ORIGOFS_PG_TEST_URL` is set.

async fn lockable() -> (Arc<CFs>, i64) {
    let fs = shared().await;
    fs.write("/f", b"0123456789").await.unwrap();
    fs.set_posix_locks_enabled(true).await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;
    (fs, ino)
}

fn excl(owner: usize, start: i64, end: i64, kind: LockKind) -> LockRequest {
    LockRequest {
        owner: format!("owner-{owner}"),
        holder: format!("mount-{owner}"),
        pid: 1,
        start,
        end,
        kind,
    }
}

/// Many owners race for the *same* exclusive range: **exactly one** may win.
///
/// Two winners is the failure the whole design is arranged to prevent — it means
/// two processes each believe they hold the bytes exclusively, which is precisely
/// the guarantee an advisory lock exists to provide.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_exclusive_locks_have_exactly_one_winner() {
    for round in 0..60u64 {
        let (fs, ino) = lockable().await;
        let mut tasks = Vec::new();
        for i in 0..8usize {
            let fs = fs.clone();
            tasks.push(tokio::spawn(async move {
                fs.vfs_setlk_as(None, ino, &excl(i, 0, 99, LockKind::Exclusive))
                    .await
            }));
        }
        let mut winners = 0;
        for t in tasks {
            if matches!(t.await.unwrap().unwrap(), LockAnswer::Free) {
                winners += 1;
            }
        }
        assert_eq!(
            winners, 1,
            "round {round}: {winners} owners each took the same exclusive range"
        );
        let held = fs.posix_locks(ino).await.unwrap();
        assert_eq!(held.len(), 1, "round {round}: stored {held:?}");
        posixlock::check_state(&held).unwrap_or_else(|e| panic!("round {round}: {e}"));
    }
}

/// Shared readers racing the same range must *all* win — the mirror of the test
/// above, and what stops "exactly one winner" being satisfied by a lock that is
/// simply always exclusive.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_shared_locks_all_succeed() {
    for round in 0..40u64 {
        let (fs, ino) = lockable().await;
        let mut tasks = Vec::new();
        for i in 0..8usize {
            let fs = fs.clone();
            tasks.push(tokio::spawn(async move {
                fs.vfs_setlk_as(None, ino, &excl(i, 0, 99, LockKind::Shared))
                    .await
            }));
        }
        for t in tasks {
            assert_eq!(
                t.await.unwrap().unwrap(),
                LockAnswer::Free,
                "round {round}: a shared lock was refused by another shared lock"
            );
        }
        assert_eq!(fs.posix_locks(ino).await.unwrap().len(), 8);
    }
}

/// Mixed traffic: many owners taking, splitting and dropping overlapping ranges
/// at once. The assertion is the schema's invariant — one owner never ends up
/// holding two overlapping ranges, which is what its primary key requires.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mixed_lock_traffic_preserves_the_state_invariant() {
    for round in 0..30u64 {
        let (fs, ino) = lockable().await;
        let mut tasks = Vec::new();
        for i in 0..6usize {
            let fs = fs.clone();
            tasks.push(tokio::spawn(async move {
                for step in 0..12i64 {
                    let start = (step * 7 + i as i64 * 5) % 60;
                    let kind = match (step + i as i64) % 3 {
                        0 => LockKind::Shared,
                        1 => LockKind::Exclusive,
                        _ => LockKind::Unlock,
                    };
                    // Refusals are expected and fine; an *error* is not.
                    fs.vfs_setlk_as(None, ino, &excl(i, start, start + 20, kind))
                        .await
                        .expect("a contended lock op must not error");
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let held = fs.posix_locks(ino).await.unwrap();
        posixlock::check_state(&held)
            .unwrap_or_else(|e| panic!("round {round}: {e}\nstate: {held:?}"));
    }
}
