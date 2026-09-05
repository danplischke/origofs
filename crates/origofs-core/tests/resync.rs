//! Offline solo → reconnect resync (`docs/DESIGN.md` §4b/§4c): object transfer,
//! the four reconciliation outcomes, the `cas_ref` retry, and — the point of the
//! whole exercise — attribution surviving the crossing between two independent
//! metadata databases.
//!
//! The Postgres case (a SQLite laptop reconciling with a Postgres/shared
//! workspace, which is the real shape of the feature) self-skips unless
//! `ORIGOFS_PG_TEST_URL` points at a reachable database.

use origofs_core::{
    ActorInit, ActorKind, Clock, ContentStore, Fs, Hash, MemStore, MergeOutcome, MetadataStore,
    PostgresMetadataStore, RefStore, ResyncOutcome, SqliteMetadataStore, SystemClock, WriteCtx,
    resync, transfer,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

type Solo = Fs<Arc<SqliteMetadataStore>, Arc<MemStore>>;

/// A standalone workspace: its own SQLite DB *and* its own content store, so a
/// transfer between two of them really has to move bytes.
async fn solo() -> Solo {
    let meta = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

/// Read a path as UTF-8, for either backend pairing.
async fn text<M: MetadataStore, C: ContentStore>(fs: &Fs<M, C>, path: &str) -> String {
    String::from_utf8(fs.read(path).await.unwrap().to_vec()).unwrap()
}

// ── object transfer ─────────────────────────────────────────────────────────

#[tokio::test]
async fn transfer_moves_only_whats_missing() {
    let a = solo().await;
    let b = solo().await;

    a.write("/f.txt", b"one\n").await.unwrap();
    let c1 = a.commit("alice", "first").await.unwrap();

    // commit + root tree + f.txt's manifest + its single chunk
    let first = transfer(&a, &b, c1).await.unwrap();
    assert_eq!(first.objects, 4, "first transfer copies the whole closure");
    assert_eq!(first.skipped, 0);
    assert!(first.bytes > 0);

    // Re-running stops dead at the head commit: nothing is copied twice.
    let again = transfer(&a, &b, c1).await.unwrap();
    assert_eq!(again.objects, 0, "a repeat transfer must copy nothing");
    assert_eq!(again.bytes, 0);
    assert_eq!(again.skipped, 1, "it stops at the head commit itself");

    // A second commit copies only what is new; the walk cuts at the old commit
    // and at the unchanged file's manifest.
    a.write("/g.txt", b"two\n").await.unwrap();
    let c2 = a.commit("alice", "second").await.unwrap();
    let delta = transfer(&a, &b, c2).await.unwrap();
    assert_eq!(delta.objects, 4, "only the new commit/tree/manifest/chunk");
    assert_eq!(
        delta.skipped, 2,
        "cut at c1 and at f.txt's unchanged manifest"
    );
}

// ── the four outcomes ───────────────────────────────────────────────────────

#[tokio::test]
async fn fast_forward_each_way_and_up_to_date_noop() {
    let a = solo().await;
    let b = solo().await;

    a.write("/base.txt", b"base\n").await.unwrap();
    let c1 = a.commit("alice", "base").await.unwrap();

    // First push: the remote has no such branch yet.
    let r = resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::Pushed(c1));
    assert!(r.remote_tree_updated, "the remote had main checked out");
    assert_eq!(b.branch_head("main").await.unwrap(), Some(c1));
    assert_eq!(text(&b, "/base.txt").await, "base\n");

    // Nothing to do the second time — and nothing is copied.
    let r = resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::UpToDate);
    assert_eq!(r.pushed.objects, 0);
    assert_eq!(r.fetched.objects, 0);

    // Local ahead → the remote fast-forwards.
    a.write("/local.txt", b"local\n").await.unwrap();
    let c2 = a.commit("alice", "more").await.unwrap();
    let r = resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::Pushed(c2));
    assert_eq!(text(&b, "/local.txt").await, "local\n");

    // Remote ahead → the local branch and working tree fast-forward.
    b.write("/remote.txt", b"remote\n").await.unwrap();
    let c3 = b.commit("bob", "remote work").await.unwrap();
    let r = resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::FastForwarded(c3));
    assert_eq!(a.head_commit().await.unwrap(), Some(c3));
    assert_eq!(text(&a, "/remote.txt").await, "remote\n");
    assert!(r.fetched.objects > 0, "the remote head had to be fetched");
    assert_eq!(r.pushed.objects, 0, "a fast-forward pushes nothing");

    let r = resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::UpToDate);
}

/// Both sides edit *different* files after a shared base: a clean three-way
/// merge, and the remote branch advances to it.
#[tokio::test]
async fn divergent_edits_to_different_files_merge_cleanly() {
    let a = solo().await;
    let b = solo().await;
    seed(&a, &b).await;

    b.write("/remote.txt", b"from remote\n").await.unwrap();
    b.commit("bob", "remote work").await.unwrap();
    a.write("/local.txt", b"from local\n").await.unwrap();
    a.commit("alice", "offline work").await.unwrap();

    let r = resync(&a, &b, "main", "alice", "reconnect").await.unwrap();
    let ResyncOutcome::Merged(merged) = r.outcome else {
        panic!("expected a merge, got {:?}", r.outcome);
    };
    assert!(r.conflicts.is_empty());
    assert_eq!(a.head_commit().await.unwrap(), Some(merged));
    assert_eq!(
        b.branch_head("main").await.unwrap(),
        Some(merged),
        "the remote branch advances to the merge"
    );
    // Both sides' edits survive, on both sides.
    for fs in [&a, &b] {
        assert_eq!(text(fs, "/base.txt").await, "base\n");
        assert_eq!(text(fs, "/local.txt").await, "from local\n");
        assert_eq!(text(fs, "/remote.txt").await, "from remote\n");
    }
    // The merge commit has both heads as parents.
    let commit = a.commit_object(&merged).await.unwrap();
    assert_eq!(commit.parents.len(), 2);
}

/// Both sides rewrite the *same lines*: conflicts are recorded exactly as an
/// ordinary merge records them, and the remote ref stays where it was.
#[tokio::test]
async fn divergent_edits_to_the_same_lines_conflict_and_spare_the_remote() {
    let a = solo().await;
    let b = solo().await;
    seed(&a, &b).await;

    b.write("/base.txt", b"remote rewrite\n").await.unwrap();
    let remote_head = b.commit("bob", "remote rewrite").await.unwrap();
    a.write("/base.txt", b"local rewrite\n").await.unwrap();
    let local_head = a.commit("alice", "local rewrite").await.unwrap();

    let r = resync(&a, &b, "main", "alice", "reconnect").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::Conflicted);
    assert_eq!(r.conflicts.len(), 1);
    assert_eq!(r.conflicts[0].path, "/base.txt");
    assert_eq!(r.conflicts[0].kind, "content");

    // The remote is untouched — a conflicted state never advances a shared branch.
    assert_eq!(b.branch_head("main").await.unwrap(), Some(remote_head));
    assert_eq!(text(&b, "/base.txt").await, "remote rewrite\n");

    // Locally this is an ordinary unresolved merge: markers, recorded conflicts,
    // MERGE_HEAD set, branch not advanced.
    assert_eq!(a.head_commit().await.unwrap(), Some(local_head));
    assert_eq!(
        a.conflicts().await.unwrap(),
        vec![("/base.txt".to_string(), "content".to_string())]
    );
    assert_eq!(
        a.backends().meta.get_ref("MERGE_HEAD").await.unwrap(),
        Some(remote_head.to_hex())
    );
    let body = text(&a, "/base.txt").await;
    assert!(
        body.contains("<<<<<<<"),
        "conflict markers in the tree: {body}"
    );

    // Resolving and committing lets the next resync land it.
    a.write("/base.txt", b"agreed\n").await.unwrap();
    let resolved = a.commit("alice", "resolve").await.unwrap();
    let r = resync(&a, &b, "main", "alice", "reconnect").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::Pushed(resolved));
    assert_eq!(text(&b, "/base.txt").await, "agreed\n");
}

// ── attribution crosses the gap ─────────────────────────────────────────────

/// The assertion the whole feature exists for: work done offline, attributed to a
/// local actor id, is queryable as *that person's* blame on the remote afterwards
/// — and the reverse for work fetched from the remote.
#[tokio::test]
async fn blame_travels_with_the_content_in_both_directions() {
    let a = solo().await;
    let b = solo().await;

    // Give the remote an unrelated actor first, so a verbatim id copy would land
    // the offline work on the wrong person.
    let decoy = b
        .create_actor(ActorInit::human("carol", Some("u:carol".into())))
        .await
        .unwrap();
    let alice = a
        .create_actor(ActorInit::human("alice", Some("u:alice".into())))
        .await
        .unwrap();
    assert_eq!(alice, decoy, "both DBs number their first actor the same");
    let alice_session = a.create_session(alice, Some("laptop")).await.unwrap();
    let ctx = WriteCtx::session(alice, alice_session);

    a.write_as(ctx, "/notes.md", b"alice wrote this\n")
        .await
        .unwrap();
    a.commit("alice", "notes").await.unwrap();

    let r = resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert!(matches!(r.outcome, ResyncOutcome::Pushed(_)));
    assert_eq!(r.blame_pushed, 1);

    // The remote can answer "who wrote this line?" for work it never saw happen.
    let blame = b.blame("/notes.md").await.unwrap();
    assert_eq!(blame.len(), 1);
    assert_eq!(blame[0].actor.display_name, "alice");
    assert_eq!(blame[0].actor.kind, ActorKind::Human);
    assert_ne!(
        blame[0].actor.id, decoy,
        "alice must not be mistaken for the remote's actor #1"
    );
    assert_eq!(blame[0].actor.auth_subject.as_deref(), Some("u:alice"));
    assert!(blame[0].session.is_some(), "session grouping is preserved");

    // Idempotent: resyncing again does not clone alice into a second actor.
    let actors_before = b.list_actors().await.unwrap().len();
    resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert_eq!(b.list_actors().await.unwrap().len(), actors_before);

    // Now the other direction, through a real merge. The remote's own agent edits
    // one file while the laptop edits another.
    let bot = b.create_agent("bot", "test-model", None).await.unwrap();
    let bot_session = b.create_session(bot, Some("server")).await.unwrap();
    b.write_as(
        WriteCtx::session(bot, bot_session),
        "/bot.md",
        b"bot wrote this\n",
    )
    .await
    .unwrap();
    b.commit("bot", "bot work").await.unwrap();

    a.write_as(ctx, "/more.md", b"alice again\n").await.unwrap();
    a.commit("alice", "more notes").await.unwrap();

    let r = resync(&a, &b, "main", "alice", "reconnect").await.unwrap();
    assert!(matches!(r.outcome, ResyncOutcome::Merged(_)));
    assert!(r.blame_fetched >= 1, "the agent's blame came back with us");
    assert!(r.blame_pushed >= 1, "our new blame went out");

    let on_remote = b.blame("/more.md").await.unwrap();
    assert_eq!(on_remote[0].actor.display_name, "alice");
    let on_local = a.blame("/bot.md").await.unwrap();
    assert_eq!(on_local[0].actor.display_name, "bot");
    assert_eq!(on_local[0].actor.kind, ActorKind::Agent);
    assert_eq!(
        on_local[0].actor.agent_model.as_deref(),
        Some("test-model"),
        "the agent's model travels with it"
    );
}

// ── merging around a live co-editing document ───────────────────────────────

/// A three-way merge over a path with an open live CRDT document is merging bytes
/// that may lag the `Y.Doc`. That is reported, not enforced: the merge still runs.
#[tokio::test]
async fn a_merge_over_a_live_document_warns_but_still_merges() {
    // Same divergence twice: once with nothing live, once with an editor open on
    // /doc.md — only the report differs.
    for live_path in [None, Some("/doc.md")] {
        let fs = solo().await;
        fs.write("/doc.md", b"base\n").await.unwrap();
        fs.write("/quiet.md", b"quiet\n").await.unwrap();
        fs.commit("a", "base").await.unwrap();
        fs.create_branch("dev").await.unwrap();

        fs.checkout("dev").await.unwrap();
        fs.write("/doc.md", b"base\ntheirs\n").await.unwrap();
        fs.write("/quiet.md", b"quiet\nalso theirs\n")
            .await
            .unwrap();
        let dev = fs.commit("a", "theirs").await.unwrap();

        fs.checkout("main").await.unwrap();
        fs.write("/ours.md", b"ours\n").await.unwrap();
        fs.commit("a", "ours").await.unwrap();

        if let Some(path) = live_path {
            let editor = fs
                .create_actor(ActorInit::human("ed", Some("u:ed".into())))
                .await
                .unwrap();
            fs.mark_live(WriteCtx::actor(editor), path).await.unwrap();
        }

        let (outcome, stale) = fs.merge_live(dev, "a", "merge").await.unwrap();
        assert!(
            matches!(outcome, MergeOutcome::Merged(_)),
            "the live marker must not block the merge, got {outcome:?}"
        );
        assert_eq!(
            stale.iter().map(|d| d.path.as_str()).collect::<Vec<_>>(),
            live_path.into_iter().collect::<Vec<_>>(),
            "only a live path the merge actually changed is reported"
        );
        // The merged bytes are the same either way.
        assert_eq!(text(&fs, "/doc.md").await, "base\ntheirs\n");
        assert_eq!(text(&fs, "/quiet.md").await, "quiet\nalso theirs\n");
        assert_eq!(text(&fs, "/ours.md").await, "ours\n");
    }
}

// ── refusals ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn refuses_a_dirty_tree_a_wrong_branch_and_versioning_off() {
    let a = solo().await;
    let b = solo().await;
    seed(&a, &b).await;

    // Uncommitted local work: a merge would rewrite it away.
    a.write("/scratch.txt", b"unsaved\n").await.unwrap();
    let err = resync(&a, &b, "main", "alice", "x").await.unwrap_err();
    assert!(
        err.to_string().contains("commit or discard"),
        "unexpected error: {err}"
    );
    a.commit("alice", "save").await.unwrap();

    // A branch that isn't checked out locally: the merge engine merges into HEAD.
    a.create_branch("side").await.unwrap();
    let err = resync(&a, &b, "side", "alice", "x").await.unwrap_err();
    assert!(err.to_string().contains("check out side"), "got: {err}");

    // Uncommitted work on the remote's checked-out branch.
    b.write("/theirs.txt", b"in progress\n").await.unwrap();
    let err = resync(&a, &b, "main", "alice", "x").await.unwrap_err();
    assert!(
        err.to_string().contains("remote workspace has uncommitted"),
        "got: {err}"
    );
    b.commit("bob", "save").await.unwrap();

    // versioning = off: there is no DAG to reconcile, and we say so.
    b.set_versioning_mode(origofs_core::VersioningMode::Off)
        .await
        .unwrap();
    let err = resync(&a, &b, "main", "alice", "x").await.unwrap_err();
    assert!(err.to_string().contains("versioning = off"), "got: {err}");
}

// ── the cas_ref race ────────────────────────────────────────────────────────

/// A clock that runs `action` exactly once, the first time it is asked for the
/// time after being armed. The engine takes every timestamp through the injected
/// clock, so this is a deterministic seam for "somebody else pushed while we were
/// mid-resync": arming it just before `resync` makes it fire while the merge
/// commit is being built — after the remote head has been read, before the push's
/// compare-and-swap.
struct RaceClock {
    armed: AtomicBool,
    fired: AtomicBool,
    action: Box<dyn Fn() + Send + Sync>,
}

impl Clock for RaceClock {
    fn now_secs(&self) -> i64 {
        if self.armed.load(Ordering::SeqCst) && !self.fired.swap(true, Ordering::SeqCst) {
            (self.action)();
        }
        SystemClock.now_secs()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_concurrent_remote_push_is_retried_not_clobbered() {
    let b = solo().await;
    // The commit the "other writer" publishes mid-resync, set before arming.
    let ahead: Arc<std::sync::Mutex<Option<Hash>>> = Arc::new(std::sync::Mutex::new(None));

    let b_meta = b.backends().meta.clone();
    let ahead_hook = ahead.clone();
    let race = Arc::new(RaceClock {
        armed: AtomicBool::new(false),
        fired: AtomicBool::new(false),
        action: Box::new(move || {
            let Some(target) = *ahead_hook.lock().unwrap() else {
                return;
            };
            // Stand in for another writer advancing the shared branch. Driving an
            // async store from a sync `now_secs` needs the blocking escape hatch.
            let meta = b_meta.clone();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    meta.set_ref("main", &target.to_hex()).await.unwrap();
                })
            });
        }),
    });
    let a = Fs::with_clock(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        Arc::new(MemStore::new()),
        race.clone(),
    );
    a.init().await.unwrap();

    // A shared base.
    a.write("/base.txt", b"base\n").await.unwrap();
    a.commit("alice", "base").await.unwrap();
    let r = resync(&a, &b, "main", "alice", "sync").await.unwrap();
    assert!(matches!(r.outcome, ResyncOutcome::Pushed(_)));

    // Two remote commits; then park the remote's HEAD on another branch and rewind
    // `main` to the first, so `main` is a plain ref that the racing writer can move
    // without the remote's working tree going dirty underneath it.
    b.write("/theirs1.txt", b"theirs one\n").await.unwrap();
    let step1 = b.commit("bob", "theirs 1").await.unwrap();
    b.write("/theirs2.txt", b"theirs two\n").await.unwrap();
    let step2 = b.commit("bob", "theirs 2").await.unwrap();
    b.create_branch("parked").await.unwrap();
    b.checkout("parked").await.unwrap();
    b.set_branch("main", step1).await.unwrap();
    *ahead.lock().unwrap() = Some(step2);

    // The laptop diverges from `step1`.
    a.write("/mine.txt", b"mine\n").await.unwrap();
    a.commit("alice", "mine").await.unwrap();

    race.armed.store(true, Ordering::SeqCst);
    let r = resync(&a, &b, "main", "alice", "reconnect").await.unwrap();
    assert!(race.fired.load(Ordering::SeqCst), "the race never fired");
    assert_eq!(r.cas_retries, 1, "the lost CAS was retried, not forced");
    let ResyncOutcome::Merged(merged) = r.outcome else {
        panic!("expected a merge, got {:?}", r.outcome);
    };
    assert_eq!(b.branch_head("main").await.unwrap(), Some(merged));
    // The other writer's commit was not clobbered: it is an ancestor of the result.
    assert!(a.is_ancestor(step2, merged).await.unwrap());
    assert_eq!(text(&a, "/theirs2.txt").await, "theirs two\n");
    assert_eq!(text(&a, "/mine.txt").await, "mine\n");
}

// ── heterogeneous pair: SQLite laptop ↔ Postgres shared workspace ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn divergent_edits_reconcile_against_postgres() {
    let Some(dsn) = std::env::var("ORIGOFS_PG_TEST_URL").ok() else {
        eprintln!("skipping divergent_edits_reconcile_against_postgres: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    reset_pg(&dsn).await;

    let laptop = solo().await;
    let shared = Fs::new(
        Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap()),
        Arc::new(MemStore::new()),
    );
    shared.init().await.unwrap();

    let alice = laptop
        .create_actor(ActorInit::human("alice", Some("u:alice".into())))
        .await
        .unwrap();
    let session = laptop.create_session(alice, Some("laptop")).await.unwrap();
    let ctx = WriteCtx::session(alice, session);

    laptop.write("/base.txt", b"base\n").await.unwrap();
    laptop.commit("alice", "base").await.unwrap();
    let r = resync(&laptop, &shared, "main", "alice", "sync")
        .await
        .unwrap();
    assert!(matches!(r.outcome, ResyncOutcome::Pushed(_)));
    assert_eq!(text(&shared, "/base.txt").await, "base\n");

    // Diverge on different files, offline.
    shared
        .write("/server.txt", b"from the server\n")
        .await
        .unwrap();
    shared.commit("bob", "server work").await.unwrap();
    laptop
        .write_as(ctx, "/offline.md", b"written on a plane\n")
        .await
        .unwrap();
    laptop.commit("alice", "offline work").await.unwrap();

    let r = resync(&laptop, &shared, "main", "alice", "reconnect")
        .await
        .unwrap();
    let ResyncOutcome::Merged(merged) = r.outcome else {
        panic!("expected a merge, got {:?}", r.outcome);
    };
    assert_eq!(shared.branch_head("main").await.unwrap(), Some(merged));
    assert_eq!(text(&laptop, "/server.txt").await, "from the server\n");
    assert_eq!(text(&laptop, "/offline.md").await, "written on a plane\n");
    assert_eq!(text(&shared, "/offline.md").await, "written on a plane\n");
    assert_eq!(text(&shared, "/server.txt").await, "from the server\n");

    // The offline attribution is queryable on the Postgres side.
    let blame = shared.blame("/offline.md").await.unwrap();
    assert_eq!(blame.len(), 1);
    assert_eq!(blame[0].actor.display_name, "alice");
    assert_eq!(blame[0].actor.auth_subject.as_deref(), Some("u:alice"));

    // And it stays one actor across repeated resyncs.
    let before = shared.list_actors().await.unwrap().len();
    resync(&laptop, &shared, "main", "alice", "sync")
        .await
        .unwrap();
    assert_eq!(shared.list_actors().await.unwrap().len(), before);
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Commit a shared base on `a` and seed it onto `b`, leaving both at the same
/// commit with clean trees.
async fn seed(a: &Solo, b: &Solo) {
    a.write("/base.txt", b"base\n").await.unwrap();
    let c1 = a.commit("alice", "base").await.unwrap();
    let r = resync(a, b, "main", "alice", "seed").await.unwrap();
    assert_eq!(r.outcome, ResyncOutcome::Pushed(c1));
}

/// Drop and recreate `public` so the Postgres case starts clean (mirrors
/// `tests/postgres.rs`).
async fn reset_pg(dsn: &str) {
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

// ── write ordering ──────────────────────────────────────────────────────────

/// A destination content store that fails the test if an object is stored before
/// something it points at. That is the invariant `transfer`'s cut depends on: it
/// stops descending at any object the destination already has, which is only
/// sound if a present object implies its whole closure is present.
struct ChildrenFirst {
    inner: Arc<MemStore>,
    violations: std::sync::Mutex<Vec<String>>,
}

impl ChildrenFirst {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemStore::new()),
            violations: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// The objects `bytes` points at. Each decoder checks its own magic, so only
    /// the matching one succeeds; a chunk or symlink target matches none.
    fn referenced(bytes: &[u8]) -> Vec<Hash> {
        use origofs_core::{Commit, Manifest, Tree};
        if let Ok(c) = Commit::decode(bytes) {
            let mut v = vec![c.tree];
            v.extend(c.parents);
            return v;
        }
        if let Ok(t) = Tree::decode(bytes) {
            return t.entries.iter().map(|e| e.hash).collect();
        }
        if let Ok(m) = Manifest::decode(bytes) {
            return m.chunks.iter().map(|c| c.hash).collect();
        }
        Vec::new()
    }
}

#[async_trait::async_trait]
impl ContentStore for ChildrenFirst {
    async fn put(&self, b: &[u8]) -> origofs_core::Result<Hash> {
        for child in Self::referenced(b) {
            if !self.inner.has(&child).await? {
                self.violations.lock().unwrap().push(format!(
                    "stored an object before {} that it points at",
                    child.to_hex()
                ));
            }
        }
        self.inner.put(b).await
    }
    async fn put_keyed(&self, k: &Hash, b: &[u8]) -> origofs_core::Result<()> {
        self.inner.put_keyed(k, b).await
    }
    async fn get(&self, h: &Hash) -> origofs_core::Result<bytes::Bytes> {
        self.inner.get(h).await
    }
    async fn get_range(&self, h: &Hash, o: u64, l: u64) -> origofs_core::Result<bytes::Bytes> {
        self.inner.get_range(h, o, l).await
    }
    async fn has(&self, h: &Hash) -> origofs_core::Result<bool> {
        self.inner.has(h).await
    }
    async fn list(&self) -> origofs_core::Result<Vec<Hash>> {
        self.inner.list().await
    }
    async fn list_with_age(&self) -> origofs_core::Result<Vec<(Hash, Option<u64>)>> {
        self.inner.list_with_age().await
    }
    async fn delete(&self, h: &Hash) -> origofs_core::Result<u64> {
        self.inner.delete(h).await
    }
}

/// An interrupted transfer must leave a *prefix* of the closure, never a hole
/// under an object that is already present — otherwise the next run's `has()` cut
/// prunes the subtree containing the hole and reports success over a store that
/// can never serve those bytes.
///
/// Recording nodes in DFS *pop* order and writing that list reversed does not
/// give this. A second commit that leaves a file untouched reuses the first
/// commit's manifest, so the manifest is discovered while walking the older
/// commit — i.e. *before* the newer tree that points at it — and reversing puts
/// the tree first. That is an ordinary two-commit history, not a corner case.
#[tokio::test]
async fn transfer_writes_children_before_parents() {
    let a = solo().await;
    a.write("/a.txt", b"first file\n").await.unwrap();
    a.commit("dan", "one").await.unwrap();
    // `/a.txt` is untouched, so commit two's tree points at commit one's manifest.
    a.write("/b.txt", b"second file\n").await.unwrap();
    let head = a.commit("dan", "two").await.unwrap();

    let checked = ChildrenFirst::new();
    let b: Fs<Arc<SqliteMetadataStore>, Arc<ChildrenFirst>> = Fs::new(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        checked.clone(),
    );
    b.init().await.unwrap();
    transfer(&a, &b, head).await.unwrap();

    let violations = checked.violations.lock().unwrap();
    assert!(
        violations.is_empty(),
        "transfer must store children before parents, got {} violation(s):\n  {}",
        violations.len(),
        violations.join("\n  ")
    );
}

/// The same invariant over a shape where an object is genuinely reachable by more
/// than one path: a merge DAG with nested directories, where both sides share a
/// subtree. A node must still be written exactly once, after every one of its
/// children, whichever path reached it first.
#[tokio::test]
async fn transfer_writes_children_before_parents_across_a_merge() {
    let a = solo().await;
    a.mkdir_p("/shared").await.unwrap();
    a.mkdir_p("/shared/deep").await.unwrap();
    a.write("/shared/deep/common.txt", b"shared by both sides\n")
        .await
        .unwrap();
    a.commit("dan", "base").await.unwrap();

    a.create_branch("side").await.unwrap();
    a.write("/ours.txt", b"ours\n").await.unwrap();
    a.commit("dan", "ours").await.unwrap();

    a.checkout("side").await.unwrap();
    a.write("/theirs.txt", b"theirs\n").await.unwrap();
    let theirs = a.commit("dan", "theirs").await.unwrap();

    a.checkout("main").await.unwrap();
    match a.merge(theirs, "dan", "merge").await.unwrap() {
        MergeOutcome::Merged(_) => {}
        other => panic!("expected a clean merge, got {other:?}"),
    }
    let head = a.head_commit().await.unwrap().unwrap();

    let checked = ChildrenFirst::new();
    let b: Fs<Arc<SqliteMetadataStore>, Arc<ChildrenFirst>> = Fs::new(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        checked.clone(),
    );
    b.init().await.unwrap();
    transfer(&a, &b, head).await.unwrap();
    // And the destination really holds the whole closure, not merely a valid
    // order over whatever subset happened to be copied.
    let mut stack = vec![head];
    let mut walked = 0usize;
    while let Some(h) = stack.pop() {
        let bytes = b
            .get_object(&h)
            .await
            .unwrap_or_else(|e| panic!("closure is incomplete at {}: {e}", h.to_hex()));
        walked += 1;
        stack.extend(ChildrenFirst::referenced(&bytes));
    }
    assert!(
        walked > 10,
        "expected a non-trivial closure, walked {walked}"
    );

    let violations = checked.violations.lock().unwrap();
    assert!(
        violations.is_empty(),
        "transfer must store children before parents, got {} violation(s):\n  {}",
        violations.len(),
        violations.join("\n  ")
    );
}
