//! The per-path **live** flag, and reconciling an out-of-band write to a live
//! path (issue #75 §3.4).
//!
//! While a path is open in a CRDT co-editing session, its durable CAS blob is a
//! checkpoint — real, attributed, but possibly behind the `Y.Doc` people are typing
//! into. Two things follow, and both are tested here:
//!
//! 1. **Byte readers can tell.** `read` is unchanged (a read must not write, and the
//!    live document is in-process state the engine cannot reach anyway), but
//!    `read_live`/`live_doc`/`live_paths` surface the staleness so the git export
//!    path, a three-way merge, or a UI can decide what to do.
//! 2. **An out-of-band write does not silently race.** The live marker records the
//!    content address of the last checkpoint, so a write that lands around the live
//!    document is *detected*, and the next checkpoint folds it in — through the same
//!    machinery `open_coedit` uses for an incoherent sidecar — instead of
//!    crystallizing over it.
//!
//! The Postgres leg self-skips without `ORIGOFS_PG_TEST_URL`.
#![cfg(feature = "coedit")]

use origofs_core::{
    CoeditDoc, Fs, MemStore, MetadataStore, PostgresMetadataStore, SqliteMetadataStore,
    WorkspaceRegistry, WriteCtx,
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

async fn sqlite_fs() -> Fs<Arc<dyn MetadataStore>, Arc<MemStore>> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

/// The marker's lifecycle, run against either engine.
async fn live_marker_lifecycle<M: MetadataStore>(fs: Fs<M, Arc<MemStore>>) {
    let alice = fs.create_human("alice", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let a = WriteCtx::session(alice, s_a);

    // An ordinary file is not live.
    fs.write_as(a, "/plain.md", b"just a file\n").await.unwrap();
    assert!(fs.live_doc("/plain.md").await.unwrap().is_none());
    assert!(fs.live_paths().await.unwrap().is_empty());
    let (bytes, live) = fs.read_live("/plain.md").await.unwrap();
    assert_eq!(&bytes[..], b"just a file\n");
    assert!(live.is_none(), "not live => the bytes are the whole truth");

    // Opening it for co-editing marks it live, naming who and since when.
    let doc = fs.open_coedit(a, "/plain.md").await.unwrap();
    let marker = fs.live_doc("/plain.md").await.unwrap().expect("live");
    assert_eq!(marker.path, "/plain.md");
    assert_eq!(marker.actor_id, alice);
    assert_eq!(marker.session_id, Some(s_a));
    assert_eq!(
        marker.content_hash,
        fs.stat("/plain.md")
            .await
            .unwrap()
            .content
            .map(|h| h.to_hex()),
        "the marker pins the content address it is coherent with"
    );
    assert_eq!(fs.live_paths().await.unwrap().len(), 1);

    // A read still succeeds and still returns the durable bytes — it does not
    // block, fail, or force a checkpoint — but now it can say they may lag.
    doc.insert(a, 0, "typed but not yet checkpointed: ");
    let (bytes, live) = fs.read_live("/plain.md").await.unwrap();
    assert_eq!(
        &bytes[..],
        b"just a file\n",
        "read is unchanged: the last checkpoint, not the live doc"
    );
    assert!(live.is_some(), "...but the caller is told it may lag");

    // Checkpointing re-pins the marker to what it just wrote.
    fs.checkpoint_coedit(a, "/plain.md", &doc).await.unwrap();
    let marker = fs.live_doc("/plain.md").await.unwrap().expect("still live");
    assert_eq!(
        marker.content_hash,
        fs.stat("/plain.md")
            .await
            .unwrap()
            .content
            .map(|h| h.to_hex())
    );
    let (bytes, _) = fs.read_live("/plain.md").await.unwrap();
    assert!(bytes.starts_with(b"typed but not yet checkpointed: "));

    // Ending the session clears it (and is idempotent).
    fs.end_coedit("/plain.md").await.unwrap();
    fs.end_coedit("/plain.md").await.unwrap();
    assert!(fs.live_doc("/plain.md").await.unwrap().is_none());
    assert!(fs.live_paths().await.unwrap().is_empty());
}

#[tokio::test]
async fn live_marker_lifecycle_sqlite() {
    live_marker_lifecycle(sqlite_fs().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_marker_lifecycle_postgres() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    live_marker_lifecycle(fs).await;
}

/// The race the marker exists to stop: somebody writes the file through the
/// ordinary write path while a co-editing session has it open. Without
/// reconciliation the next checkpoint crystallizes the CRDT over the file and the
/// out-of-band change disappears with no conflict and no trace.
async fn out_of_band_write_is_reconciled<M: MetadataStore>(fs: Fs<M, Arc<MemStore>>) {
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let (a, b) = (WriteCtx::actor(alice), WriteCtx::actor(bob));

    // Alice is co-editing; the document is checkpointed and the path is live.
    let doc = fs.open_coedit(a, "/notes.md").await.unwrap();
    doc.insert(a, 0, "one\ntwo\n");
    fs.checkpoint_coedit(a, "/notes.md", &doc).await.unwrap();

    // Bob writes the file out of band — a script, a merge, an accepted byte
    // suggestion — appending a line while Alice's room is still open.
    fs.write_as(b, "/notes.md", b"one\ntwo\nbob was here\n")
        .await
        .unwrap();

    // Alice, who never saw that, keeps typing in her live document and checkpoints.
    doc.insert(a, 8, "three\n"); // her doc: "one\ntwo\nthree\n"
    fs.checkpoint_coedit(a, "/notes.md", &doc).await.unwrap();

    let text = String::from_utf8(fs.read("/notes.md").await.unwrap().to_vec()).unwrap();
    assert!(
        text.contains("bob was here\n"),
        "the out-of-band write must survive the checkpoint, not be clobbered: {text:?}"
    );
    assert!(
        text.contains("three\n"),
        "and so must the live edit: {text:?}"
    );
    assert!(text.contains("one\n") && text.contains("two\n"), "{text:?}");

    // Both keep their own author: Bob's line is Bob's, Alice's is Alice's.
    let blame = fs.blame("/notes.md").await.unwrap();
    let owner_of = |needle: &str| {
        let off = text.find(needle).unwrap() as u64;
        blame
            .iter()
            .find(|r| r.byte_start <= off && off < r.byte_end)
            .unwrap_or_else(|| panic!("no blame covering {needle:?}"))
            .actor
            .id
    };
    assert_eq!(
        owner_of("bob was here"),
        bob,
        "reconciled-in text keeps the author blame recorded for it"
    );
    assert_eq!(owner_of("three"), alice);

    // The live document itself now holds both, so editing continues coherently
    // rather than diverging from the file.
    assert_eq!(doc.text(), text);
}

#[tokio::test]
async fn out_of_band_write_is_reconciled_sqlite() {
    out_of_band_write_is_reconciled(sqlite_fs().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_band_write_is_reconciled_postgres() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    out_of_band_write_is_reconciled(fs).await;
}

// Reconciliation is opt-in on *liveness*, not on every checkpoint: with no live
// marker (a Rust caller that checkpoints a document it never opened through
// `open_coedit`) the historical whole-file behaviour is unchanged.
#[tokio::test]
async fn a_path_that_is_not_live_keeps_the_old_checkpoint_behaviour() {
    let fs = sqlite_fs().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let (a, b) = (WriteCtx::actor(alice), WriteCtx::actor(bob));

    let doc = origofs_core::CoeditDoc::new();
    doc.insert(a, 0, "mine\n");
    fs.checkpoint_coedit(a, "/x.md", &doc).await.unwrap();
    assert!(fs.live_doc("/x.md").await.unwrap().is_none());

    fs.write_as(b, "/x.md", b"theirs\n").await.unwrap();
    fs.checkpoint_coedit(a, "/x.md", &doc).await.unwrap();
    assert_eq!(
        &fs.read("/x.md").await.unwrap()[..],
        b"mine\n",
        "not live => a checkpoint is still a whole-file crystallization"
    );
}

// A no-op checkpoint on a live path must not churn: nothing wrote around us, so
// there is nothing to reconcile and the file is unchanged.
#[tokio::test]
async fn a_quiet_live_path_reconciles_to_nothing() {
    let fs = sqlite_fs().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let a = WriteCtx::actor(alice);

    let doc = fs.open_coedit(a, "/quiet.md").await.unwrap();
    doc.insert(a, 0, "steady\n");
    fs.checkpoint_coedit(a, "/quiet.md", &doc).await.unwrap();
    let before = fs.stat("/quiet.md").await.unwrap().content;

    fs.checkpoint_coedit(a, "/quiet.md", &doc).await.unwrap();
    assert_eq!(fs.stat("/quiet.md").await.unwrap().content, before);
    assert_eq!(doc.text(), "steady\n", "no phantom reconciliation");
    assert_eq!(&fs.read("/quiet.md").await.unwrap()[..], b"steady\n");
}

// The marker is workspace-scoped like `conflict`/`file_lock`: the same path in
// two workspaces is two different files, and one being live says nothing about
// the other.
#[tokio::test]
async fn the_live_marker_is_workspace_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("meta.db")).unwrap());
    let store = Arc::new(MemStore::new());
    let fs = Fs::new(meta.clone(), store.clone());
    fs.init().await.unwrap();

    let (id, root) = meta.create_workspace("other").await.unwrap();
    let other = fs.rebind(meta.with_workspace(id), root);
    other.init().await.unwrap();

    let alice = fs.create_human("alice", None).await.unwrap();
    let a = WriteCtx::actor(alice);

    let doc = fs.open_coedit(a, "/shared-name.md").await.unwrap();
    doc.insert(a, 0, "default workspace\n");
    fs.checkpoint_coedit(a, "/shared-name.md", &doc)
        .await
        .unwrap();

    assert!(fs.live_doc("/shared-name.md").await.unwrap().is_some());
    assert!(
        other.live_doc("/shared-name.md").await.unwrap().is_none(),
        "another workspace's path of the same name is not live"
    );
    assert!(other.live_paths().await.unwrap().is_empty());
}

// ─── catching up a socket dropped from the fan-out ───────────────────────────

/// A client that misses a broadcast frame does **not** reconverge by itself, and
/// `state_frame` is what repairs it.
///
/// The fan-out is a bounded `broadcast` channel, so a socket that falls behind is
/// dropped from it with `RecvError::Lagged` and loses those frames permanently.
/// The arm used to do nothing, on the reasoning that the CRDT would reconverge on
/// the next edit. It does not: a later delta is encoded against a state vector the
/// client never reached, so Yjs parks it as pending on origins that never arrive
/// — and every subsequent edit parks behind it too. The client's document freezes
/// at the moment it lagged while its user keeps typing into it.
///
/// This is at the document level rather than driven through a real socket on
/// purpose: forcing a genuine `Lagged` means overflowing a 256-frame channel by
/// filling the peer's TCP receive buffer, which makes the test a hostage to socket
/// buffer sizes. The property the fix rests on is exactly what is asserted here.
#[test]
fn a_client_that_missed_a_frame_is_repaired_by_the_state_frame() {
    let ctx = WriteCtx::actor(1);
    let server = CoeditDoc::new();
    let client = CoeditDoc::load(&server.state_update()).unwrap();
    let peer = CoeditDoc::load(&server.state_update()).unwrap();

    // Frame 1 is broadcast — and dropped for this client.
    peer.insert(ctx, 0, "AAA");
    let _dropped = server.apply_update_as(ctx, &peer.state_update()).unwrap();

    // Frame 2 is delivered, but lands on a gap.
    peer.insert(ctx, 3, "BBB");
    let delivered = server.apply_update_as(ctx, &peer.state_update()).unwrap();
    client.apply_update(&delivered).unwrap();

    assert_eq!(server.text(), "AAABBB");
    assert_eq!(
        client.text(),
        "",
        "a missed frame leaves the client stuck, not merely one edit behind"
    );

    // A further edit does not heal it either — the gap is permanent.
    peer.insert(ctx, 6, "CCC");
    let next = server.apply_update_as(ctx, &peer.state_update()).unwrap();
    client.apply_update(&next).unwrap();
    assert_eq!(client.text(), "", "later edits pile up behind the same gap");

    // The whole-state frame the `Lagged` arm now sends closes it in one shot.
    // `apply_relayed` is the y-sync frame decoder, which is what a client's own
    // protocol handler does with it.
    client.apply_relayed(&server.state_frame()).unwrap();
    assert_eq!(client.text(), server.text());
    assert_eq!(client.text(), "AAABBBCCC");
}
