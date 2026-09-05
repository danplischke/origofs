//! CRDT-shaped suggestions (issue #75 §3.2): proposing a *merge* into a co-edited
//! document instead of a whole file body.
//!
//! The point of the kind split is that "stale" means something different for each.
//! A byte suggestion carries a content hash as its base and must be refused once
//! the file moves. A CRDT suggestion carries a **state vector** and an opaque
//! `encodeStateAsUpdate` blob, and a CRDT merge is defined for any pair of states —
//! so a disjoint concurrent edit is not a conflict, and the byte staleness guard
//! false-rejected every one of them.
//!
//! Every attribution invariant still holds: the merged text is attributed to the
//! *original author*, the approver is recorded, and a reviewer must differ from the
//! author. The Postgres leg self-skips without `ORIGOFS_PG_TEST_URL`.
#![cfg(feature = "coedit")]

use origofs_core::{
    CoeditDoc, Fs, MemStore, MetadataStore, OrigoFSError, PostgresMetadataStore,
    SqliteMetadataStore, SuggestionKind, SuggestionStatus, WriteCtx,
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

/// The scenario the byte path gets wrong, run against either engine.
///
/// A document is co-edited and checkpointed. An agent forks it and proposes an
/// addition at the end. *Meanwhile* a human edits a disjoint region and
/// checkpoints, so the file's content hash moves. The proposal is still perfectly
/// mergeable — and accepting it must merge, keeping both edits and both authors.
async fn disjoint_concurrent_edit_still_merges<M: MetadataStore>(fs: Fs<M, Arc<MemStore>>) {
    let human = fs.create_human("dan", None).await.unwrap();
    let agent = fs
        .create_agent("claude", "opus", Some(human))
        .await
        .unwrap();
    let s_h = fs.create_session(human, None).await.unwrap();
    let s_a = fs.create_session(agent, None).await.unwrap();
    let h = WriteCtx::session(human, s_h);
    let a = WriteCtx::session(agent, s_a);

    // A live document, checkpointed.
    let doc = fs.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "alpha\n");
    fs.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();
    let base_hash_at_propose = fs.stat("/notes.md").await.unwrap().content;

    // The agent forks the document and proposes appending to it.
    let fork = fs.open_coedit(a, "/notes.md").await.unwrap();
    fork.insert(a, 6, "gamma\n");
    let id = fs
        .suggest_coedit(a, "/notes.md", &fork, Some("append gamma"), None)
        .await
        .unwrap();
    let s = fs.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.kind, SuggestionKind::Crdt);
    assert_eq!(s.actor_id, agent);
    assert!(
        s.base_hash.is_some() && s.proposed_hash.is_some(),
        "both blobs are addressed in the CAS, never inlined in the metadata DB"
    );

    // Concurrently, the human edits a *disjoint* region and checkpoints: the
    // file's content hash moves out from under the proposal's base.
    doc.insert(h, 0, "beta\n");
    fs.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();
    assert_ne!(
        fs.stat("/notes.md").await.unwrap().content,
        base_hash_at_propose,
        "the file really did move — this is what used to false-reject"
    );

    // The reviewer previews it: the diff is the *effect of the merge* against the
    // document as it is now, not a nonsense diff of two binary blobs.
    let diff = fs.suggestion_diff(id).await.unwrap();
    assert!(diff.contains("gamma"), "{diff}");
    assert!(
        !diff.contains("-beta"),
        "the preview must not show the concurrent edit as a deletion: {diff}"
    );

    // Accept: merges, does not clobber, is not refused as stale.
    fs.accept_suggestion(id, h).await.unwrap();
    let text = String::from_utf8(fs.read("/notes.md").await.unwrap().to_vec()).unwrap();
    assert!(text.contains("gamma\n"), "the proposal landed: {text:?}");
    assert!(
        text.contains("beta\n"),
        "the concurrent disjoint edit survived: {text:?}"
    );
    assert!(text.contains("alpha\n"), "{text:?}");

    let s = fs.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.status, SuggestionStatus::Accepted);
    assert_eq!(
        s.resolved_by,
        Some(human),
        "the approver is recorded, distinct from the author"
    );

    // Attribution: the merged-in text is credited to the *original author*, and
    // everyone else keeps theirs.
    let blame = fs.blame("/notes.md").await.unwrap();
    let owner_of = |needle: &str| {
        let off = text.find(needle).unwrap() as u64;
        blame
            .iter()
            .find(|r| r.byte_start <= off && off < r.byte_end)
            .unwrap_or_else(|| panic!("no blame covering {needle:?} at {off}"))
            .actor
            .id
    };
    assert_eq!(owner_of("gamma"), agent, "the proposal is the agent's");
    assert_eq!(owner_of("beta"), human);
    assert_eq!(owner_of("alpha"), human);
}

#[tokio::test]
async fn crdt_suggestion_merges_over_a_concurrent_edit_sqlite() {
    disjoint_concurrent_edit_still_merges(sqlite_fs().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_suggestion_merges_over_a_concurrent_edit_postgres() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    disjoint_concurrent_edit_still_merges(fs).await;
}

// The same file, the same concurrent edit, proposed the *old* way: a byte
// suggestion is (correctly) refused and retired. This is the contrast that shows
// the CRDT path is fixing a real false-reject and not just loosening a check.
#[tokio::test]
async fn a_byte_suggestion_over_the_same_edit_is_still_refused() {
    let fs = sqlite_fs().await;
    let human = fs.create_human("dan", None).await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));

    let doc = fs.open_coedit(h, "/notes.md").await.unwrap();
    doc.insert(h, 0, "alpha\n");
    fs.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();

    let id = fs
        .suggest(a, "/notes.md", b"alpha\ngamma\n", None, None)
        .await
        .unwrap();
    doc.insert(h, 0, "beta\n");
    fs.checkpoint_coedit(h, "/notes.md", &doc).await.unwrap();

    let err = fs.accept_suggestion(id, h).await.unwrap_err();
    assert!(matches!(err, OrigoFSError::StaleBase(_)), "{err:?}");
    assert_eq!(
        fs.get_suggestion(id).await.unwrap().unwrap().status,
        SuggestionStatus::Superseded
    );
    let text = String::from_utf8(fs.read("/notes.md").await.unwrap().to_vec()).unwrap();
    assert!(text.contains("beta\n"), "not clobbered: {text:?}");
}

// The review gate is the same for both kinds: a proposer cannot approve itself,
// and the server never trusts authorship carried in the client's blob.
#[tokio::test]
async fn crdt_suggestion_keeps_the_review_gate_and_reattributes() {
    let fs = sqlite_fs().await;
    let human = fs.create_human("dan", None).await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let mallory = fs.create_human("mallory", None).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));

    let doc = fs.open_coedit(h, "/doc.md").await.unwrap();
    doc.insert(h, 0, "hello\n");
    fs.checkpoint_coedit(h, "/doc.md", &doc).await.unwrap();

    // The agent proposes text its blob *claims* mallory wrote.
    let fork = fs.open_coedit(a, "/doc.md").await.unwrap();
    fork.insert(WriteCtx::actor(mallory), 6, "forged\n");
    let id = fs
        .suggest_coedit(a, "/doc.md", &fork, None, None)
        .await
        .unwrap();

    // Its own author cannot accept it.
    let err = fs.accept_suggestion(id, a).await.unwrap_err();
    assert!(
        matches!(&err, OrigoFSError::InvalidArgument(m) if m.contains("different reviewer")),
        "{err:?}"
    );
    assert_eq!(
        fs.get_suggestion(id).await.unwrap().unwrap().status,
        SuggestionStatus::Pending
    );

    // A different reviewer can — and the merged text is credited to the *agent*
    // (the suggestion's real author), not to whoever the blob named.
    fs.accept_suggestion(id, h).await.unwrap();
    let text = String::from_utf8(fs.read("/doc.md").await.unwrap().to_vec()).unwrap();
    let off = text.find("forged").unwrap() as u64;
    let blame = fs.blame("/doc.md").await.unwrap();
    let owner = blame
        .iter()
        .find(|r| r.byte_start <= off && off < r.byte_end)
        .unwrap();
    assert_eq!(
        owner.actor.id, agent,
        "the server re-stamps proposed content with the suggestion's author"
    );
    assert_ne!(owner.actor.id, mallory);
}

// The raw primitive a browser client uses, and its input validation.
#[tokio::test]
async fn crdt_suggestion_from_raw_blobs_validates_its_update() {
    let fs = sqlite_fs().await;
    let human = fs.create_human("dan", None).await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));

    let doc = fs.open_coedit(h, "/raw.md").await.unwrap();
    doc.insert(h, 0, "seed\n");
    fs.checkpoint_coedit(h, "/raw.md", &doc).await.unwrap();

    let base_sv = doc.state_vector();
    let fork = CoeditDoc::load(&doc.state_update()).unwrap();
    fork.insert(a, 5, "added\n");

    // An empty or malformed update is refused at propose time, not at review time.
    let err = fs
        .suggest_coedit_update(a, "/raw.md", &base_sv, b"", None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, OrigoFSError::InvalidArgument(_)), "{err:?}");
    let err = fs
        .suggest_coedit_update(
            a,
            "/raw.md",
            &base_sv,
            b"\xff\xff not a yjs update",
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, OrigoFSError::InvalidArgument(_)), "{err:?}");

    let id = fs
        .suggest_coedit_update(
            a,
            "/raw.md",
            &base_sv,
            &fork.state_update(),
            Some("raw"),
            None,
        )
        .await
        .unwrap();
    fs.accept_suggestion(id, h).await.unwrap();
    let text = String::from_utf8(fs.read("/raw.md").await.unwrap().to_vec()).unwrap();
    assert_eq!(text, "seed\nadded\n");
}

// A CRDT suggestion is *not* swept to `Superseded` when the file moves — that is
// the whole point: it merges into whatever the document has become.
#[tokio::test]
async fn crdt_suggestions_are_never_superseded_by_a_moving_file() {
    let fs = sqlite_fs().await;
    let human = fs.create_human("dan", None).await.unwrap();
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let (h, a) = (WriteCtx::actor(human), WriteCtx::actor(agent));

    let doc = fs.open_coedit(h, "/d.md").await.unwrap();
    doc.insert(h, 0, "x\n");
    fs.checkpoint_coedit(h, "/d.md", &doc).await.unwrap();

    let fork = fs.open_coedit(a, "/d.md").await.unwrap();
    fork.insert(a, 2, "y\n");
    let crdt = fs
        .suggest_coedit(a, "/d.md", &fork, None, None)
        .await
        .unwrap();
    let bytes = fs.suggest(a, "/d.md", b"x\nz\n", None, None).await.unwrap();

    // Move the file well past both proposals' bases.
    fs.write_as(h, "/d.md", b"x\nmoved\n").await.unwrap();
    assert_eq!(
        fs.supersede_stale_byte_suggestions("/d.md", None)
            .await
            .unwrap(),
        1,
        "only the byte proposal is retired"
    );
    assert_eq!(
        fs.get_suggestion(bytes).await.unwrap().unwrap().status,
        SuggestionStatus::Superseded
    );
    assert_eq!(
        fs.get_suggestion(crdt).await.unwrap().unwrap().status,
        SuggestionStatus::Pending
    );
    fs.accept_suggestion(crdt, h).await.unwrap();
    let text = String::from_utf8(fs.read("/d.md").await.unwrap().to_vec()).unwrap();
    assert!(text.contains("y\n") && text.contains("moved\n"), "{text:?}");
}
