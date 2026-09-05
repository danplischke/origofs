//! `Superseded` is a real terminal state, not a decorative enum variant
//! (issue #75 §3.2).
//!
//! A byte suggestion proposes a whole file body against a specific base. Once the
//! file moves on, that proposal is about a version that no longer exists: applying
//! it would silently discard the intervening change, so it must not be applied —
//! but leaving it `Pending` forever is not honest either. It resolves to
//! [`SuggestionStatus::Superseded`], both when a reviewer tries to accept it and
//! (proactively) when a *different* accept on the same path moves the file.
//!
//! The Postgres leg self-skips unless `ORIGOFS_PG_TEST_URL` points at a reachable
//! database, matching `tests/postgres.rs`.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, PostgresMetadataStore, SqliteMetadataStore,
    SuggestionKind, SuggestionStatus, WriteCtx,
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

/// The body of both engine legs: accepting a byte suggestion whose base moved
/// refuses *and* retires the proposal.
async fn stale_accept_supersedes<M: MetadataStore>(fs: Fs<M, Arc<MemStore>>) {
    let author = fs.create_agent("claude", "m", None).await.unwrap();
    let reviewer = fs.create_human("dan", None).await.unwrap();
    let s_a = fs.create_session(author, None).await.unwrap();
    let a = WriteCtx::session(author, s_a);
    let r = WriteCtx::actor(reviewer);

    fs.write_as(r, "/notes.md", b"one\n").await.unwrap();
    let id = fs
        .suggest(a, "/notes.md", b"one\ntwo\n", Some("add a line"))
        .await
        .unwrap();
    assert_eq!(
        fs.get_suggestion(id).await.unwrap().unwrap().kind,
        SuggestionKind::Bytes,
        "a plain suggest is a byte suggestion"
    );

    // The file moves on underneath the proposal.
    fs.write_as(r, "/notes.md", b"one\nsomething else\n")
        .await
        .unwrap();

    // Accepting is refused...
    let err = fs.accept_suggestion(id, r).await.unwrap_err();
    // `StaleBase`, not a bare `Conflict` (#159): the caller has to be able to tell
    // "re-diff and re-suggest" from "reseed your co-edit document" without reading
    // the message, and both used to arrive as the same class.
    assert!(
        matches!(err, OrigoFSError::StaleBase(_)),
        "a stale byte suggestion must not be applied: {err:?}"
    );
    assert_eq!(err.code(), "stale_base");
    // ...the file is untouched (no silent clobber)...
    assert_eq!(
        &fs.read("/notes.md").await.unwrap()[..],
        b"one\nsomething else\n"
    );
    // ...and the proposal is retired rather than left Pending forever.
    let s = fs.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.status, SuggestionStatus::Superseded);
    assert!(s.resolved_ts.is_some());
    assert!(
        fs.list_suggestions(Some(SuggestionStatus::Pending), None)
            .await
            .unwrap()
            .is_empty(),
        "nothing is left pending"
    );
    assert_eq!(
        fs.list_suggestions(Some(SuggestionStatus::Superseded), None)
            .await
            .unwrap()
            .len(),
        1
    );

    // A second accept reports it as already resolved, not as a fresh conflict.
    let err = fs.accept_suggestion(id, r).await.unwrap_err();
    assert!(
        matches!(&err, OrigoFSError::InvalidArgument(m) if m.contains("superseded")),
        "{err:?}"
    );
}

#[tokio::test]
async fn stale_byte_suggestion_is_superseded_sqlite() {
    stale_accept_supersedes(sqlite_fs().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_byte_suggestion_is_superseded_postgres() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    stale_accept_supersedes(fs).await;
}

// Accepting one proposal retires the *other* pending proposals it invalidated, so
// a queue of competing suggestions doesn't accumulate rows nobody can ever accept.
#[tokio::test]
async fn accepting_one_supersedes_the_others_on_that_path() {
    let fs = sqlite_fs().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let carol = fs.create_human("carol", None).await.unwrap();
    let (a, b, c) = (
        WriteCtx::actor(alice),
        WriteCtx::actor(bob),
        WriteCtx::actor(carol),
    );

    fs.write_as(c, "/doc.md", b"base\n").await.unwrap();
    let first = fs.suggest(a, "/doc.md", b"alice\n", None).await.unwrap();
    let second = fs.suggest(b, "/doc.md", b"bob\n", None).await.unwrap();
    // A proposal on another path is none of this path's business.
    fs.write_as(c, "/other.md", b"x\n").await.unwrap();
    let elsewhere = fs.suggest(a, "/other.md", b"y\n", None).await.unwrap();

    fs.accept_suggestion(first, c).await.unwrap();

    assert_eq!(&fs.read("/doc.md").await.unwrap()[..], b"alice\n");
    assert_eq!(
        fs.get_suggestion(first).await.unwrap().unwrap().status,
        SuggestionStatus::Accepted
    );
    assert_eq!(
        fs.get_suggestion(second).await.unwrap().unwrap().status,
        SuggestionStatus::Superseded,
        "bob's proposal was against a base that no longer exists"
    );
    assert_eq!(
        fs.get_suggestion(elsewhere).await.unwrap().unwrap().status,
        SuggestionStatus::Pending,
        "a different path is unaffected"
    );

    // The supersession is on the change feed, so a UI can explain it.
    let events = fs.events_since(0, 100).await.unwrap();
    let sup = events
        .iter()
        .find(|e| e.kind == "supersede")
        .expect("a supersede event");
    assert_eq!(sup.path, "/doc.md");
    assert_eq!(sup.actor_id, Some(bob), "credited to whose proposal it was");
}

// A pending proposal whose base still matches is *not* superseded by an unrelated
// accept: supersession means "the base moved", not "somebody else was accepted".
#[tokio::test]
async fn a_still_current_proposal_survives() {
    let fs = sqlite_fs().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let (a, b) = (WriteCtx::actor(alice), WriteCtx::actor(bob));

    fs.write_as(b, "/a.md", b"one\n").await.unwrap();
    fs.write_as(b, "/b.md", b"one\n").await.unwrap();
    let keep = fs.suggest(a, "/b.md", b"two\n", None).await.unwrap();
    let apply = fs.suggest(a, "/a.md", b"two\n", None).await.unwrap();

    fs.accept_suggestion(apply, b).await.unwrap();
    assert_eq!(
        fs.get_suggestion(keep).await.unwrap().unwrap().status,
        SuggestionStatus::Pending
    );

    // And the explicit sweep is a no-op while the base still matches.
    assert_eq!(
        fs.supersede_stale_byte_suggestions("/b.md", None)
            .await
            .unwrap(),
        0
    );
    fs.write_as(b, "/b.md", b"moved\n").await.unwrap();
    assert_eq!(
        fs.supersede_stale_byte_suggestions("/b.md", None)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fs.get_suggestion(keep).await.unwrap().unwrap().status,
        SuggestionStatus::Superseded
    );
}
