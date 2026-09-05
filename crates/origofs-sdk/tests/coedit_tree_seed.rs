//! A tree checkpoint may only land over bytes its document accounts for
//! (issues #158, #161).
//!
//! origofs cannot parse a flat file back into tree nodes — that needs the host's
//! schema — so a tree room opened over a file it cannot resume from starts
//! **empty**. Checkpointing that document replaced the file's content with
//! nothing, and nothing failed at any earlier point: the reported cost was a
//! 14219-byte document replaced by 222 bytes nine seconds later, with no error
//! anywhere in between.
//!
//! The guard used to hang off the **live marker**, which made it both absent and
//! bypassable. #161 measured all four combinations; the two that clobbered
//! silently are the two a stateless request handler naturally produces:
//!
//! | sequence | before | now |
//! |---|---|---|
//! | `load_` only, no marker ever created | clobbers | refuses |
//! | marker held, checkpoint via `load_` | refuses | refuses |
//! | marker held, checkpoint via the held room | refuses | refuses |
//! | `open_` again before each checkpoint | clobbers (the re-open refreshes the marker) | refuses |
//!
//! So the coherence base lives on the **document** — the bytes it resumed from,
//! was seeded from, or last crystallized — where re-reading the file cannot
//! refresh it. The persisted sidecar is consulted as a second opinion, which is
//! what keeps a *second worker's* replica entitled to checkpoint after the first
//! one landed a body.
#![cfg(feature = "coedit")]

use origofs_sdk::{OrigoFSError, TreeSpan, Workspace, WriteCtx};

const ROOT: &str = "content";
const NO_SPANS: &[TreeSpan] = &[];

async fn ws() -> (Workspace, tempfile::TempDir) {
    let d = tempfile::tempdir().unwrap();
    let w = Workspace::open_local(d.path().join("meta.db"), d.path().join("cas"))
        .await
        .unwrap();
    (w, d)
}

/// #158, exactly as reported: write a real document, open a tree room over it,
/// checkpoint. The room never resumed, so its body is empty — and the file's
/// content must not go with it.
#[tokio::test]
async fn an_unseeded_room_may_not_blank_a_file_with_content() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    let body = b"# Title\n\nthe document nobody wants to lose\n";
    w.write_as(ctx, "/n.md", body).await.unwrap();

    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    assert!(!doc.resumed(), "there is no sidecar to resume from");
    assert!(doc.base_hash().is_none(), "and so nothing it agrees with");

    let r = w
        .checkpoint_coedit_tree(ctx, "/n.md", &doc, b"", NO_SPANS)
        .await;
    assert!(matches!(r, Err(OrigoFSError::ForeignWrite(_))), "{r:?}");
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), body);
    // The message has to name the way out, because the caller cannot work it out
    // from the API: origofs will never be able to seed this document itself.
    let msg = r.unwrap_err().to_string();
    assert!(msg.contains("seeded_from"), "{msg}");
}

/// ...and the way out works. A host that parses the file into the tree itself
/// declares that with `seeded_from`, and then owns the bytes.
#[tokio::test]
async fn seeding_the_document_from_the_file_unblocks_the_checkpoint() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    w.write_as(ctx, "/n.md", b"original\n").await.unwrap();

    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    let body = w.read("/n.md").await.unwrap();
    doc.append_text(ctx, "p", "original\n"); // the host's parser, in miniature
    doc.seeded_from(&body);

    w.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"original\nand more\n", NO_SPANS)
        .await
        .unwrap();
    assert_eq!(
        w.read("/n.md").await.unwrap().as_ref(),
        b"original\nand more\n"
    );
}

/// A brand-new file is not the guarded case: there is nothing to lose, so the
/// first checkpoint of an unseeded room lands. This is the ordinary "create a
/// document in the editor" path and the refusal must not have taken it.
#[tokio::test]
async fn an_unseeded_room_over_an_absent_or_empty_file_still_checkpoints() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());

    let doc = w.open_coedit_tree(ctx, "/new.md", ROOT).await.unwrap();
    w.checkpoint_coedit_tree(ctx, "/new.md", &doc, b"# fresh\n", NO_SPANS)
        .await
        .unwrap();
    assert_eq!(w.read("/new.md").await.unwrap().as_ref(), b"# fresh\n");

    w.write_as(ctx, "/empty.md", b"").await.unwrap();
    let doc = w.open_coedit_tree(ctx, "/empty.md", ROOT).await.unwrap();
    w.checkpoint_coedit_tree(ctx, "/empty.md", &doc, b"typed\n", NO_SPANS)
        .await
        .unwrap();
    assert_eq!(w.read("/empty.md").await.unwrap().as_ref(), b"typed\n");
}

/// Consecutive checkpoints from one room: the base advances with each, so the
/// second is not read as a foreign write of the first.
#[tokio::test]
async fn a_room_may_checkpoint_repeatedly() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    for body in [&b"one\n"[..], b"one\ntwo\n", b"one\ntwo\nthree\n"] {
        w.checkpoint_coedit_tree(ctx, "/n.md", &doc, body, NO_SPANS)
            .await
            .unwrap();
    }
    assert_eq!(
        w.read("/n.md").await.unwrap().as_ref(),
        b"one\ntwo\nthree\n"
    );
}

/// Row 1 of #161's table: a socket-less checkpoint, no live marker anywhere. The
/// old guard read the marker, found none, and concluded there was nothing to
/// protect.
#[tokio::test]
async fn a_socketless_checkpoint_is_guarded_even_with_no_live_marker() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    let bob = WriteCtx::actor(w.create_human("b", None).await.unwrap());

    // A room that legitimately owns the file.
    let doc = w.load_coedit_tree_as(ctx, "/n.md", ROOT).await.unwrap();
    w.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine\n", NO_SPANS)
        .await
        .unwrap();
    assert!(
        w.live_doc("/n.md").await.unwrap().is_none(),
        "`load_` must not claim the path (#161)"
    );

    // Somebody writes the file around it.
    w.write_as(bob, "/n.md", b"bob was here\n").await.unwrap();

    let r = w
        .checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine, edited\n", NO_SPANS)
        .await;
    assert!(matches!(r, Err(OrigoFSError::ForeignWrite(_))), "{r:?}");
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"bob was here\n");
}

/// Row 4, the sharp one: re-opening the room before each checkpoint used to
/// *refresh* the marker's `content_hash` from the file, so the guard compared the
/// foreign write against itself and passed. This is the natural shape of a
/// stateless request handler.
#[tokio::test]
async fn reopening_the_room_before_each_checkpoint_does_not_defeat_the_guard() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    let bob = WriteCtx::actor(w.create_human("b", None).await.unwrap());

    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    w.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine\n", NO_SPANS)
        .await
        .unwrap();
    w.write_as(bob, "/n.md", b"bob was here\n").await.unwrap();

    // The handler re-opens rather than holding the room across requests.
    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    assert!(!doc.resumed(), "the sidecar no longer matches the file");
    let r = w
        .checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine, edited\n", NO_SPANS)
        .await;
    assert!(matches!(r, Err(OrigoFSError::ForeignWrite(_))), "{r:?}");
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"bob was here\n");
}

/// A second replica of the same document — another worker holding the same room —
/// may still checkpoint after the first one landed a body. Its own base lags, but
/// the sidecar the first checkpoint wrote vouches for the file's current bytes,
/// and that is the cross-worker record a local base cannot know about.
#[tokio::test]
async fn a_second_replica_may_checkpoint_after_the_first_one_landed() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    w.write_as(ctx, "/n.md", b"seed\n").await.unwrap();

    let worker_a = w.load_coedit_tree_as(ctx, "/n.md", ROOT).await.unwrap();
    worker_a.seeded_from(b"seed\n");
    w.checkpoint_coedit_tree(ctx, "/n.md", &worker_a, b"seed\nfrom a\n", NO_SPANS)
        .await
        .unwrap();

    // Worker B resumes from the sidecar A just wrote and checkpoints its own
    // serialization — the file is exactly what A crystallized, so nothing was
    // written around either of them.
    let worker_b = w.load_coedit_tree_as(ctx, "/n.md", ROOT).await.unwrap();
    assert!(worker_b.resumed());
    w.checkpoint_coedit_tree(ctx, "/n.md", &worker_b, b"seed\nfrom a\nfrom b\n", NO_SPANS)
        .await
        .unwrap();
    assert_eq!(
        w.read("/n.md").await.unwrap().as_ref(),
        b"seed\nfrom a\nfrom b\n"
    );
}

/// The sweeper persists a room's CRDT state on a timer without landing a body.
/// It must not re-frame the sidecar against a **foreign write**, which would
/// launder that write into "coherent" and let the next open resume this document
/// and checkpoint straight over it.
#[tokio::test]
async fn persisting_does_not_launder_a_foreign_write_into_coherence() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    let bob = WriteCtx::actor(w.create_human("b", None).await.unwrap());

    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    w.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine\n", NO_SPANS)
        .await
        .unwrap();
    w.write_as(bob, "/n.md", b"bob was here\n").await.unwrap();

    w.persist_coedit_tree("/n.md", &doc).await.unwrap();

    // Both the held room and a freshly opened one still see the foreign write.
    let r = w
        .checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine, edited\n", NO_SPANS)
        .await;
    assert!(matches!(r, Err(OrigoFSError::ForeignWrite(_))), "{r:?}");
    let reopened = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    assert!(
        !reopened.resumed(),
        "a sweep must not make a moved file look resumable"
    );
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"bob was here\n");
}

/// The guard is about *content*, not about whether somebody else touched the
/// file: a foreign write that lands the bytes the document already agrees with is
/// not a conflict. Pinned because the ordinary case is answered from the file's
/// content address (one metadata lookup, as the old marker-scoped guard was)
/// rather than by re-hashing the body on every save, and an address comparison
/// that drifted from the hash would show up exactly here.
#[tokio::test]
async fn a_foreign_write_of_identical_bytes_is_not_a_conflict() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    let bob = WriteCtx::actor(w.create_human("b", None).await.unwrap());

    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    w.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine\n", NO_SPANS)
        .await
        .unwrap();
    w.write_as(bob, "/n.md", b"mine\n").await.unwrap();

    w.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"mine, edited\n", NO_SPANS)
        .await
        .unwrap();
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"mine, edited\n");
}

/// A sweep must not hand an **unseeded** document a coherence claim it has not
/// earned. Framing its sidecar against the file's current bytes would have made
/// the very next open resume it and checkpoint the empty tree over the content —
/// re-arming #158 through the timer rather than through the caller.
#[tokio::test]
async fn persisting_an_unseeded_room_does_not_arm_the_overwrite() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());
    w.write_as(ctx, "/n.md", b"content worth keeping\n")
        .await
        .unwrap();

    let doc = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    assert!(!doc.resumed());
    w.persist_coedit_tree("/n.md", &doc).await.unwrap(); // the sweeper's tick

    let reopened = w.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    assert!(!reopened.resumed(), "a sweep cannot invent a seed");
    let r = w
        .checkpoint_coedit_tree(ctx, "/n.md", &reopened, b"", NO_SPANS)
        .await;
    assert!(matches!(r, Err(OrigoFSError::ForeignWrite(_))), "{r:?}");
    assert_eq!(
        w.read("/n.md").await.unwrap().as_ref(),
        b"content worth keeping\n"
    );
}

/// The sweeper's own job still works: a room on a *new* file keeps its editing
/// history across a restart without any checkpoint having landed.
#[tokio::test]
async fn persisting_still_makes_a_new_rooms_history_resumable() {
    let (w, _d) = ws().await;
    let ctx = WriteCtx::actor(w.create_human("a", None).await.unwrap());

    let doc = w.open_coedit_tree(ctx, "/new.md", ROOT).await.unwrap();
    doc.append_text(ctx, "p", "typed but never checkpointed");
    w.persist_coedit_tree("/new.md", &doc).await.unwrap();

    let resumed = w.open_coedit_tree(ctx, "/new.md", ROOT).await.unwrap();
    assert!(resumed.resumed(), "a crash must not cost the typing");
    assert!(
        resumed
            .plain_text()
            .contains("typed but never checkpointed")
    );
}

/// The refusal is `ForeignWrite`, and a stale suggestion base is `StaleBase`
/// (#159). They arrive as the same `ConflictError` in Python and the same 409 on
/// HTTP, but they ask for opposite recoveries, so a caller must be able to tell
/// them apart without matching the message.
#[tokio::test]
async fn the_two_conflicts_are_distinguishable_without_reading_the_message() {
    let (w, _d) = ws().await;
    let author = WriteCtx::actor(w.create_agent("agent", "m", None).await.unwrap());
    let human = WriteCtx::actor(w.create_human("h", None).await.unwrap());

    w.write_as(human, "/n.md", b"base\n").await.unwrap();
    let id = w
        .suggest(author, "/n.md", b"proposed\n", None, None)
        .await
        .unwrap();
    w.write_as(human, "/n.md", b"moved on\n").await.unwrap();
    let stale = w.accept_suggestion(id, human).await.unwrap_err();
    assert_eq!(stale.code(), "stale_base");
    assert!(stale.is_conflict());

    let doc = w.open_coedit_tree(human, "/t.md", ROOT).await.unwrap();
    w.write_as(human, "/t.md", b"content\n").await.unwrap();
    let foreign = w
        .checkpoint_coedit_tree(human, "/t.md", &doc, b"x\n", NO_SPANS)
        .await
        .unwrap_err();
    assert_eq!(foreign.code(), "foreign_write");
    assert!(foreign.is_conflict());
}
