//! A checkpoint must never overwrite a file that changed underneath it.
//!
//! The flat shape's `reconcile_out_of_band` folds an out-of-band write into the
//! document before landing it — but every arm that *could not* fold returned
//! `Ok(())`, and the caller read "I could not reconcile" as "there was nothing to
//! reconcile" and overwrote the file anyway. The tree shape has always refused
//! (`refuse_out_of_band`); these pin the flat shape to the same line.
//!
//! A branch checkout is the case that makes it concrete, and it is the one a
//! branching app hits: `checkout` rematerializes the file *and* swaps away the
//! CRDT sidecar (it lives in the working tree), while the live marker is metadata
//! and survives — so the one input reconciliation needs is missing exactly when it
//! is needed.
#![cfg(feature = "coedit")]

use origofs_sdk::{OrigoFSError, TreeSpan, Workspace, WriteCtx};

async fn ws() -> (Workspace, tempfile::TempDir) {
    let d = tempfile::tempdir().unwrap();
    let w = Workspace::open_local(d.path().join("meta.db"), d.path().join("cas"))
        .await
        .unwrap();
    (w, d)
}

/// Two branches, a room opened on one of them, then a checkout. The room's next
/// checkpoint must not write the old branch's content onto the new branch.
#[tokio::test]
async fn a_checkout_does_not_let_a_room_clobber_the_other_branch() {
    let (w, _d) = ws().await;
    let a = w.create_human("a", None).await.unwrap();
    let ctx = WriteCtx::actor(a);

    w.write_as(ctx, "/n.md", b"main content\n").await.unwrap();
    w.commit_as(ctx, "a", "base").await.unwrap();
    w.create_branch_as(ctx, "feature").await.unwrap();
    w.checkout_as(ctx, "feature").await.unwrap();
    w.write_as(ctx, "/n.md", b"feature content\n")
        .await
        .unwrap();
    w.commit_as(ctx, "a", "on feature").await.unwrap();

    // Somebody is live-editing the file on `feature`.
    let doc = w.open_coedit(ctx, "/n.md").await.unwrap();
    doc.insert(ctx, 0, "LIVE EDIT\n");

    // The app switches branches out from under the room.
    w.checkout_as(ctx, "main").await.unwrap();
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"main content\n");

    // The room checkpoints — on its idle timer, or on the last socket leaving.
    let r = w.checkpoint_coedit(ctx, "/n.md", &doc).await;
    assert!(
        matches!(r, Err(OrigoFSError::Conflict(_))),
        "a room opened on another branch must not land here: {r:?}"
    );
    assert_eq!(
        w.read("/n.md").await.unwrap().as_ref(),
        b"main content\n",
        "main's content must survive"
    );
}

/// The same guard with a sidecar written first — the checkout swaps it away, which
/// is *why* reconciliation cannot complete.
#[tokio::test]
async fn a_checkout_refuses_even_after_the_room_checkpointed_once() {
    let (w, _d) = ws().await;
    let a = w.create_human("a", None).await.unwrap();
    let ctx = WriteCtx::actor(a);

    w.write_as(ctx, "/n.md", b"main content\n").await.unwrap();
    w.commit_as(ctx, "a", "base").await.unwrap();
    w.create_branch_as(ctx, "feature").await.unwrap();
    w.checkout_as(ctx, "feature").await.unwrap();
    w.write_as(ctx, "/n.md", b"feature content\n")
        .await
        .unwrap();
    w.commit_as(ctx, "a", "on feature").await.unwrap();

    let doc = w.open_coedit(ctx, "/n.md").await.unwrap();
    doc.insert(ctx, 0, "LIVE\n");
    w.checkpoint_coedit(ctx, "/n.md", &doc).await.unwrap();
    w.commit_as(ctx, "a", "live work").await.unwrap();

    w.checkout_as(ctx, "main").await.unwrap();
    doc.insert(ctx, 0, "MORE\n");
    let r = w.checkpoint_coedit(ctx, "/n.md", &doc).await;
    assert!(matches!(r, Err(OrigoFSError::Conflict(_))), "{r:?}");
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"main content\n");
}

/// The reconcilable case still reconciles: an ordinary out-of-band write, sidecar
/// intact. This is the behaviour the refusal must not have replaced.
#[tokio::test]
async fn an_ordinary_out_of_band_write_is_still_folded_in() {
    let (w, _d) = ws().await;
    let a = w.create_human("a", None).await.unwrap();
    let b = w.create_human("b", None).await.unwrap();
    let ctx = WriteCtx::actor(a);

    w.write_as(ctx, "/n.md", b"line one\n").await.unwrap();
    let doc = w.open_coedit(ctx, "/n.md").await.unwrap();
    w.checkpoint_coedit(ctx, "/n.md", &doc).await.unwrap();

    // Somebody writes the file directly while the room is open.
    w.write_as(WriteCtx::actor(b), "/n.md", b"line one\nfrom bob\n")
        .await
        .unwrap();
    // The room types and checkpoints: bob's line must survive, not 409.
    doc.insert(ctx, 0, "from the room\n");
    w.checkpoint_coedit(ctx, "/n.md", &doc).await.unwrap();

    let text = String::from_utf8(w.read("/n.md").await.unwrap().to_vec()).unwrap();
    assert!(text.contains("from bob"), "bob's write was lost: {text:?}");
    assert!(text.contains("from the room"), "{text:?}");
}

/// A checkpoint that follows the document straight through — the ordinary path —
/// is untouched by the guard.
#[tokio::test]
async fn the_ordinary_checkpoint_path_still_works() {
    let (w, _d) = ws().await;
    let a = w.create_human("a", None).await.unwrap();
    let ctx = WriteCtx::actor(a);
    let doc = w.open_coedit(ctx, "/n.md").await.unwrap();
    doc.insert(ctx, 0, "hello\n");
    w.checkpoint_coedit(ctx, "/n.md", &doc).await.unwrap();
    doc.insert(ctx, 6, "world\n");
    w.checkpoint_coedit(ctx, "/n.md", &doc).await.unwrap();
    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"hello\nworld\n");
}

/// A file deleted out of band is not resurrected by a stale room.
#[tokio::test]
async fn a_removed_file_is_not_resurrected_by_a_stale_room() {
    let (w, _d) = ws().await;
    let a = w.create_human("a", None).await.unwrap();
    let ctx = WriteCtx::actor(a);
    w.write_as(ctx, "/n.md", b"here\n").await.unwrap();
    let doc = w.open_coedit(ctx, "/n.md").await.unwrap();
    w.checkpoint_coedit(ctx, "/n.md", &doc).await.unwrap();

    w.remove_or_propose(ctx, "/n.md", None).await.unwrap();
    doc.insert(ctx, 0, "typed\n");
    let r = w.checkpoint_coedit(ctx, "/n.md", &doc).await;
    assert!(matches!(r, Err(OrigoFSError::Conflict(_))), "{r:?}");
}

/// A socket-less tree checkpoint — a "Save" with no editor attached — must not
/// leave the path marked live. It used to open a real session, whose matching
/// clear lives on a disconnect path this flow never reaches.
#[tokio::test]
async fn a_socketless_tree_checkpoint_leaves_no_live_marker() {
    let (w, _d) = ws().await;
    let a = w.create_human("a", None).await.unwrap();
    let ctx = WriteCtx::actor(a);

    let doc = w
        .load_coedit_tree_as(ctx, "/n.md", "content")
        .await
        .unwrap();
    w.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"# hi\n", &[] as &[TreeSpan])
        .await
        .unwrap();

    assert_eq!(w.read("/n.md").await.unwrap().as_ref(), b"# hi\n");
    assert!(
        w.live_doc("/n.md").await.unwrap().is_none(),
        "a checkpoint with no socket attached must not claim the path"
    );
    assert!(w.live_paths().await.unwrap().is_empty());
}

/// And it still takes the write check.
#[tokio::test]
async fn the_socketless_tree_loader_still_requires_write() {
    let (w, _d) = ws().await;
    let owner = w.create_human("owner", None).await.unwrap();
    let bob = w.create_human("bob", None).await.unwrap();
    w.grant(bob, "/", origofs_sdk::Perms::READ, Some(owner))
        .await
        .unwrap();
    let r = w
        .load_coedit_tree_as(WriteCtx::actor(bob), "/n.md", "content")
        .await;
    assert!(matches!(r, Err(OrigoFSError::Denied(_))), "{:?}", r.is_ok());
}
