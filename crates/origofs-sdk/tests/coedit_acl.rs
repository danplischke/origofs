//! The co-editing surface takes the same path-scoped ACL check every other
//! attributed mutation takes (#123).
//!
//! Before this, co-editing was the way around the ACL: `write_or_propose` refused
//! an actor with no write right at a path, and the same actor opened that path as
//! a co-edited document and landed the identical bytes through
//! `checkpoint_coedit` — which reaches `write_as_blamed`, exempt by construction
//! because it is the CRDT coordinator's own write path. Both document shapes had
//! it, and the HTTP socket exposed it to any *authenticated* caller, since the
//! upgrade checked identity but never permission.
#![cfg(feature = "coedit")]

use origofs_sdk::{CoeditDoc, OrigoFSError, Perms, TreeSpan, Workspace, WriteCtx};

async fn workspace() -> (Workspace, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    (ws, dir)
}

fn is_denied(e: &OrigoFSError) -> bool {
    matches!(e, OrigoFSError::Denied(_))
}

/// A read-only actor cannot open, or check point, a flat co-edited document —
/// and the file it was refused on is untouched.
#[tokio::test]
async fn coedit_refuses_an_actor_without_write_at_the_path() {
    let (ws, _dir) = workspace().await;
    let owner = ws.create_human("owner", None).await.unwrap();
    let bob = ws.create_human("bob", None).await.unwrap();
    let bs = ws.create_session(bob, Some("web")).await.unwrap();
    let ctx = WriteCtx::session(bob, bs);

    ws.write_as(WriteCtx::actor(owner), "/doc", b"owned\n")
        .await
        .unwrap();
    ws.grant(bob, "/", Perms::READ, Some(owner)).await.unwrap();

    // The baseline: the ordinary write path refuses him.
    let direct = ws.write_or_propose(ctx, "/doc", b"bob\n", None, None).await;
    assert!(is_denied(&direct.unwrap_err()));

    // So must co-editing, at the door rather than after a session of typing.
    match ws.open_coedit(ctx, "/doc").await {
        Err(e) => assert!(is_denied(&e), "expected Denied, got {e:?}"),
        Ok(_) => panic!("open_coedit must take the same check as write_or_propose"),
    }

    // And a caller holding a document from elsewhere is refused at the checkpoint,
    // which is the call that actually reaches the working tree.
    let smuggled = CoeditDoc::new();
    smuggled.insert(ctx, 0, "bob was here\n");
    let landed = ws.checkpoint_coedit(ctx, "/doc", &smuggled).await;
    assert!(is_denied(&landed.unwrap_err()));

    assert_eq!(&ws.read("/doc").await.unwrap()[..], b"owned\n");
}

/// The tree shape has its own open/checkpoint pair and needs the same gate — more
/// so, since `checkpoint_coedit_tree` replaces the whole body with the host's.
#[tokio::test]
async fn coedit_tree_refuses_an_actor_without_write_at_the_path() {
    let (ws, _dir) = workspace().await;
    let owner = ws.create_human("owner", None).await.unwrap();
    let bob = ws.create_human("bob", None).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    ws.write_as(WriteCtx::actor(owner), "/tree", b"owned\n")
        .await
        .unwrap();
    ws.grant(bob, "/", Perms::READ, Some(owner)).await.unwrap();

    match ws.open_coedit_tree(ctx, "/tree", "default").await {
        Err(e) => assert!(is_denied(&e), "expected Denied, got {e:?}"),
        Ok(_) => panic!("open_coedit_tree must refuse an actor without write"),
    }

    // The owner opens it legitimately; bob still cannot land bytes through it.
    let doc = ws
        .open_coedit_tree(WriteCtx::actor(owner), "/tree", "default")
        .await
        .unwrap();
    let landed = ws
        .checkpoint_coedit_tree(ctx, "/tree", &doc, b"bob tree\n", &[] as &[TreeSpan])
        .await;
    assert!(is_denied(&landed.unwrap_err()));

    assert_eq!(&ws.read("/tree").await.unwrap()[..], b"owned\n");
}

/// A grant that allows neither write nor propose stops suggestions too — every
/// shape of them, including the CRDT one, which shares `record_suggestion`.
#[tokio::test]
async fn suggestions_require_the_propose_right() {
    let (ws, _dir) = workspace().await;
    let owner = ws.create_human("owner", None).await.unwrap();
    let bob = ws.create_human("bob", None).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    ws.write_as(WriteCtx::actor(owner), "/doc", b"owned\n")
        .await
        .unwrap();
    ws.grant(bob, "/", Perms::READ, Some(owner)).await.unwrap();

    assert!(is_denied(
        &ws.suggest(ctx, "/doc", b"proposed\n", None, None)
            .await
            .unwrap_err()
    ));
    assert!(is_denied(
        &ws.suggest_delete(ctx, "/doc", None, None)
            .await
            .unwrap_err()
    ));

    let d = CoeditDoc::new();
    d.insert(ctx, 0, "proposed\n");
    assert!(is_denied(
        &ws.suggest_coedit(ctx, "/doc", &d, None).await.unwrap_err()
    ));

    assert!(ws.list_suggestions(None, None).await.unwrap().is_empty());
}

/// The propose right still reaches the queue: gating `record_suggestion` must not
/// break the propose-only actor the queue exists for.
#[tokio::test]
async fn a_propose_only_actor_can_still_suggest() {
    let (ws, _dir) = workspace().await;
    let owner = ws.create_human("owner", None).await.unwrap();
    let bob = ws.create_human("bob", None).await.unwrap();
    let ctx = WriteCtx::actor(bob);

    ws.write_as(WriteCtx::actor(owner), "/doc", b"owned\n")
        .await
        .unwrap();
    ws.grant(bob, "/", Perms::PROPOSE, Some(owner))
        .await
        .unwrap();

    assert!(
        ws.suggest(ctx, "/doc", b"proposed\n", None, None)
            .await
            .is_ok()
    );
    assert_eq!(ws.list_suggestions(None, None).await.unwrap().len(), 1);
}

/// An actor with write rights is unaffected — the ordinary co-editing flow still
/// works end to end.
#[tokio::test]
async fn a_writer_co_edits_normally() {
    let (ws, _dir) = workspace().await;
    let alice = ws.create_human("alice", None).await.unwrap();
    let ctx = WriteCtx::actor(alice);
    ws.grant(alice, "/", Perms::WRITE, Some(alice))
        .await
        .unwrap();

    let doc = ws.open_coedit(ctx, "/doc").await.unwrap();
    doc.insert(ctx, 0, "alice wrote this\n");
    ws.checkpoint_coedit(ctx, "/doc", &doc).await.unwrap();
    assert_eq!(&ws.read("/doc").await.unwrap()[..], b"alice wrote this\n");
}
