//! A git export of a workspace with a **live co-editing document** (issue #75 §3.4).
//!
//! The exported bytes for a live path are its last checkpoint: real, fully
//! attributed, and possibly behind the `Y.Doc` somebody has open. Exporting that
//! silently is the bug — a snapshot people will read as the truth, with nothing
//! saying it might not be. So the export *surfaces* it (a warning + the paths on
//! the result) and otherwise carries on: it does not block, does not fail, and does
//! not force a checkpoint, exactly as `Fs::read_live` documents for every byte
//! reader.
#![cfg(all(feature = "git", feature = "coedit"))]

use origofs_sdk::Workspace;
use origofs_sdk::git::{ExportOptions, export_git};

#[tokio::test]
async fn export_reports_live_paths_and_still_exports() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(tmp.path().join("meta.db"), tmp.path().join("cas"))
        .await
        .unwrap();
    let dan = ws.create_human("dan", None).await.unwrap();
    let sess = ws.create_session(dan, Some("editor")).await.unwrap();
    let ctx = origofs_sdk::WriteCtx::session(dan, sess);

    ws.write_as(ctx, "/quiet.md", b"settled\n").await.unwrap();

    // A co-edited document, checkpointed and committed — so the commit tree the
    // export walks holds the checkpointed bytes.
    let doc = ws.open_coedit(ctx, "/notes.md").await.unwrap();
    doc.insert(ctx, 0, "checkpointed\n");
    ws.checkpoint_coedit(ctx, "/notes.md", &doc).await.unwrap();
    ws.commit("Dan <dan@example.com>", "first").await.unwrap();

    // Still open: the marker is set, so the export must say so.
    assert!(ws.live_doc("/notes.md").await.unwrap().is_some());

    let repo = tmp.path().join("exported");
    let export = export_git(&ws, &repo, &ExportOptions::default())
        .await
        .unwrap();

    assert_eq!(
        export.live_paths,
        vec!["/notes.md".to_string()],
        "the live path must be reported, and only the live one"
    );
    // Surfacing staleness never costs the export: it completed normally.
    assert_eq!(export.commits, 1);
    assert!(repo.join(".git").join("HEAD").exists());

    // Once the session ends, an export of the same tree is clean again — the
    // report tracks the marker, not the file.
    ws.end_coedit("/notes.md").await.unwrap();
    let repo2 = tmp.path().join("exported2");
    let again = export_git(&ws, &repo2, &ExportOptions::default())
        .await
        .unwrap();
    assert!(again.live_paths.is_empty(), "{:?}", again.live_paths);
    assert_eq!(again.head, export.head, "the objects are unchanged");
}

#[tokio::test]
async fn a_workspace_with_no_live_documents_reports_none() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(tmp.path().join("meta.db"), tmp.path().join("cas"))
        .await
        .unwrap();
    ws.write("/readme.md", b"# hi\n").await.unwrap();
    ws.commit("Dan <dan@example.com>", "first").await.unwrap();

    let export = export_git(&ws, &tmp.path().join("exported"), &ExportOptions::default())
        .await
        .unwrap();
    assert!(export.live_paths.is_empty());
}
