//! Sandbox end-to-end: run a real command over an isolated CoW view, then import
//! its delta (create / modify / delete) back into origofs with attribution.
//!
//! Self-skips where unprivileged overlayfs isn't available.

use origofs_sandbox::{overlay_supported, run, LiveSync, RunOpts};
use origofs_sdk::{ActorKind, Workspace};

async fn workspace(dir: &std::path::Path) -> Workspace {
    Workspace::open_local(dir.join("meta.db"), dir.join("cas"))
        .await
        .unwrap()
}

#[tokio::test]
async fn imports_delta_with_attribution() {
    if !overlay_supported() {
        eprintln!("skipping: unprivileged overlayfs unavailable");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    ws.write("/keep.txt", b"original\n").await.unwrap();
    ws.write("/gone.txt", b"delete me\n").await.unwrap();
    let agent = ws.create_agent("builder", "m", None).await.unwrap();

    // The sandboxed command modifies, creates, and deletes files.
    let cmd = vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        "echo modified >> keep.txt; echo created > new.txt; rm gone.txt".to_string(),
    ];
    let out = run(
        &ws,
        RunOpts {
            actor: Some(agent),
            discard: false,
            work_root: dir.path().join("sbx"),
            isolate: false,
        },
        &cmd,
    )
    .await
    .unwrap();
    assert_eq!(out.exit_code, 0);
    assert!(out.imported);

    // The delta landed in origofs.
    assert_eq!(
        &ws.read("/keep.txt").await.unwrap()[..],
        b"original\nmodified\n"
    );
    assert_eq!(&ws.read("/new.txt").await.unwrap()[..], b"created\n");
    assert!(ws.stat("/gone.txt").await.is_err(), "gone.txt was deleted");

    // The new file is attributed to the sandbox's agent.
    let blame = ws.blame("/new.txt").await.unwrap();
    assert_eq!(blame[0].actor.id, agent);
    assert_eq!(blame[0].actor.kind, ActorKind::Agent);
}

#[tokio::test]
async fn discard_leaves_workspace_untouched() {
    if !overlay_supported() {
        eprintln!("skipping: unprivileged overlayfs unavailable");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    ws.write("/f.txt", b"before\n").await.unwrap();

    let cmd = vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        "echo clobbered > f.txt; echo junk > extra.txt".to_string(),
    ];
    let out = run(
        &ws,
        RunOpts {
            actor: None,
            discard: true,
            work_root: dir.path().join("sbx"),
            isolate: false,
        },
        &cmd,
    )
    .await
    .unwrap();
    assert_eq!(out.exit_code, 0);
    assert!(!out.imported);

    // Nothing changed in origofs.
    assert_eq!(&ws.read("/f.txt").await.unwrap()[..], b"before\n");
    assert!(ws.stat("/extra.txt").await.is_err());
}

// --- live incremental sync (no overlay / root needed) -----------------------

/// `LiveSync` streams an overlay `upper/` delta into origofs, importing only what
/// changed since the last tick — new files, real edits, subdirs, symlinks — and
/// skipping unchanged files so an idle agent produces no churn (and no spurious
/// re-attribution). Deletions (whiteouts) need a real overlay, covered above.
#[tokio::test]
async fn live_sync_imports_only_changes() {
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    let agent = ws.create_agent("live", "m", None).await.unwrap();
    let session = ws.create_session(agent, Some("overlay")).await.unwrap();

    // A stand-in for an overlay upper/: the agent's scratch write layer.
    let upper = dir.path().join("upper");
    tokio::fs::create_dir_all(&upper).await.unwrap();
    let mut sync = LiveSync::new(Some(agent), Some(session));

    // 1) a new file is imported and attributed.
    tokio::fs::write(upper.join("a.txt"), b"one").await.unwrap();
    assert_eq!(sync.sync(&ws, &upper).await.unwrap(), 1);
    assert_eq!(&ws.read("/a.txt").await.unwrap()[..], b"one");
    assert_eq!(ws.blame("/a.txt").await.unwrap()[0].actor.id, agent);

    // 2) an idle tick imports nothing.
    assert_eq!(sync.sync(&ws, &upper).await.unwrap(), 0);

    // 3) an edit (size differs) is re-imported; unrelated files stay put.
    tokio::fs::write(upper.join("a.txt"), b"one-plus-more")
        .await
        .unwrap();
    assert_eq!(sync.sync(&ws, &upper).await.unwrap(), 1);
    assert_eq!(&ws.read("/a.txt").await.unwrap()[..], b"one-plus-more");

    // 4) a nested file and a symlink in one tick.
    tokio::fs::create_dir_all(upper.join("sub")).await.unwrap();
    tokio::fs::write(upper.join("sub/b.txt"), b"nested")
        .await
        .unwrap();
    std::os::unix::fs::symlink("a.txt", upper.join("link")).unwrap();
    assert_eq!(sync.sync(&ws, &upper).await.unwrap(), 2);
    assert_eq!(&ws.read("/sub/b.txt").await.unwrap()[..], b"nested");
    assert_eq!(ws.readlink("/link").await.unwrap(), "a.txt");

    // 5) steady state again: nothing to do.
    assert_eq!(sync.sync(&ws, &upper).await.unwrap(), 0);
}

/// `run_live` runs an agent in the native overlay and streams its changes into
/// origofs *during* the run (on the sync timer) and once at exit — creates, edits
/// (copy-up + append), and deletes (whiteout) all land, attributed to the agent.
#[tokio::test]
async fn run_live_streams_changes_to_origofs() {
    if !overlay_supported() {
        eprintln!("skipping: unprivileged overlayfs unavailable");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    ws.write("/keep.txt", b"original\n").await.unwrap();
    ws.write("/gone.txt", b"delete me\n").await.unwrap();
    let agent = ws.create_agent("live-builder", "m", None).await.unwrap();

    // Create early, wait past a sync tick, then edit an existing file and delete one.
    let cmd = vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        "echo created > new.txt; sleep 0.5; echo more >> keep.txt; rm gone.txt".to_string(),
    ];
    let out = origofs_sandbox::run_live(
        &ws,
        origofs_sandbox::LiveOpts {
            actor: Some(agent),
            work_root: dir.path().join("ovl"),
            sync_interval: std::time::Duration::from_millis(150),
            isolate: false,
        },
        &cmd,
    )
    .await
    .unwrap();
    assert_eq!(out.exit_code, 0);
    assert!(out.imported);

    // origofs reflects every change, attributed to the agent.
    assert_eq!(&ws.read("/new.txt").await.unwrap()[..], b"created\n");
    assert_eq!(
        &ws.read("/keep.txt").await.unwrap()[..],
        b"original\nmore\n"
    );
    assert!(
        ws.stat("/gone.txt").await.is_err(),
        "the deletion was synced"
    );
    assert_eq!(ws.blame("/new.txt").await.unwrap()[0].actor.id, agent);
}
