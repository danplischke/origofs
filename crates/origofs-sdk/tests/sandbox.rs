//! Sandbox end-to-end: run a real command over an isolated CoW view, then import
//! its delta (create / modify / delete) back into origofs with attribution.
//!
//! Self-skips where unprivileged overlayfs isn't available.
#![cfg(feature = "sandbox")]

use origofs_sdk::sandbox::{
    LiveSync, RunOpts, import_upper, is_opaque_dir, overlay_supported, run,
};
use origofs_sdk::{ActorKind, Workspace};

async fn workspace(dir: &std::path::Path) -> Workspace {
    Workspace::open_local(dir.join("meta.db"), dir.join("cas"))
        .await
        .unwrap()
}

/// Mark a host directory the way overlayfs marks an **opaque** directory ("this
/// upper dir replaces the lower one"). Uses the `user.` name because `trusted.`
/// needs CAP_SYS_ADMIN; the import honors both.
///
/// Returns `false` when the filesystem under the temp dir has no user xattrs (or
/// we're not on Linux), so the test can self-skip the way the Postgres-backed
/// tests do without `ORIGOFS_PG_TEST_URL`. Support is probed with the test's own
/// `lgetxattr` read-back rather than through [`is_opaque_dir`], so a regression in
/// the detection under test fails the test instead of silently skipping it.
#[cfg(target_os = "linux")]
fn mark_opaque(path: &std::path::Path) -> bool {
    use std::ffi::{CString, c_char, c_int, c_void};
    use std::os::unix::ffi::OsStrExt;

    unsafe extern "C" {
        fn lsetxattr(
            path: *const c_char,
            name: *const c_char,
            value: *const c_void,
            size: usize,
            flags: c_int,
        ) -> c_int;
        fn lgetxattr(
            path: *const c_char,
            name: *const c_char,
            value: *mut c_void,
            size: usize,
        ) -> isize;
    }

    let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let c_name = CString::new("user.overlay.opaque").unwrap();
    // SAFETY: both strings are NUL-terminated and outlive the call; `value` points
    // at exactly `size` readable bytes.
    let rc = unsafe {
        lsetxattr(
            c_path.as_ptr(),
            c_name.as_ptr(),
            b"y".as_ptr().cast::<c_void>(),
            1,
            0,
        )
    };
    if rc != 0 {
        return false; // no user xattrs here (EOPNOTSUPP on some filesystems)
    }
    // Read it back independently: a filesystem that accepts the set but can't
    // return the value is no use to us either.
    let mut buf = [0u8; 8];
    // SAFETY: `buf` is a live allocation of exactly `buf.len()` writable bytes.
    let n = unsafe {
        lgetxattr(
            c_path.as_ptr(),
            c_name.as_ptr(),
            buf.as_mut_ptr().cast::<c_void>(),
            buf.len(),
        )
    };
    n == 1 && buf[0] == b'y'
}

#[cfg(not(target_os = "linux"))]
fn mark_opaque(_path: &std::path::Path) -> bool {
    false
}

async fn names(ws: &Workspace, dir: &str) -> Vec<String> {
    let mut n: Vec<String> = ws
        .ls(dir)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    n.sort();
    n
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

// --- opaque directories (no overlay mount / root needed) --------------------
//
// overlayfs records a deletion two ways: a character-device whiteout per removed
// name, and an *opaque directory* — an upper dir carrying `…overlay.opaque=y`,
// meaning "this replaces the lower dir; ignore every lower entry under it" (what
// the kernel writes for e.g. `rm -rf d && mkdir d`). The import must prune, or
// the deleted children silently reappear. A real overlay mount needs privileges,
// so these drive the import against a hand-built `upper/` tree.

/// The marker itself is detected — and only the marker.
#[test]
fn is_opaque_dir_reads_the_overlay_marker() {
    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("plain");
    let marked = dir.path().join("marked");
    std::fs::create_dir(&plain).unwrap();
    std::fs::create_dir(&marked).unwrap();
    assert!(!is_opaque_dir(&plain), "an unmarked dir is not opaque");
    if !mark_opaque(&marked) {
        eprintln!(
            "skipping: no user xattr support on this filesystem (needed to mark an opaque dir)"
        );
        return;
    }
    assert!(is_opaque_dir(&marked), "user.overlay.opaque=y is opaque");
    assert!(!is_opaque_dir(&plain));
}

/// An opaque upper dir *replaces* the workspace dir: children it doesn't list are
/// deleted, recursively — files and subtrees alike.
#[tokio::test]
async fn opaque_upper_dir_replaces_workspace_dir() {
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    let agent = ws.create_agent("opaque", "m", None).await.unwrap();
    let session = ws.create_session(agent, Some("sandbox")).await.unwrap();

    ws.mkdir_p("/d/sub").await.unwrap();
    ws.write("/d/a.txt", b"old-a\n").await.unwrap();
    ws.write("/d/b.txt", b"b\n").await.unwrap();
    ws.write("/d/c.txt", b"c\n").await.unwrap();
    ws.write("/d/sub/deep.txt", b"deep\n").await.unwrap();
    ws.write("/outside.txt", b"untouched\n").await.unwrap();

    // The write layer: `d/` was replaced, and only `a.txt` survives in it.
    let upper = dir.path().join("upper");
    let upper_d = upper.join("d");
    tokio::fs::create_dir_all(&upper_d).await.unwrap();
    tokio::fs::write(upper_d.join("a.txt"), b"new-a\n")
        .await
        .unwrap();
    if !mark_opaque(&upper_d) {
        eprintln!(
            "skipping: no user xattr support on this filesystem (needed to mark an opaque dir)"
        );
        return;
    }

    let n = import_upper(&ws, &upper, Some(agent), Some(session))
        .await
        .unwrap();
    assert!(n >= 4, "3 pruned children + 1 import, got {n}");

    // Only what the opaque dir listed survives.
    assert_eq!(names(&ws, "/d").await, vec!["a.txt".to_string()]);
    assert_eq!(&ws.read("/d/a.txt").await.unwrap()[..], b"new-a\n");
    assert!(
        ws.stat("/d/b.txt").await.is_err(),
        "b.txt was replaced away"
    );
    assert!(
        ws.stat("/d/c.txt").await.is_err(),
        "c.txt was replaced away"
    );
    assert!(ws.stat("/d/sub").await.is_err(), "the subtree went with it");
    assert!(ws.stat("/d/sub/deep.txt").await.is_err());
    // Nothing outside the opaque dir is touched.
    assert_eq!(&ws.read("/outside.txt").await.unwrap()[..], b"untouched\n");

    // The surviving file is attributed to the sandbox's agent.
    assert_eq!(ws.blame("/d/a.txt").await.unwrap()[0].actor.id, agent);
}

/// Control: the same upper dir *without* the marker merges, as before — the
/// workspace children the agent didn't touch stay put.
#[tokio::test]
async fn non_opaque_upper_dir_still_merges() {
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;

    ws.mkdir_p("/d").await.unwrap();
    ws.write("/d/a.txt", b"old-a\n").await.unwrap();
    ws.write("/d/b.txt", b"b\n").await.unwrap();
    ws.write("/d/c.txt", b"c\n").await.unwrap();

    let upper = dir.path().join("upper");
    let upper_d = upper.join("d");
    tokio::fs::create_dir_all(&upper_d).await.unwrap();
    tokio::fs::write(upper_d.join("a.txt"), b"new-a\n")
        .await
        .unwrap();

    import_upper(&ws, &upper, None, None).await.unwrap();

    assert_eq!(
        names(&ws, "/d").await,
        vec![
            "a.txt".to_string(),
            "b.txt".to_string(),
            "c.txt".to_string()
        ]
    );
    assert_eq!(&ws.read("/d/a.txt").await.unwrap()[..], b"new-a\n");
    assert_eq!(&ws.read("/d/b.txt").await.unwrap()[..], b"b\n");
}

/// The live overlay sync honors the same marker — and applies it once, so a later
/// tick doesn't keep re-pruning the directory.
#[tokio::test]
async fn live_sync_applies_opaque_once() {
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    let agent = ws.create_agent("live-opaque", "m", None).await.unwrap();
    let session = ws.create_session(agent, Some("overlay")).await.unwrap();

    ws.mkdir_p("/d").await.unwrap();
    ws.write("/d/a.txt", b"old-a\n").await.unwrap();
    ws.write("/d/b.txt", b"b\n").await.unwrap();

    let upper = dir.path().join("upper");
    let upper_d = upper.join("d");
    tokio::fs::create_dir_all(&upper_d).await.unwrap();
    tokio::fs::write(upper_d.join("a.txt"), b"new-a\n")
        .await
        .unwrap();
    if !mark_opaque(&upper_d) {
        eprintln!(
            "skipping: no user xattr support on this filesystem (needed to mark an opaque dir)"
        );
        return;
    }

    let mut sync = LiveSync::new(Some(agent), Some(session));
    assert_eq!(sync.sync(&ws, &upper).await.unwrap(), 2); // 1 pruned + 1 imported
    assert_eq!(names(&ws, "/d").await, vec!["a.txt".to_string()]);

    // A file another writer adds afterwards isn't re-pruned on the next tick: the
    // replacement is a one-time event, not a standing rule.
    ws.write("/d/other.txt", b"from elsewhere\n").await.unwrap();
    assert_eq!(sync.sync(&ws, &upper).await.unwrap(), 0);
    assert_eq!(
        names(&ws, "/d").await,
        vec!["a.txt".to_string(), "other.txt".to_string()]
    );
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
    let out = origofs_sdk::sandbox::run_live(
        &ws,
        origofs_sdk::sandbox::LiveOpts {
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

/// A sandboxed deletion must be attributed, not just applied.
///
/// Only file *writes* went through `write_as`; deletions, directory creation, and
/// symlinks imported through the unattributed engine methods. So
/// `origofs sandbox --actor N -- rm -rf src/` recorded nothing about who removed
/// the tree — no blame, no edit-op, no audit row — in a system whose whole premise
/// is that every change is attributable. `imports_delta_with_attribution` above
/// asserts the file is *gone*, never who removed it, which is why this went
/// unnoticed.
#[tokio::test]
async fn a_sandboxed_deletion_is_attributed() {
    if !overlay_supported() {
        eprintln!("skipping: unprivileged overlayfs unavailable");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    ws.write("/doomed.txt", b"bye\n").await.unwrap();
    let agent = ws.create_agent("deleter", "m", None).await.unwrap();

    let out = run(
        &ws,
        RunOpts {
            actor: Some(agent),
            discard: false,
            work_root: dir.path().join("sbx"),
            isolate: false,
        },
        &[
            "/bin/sh".to_string(),
            "-c".to_string(),
            "rm doomed.txt".into(),
        ],
    )
    .await
    .unwrap();
    assert_eq!(out.exit_code, 0);
    assert!(out.imported);
    assert!(
        ws.stat("/doomed.txt").await.is_err(),
        "the file is still there"
    );

    // The deletion is on the record, against this agent.
    let ops = ws.edit_ops(agent, None).await.unwrap();
    assert!(
        ops.iter().any(|o| o.path == "/doomed.txt"),
        "the sandboxed deletion of /doomed.txt was imported with no edit-op, so \
         nothing records who removed it. Recorded ops: {:?}",
        ops.iter().map(|o| (&o.path, &o.op)).collect::<Vec<_>>()
    );
}

/// A sandboxed directory creation is attributed too.
#[tokio::test]
async fn a_sandboxed_mkdir_is_attributed() {
    if !overlay_supported() {
        eprintln!("skipping: unprivileged overlayfs unavailable");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ws = workspace(dir.path()).await;
    let agent = ws.create_agent("maker", "m", None).await.unwrap();

    let out = run(
        &ws,
        RunOpts {
            actor: Some(agent),
            discard: false,
            work_root: dir.path().join("sbx"),
            isolate: false,
        },
        &[
            "/bin/sh".to_string(),
            "-c".to_string(),
            "mkdir -p sub/deeper; echo hi > sub/deeper/f.txt".into(),
        ],
    )
    .await
    .unwrap();
    assert_eq!(out.exit_code, 0);

    assert_eq!(&ws.read("/sub/deeper/f.txt").await.unwrap()[..], b"hi\n");
    let ops = ws.edit_ops(agent, None).await.unwrap();
    assert!(
        ops.iter().any(|o| o.path == "/sub/deeper/f.txt"),
        "the write was not attributed: {:?}",
        ops.iter().map(|o| &o.path).collect::<Vec<_>>()
    );
}
