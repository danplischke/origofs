//! Two-branch comparison: the content-addressed changed-path list and the
//! per-file unified line diff a multi-branch UI needs.

use origofs_sdk::{DiffStatus, Workspace};

async fn workspace() -> (Workspace, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    (ws, dir)
}

/// Set up `main` with three files, branch off, then on `feature` modify one,
/// add one, delete one — the three diff statuses.
async fn seeded() -> (Workspace, tempfile::TempDir) {
    let (ws, dir) = workspace().await;
    ws.write("/keep.txt", b"unchanged\n").await.unwrap();
    ws.write("/edit.txt", b"line one\nline two\n")
        .await
        .unwrap();
    ws.write("/gone.txt", b"delete me\n").await.unwrap();
    ws.commit("main", "base").await.unwrap();

    ws.create_branch("feature").await.unwrap();
    ws.checkout("feature").await.unwrap();
    ws.write("/edit.txt", b"line one\nline TWO changed\n")
        .await
        .unwrap();
    ws.write("/new.txt", b"brand new\n").await.unwrap();
    ws.remove("/gone.txt").await.unwrap();
    ws.commit("dev", "feature work").await.unwrap();
    (ws, dir)
}

#[tokio::test]
async fn diff_lists_changed_paths_by_status() {
    let (ws, _dir) = seeded().await;

    let changes = ws.diff("main", "feature").await.unwrap();
    let by_path: std::collections::HashMap<_, _> = changes
        .iter()
        .map(|d| (d.path.as_str(), d.status))
        .collect();

    assert_eq!(by_path.get("/edit.txt"), Some(&DiffStatus::Modified));
    assert_eq!(by_path.get("/new.txt"), Some(&DiffStatus::Added));
    assert_eq!(by_path.get("/gone.txt"), Some(&DiffStatus::Deleted));
    // an unchanged file (equal content address) is not reported
    assert!(!by_path.contains_key("/keep.txt"));
    assert_eq!(changes.len(), 3);

    // direction matters: from feature -> main, add/delete invert
    let rev = ws.diff("feature", "main").await.unwrap();
    let rev_by: std::collections::HashMap<_, _> =
        rev.iter().map(|d| (d.path.as_str(), d.status)).collect();
    assert_eq!(rev_by.get("/new.txt"), Some(&DiffStatus::Deleted));
    assert_eq!(rev_by.get("/gone.txt"), Some(&DiffStatus::Added));
}

#[tokio::test]
async fn diff_file_gives_a_unified_patch() {
    let (ws, _dir) = seeded().await;

    let patch = ws.diff_file("main", "feature", "/edit.txt").await.unwrap();
    assert!(
        patch.contains("-line two"),
        "patch shows the removed line:\n{patch}"
    );
    assert!(
        patch.contains("+line TWO changed"),
        "patch shows the added line:\n{patch}"
    );

    // an unchanged file diffs to nothing (and costs no content read)
    assert!(
        ws.diff_file("main", "feature", "/keep.txt")
            .await
            .unwrap()
            .is_empty()
    );

    // an added file shows its whole body as additions
    let added = ws.diff_file("main", "feature", "/new.txt").await.unwrap();
    assert!(added.contains("+brand new"));
}

#[tokio::test]
async fn diff_resolves_branches_and_commit_hashes() {
    let (ws, _dir) = seeded().await;
    let branches = ws.list_branches().await.unwrap();
    let main_hash = branches
        .iter()
        .find(|(n, _)| n == "main")
        .map(|(_, h)| h.to_hex())
        .unwrap();

    // a raw commit hex resolves the same as the branch name
    let by_name = ws.diff("main", "feature").await.unwrap();
    let by_hash = ws.diff(&main_hash, "feature").await.unwrap();
    assert_eq!(by_name.len(), by_hash.len());

    // an unknown ref is an error, not a panic
    assert!(ws.diff("main", "nope").await.is_err());
}
