//! Disaster recovery: rebuild a workspace's metadata (refs + working tree) from
//! the surviving content store after the metadata DB is lost. The content store
//! holds a self-describing object graph plus a ref mirror, so a fresh DB pointed
//! at it can bootstrap without any DB backup.

use origofs_sdk::Workspace;
use tempfile::tempdir;

#[tokio::test]
async fn rebuild_recovers_files_and_branch_names_from_content() {
    let dir = tempdir().unwrap();
    let cas = dir.path().join("cas");

    // Build a workspace: two files (one nested), a commit, and a second branch.
    let ws = Workspace::open_local(dir.path().join("meta.db"), &cas)
        .await
        .unwrap();
    ws.mkdir_p("/src").await.unwrap();
    ws.write("/README.md", b"hello").await.unwrap();
    ws.write("/src/app.txt", b"line1\nline2\n").await.unwrap();
    let commit = ws.commit("dan", "initial").await.unwrap();
    ws.create_branch("feature").await.unwrap();
    drop(ws);

    // Catastrophe: the metadata DB is gone. Open a FRESH DB over the SAME content.
    let recovered = Workspace::open_local(dir.path().join("meta2.db"), &cas)
        .await
        .unwrap();
    assert!(
        recovered.read("/README.md").await.is_err(),
        "fresh DB starts empty"
    );

    let report = recovered.rebuild().await.unwrap();

    // Branch names + tips came back via the mirror (recovered names, not synthetic).
    assert!(report.used_mirror, "expected recovery via the ref mirror");
    assert_eq!(report.commits_found, 1);
    let mut names: Vec<_> = report.branches.iter().map(|(n, _)| n.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["feature".to_string(), "main".to_string()]);
    assert_eq!(report.checked_out.as_deref(), Some("main"));
    assert_eq!(report.files, 2);
    assert_eq!(report.dirs, 1);

    // The files themselves are readable again, chunked content reassembled.
    assert_eq!(&recovered.read("/README.md").await.unwrap()[..], b"hello");
    assert_eq!(
        &recovered.read("/src/app.txt").await.unwrap()[..],
        b"line1\nline2\n"
    );
    // Both branches point at the original commit.
    let branches = recovered.list_branches().await.unwrap();
    assert!(branches.iter().any(|(n, h)| n == "feature" && *h == commit));
    assert!(branches.iter().any(|(n, h)| n == "main" && *h == commit));
}

#[tokio::test]
async fn gc_keeps_the_ref_mirror_so_recovery_still_works() {
    let dir = tempdir().unwrap();
    let cas = dir.path().join("cas");
    let ws = Workspace::open_local(dir.path().join("meta.db"), &cas)
        .await
        .unwrap();
    ws.write("/f.txt", b"x").await.unwrap();
    ws.commit("dan", "c1").await.unwrap();
    // GC must keep the live ref-mirror snapshot (else recovery loses branch names).
    ws.gc().await.unwrap();
    drop(ws);

    let recovered = Workspace::open_local(dir.path().join("meta2.db"), &cas)
        .await
        .unwrap();
    let report = recovered.rebuild().await.unwrap();
    assert!(report.used_mirror, "gc should keep the ref mirror");
    assert_eq!(
        report
            .branches
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>(),
        vec!["main"]
    );
    assert_eq!(&recovered.read("/f.txt").await.unwrap()[..], b"x");
}

#[tokio::test]
async fn scan_reports_without_mutating() {
    let dir = tempdir().unwrap();
    let cas = dir.path().join("cas");
    let ws = Workspace::open_local(dir.path().join("meta.db"), &cas)
        .await
        .unwrap();
    ws.write("/f.txt", b"x").await.unwrap();
    ws.commit("dan", "c1").await.unwrap();
    drop(ws);

    let fresh = Workspace::open_local(dir.path().join("meta2.db"), &cas)
        .await
        .unwrap();
    let report = fresh.scan().await.unwrap();
    assert_eq!(report.commits_found, 1);
    assert!(report.used_mirror);
    assert_eq!(report.checked_out.as_deref(), Some("main"));
    // Read-only: the fresh workspace still has no working tree and no branches.
    assert!(fresh.read("/f.txt").await.is_err());
    assert!(fresh.list_branches().await.unwrap().is_empty());
}
