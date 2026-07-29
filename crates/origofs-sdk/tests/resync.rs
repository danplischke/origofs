//! `Workspace::resync` — the SDK front door to offline → reconnect reconciliation
//! (`docs/DESIGN.md` §4b/§4c), over two real on-disk SQLite + local-CAS
//! workspaces, which is the shape `origofs resync` drives from the CLI.

use origofs_sdk::{ResyncOutcome, Workspace, WriteCtx};
use tempfile::TempDir;

async fn workspace(dir: &TempDir, name: &str) -> Workspace {
    let root = dir.path().join(name);
    std::fs::create_dir_all(&root).unwrap();
    Workspace::open_local(root.join("meta.db"), root.join("cas"))
        .await
        .unwrap()
}

#[tokio::test]
async fn offline_workspace_reconnects_and_merges() {
    let dir = TempDir::new().unwrap();
    let laptop = workspace(&dir, "laptop").await;
    let shared = workspace(&dir, "shared").await;

    // A human on the laptop, offline.
    let alice = laptop
        .find_or_create_human("u:alice", "alice")
        .await
        .unwrap();
    let session = laptop.create_session(alice, Some("laptop")).await.unwrap();
    let ctx = WriteCtx::session(alice, session);

    laptop.write("/README.md", b"shared base\n").await.unwrap();
    laptop.commit("alice", "base").await.unwrap();

    // First reconnect: the shared workspace has never seen this branch.
    let report = laptop
        .resync(&shared, "main", "alice", "reconnect")
        .await
        .unwrap();
    assert!(matches!(report.outcome, ResyncOutcome::Pushed(_)));
    assert!(report.pushed.objects > 0);
    assert_eq!(report.pushed.skipped, 0);
    assert_eq!(
        &shared.read("/README.md").await.unwrap()[..],
        b"shared base\n"
    );

    // Both sides work on different files while disconnected.
    shared.write("/server.txt", b"server side\n").await.unwrap();
    shared.commit("bob", "server work").await.unwrap();
    laptop
        .write_as(ctx, "/plane.md", b"written offline\n")
        .await
        .unwrap();
    laptop.commit("alice", "offline work").await.unwrap();

    let report = laptop
        .resync(&shared, "main", "alice", "reconnect")
        .await
        .unwrap();
    let ResyncOutcome::Merged(merged) = report.outcome else {
        panic!("expected a merge, got {:?}", report.outcome);
    };
    assert!(report.conflicts.is_empty());
    assert!(report.remote_tree_updated);
    assert_eq!(
        shared.list_branches().await.unwrap(),
        vec![("main".to_string(), merged)]
    );
    for ws in [&laptop, &shared] {
        assert_eq!(
            &ws.read("/plane.md").await.unwrap()[..],
            b"written offline\n"
        );
        assert_eq!(&ws.read("/server.txt").await.unwrap()[..], b"server side\n");
    }

    // The offline attribution is answerable on the shared workspace.
    let blame = shared.blame("/plane.md").await.unwrap();
    assert_eq!(blame.len(), 1);
    assert_eq!(blame[0].actor.display_name, "alice");
    assert_eq!(blame[0].actor.auth_subject.as_deref(), Some("u:alice"));

    // Idempotent: nothing left to do, nothing copied.
    let report = laptop
        .resync(&shared, "main", "alice", "reconnect")
        .await
        .unwrap();
    assert_eq!(report.outcome, ResyncOutcome::UpToDate);
    assert_eq!(report.pushed.objects, 0);
    assert_eq!(report.fetched.objects, 0);
}

#[tokio::test]
async fn push_and_fetch_objects_move_only_whats_missing() {
    let dir = TempDir::new().unwrap();
    let laptop = workspace(&dir, "laptop").await;
    let shared = workspace(&dir, "shared").await;

    laptop.write("/a.txt", b"one\n").await.unwrap();
    let head = laptop.commit("alice", "one").await.unwrap();

    let first = laptop.push_objects(&shared, head).await.unwrap();
    assert!(first.objects > 0);
    assert_eq!(first.skipped, 0);

    let again = laptop.push_objects(&shared, head).await.unwrap();
    assert_eq!(again.objects, 0, "a repeat push copies nothing");
    assert_eq!(again.skipped, 1);

    // The reverse direction is a no-op too — the laptop already has it all.
    let back = laptop.fetch_objects(&shared, head).await.unwrap();
    assert_eq!(back.objects, 0);

    // Neither call touched a ref: the shared workspace still has no branch.
    assert!(shared.list_branches().await.unwrap().is_empty());
}
