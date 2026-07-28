//! Multi-workspace in one store (`docs/MULTI_TENANCY.md`): a single metadata +
//! content store holds many workspaces, separated by a `workspace_id`. Each has
//! its own root inode, working tree, refs, and versioning; content and identity
//! are shared. These tests pin the isolation, independent versioning, and
//! persistence guarantees.

use origofs_sdk::{MemStore, OrigoFSError, Workspace};

/// Files, directory listings, and reads are isolated per workspace, and each
/// workspace roots at its own inode (only the `default` workspace is `INO_ROOT`).
#[tokio::test]
async fn workspaces_isolate_their_trees() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();

    // Three workspaces sharing one store: the implicit `default` plus two named.
    ws.write("/d.txt", b"in-default").await.unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    alpha.write("/a.txt", b"in-alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();
    beta.write("/b.txt", b"in-beta").await.unwrap();

    // Each root lists only its own entries.
    assert_eq!(names(&ws).await, vec!["d.txt"]);
    assert_eq!(names(&alpha).await, vec!["a.txt"]);
    assert_eq!(names(&beta).await, vec!["b.txt"]);

    // Reads resolve within the workspace…
    assert_eq!(&alpha.read("/a.txt").await.unwrap()[..], b"in-alpha");
    assert_eq!(&beta.read("/b.txt").await.unwrap()[..], b"in-beta");

    // …and a file from another workspace is simply not there.
    assert!(matches!(
        alpha.read("/d.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));
    assert!(matches!(
        ws.read("/a.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));

    // The default workspace roots at INO_ROOT (1); a named one gets its own root.
    assert_eq!(ws.stat("/").await.unwrap().ino, 1);
    assert_ne!(alpha.stat("/").await.unwrap().ino, 1);
    assert_ne!(
        alpha.stat("/").await.unwrap().ino,
        beta.stat("/").await.unwrap().ino
    );

    // The registry enumerates every workspace, oldest first.
    assert_eq!(
        ws.workspaces().await.unwrap(),
        vec!["default", "alpha", "beta"]
    );
}

/// Refs, branches, and commit history are per workspace: a commit on one is
/// invisible to another sharing the same store.
#[tokio::test]
async fn workspaces_version_independently() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();

    let alpha = ws.workspace("alpha").await.unwrap();
    let beta = ws.workspace("beta").await.unwrap();

    alpha.write("/a.txt", b"hello").await.unwrap();
    alpha.commit("tester", "add a").await.unwrap();
    alpha.create_branch("feature").await.unwrap();

    // Alpha has one commit and two branches…
    assert_eq!(alpha.log().await.unwrap().len(), 1);
    let mut alpha_branches: Vec<String> = alpha
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    alpha_branches.sort();
    assert_eq!(alpha_branches, vec!["feature", "main"]);

    // …while beta, sharing the store, has neither the commit nor the branch.
    assert_eq!(beta.log().await.unwrap().len(), 0);
    let beta_branches: Vec<String> = beta
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert!(!beta_branches.contains(&"feature".to_string()));
}

/// Workspaces persist: reopening the store and asking for a named workspace
/// returns the same one, with its tree intact.
#[tokio::test]
async fn workspaces_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("meta.db");
    let cas = dir.path().join("cas");

    let root_ino = {
        let ws = Workspace::open_local(&db, &cas).await.unwrap();
        let alpha = ws.workspace("alpha").await.unwrap();
        alpha.write("/keep.txt", b"durable").await.unwrap();
        alpha.stat("/").await.unwrap().ino
    };

    // Reopen the store from scratch and re-resolve the workspace by name.
    let ws = Workspace::open_local(&db, &cas).await.unwrap();
    let alpha = ws.workspace("alpha").await.unwrap();
    assert_eq!(alpha.stat("/").await.unwrap().ino, root_ino);
    assert_eq!(&alpha.read("/keep.txt").await.unwrap()[..], b"durable");
    // Opening the same name again is idempotent, not a duplicate.
    assert_eq!(ws.workspaces().await.unwrap(), vec!["default", "alpha"]);
}

/// The same isolation + independent-versioning guarantees hold on the Postgres
/// backend (many workspaces in one database via `workspace_id`). Self-skips unless
/// `ORIGOFS_PG_TEST_URL` points at a reachable database. Uses uniquely-named
/// workspaces so repeated runs against a shared DB never collide.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspaces_isolate_on_postgres() {
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!("skipping workspaces_isolate_on_postgres: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let ws = Workspace::open_pg(&dsn, std::sync::Arc::new(MemStore::new()))
        .await
        .unwrap();

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let (na, nb) = (format!("a_{nonce}"), format!("b_{nonce}"));

    let a = ws.workspace(&na).await.unwrap();
    let b = ws.workspace(&nb).await.unwrap();
    a.write("/only-a.txt", b"A").await.unwrap();
    b.write("/only-b.txt", b"B").await.unwrap();

    // Isolated trees, distinct roots, no cross-workspace reads.
    assert_eq!(names(&a).await, vec!["only-a.txt"]);
    assert_eq!(names(&b).await, vec!["only-b.txt"]);
    assert!(matches!(
        a.read("/only-b.txt").await,
        Err(OrigoFSError::NotFound(_))
    ));
    assert_ne!(
        a.stat("/").await.unwrap().ino,
        b.stat("/").await.unwrap().ino
    );

    // Independent refs/history on Postgres too.
    a.commit("tester", "add only-a").await.unwrap();
    assert_eq!(a.log().await.unwrap().len(), 1);
    assert_eq!(b.log().await.unwrap().len(), 0);
}

/// The sorted-by-insertion entry names directly under a workspace's root.
async fn names(ws: &Workspace) -> Vec<String> {
    ws.ls("/")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect()
}
