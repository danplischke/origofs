//! Rebuild inferring heads from the commit DAG when no ref mirror is present
//! (an older store, or the mirror lost). The object graph is built by hand from
//! primitives so there is deliberately no `RefSnapshot` to fall back on.

use origofs_core::{
    ChunkRef, Commit, ContentStore, Fs, Manifest, MemStore, SqliteMetadataStore, Tree, TreeEntry,
    TreeKind,
};
use std::sync::Arc;

#[tokio::test]
async fn rebuild_infers_head_when_no_mirror() {
    let store = Arc::new(MemStore::new());

    // Hand-build a single-commit graph: "hi" -> manifest -> tree -> commit.
    let chunk = store.put(b"hi").await.unwrap();
    let manifest = store
        .put(
            &Manifest {
                size: 2,
                chunks: vec![ChunkRef {
                    hash: chunk,
                    len: 2,
                }],
            }
            .encode(),
        )
        .await
        .unwrap();
    let tree = store
        .put(
            &Tree {
                entries: vec![TreeEntry {
                    name: "greet.txt".into(),
                    mode: 0o644,
                    kind: TreeKind::File,
                    hash: manifest,
                }],
            }
            .encode(),
        )
        .await
        .unwrap();
    let commit = store
        .put(
            &Commit {
                tree,
                parents: vec![],
                author: "dan".into(),
                message: "c1".into(),
                timestamp: 1,
            }
            .encode(),
        )
        .await
        .unwrap();

    // A fresh, empty metadata DB over that content; rebuild with no mirror.
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        store.clone(),
    );
    fs.init().await.unwrap();
    let report = fs.rebuild_from_content().await.unwrap();

    assert!(
        !report.used_mirror,
        "no snapshot exists → the head must be inferred"
    );
    assert_eq!(report.commits_found, 1);
    assert_eq!(report.branches, vec![("main".to_string(), commit.to_hex())]);
    assert_eq!(report.checked_out.as_deref(), Some("main"));
    assert_eq!(report.files, 1);
    assert_eq!(&fs.read("/greet.txt").await.unwrap()[..], b"hi");
}
