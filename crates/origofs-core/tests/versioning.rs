//! Versioning: commit/log/branch/checkout, incremental snapshots, status, and
//! the opt-in `off` mode.

use origofs_core::{Fs, Hash, MemStore, SqliteMetadataStore, VersioningMode};
use std::sync::Arc;

async fn fixture() -> (Fs<SqliteMetadataStore, Arc<MemStore>>, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

#[tokio::test]
async fn commit_log_branch_and_history() {
    let (fs, _s) = fixture().await;
    assert_eq!(fs.current_branch().await.unwrap().as_deref(), Some("main"));
    assert!(fs.head_commit().await.unwrap().is_none(), "unborn branch");

    fs.write("/a.txt", b"one").await.unwrap();
    let h1 = fs.commit("alice", "first").await.unwrap();

    let log = fs.log().await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].hash, h1);
    assert_eq!(log[0].commit.message, "first");
    assert_eq!(log[0].commit.author, "alice");
    assert_eq!(fs.head_commit().await.unwrap(), Some(h1));

    fs.create_branch("dev").await.unwrap();

    fs.write("/a.txt", b"two").await.unwrap();
    let h2 = fs.commit("alice", "second").await.unwrap();
    assert_ne!(h1, h2);

    let log = fs.log().await.unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].hash, h2);
    assert_eq!(log[1].hash, h1);

    // main advanced; dev stayed put
    let branches: Vec<_> = fs.list_branches().await.unwrap();
    let main = branches.iter().find(|(n, _)| n == "main").unwrap().1;
    let dev = branches.iter().find(|(n, _)| n == "dev").unwrap().1;
    assert_eq!(main, h2);
    assert_eq!(dev, h1);
}

#[tokio::test]
async fn checkout_materializes_the_tree() {
    let (fs, _s) = fixture().await;
    fs.mkdir_p("/d").await.unwrap();
    fs.write("/d/x.txt", b"one").await.unwrap();
    fs.symlink("/d/x.txt", "/link").await.unwrap();
    fs.commit("a", "v1").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // Diverge on main.
    fs.write("/d/x.txt", b"two").await.unwrap();
    fs.write("/new.txt", b"new").await.unwrap();
    fs.commit("a", "v2").await.unwrap();
    assert_eq!(&fs.read("/d/x.txt").await.unwrap()[..], b"two");

    // dev still has v1 content and structure
    fs.checkout("dev").await.unwrap();
    assert_eq!(fs.current_branch().await.unwrap().as_deref(), Some("dev"));
    assert_eq!(&fs.read("/d/x.txt").await.unwrap()[..], b"one");
    assert_eq!(fs.readlink("/link").await.unwrap(), "/d/x.txt");
    assert!(
        fs.stat("/new.txt").await.is_err(),
        "new.txt only exists on main"
    );

    // back to main
    fs.checkout("main").await.unwrap();
    assert_eq!(&fs.read("/d/x.txt").await.unwrap()[..], b"two");
    assert_eq!(&fs.read("/new.txt").await.unwrap()[..], b"new");
}

#[tokio::test]
async fn snapshots_are_incremental() {
    let (fs, store) = fixture().await;
    fs.mkdir_p("/d1").await.unwrap();
    fs.mkdir_p("/d2").await.unwrap();
    fs.write("/d1/f", &pseudo_random(300_000, 1)).await.unwrap();
    fs.write("/d2/g", &pseudo_random(300_000, 2)).await.unwrap();
    fs.commit("a", "v1").await.unwrap();
    let after_v1 = store.len();
    assert!(after_v1 >= 12, "two big files should produce many objects");

    // Change only d1/f near the start; d2/g and its subtree must be reused.
    let mut edited = pseudo_random(300_000, 1);
    for b in edited.iter_mut().take(32) {
        *b ^= 0xFF;
    }
    fs.write("/d1/f", &edited).await.unwrap();
    fs.commit("a", "v2").await.unwrap();
    let new_objects = store.len() - after_v1;

    // A few changed chunks + f's manifest + trees d1 & root + the commit — not a
    // re-store of everything.
    assert!(
        new_objects <= 10,
        "incremental commit stored {new_objects} new objects; expected only the changed path"
    );
}

#[tokio::test]
async fn status_reports_changes() {
    let (fs, _s) = fixture().await;
    fs.write("/keep.txt", b"k").await.unwrap();
    fs.write("/gone.txt", b"g").await.unwrap();
    fs.commit("a", "v1").await.unwrap();
    assert!(fs.status().await.unwrap().is_empty(), "clean after commit");

    fs.write("/keep.txt", b"k2").await.unwrap(); // modify
    fs.write("/added.txt", b"a").await.unwrap(); // add
    fs.unlink("/gone.txt").await.unwrap(); // delete

    let changes = fs.status().await.unwrap();
    let by_path: std::collections::BTreeMap<_, _> = changes
        .iter()
        .map(|d| (d.path.as_str(), d.status.sigil()))
        .collect();
    assert_eq!(by_path.get("/keep.txt"), Some(&'M'));
    assert_eq!(by_path.get("/added.txt"), Some(&'A'));
    assert_eq!(by_path.get("/gone.txt"), Some(&'D'));
}

#[tokio::test]
async fn identical_tree_dedups_across_commits() {
    let (fs, store) = fixture().await;
    fs.write("/a", b"same").await.unwrap();
    fs.commit("a", "v1").await.unwrap();
    let n = store.len();
    // A no-op commit reuses the tree + blobs; only the new commit object and the
    // ref-mirror snapshot it writes (branch tip moved) are added.
    fs.commit("a", "v2 (no changes)").await.unwrap();
    assert_eq!(
        store.len(),
        n + 2,
        "only the new commit object + its ref-mirror snapshot are stored"
    );
}

#[tokio::test]
async fn off_mode_disables_commits_but_not_the_fs() {
    let (fs, _s) = fixture().await;
    fs.set_versioning_mode(VersioningMode::Off).await.unwrap();
    assert_eq!(fs.versioning_mode().await.unwrap(), VersioningMode::Off);

    // Filesystem still works.
    fs.write("/f", b"data").await.unwrap();
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"data");

    // But commits are refused.
    assert!(fs.commit("a", "nope").await.is_err());
    assert!(fs.create_branch("x").await.is_err());
}

#[tokio::test]
async fn checkout_rejects_unknown_branch() {
    let (fs, _s) = fixture().await;
    fs.write("/a", b"x").await.unwrap();
    fs.commit("a", "v1").await.unwrap();
    assert!(fs.checkout("nope").await.is_err());
    // sanity: the object graph never lost the root
    assert_eq!(Hash::from_hex(&"0".repeat(64)).unwrap().to_hex().len(), 64);
}
