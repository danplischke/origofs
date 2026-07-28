//! Garbage collection: orphaned content (uncommitted churn, superseded bodies)
//! is reclaimed, while everything reachable from a ref or the live working tree
//! survives — verified against both the in-memory and on-disk content stores.

use origofs_core::{ContentStore, Fs, LocalCasStore, MemStore, SqliteMetadataStore};
use std::sync::Arc;

async fn mem_fixture() -> (Fs<SqliteMetadataStore, Arc<MemStore>>, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

/// A multi-chunk body; distinct seeds share no chunks.
fn blob(len: usize, seed: u64) -> Vec<u8> {
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
async fn reclaims_uncommitted_churn_keeps_live_body() {
    let (fs, store) = mem_fixture().await;
    let v1 = blob(200 * 1024, 1);
    let v2 = blob(200 * 1024, 2);

    fs.write("/a.bin", &v1).await.unwrap();
    let after_v1 = store.len();
    fs.write("/a.bin", &v2).await.unwrap(); // overwrite; v1 now orphaned
    let after_v2 = store.len();
    assert!(after_v2 > after_v1, "v2 added distinct objects");

    let stats = fs.gc().await.unwrap();

    // Every v1 object was unreachable and reclaimed; only v2's remain.
    assert_eq!(stats.deleted, after_v1, "all of v1 collected");
    assert_eq!(store.len(), after_v2 - after_v1, "only v2 survives");
    assert_eq!(stats.reachable, store.len());
    assert!(stats.bytes_freed >= 200 * 1024, "freed at least v1's bytes");

    // The live body still reads back intact.
    assert_eq!(&fs.read("/a.bin").await.unwrap()[..], &v2[..]);

    // GC is idempotent: a second pass finds nothing to reclaim.
    let again = fs.gc().await.unwrap();
    assert_eq!(again.deleted, 0);
}

#[tokio::test]
async fn committed_content_survives_working_tree_moving_on() {
    let (fs, _store) = mem_fixture().await;
    let x = blob(128 * 1024, 10);
    let y = blob(128 * 1024, 20);
    let z = blob(128 * 1024, 30);

    fs.write("/a.bin", &x).await.unwrap();
    fs.commit("alice", "keep x").await.unwrap(); // x reachable via commit

    fs.write("/a.bin", &y).await.unwrap(); // uncommitted
    fs.write("/a.bin", &z).await.unwrap(); // orphans y; z is the live body

    let stats = fs.gc().await.unwrap();
    assert!(stats.deleted > 0, "the orphaned y body was reclaimed");

    // Working tree is untouched.
    assert_eq!(&fs.read("/a.bin").await.unwrap()[..], &z[..]);

    // And the committed x survived GC: checking out the branch restores it.
    fs.checkout("main").await.unwrap();
    assert_eq!(&fs.read("/a.bin").await.unwrap()[..], &x[..]);
}

#[tokio::test]
async fn gc_never_touches_a_clean_committed_workspace() {
    let (fs, store) = mem_fixture().await;
    fs.mkdir_p("/dir").await.unwrap();
    fs.write("/dir/a.txt", b"alpha").await.unwrap();
    fs.write("/b.txt", b"beta").await.unwrap();
    fs.commit("alice", "snapshot").await.unwrap();

    let before = store.len();
    let stats = fs.gc().await.unwrap();
    assert_eq!(stats.deleted, 0, "nothing reachable was collected");
    assert_eq!(store.len(), before);
    assert_eq!(&fs.read("/dir/a.txt").await.unwrap()[..], b"alpha");
}

#[tokio::test]
async fn reclaims_on_the_local_on_disk_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalCasStore::open(dir.path().join("cas")).await.unwrap());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();

    let v1 = blob(300 * 1024, 100);
    let v2 = blob(300 * 1024, 200);
    fs.write("/big.bin", &v1).await.unwrap();
    fs.write("/big.bin", &v2).await.unwrap();

    let before = store.list().await.unwrap().len();
    let stats = fs.gc().await.unwrap();
    assert!(stats.deleted > 0);
    assert!(stats.bytes_freed >= 300 * 1024);
    assert_eq!(store.list().await.unwrap().len(), before - stats.deleted);

    // Body still intact, and a re-run is a no-op.
    assert_eq!(&fs.read("/big.bin").await.unwrap()[..], &v2[..]);
    assert_eq!(fs.gc().await.unwrap().deleted, 0);
}
