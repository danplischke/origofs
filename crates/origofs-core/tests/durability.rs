//! Durability barrier (Phase 2 of the failure-surface audit, #34): content is
//! durable before the metadata that references it is committed.
//!
//! - **C3** — [`LocalCasStore`] fsyncs an object's bytes *and* its parent
//!   directory on every write, so a crash can't leave the object durably named
//!   over unwritten (zero/torn) bytes.
//! - **C4** — the engine flushes the content store on every logical write, so a
//!   [`PackStore`]'s chunks can't be lost while still buffered in memory yet
//!   already referenced by committed metadata.

use origofs_core::{ContentStore, Fs, LocalCasStore, MemStore, PackStore, SqliteMetadataStore};
use std::sync::Arc;

// C3: a durable write fsyncs the object file AND its parent directory. fsync
// durability isn't crash-injectable in-process, so we assert the barrier *ran*
// via its sync counter — which is precisely what dropping the fsync regresses.
#[tokio::test]
async fn local_cas_write_fsyncs_object_and_directory() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalCasStore::open(dir.path()).await.unwrap();

    assert_eq!(store.sync_count(), 0);
    store.put(b"durable bytes").await.unwrap();
    // One fsync for the object file, one for its parent directory (unix).
    assert!(
        store.sync_count() >= 2,
        "a durable write must fsync the object and its directory, saw {}",
        store.sync_count()
    );

    // A second identical put is deduplicated (already present) — no new fsync,
    // so the counter proves the write path, not just that puts happen.
    let before = store.sync_count();
    store.put(b"durable bytes").await.unwrap();
    assert_eq!(
        store.sync_count(),
        before,
        "a dedup'd put must not re-fsync"
    );
}

// C4: a packed write must survive a "crash", modeled as a brand-new PackStore
// over the same data + index backends but with an EMPTY pending buffer. The
// bytes are recoverable only if the write sealed (flushed) the pack. Before the
// barrier the chunks lived solely in the first store's memory and were lost,
// while the metadata already pointed at them — a permanent ContentMissing.
#[tokio::test]
async fn packed_write_survives_restart() {
    let data = Arc::new(MemStore::new());
    let index = Arc::new(MemStore::new());
    // A target far larger than the write, so the pack never auto-seals on size:
    // only the flush-on-write barrier can make this durable.
    let huge = 1usize << 30;
    let meta = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());

    let pack1 = Arc::new(PackStore::with_target(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
        huge,
    ));
    let fs = Fs::new(meta.clone(), pack1.clone());
    fs.init().await.unwrap();
    fs.write("/f", b"packed durable bytes").await.unwrap();

    // Simulated restart: a fresh pack store (empty pending) over the SAME
    // backends. A prior seal is the only way its bytes are reachable now.
    let pack2 = Arc::new(PackStore::with_target(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
        huge,
    ));
    let fs2 = Fs::new(meta.clone(), pack2);
    let got = fs2.read("/f").await.unwrap();
    assert_eq!(&got[..], b"packed durable bytes");
}
