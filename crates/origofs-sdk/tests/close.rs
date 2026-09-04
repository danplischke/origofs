//! Deterministic shutdown: `Workspace::close` (issue #154).
//!
//! A long-lived host — a FastAPI lifespan, a worker supervisor — opens a
//! workspace at startup and had nothing to call at shutdown: the backends are
//! released when the last `Arc` drops, and an embedder cannot make a drop happen
//! on demand. A reload or a second lifespan therefore left the old pool alive
//! holding its connections.
//!
//! Three properties are worth pinning, because each fails differently:
//! close **flushes** (a packed store that dropped its buffer would lose writes a
//! caller believes it made), close is **idempotent** (a teardown hook that runs
//! twice is ordinary), and close **reaches through the decorators** (a stack is
//! what a real workspace is, so a close that stopped at the outermost store would
//! release nothing).

use bytes::Bytes;
use origofs_core::{
    ContentStore, Hash, MemStore, MetadataStore, PackStore, Result, VerifyingStore,
};
use origofs_sdk::Workspace;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A content store that counts closes and forwards everything else, so a test can
/// assert a close *arrived* rather than that it did not error.
#[derive(Default)]
struct CountingStore {
    inner: MemStore,
    closes: AtomicUsize,
    flushes: AtomicUsize,
}

#[async_trait::async_trait]
impl ContentStore for CountingStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        self.inner.put(bytes).await
    }
    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.inner.put_keyed(key, bytes).await
    }
    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.inner.get(hash).await
    }
    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        self.inner.get_range(hash, off, len).await
    }
    async fn has(&self, hash: &Hash) -> Result<bool> {
        self.inner.has(hash).await
    }
    async fn delete(&self, hash: &Hash) -> Result<u64> {
        self.inner.delete(hash).await
    }
    async fn list(&self) -> Result<Vec<Hash>> {
        self.inner.list().await
    }
    async fn flush(&self) -> Result<()> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        self.inner.flush().await
    }
    async fn close(&self) -> Result<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.inner.close().await
    }
}

async fn sqlite_meta() -> Arc<dyn MetadataStore> {
    Arc::new(origofs_core::SqliteMetadataStore::open_in_memory().unwrap())
}

/// The headline: an explicit close reaches the content store, and does not need
/// the workspace to be dropped to do it.
#[tokio::test]
async fn close_reaches_the_content_store_while_the_handle_is_still_alive() {
    let counter = Arc::new(CountingStore::default());
    let ws = Workspace::open(sqlite_meta().await, counter.clone())
        .await
        .unwrap();
    ws.write("/a.txt", b"hello").await.unwrap();

    assert_eq!(counter.closes.load(Ordering::SeqCst), 0);
    ws.close().await.unwrap();
    assert_eq!(counter.closes.load(Ordering::SeqCst), 1);

    // The workspace handle is deliberately still in scope: the point of #154 is
    // that shutdown does not wait for a drop.
    drop(ws);
}

/// A teardown hook that runs twice — a re-entering test app, a lifespan that
/// unwinds through an error path — must not be an error.
#[tokio::test]
async fn closing_twice_is_not_an_error() {
    let counter = Arc::new(CountingStore::default());
    let ws = Workspace::open(sqlite_meta().await, counter.clone())
        .await
        .unwrap();
    ws.close().await.unwrap();
    ws.close().await.unwrap();
    assert_eq!(counter.closes.load(Ordering::SeqCst), 2);
}

/// Close flushes first. This is the property that makes close safe on a packed
/// store, where a chunk lives in memory until a pack is sealed: closing without
/// sealing would discard a write the caller was told succeeded.
#[tokio::test]
async fn close_flushes_before_it_closes() {
    let counter = Arc::new(CountingStore::default());
    let ws = Workspace::open(sqlite_meta().await, counter.clone())
        .await
        .unwrap();
    ws.close().await.unwrap();
    assert_eq!(counter.flushes.load(Ordering::SeqCst), 1);
}

/// The flush is real on a store that batches — but note *what* it rescues.
///
/// The engine already pays a durability barrier on the write path (content sealed
/// before the metadata referencing it commits), so a file written through the
/// workspace is never pending by the time anything closes. This drives the case
/// that barrier does not cover: content put straight into the store, which is
/// what an object-transfer or import path does. Without the flush the chunk dies
/// with the buffer.
#[tokio::test]
async fn close_seals_content_put_outside_the_engines_write_path() {
    let data: Arc<dyn ContentStore> = Arc::new(MemStore::new());
    let index: Arc<dyn ContentStore> = Arc::new(MemStore::new());
    let pack: Arc<dyn ContentStore> = Arc::new(PackStore::new(data.clone(), index.clone()));

    let ws = Workspace::open(sqlite_meta().await, pack.clone())
        .await
        .unwrap();
    // Straight into the store, so nothing flushes on our behalf.
    let hash = ws
        .fs()
        .content
        .put(b"staged, never explicitly flushed")
        .await
        .unwrap();
    assert!(
        data.list().await.unwrap().is_empty(),
        "precondition: the chunk must still be buffered, or this test proves nothing"
    );

    ws.close().await.unwrap();

    // A fresh `PackStore` over the same durable halves can only find the chunk if
    // the close sealed it.
    let reopened = PackStore::new(data, index);
    assert_eq!(
        &reopened.get(&hash).await.unwrap()[..],
        b"staged, never explicitly flushed"
    );
}

/// Close forwards through a decorator stack. A real workspace is never a bare
/// store — `open_pg_s3_packed` is `VerifyingStore(PackStore(ObjectContentStore))`
/// — so a close that stopped at the outermost layer would release nothing.
#[tokio::test]
async fn close_forwards_through_the_decorators() {
    let counter = Arc::new(CountingStore::default());
    let verifying: Arc<dyn ContentStore> = Arc::new(VerifyingStore::new(counter.clone()));
    let ws = Workspace::open(sqlite_meta().await, verifying)
        .await
        .unwrap();
    ws.close().await.unwrap();
    assert_eq!(
        counter.closes.load(Ordering::SeqCst),
        1,
        "a close on the outermost store must reach the real backend, like `ping` does"
    );
}
