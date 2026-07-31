//! Chunk uploads run concurrently, bounded, and in order.
//!
//! Writes used to store one chunk at a time — `put().await` in a loop — costing a
//! full round trip per chunk. Invisible locally, dominant on object storage:
//! content-defined chunking turns 1 GiB of incompressible data (media, archives,
//! anything already compressed) into ~13,700 chunks, so at 30 ms RTT one gigabyte
//! was ~7 minutes of pure latency with the link nearly idle.
//!
//! Tested with a store that injects latency, because on a local or in-memory store
//! the difference is unobservable — which is exactly why it went unnoticed.

use async_trait::async_trait;
use bytes::Bytes;
use origofs_core::{ContentStore, Fs, MemStore, MetadataStore, SqliteMetadataStore, WriteCtx};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// A `MemStore` with a per-`put` delay, standing in for a network round trip, and
/// a high-water mark of concurrent puts so the window can be asserted.
struct Slow {
    inner: MemStore,
    delay: Duration,
    inflight: AtomicUsize,
    peak: AtomicUsize,
}

impl Slow {
    fn new(delay: Duration) -> Self {
        Self {
            inner: MemStore::new(),
            delay,
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl ContentStore for Slow {
    async fn put(&self, bytes: &[u8]) -> Result<origofs_core::Hash, origofs_core::OrigoFSError> {
        let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        let r = self.inner.put(bytes).await;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        r
    }
    async fn get(&self, hash: &origofs_core::Hash) -> Result<Bytes, origofs_core::OrigoFSError> {
        self.inner.get(hash).await
    }
    async fn has(&self, hash: &origofs_core::Hash) -> Result<bool, origofs_core::OrigoFSError> {
        self.inner.has(hash).await
    }
    async fn delete(&self, hash: &origofs_core::Hash) -> Result<u64, origofs_core::OrigoFSError> {
        self.inner.delete(hash).await
    }
    async fn put_keyed(
        &self,
        key: &origofs_core::Hash,
        bytes: &[u8],
    ) -> Result<(), origofs_core::OrigoFSError> {
        self.inner.put_keyed(key, bytes).await
    }
    async fn get_range(
        &self,
        hash: &origofs_core::Hash,
        off: u64,
        len: u64,
    ) -> Result<Bytes, origofs_core::OrigoFSError> {
        self.inner.get_range(hash, off, len).await
    }
    async fn list(&self) -> Result<Vec<origofs_core::Hash>, origofs_core::OrigoFSError> {
        self.inner.list().await
    }
}

/// Incompressible, like encoded media: chunk boundaries are effectively random and
/// nothing deduplicates, which is the case that produces the most objects.
fn media(len: usize) -> Vec<u8> {
    let mut x = 0x2545F4914F6CDD1Du64;
    let mut out = vec![0u8; len];
    for b in out.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    out
}

async fn fs_with(store: Arc<Slow>) -> Fs<Arc<dyn MetadataStore>, Arc<Slow>> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

/// A buffered write overlaps its uploads, and the result is byte-identical.
#[tokio::test]
async fn a_buffered_write_uploads_concurrently() {
    let store = Arc::new(Slow::new(Duration::from_millis(5)));
    let fs = fs_with(store.clone()).await;
    let body = media(4 * 1024 * 1024);

    let t0 = Instant::now();
    fs.write("/clip.mp4", &body).await.unwrap();
    let elapsed = t0.elapsed();

    let peak = store.peak.load(Ordering::SeqCst);
    let objects = store.inner.list().await.unwrap().len();
    assert!(
        peak > 1,
        "uploads were sequential: {objects} objects, peak concurrency {peak}"
    );

    // Sequential would be ~objects x 5ms. Assert comfortably below that: the point
    // is overlap, not a precise speedup, and CI machines vary.
    let sequential = Duration::from_millis(5) * objects as u32;
    assert!(
        elapsed < sequential / 2,
        "took {elapsed:?}; sequential would be ~{sequential:?} for {objects} objects"
    );

    // Concurrency must not reorder the file.
    assert_eq!(fs.read("/clip.mp4").await.unwrap(), body);
}

/// The streaming write does the same, and still records attribution.
#[tokio::test]
async fn a_streaming_write_uploads_concurrently() {
    let store = Arc::new(Slow::new(Duration::from_millis(5)));
    let fs = fs_with(store.clone()).await;
    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(agent, Some("t")).await.unwrap();
    let body = media(4 * 1024 * 1024);

    let t0 = Instant::now();
    fs.write_reader_as(
        WriteCtx::session(agent, session),
        "/clip.mp4",
        std::io::Cursor::new(body.clone()),
    )
    .await
    .unwrap();
    let elapsed = t0.elapsed();

    let peak = store.peak.load(Ordering::SeqCst);
    let objects = store.inner.list().await.unwrap().len();
    assert!(peak > 1, "streaming uploads were sequential (peak {peak})");
    let sequential = Duration::from_millis(5) * objects as u32;
    assert!(
        elapsed < sequential / 2,
        "took {elapsed:?}; sequential would be ~{sequential:?}"
    );

    assert_eq!(fs.read("/clip.mp4").await.unwrap(), body);
    assert!(
        fs.blame("/clip.mp4")
            .await
            .unwrap()
            .iter()
            .all(|b| b.actor.id == agent),
        "parallel upload lost the attribution"
    );
}

/// The window is bounded — memory is `window x MAX_CHUNK`, and an object store
/// will rate-limit an unbounded fan-out.
#[tokio::test]
async fn concurrency_is_bounded() {
    let store = Arc::new(Slow::new(Duration::from_millis(2)));
    let fs = fs_with(store.clone()).await;
    fs.write("/big.bin", &media(8 * 1024 * 1024)).await.unwrap();

    let peak = store.peak.load(Ordering::SeqCst);
    // The default window is 16; allow slack for the `buffered` prefetch, but this
    // must not be "all of them at once".
    assert!(
        peak <= 32,
        "upload window is unbounded (peak {peak}); memory and rate limits both care"
    );
}
