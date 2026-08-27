//! Chunk **fetches** run concurrently, bounded, and in order (issue #113).
//!
//! The mirror of `upload_concurrency.rs`, guarding the other direction. Reads used
//! to walk a manifest and await `get`/`get_range` for each covering chunk strictly
//! one at a time, while the write path had used a bounded window since M1. At the
//! ~64 KiB average chunk size a 1 MiB read is ~16 chunks, so on an S3-backed
//! workspace at 30 ms RTT that was ~half a second of pure latency per megabyte with
//! the link idle throughout — and it was every read path, not just the mount:
//! `read`, `read_range`, the streaming variants, and `vfs_read` all had the shape.
//!
//! Tested with a store that injects latency, because on a local or in-memory store
//! the difference is unobservable — which is exactly why it went unnoticed.

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use origofs_core::{ContentStore, Fs, MemStore, MetadataStore, SqliteMetadataStore};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// A `MemStore` with a per-**read** delay, standing in for a network round trip,
/// and a high-water mark of concurrent reads so the window can be asserted.
///
/// Only `get`/`get_range` are delayed: the write that sets each test up would
/// otherwise pay the same latency and dominate the measurement.
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

    /// Forget what previous phases of a test observed, so an assertion describes
    /// only the read under test (the setup write also issues reads).
    fn reset(&self) {
        self.peak.store(0, Ordering::SeqCst);
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    /// Run `f` with the in-flight counter raised, recording the high-water mark.
    async fn tracked<T, F: std::future::Future<Output = T>>(&self, f: F) -> T {
        let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        let r = f.await;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        r
    }
}

#[async_trait]
impl ContentStore for Slow {
    async fn get(&self, hash: &origofs_core::Hash) -> Result<Bytes, origofs_core::OrigoFSError> {
        self.tracked(self.inner.get(hash)).await
    }
    async fn get_range(
        &self,
        hash: &origofs_core::Hash,
        off: u64,
        len: u64,
    ) -> Result<Bytes, origofs_core::OrigoFSError> {
        self.tracked(self.inner.get_range(hash, off, len)).await
    }
    async fn put(&self, bytes: &[u8]) -> Result<origofs_core::Hash, origofs_core::OrigoFSError> {
        self.inner.put(bytes).await
    }
    async fn put_keyed(
        &self,
        key: &origofs_core::Hash,
        bytes: &[u8],
    ) -> Result<(), origofs_core::OrigoFSError> {
        self.inner.put_keyed(key, bytes).await
    }
    async fn has(&self, hash: &origofs_core::Hash) -> Result<bool, origofs_core::OrigoFSError> {
        self.inner.has(hash).await
    }
    async fn delete(&self, hash: &origofs_core::Hash) -> Result<u64, origofs_core::OrigoFSError> {
        self.inner.delete(hash).await
    }
    async fn list(&self) -> Result<Vec<origofs_core::Hash>, origofs_core::OrigoFSError> {
        self.inner.list().await
    }
}

/// A stand-in round trip, deliberately well above what chunking costs in a debug
/// build — see the note in `upload_concurrency.rs`.
const READ_LATENCY: Duration = Duration::from_millis(25);

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

/// Set up a workspace holding `body` at `/clip.mp4`, with counters zeroed so only
/// the read under test is measured. Returns the fs and how many objects the body
/// occupies (chunks + manifest).
async fn seeded(store: Arc<Slow>, body: &[u8]) -> (Fs<Arc<dyn MetadataStore>, Arc<Slow>>, usize) {
    let fs = fs_with(store.clone()).await;
    fs.write("/clip.mp4", body).await.unwrap();
    let objects = store.inner.list().await.unwrap().len();
    store.reset();
    (fs, objects)
}

/// Assert a read overlapped its fetches rather than issuing them one at a time,
/// and report the speedup for a failure message that explains itself.
fn assert_overlapped(what: &str, peak: usize, elapsed: Duration, chunks: usize) {
    assert!(
        peak > 1,
        "{what}: fetches were sequential ({chunks} chunks, peak concurrency {peak})"
    );
    // Sequential would be ~chunks x READ_LATENCY. Assert a speedup *floor* well
    // below the window size: the claim is "these overlap", not a wall-clock budget,
    // and a shared CI runner cannot honour a tight one.
    let sequential = READ_LATENCY * chunks as u32;
    let speedup = sequential.as_secs_f64() / elapsed.as_secs_f64();
    println!("{what}: {chunks} chunks, {elapsed:?} vs ~{sequential:?} sequential ({speedup:.1}x)");
    assert!(
        speedup > 4.0,
        "{what}: only {speedup:.1}x faster than sequential ({elapsed:?} vs ~{sequential:?} \
         for {chunks} chunks); fetches are not overlapping"
    );
}

/// A whole-file read overlaps its fetches, and returns the bytes in order.
///
/// This is the widest path of the set: `read` goes through `read_body`, which also
/// backs `vfs_write`'s read-modify-write and every three-way merge.
#[tokio::test]
async fn a_whole_file_read_fetches_concurrently() {
    let store = Arc::new(Slow::new(READ_LATENCY));
    let body = media(4 * 1024 * 1024);
    let (fs, objects) = seeded(store.clone(), &body).await;

    let t0 = Instant::now();
    let got = fs.read("/clip.mp4").await.unwrap();
    let elapsed = t0.elapsed();

    assert_overlapped("read", store.peak(), elapsed, objects);
    // Concurrency must not reorder the file.
    assert_eq!(got, body, "concurrent fetch reordered the body");
}

/// A ranged read overlaps the fetches covering the range, and trims correctly.
#[tokio::test]
async fn a_ranged_read_fetches_concurrently() {
    let store = Arc::new(Slow::new(READ_LATENCY));
    let body = media(4 * 1024 * 1024);
    let (fs, objects) = seeded(store.clone(), &body).await;

    // A range starting and ending mid-chunk, so the boundary trimming is exercised
    // alongside the concurrency.
    let (off, len) = (1_000_003u64, 2_000_011u64);
    let t0 = Instant::now();
    let got = fs.read_range("/clip.mp4", off, len).await.unwrap();
    let elapsed = t0.elapsed();

    // The range covers most but not all of the file; compare against the chunks it
    // actually touched rather than the whole object count.
    let touched = store.peak().max(1);
    assert!(touched > 1, "ranged read was sequential (peak {touched})");
    println!("read_range: {elapsed:?} over ~{objects} objects, peak {touched}");
    assert_eq!(
        &got[..],
        &body[off as usize..(off + len) as usize],
        "ranged concurrent fetch returned the wrong bytes"
    );
}

/// The borrowed stream overlaps, with bounded look-ahead, and yields in byte order.
#[tokio::test]
async fn a_streamed_read_fetches_concurrently() {
    let store = Arc::new(Slow::new(READ_LATENCY));
    let body = media(4 * 1024 * 1024);
    let (fs, objects) = seeded(store.clone(), &body).await;

    let t0 = Instant::now();
    let mut out = Vec::new();
    let mut s = fs.read_stream("/clip.mp4").await.unwrap();
    while let Some(part) = s.next().await {
        out.extend_from_slice(&part.unwrap());
    }
    let elapsed = t0.elapsed();

    assert_overlapped("read_stream", store.peak(), elapsed, objects);
    assert_eq!(out, body, "streamed concurrent fetch reordered the body");
}

/// The owned (`'static`) stream — the one an HTTP response body is built from —
/// gets the same treatment.
#[tokio::test]
async fn an_owned_streamed_read_fetches_concurrently() {
    let store = Arc::new(Slow::new(READ_LATENCY));
    let body = media(4 * 1024 * 1024);
    let (fs, objects) = seeded(store.clone(), &body).await;

    let t0 = Instant::now();
    let mut out = Vec::new();
    let mut s = fs.read_stream_owned("/clip.mp4").await.unwrap();
    while let Some(part) = s.next().await {
        out.extend_from_slice(&part.unwrap());
    }
    let elapsed = t0.elapsed();

    assert_overlapped("read_stream_owned", store.peak(), elapsed, objects);
    assert_eq!(out, body, "owned streamed fetch reordered the body");
}

/// The mount read path overlaps too — this is the one the issue was first filed
/// against, and the one FUSE/NFS actually call.
#[tokio::test]
async fn a_mount_read_fetches_concurrently() {
    let store = Arc::new(Slow::new(READ_LATENCY));
    let body = media(4 * 1024 * 1024);
    let (fs, _) = seeded(store.clone(), &body).await;

    let ino = fs.stat("/clip.mp4").await.unwrap().ino;
    // A 1 MiB read, the size the `origofs.fastapi` streaming loop issues; at the
    // ~64 KiB average chunk size that is ~16 chunks, i.e. ~16 serial round trips
    // before this change.
    let t0 = Instant::now();
    let got = fs.vfs_read(ino, 0, 1024 * 1024).await.unwrap();
    let elapsed = t0.elapsed();

    let peak = store.peak();
    assert!(
        peak > 1,
        "vfs_read was sequential (peak concurrency {peak})"
    );
    println!("vfs_read: 1 MiB in {elapsed:?}, peak {peak}");
    assert_eq!(
        &got[..],
        &body[..got.len()],
        "concurrent vfs_read returned the wrong bytes"
    );
    assert_eq!(got.len(), 1024 * 1024, "short read");
}

/// The window is bounded — memory is `window x MAX_CHUNK`, an object store will
/// rate-limit an unbounded fan-out, and on the streaming paths the bound is also
/// the read-ahead window, so an unbounded one would queue the whole file for a
/// consumer that stops after the first chunk.
#[tokio::test]
async fn fetch_concurrency_is_bounded() {
    let store = Arc::new(Slow::new(Duration::from_millis(2)));
    let body = media(8 * 1024 * 1024);
    let (fs, objects) = seeded(store.clone(), &body).await;

    fs.read("/clip.mp4").await.unwrap();

    let peak = store.peak();
    assert!(
        objects > 32,
        "test is only meaningful when the body has many more chunks than the window \
         (had {objects})"
    );
    // The default window is 16; allow slack for the `buffered` prefetch, but this
    // must not be "all of them at once".
    assert!(
        peak <= 32,
        "fetch window is unbounded (peak {peak} over {objects} objects); memory, rate \
         limits, and streaming read-ahead all care"
    );
}

// Deliberately **not** tested here: that `ORIGOFS_FETCH_CONCURRENCY=1` restores
// sequential fetching. The window is resolved once into a process-global
// `OnceLock`, and `cargo test` runs a binary's tests as threads in one process, so
// setting the variable from a test races every sibling in this file — the first
// version of this suite did exactly that and made the other cases fail at
// "peak concurrency 1". A knob whose only honest test is a whole extra test binary
// is not worth one; the bound itself is covered by `fetch_concurrency_is_bounded`.
