//! Pack layer: many small chunks collapse into few large objects, reads are
//! ranged into a pack, the index survives a reopen, and repack reclaims the
//! space of deleted chunks — plus end-to-end through the engine.

use origofs_core::{ContentStore, Fs, Hash, MemStore, PackStore, SqliteMetadataStore};
use std::sync::Arc;

/// (pack_store, data backend, index backend), target = `target` bytes.
fn packed(target: usize) -> (PackStore, Arc<MemStore>, Arc<MemStore>) {
    let data = Arc::new(MemStore::new());
    let index = Arc::new(MemStore::new());
    let store = PackStore::with_target(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
        target,
    );
    (store, data, index)
}

fn blob(len: usize, seed: u64) -> Vec<u8> {
    // Distinct seeds must yield distinct bytes; xorshift only needs non-zero state.
    let mut x = if seed == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        seed
    };
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
async fn many_chunks_collapse_into_few_packs() {
    let (store, data, index) = packed(4096);

    let mut hashes = Vec::new();
    for i in 0..100u64 {
        hashes.push(store.put(&blob(200, i)).await.unwrap());
    }
    store.flush().await.unwrap();

    // 100 chunks × 200 B into 4 KiB packs ≈ 5 objects, nowhere near 100.
    assert!(data.len() <= 8, "packs: {}", data.len());
    assert!(data.len() < 100);
    assert_eq!(index.len(), 100, "one index entry per chunk");

    // every chunk still reads back exactly
    for (i, h) in hashes.iter().enumerate() {
        assert_eq!(&store.get(h).await.unwrap()[..], &blob(200, i as u64)[..]);
    }
}

#[tokio::test]
async fn unflushed_reads_and_dedup() {
    let (store, data, _index) = packed(1 << 20);
    let h = store.put(b"hello pack").await.unwrap();
    assert_eq!(h, Hash::of(b"hello pack"));

    // readable before sealing (served from the open buffer), and no pack yet
    assert_eq!(&store.get(&h).await.unwrap()[..], b"hello pack");
    assert!(store.has(&h).await.unwrap());
    assert!(store.list().await.unwrap().contains(&h));
    assert_eq!(data.len(), 0, "nothing sealed yet");

    // storing identical bytes is a no-op
    store.put(b"hello pack").await.unwrap();

    store.flush().await.unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(&store.get(&h).await.unwrap()[..], b"hello pack");
}

#[tokio::test]
async fn ranged_read_into_a_pack() {
    let (store, _data, _index) = packed(1 << 20);
    let body = blob(1000, 9);
    let h = store.put(&body).await.unwrap();
    store.flush().await.unwrap();
    assert_eq!(
        &store.get_range(&h, 10, 5).await.unwrap()[..],
        &body[10..15]
    );
    assert_eq!(
        &store.get_range(&h, 995, 50).await.unwrap()[..],
        &body[995..1000]
    );
}

#[tokio::test]
async fn index_survives_a_reopen() {
    let data = Arc::new(MemStore::new());
    let index = Arc::new(MemStore::new());

    let hashes = {
        let store = PackStore::new(
            data.clone() as Arc<dyn ContentStore>,
            index.clone() as Arc<dyn ContentStore>,
        );
        let mut hs = Vec::new();
        for i in 0..20u64 {
            hs.push(store.put(&blob(500, i)).await.unwrap());
        }
        store.flush().await.unwrap();
        hs
    };

    // A brand-new PackStore over the same backends (no in-memory buffer) still
    // resolves every chunk through the persisted index + packs.
    let reopened = PackStore::new(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
    );
    for (i, h) in hashes.iter().enumerate() {
        assert_eq!(
            &reopened.get(h).await.unwrap()[..],
            &blob(500, i as u64)[..]
        );
    }
}

#[tokio::test]
async fn repack_reclaims_deleted_chunks() {
    // target 1500 with 1000-byte chunks => two chunks per pack.
    let (store, data, _index) = packed(1500);
    let mut h = Vec::new();
    for i in 0..10u64 {
        h.push(store.put(&blob(1000, i)).await.unwrap());
    }
    store.flush().await.unwrap();
    let packs_before = data.len();
    assert_eq!(packs_before, 5);

    // pack0 = {h0,h1} fully dead; pack2 = {h4,h5} partially dead.
    store.delete(&h[0]).await.unwrap();
    store.delete(&h[1]).await.unwrap();
    store.delete(&h[4]).await.unwrap();

    let reclaimed = store.repack().await.unwrap();
    assert!(reclaimed > 0, "dead pack bytes were reclaimed");
    assert!(data.len() < packs_before, "fewer pack objects after repack");

    // deleted chunks are gone; everything else still reads.
    for i in [0usize, 1, 4] {
        assert!(store.get(&h[i]).await.is_err());
    }
    for i in [2usize, 3, 5, 6, 7, 8, 9] {
        assert_eq!(
            &store.get(&h[i]).await.unwrap()[..],
            &blob(1000, i as u64)[..]
        );
    }
}

#[tokio::test]
async fn engine_writes_land_in_packs() {
    let data = Arc::new(MemStore::new());
    let index = Arc::new(MemStore::new());
    let store = Arc::new(PackStore::new(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
    ));
    let fs = Fs::new(SqliteMetadataStore::open_in_memory().unwrap(), store);
    fs.init().await.unwrap();

    let big = blob(2 * 1024 * 1024, 42); // many FastCDC chunks
    fs.write("/big.bin", &big).await.unwrap();
    fs.commit("packer", "snapshot").await.unwrap(); // flushes the open pack

    // The workspace round-trips...
    assert_eq!(&fs.read("/big.bin").await.unwrap()[..], &big[..]);
    // ...and the many logical objects (chunks + manifest + tree + commit, plus
    // the ref-mirror snapshot the commit seals for recovery) live in far fewer
    // physical pack objects. (Per-commit mirror packs are later coalesced by
    // `repack`.)
    assert!(
        data.len() < index.len(),
        "packs {} should be far fewer than objects {}",
        data.len(),
        index.len()
    );
    assert!(index.len() > 8, "the big file produced many chunks");
    assert!(
        data.len() <= 3,
        "they packed into a handful of objects: {}",
        data.len()
    );
}

/// A data backend that starts failing `put` on demand — enough to stop a repack
/// exactly where a crash would, in the middle of moving survivors.
struct FailAfter {
    inner: Arc<MemStore>,
    fail: std::sync::atomic::AtomicBool,
}

impl FailAfter {
    fn new(inner: Arc<MemStore>) -> Self {
        Self {
            inner,
            fail: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn arm(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn armed(&self) -> bool {
        self.fail.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ContentStore for FailAfter {
    async fn put(&self, bytes: &[u8]) -> origofs_core::Result<Hash> {
        if self.armed() {
            return Err(origofs_core::OrigoFSError::Content("injected".into()));
        }
        self.inner.put(bytes).await
    }
    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> origofs_core::Result<()> {
        self.inner.put_keyed(key, bytes).await
    }
    async fn get(&self, hash: &Hash) -> origofs_core::Result<bytes::Bytes> {
        self.inner.get(hash).await
    }
    async fn get_range(
        &self,
        hash: &Hash,
        off: u64,
        len: u64,
    ) -> origofs_core::Result<bytes::Bytes> {
        self.inner.get_range(hash, off, len).await
    }
    async fn has(&self, hash: &Hash) -> origofs_core::Result<bool> {
        self.inner.has(hash).await
    }
    async fn list(&self) -> origofs_core::Result<Vec<Hash>> {
        self.inner.list().await
    }
    async fn delete(&self, hash: &Hash) -> origofs_core::Result<u64> {
        self.inner.delete(hash).await
    }
}

/// A repack that fails partway must leave every live chunk readable.
///
/// `repack` moves the survivors of a partially-dead pack into a fresh one. It used
/// to clear a survivor's index pointer *before* staging it — and staging only
/// buffers in memory. So if the seal then failed (a crash, a full disk, an S3
/// error), the chunk had no index entry while its bytes still sat in the old pack.
/// A chunk with no index entry is invisible to the *next* repack, which reads that
/// pack as fully dead and deletes it: permanent loss, from the one operation whose
/// job is to reclaim space safely.
///
/// Here the data backend is rigged to fail the new pack's write, which is exactly
/// that window. The survivor must still be readable afterwards, and a later
/// successful repack must still find it.
#[tokio::test]
async fn a_failed_repack_leaves_live_chunks_readable() {
    let raw = Arc::new(MemStore::new());
    let data = Arc::new(FailAfter::new(raw.clone()));
    let index = Arc::new(MemStore::new());
    let store = PackStore::with_target(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
        1 << 20,
    );

    let survivor = blob(4096, 1);
    let doomed = blob(4096, 2);
    let sh = store.put(&survivor).await.unwrap();
    let dh = store.put(&doomed).await.unwrap();
    store.flush().await.unwrap();
    // One pack, two members, one now dead -> the partially-dead case.
    store.delete(&dh).await.unwrap();

    // Fail the new pack's write, mid-repack.
    data.arm();
    assert!(
        store.repack().await.is_err(),
        "the injected failure should surface"
    );

    // Restart. This is what makes the window matter: the failed repack left
    // survivors in the *in-memory* staging buffer, so the same process can still
    // read them and the damage is invisible. A fresh store over the same durable
    // backends is what an operator actually has after a crash.
    drop(store);
    data.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    let after_crash = PackStore::with_target(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
        1 << 20,
    );

    // The decisive assertion: the old pack is still there and the index still
    // points at it, so the chunk is reachable from durable state alone.
    let got = after_crash
        .get(&sh)
        .await
        .expect("a live chunk must survive a failed repack plus a restart");
    assert_eq!(&got[..], &survivor[..]);
    assert_eq!(Hash::of(&got), sh);

    // And a re-run — what an operator does next — completes and keeps it.
    after_crash.repack().await.unwrap();
    assert_eq!(&after_crash.get(&sh).await.unwrap()[..], &survivor[..]);
}

/// The index is a *mutable* keyed store: a chunk's entry changes when a repack
/// moves it. `put_keyed` is insert-if-absent in every backend, so it silently
/// dropped that update — which is why repack had to delete the old pointer first.
#[tokio::test]
async fn index_entries_can_be_updated_in_place() {
    let index = Arc::new(MemStore::new());
    let key = Hash::of(b"chunk");

    index.put_keyed(&key, b"first").await.unwrap();
    // Insert-if-absent: the update is dropped.
    index.put_keyed(&key, b"second").await.unwrap();
    assert_eq!(&index.get(&key).await.unwrap()[..], b"first");

    // Replace actually replaces, without the key ever resolving to nothing.
    index.replace_keyed(&key, b"second").await.unwrap();
    assert_eq!(&index.get(&key).await.unwrap()[..], b"second");
}
