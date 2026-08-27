//! The cache tier is bounded, evicts, prefetches concurrently, and treats a bad
//! cache entry as a miss (issue #114).
//!
//! `TieredStore` was complete and tested and reachable from **no** `open_*`
//! constructor, so in practice every remote-backed workspace re-fetched every chunk
//! from the bucket on every read. The reason it could not simply be switched on is
//! the first test below: it had no size bound, no eviction, and no free-space
//! floor, so turning it on would have grown without limit until the disk filled.

use async_trait::async_trait;
use bytes::Bytes;
use origofs_core::{CacheLimits, ContentStore, Hash, MemStore, OrigoFSError, TieredStore};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// A backend that counts reads and can inject latency, so "did this come from the
/// cache" and "did these overlap" are both observable.
struct CountingBackend {
    inner: MemStore,
    gets: AtomicUsize,
    inflight: AtomicUsize,
    peak: AtomicUsize,
    delay: Duration,
}

impl CountingBackend {
    fn new(delay: Duration) -> Self {
        Self {
            inner: MemStore::new(),
            gets: AtomicUsize::new(0),
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            delay,
        }
    }
    fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ContentStore for CountingBackend {
    async fn get(&self, hash: &Hash) -> Result<Bytes, OrigoFSError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let n = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(n, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let r = self.inner.get(hash).await;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        r
    }
    async fn put(&self, bytes: &[u8]) -> Result<Hash, OrigoFSError> {
        self.inner.put(bytes).await
    }
    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<(), OrigoFSError> {
        self.inner.put_keyed(key, bytes).await
    }
    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes, OrigoFSError> {
        self.inner.get_range(hash, off, len).await
    }
    async fn has(&self, hash: &Hash) -> Result<bool, OrigoFSError> {
        self.inner.has(hash).await
    }
    async fn delete(&self, hash: &Hash) -> Result<u64, OrigoFSError> {
        self.inner.delete(hash).await
    }
    async fn list(&self) -> Result<Vec<Hash>, OrigoFSError> {
        self.inner.list().await
    }
}

fn body(seed: u8, len: usize) -> Vec<u8> {
    vec![seed; len]
}

/// The headline: the cache stays under its byte bound, evicting as it goes.
///
/// This is the property whose absence kept `TieredStore` out of every `open_*`
/// recipe — an unbounded on-disk cache eventually fills the user's disk.
#[tokio::test]
async fn the_cache_stays_under_its_byte_bound() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());
    // Room for ~4 of the 1000-byte objects below.
    let tier = TieredStore::with_limits(cache.clone(), backend.clone(), CacheLimits::bytes(4_000));

    let mut hashes = Vec::new();
    for i in 0..20u8 {
        hashes.push(tier.put(&body(i, 1000)).await.unwrap());
    }

    assert!(
        tier.cached_bytes() <= 4_000,
        "cache grew past its bound: {} bytes",
        tier.cached_bytes()
    );
    assert!(
        cache.list().await.unwrap().len() < 20,
        "nothing was evicted; the bound is not being enforced"
    );
    // Everything is still readable — eviction drops cache copies, never data.
    for (i, h) in hashes.iter().enumerate() {
        assert_eq!(
            &tier.get(h).await.unwrap()[..],
            &body(i as u8, 1000)[..],
            "an evicted object must still be readable from the backend"
        );
    }
}

/// Eviction is least-**recently-used**, not first-in-first-out: an object that
/// keeps being read survives while colder ones are dropped. A FIFO would evict
/// exactly the hot entry a cache exists to keep.
#[tokio::test]
async fn eviction_is_least_recently_used() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());
    let tier = TieredStore::with_limits(cache.clone(), backend.clone(), CacheLimits::bytes(3_000));

    let hot = tier.put(&body(1, 1000)).await.unwrap();
    let cold = tier.put(&body(2, 1000)).await.unwrap();

    // Keep touching `hot` while new objects push the cache past its bound.
    for i in 10..20u8 {
        tier.get(&hot).await.unwrap();
        tier.put(&body(i, 1000)).await.unwrap();
    }

    assert!(
        cache.has(&hot).await.unwrap(),
        "the repeatedly-read object was evicted; eviction is not LRU"
    );
    assert!(
        !cache.has(&cold).await.unwrap(),
        "the never-touched object should have been evicted first"
    );
}

/// A **corrupt** cache entry is a refetch, not an error.
///
/// This has to happen at the tier boundary. The `VerifyingStore` in the `open_*`
/// stacks sits *outside* the split, so it cannot tell which tier produced the bytes
/// and would reject a read the backend could still serve perfectly well.
#[tokio::test]
async fn a_corrupt_cache_entry_is_refetched_not_failed() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());
    let tier = TieredStore::new(cache.clone(), backend.clone());

    let data = body(7, 500);
    let h = tier.put(&data).await.unwrap();
    assert!(
        cache.has(&h).await.unwrap(),
        "the put should have cached it"
    );

    // Corrupt the cached copy behind the tier's back, keyed to the same address.
    cache.put_keyed(&h, b"tampered").await.unwrap();

    let got = tier.get(&h).await.unwrap();
    assert_eq!(
        &got[..],
        &data[..],
        "a corrupt cache entry must be refetched from the backend, not served or failed"
    );
    assert!(
        !cache.has(&h).await.unwrap() || cache.get(&h).await.unwrap()[..] == data[..],
        "the bad entry must have been evicted or replaced, not left to fail the next read"
    );
}

/// A cache that has simply lost an object (a full disk truncating a write, an
/// external cleaner) is also a miss rather than an error.
#[tokio::test]
async fn a_missing_cache_entry_is_a_miss_not_an_error() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());
    let tier = TieredStore::new(cache.clone(), backend.clone());

    let data = body(3, 500);
    let h = tier.put(&data).await.unwrap();
    cache.delete(&h).await.unwrap();

    assert_eq!(&tier.get(&h).await.unwrap()[..], &data[..]);
    assert!(
        tier.has(&h).await.unwrap(),
        "`has` must fall through to the backend rather than trusting an empty cache"
    );
}

/// The cache actually serves reads — the whole point. A second read of the same
/// object must not touch the backend again.
#[tokio::test]
async fn a_cached_read_does_not_reach_the_backend() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());
    let tier = TieredStore::new(cache.clone(), backend.clone());

    let h = backend.put(&body(1, 500)).await.unwrap();
    tier.get(&h).await.unwrap();
    let after_first = backend.gets();
    tier.get(&h).await.unwrap();
    tier.get(&h).await.unwrap();

    assert_eq!(
        backend.gets(),
        after_first,
        "repeat reads must be served from the cache"
    );
}

/// `prefetch` overlaps its fetches. It was a sequential `has`/`get`/`put` loop, so
/// warming a file's chunks cost one full round trip per chunk against exactly the
/// backend latency the cache exists to hide.
#[tokio::test]
async fn prefetch_is_concurrent() {
    let backend = Arc::new(CountingBackend::new(Duration::from_millis(20)));
    let cache = Arc::new(MemStore::new());
    let tier = TieredStore::new(cache.clone(), backend.clone());

    let mut hashes = Vec::new();
    for i in 0..32u8 {
        hashes.push(backend.put(&body(i, 100)).await.unwrap());
    }

    tier.prefetch(&hashes).await.unwrap();

    assert!(
        backend.peak() > 1,
        "prefetch fetched serially (peak concurrency {})",
        backend.peak()
    );
    for h in &hashes {
        assert!(
            cache.has(h).await.unwrap(),
            "prefetch left a chunk uncached"
        );
    }
}

/// One unfetchable chunk must not abandon the warm-up of the rest: a prefetch is
/// an optimization, and failing it wholesale would make a single missing object
/// cost every other chunk's warm read.
#[tokio::test]
async fn prefetch_survives_an_unfetchable_chunk() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());
    let tier = TieredStore::new(cache.clone(), backend.clone());

    let good: Vec<Hash> = {
        let mut v = Vec::new();
        for i in 0..4u8 {
            v.push(backend.put(&body(i, 100)).await.unwrap());
        }
        v
    };
    // A hash the backend has never heard of.
    let missing = Hash::of(b"never stored anywhere");
    let mut all = good.clone();
    all.push(missing);

    tier.prefetch(&all).await.unwrap();

    for h in &good {
        assert!(
            cache.has(h).await.unwrap(),
            "a missing chunk aborted the rest of the prefetch"
        );
    }
}

/// `warm_index` accounts for what a cache directory already held, so a bound set
/// on a restarted process covers the existing contents rather than only what that
/// process happens to touch.
#[tokio::test]
async fn a_warmed_index_accounts_for_pre_existing_cache_contents() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());

    // Fill the cache directly, as a previous process would have left it.
    for i in 0..10u8 {
        let b = body(i, 1000);
        backend.put(&b).await.unwrap();
        cache.put(&b).await.unwrap();
    }

    let tier = TieredStore::with_limits(cache.clone(), backend.clone(), CacheLimits::bytes(4_000));
    assert_eq!(
        tier.cached_bytes(),
        0,
        "a fresh tier has not looked at the cache yet"
    );

    tier.warm_index().await.unwrap();

    assert!(
        tier.cached_bytes() <= 4_000,
        "warm_index must bring a pre-existing cache back inside its bound, had {}",
        tier.cached_bytes()
    );
    assert!(
        cache.list().await.unwrap().len() < 10,
        "warm_index did not evict down to the bound"
    );
}

/// `size_of` survives the trip through `Arc<dyn ContentStore>`.
///
/// The blanket `impl ContentStore for Arc<T>` must forward every method that has a
/// **default** body, because omitting one compiles fine and silently downgrades
/// every backend reached through an `Arc` to that default. `replace_keyed` was the
/// first to be caught this way (see `pack.rs::arc_forwards_replace_keyed_atomically`);
/// `size_of` was the second, and it cost `warm_index` its ability to see a
/// pre-existing cache at all — it accounted for nothing and evicted nothing, while
/// every assertion about the *tracked* size still passed.
///
/// Asserting through all three shapes the blanket impl serves.
#[tokio::test]
async fn arc_forwards_size_of() {
    let store = Arc::new(MemStore::new());
    let h = store.put(b"twelve bytes").await.unwrap();

    assert_eq!(store.size_of(&h).await.unwrap(), Some(12), "concrete impl");

    let erased: Arc<dyn ContentStore> = store.clone();
    assert_eq!(
        erased.size_of(&h).await.unwrap(),
        Some(12),
        "Arc<dyn ContentStore> fell through to the `None` default"
    );

    let concrete: Arc<MemStore> = store.clone();
    assert_eq!(
        concrete.size_of(&h).await.unwrap(),
        Some(12),
        "Arc<ConcreteStore> fell through to the `None` default"
    );
}

/// An unbounded tier is still available and still does no eviction — the old
/// behaviour, kept for a cache that is bounded by other means (a tmpfs with its
/// own limit, a fixed-size `MemStore`) and for tests.
#[tokio::test]
async fn an_unbounded_tier_never_evicts() {
    let backend = Arc::new(CountingBackend::new(Duration::ZERO));
    let cache = Arc::new(MemStore::new());
    let tier = TieredStore::new(cache.clone(), backend.clone());

    for i in 0..20u8 {
        tier.put(&body(i, 1000)).await.unwrap();
    }
    assert_eq!(
        cache.list().await.unwrap().len(),
        20,
        "an explicitly unbounded cache must not evict"
    );
}
