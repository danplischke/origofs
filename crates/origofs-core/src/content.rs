//! The content store: a pluggable, content-addressed blob store (`docs/DESIGN.md` §4a).
//!
//! M0 ships one backend, [`LocalCasStore`], which keeps blobs in a sharded
//! directory. M1 adds FastCDC chunking + manifests and an S3 backend behind the
//! same [`ContentStore`] trait.

use crate::error::{OrigoFSError, Result};
use crate::types::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;

/// A content-addressed blob store. Writes are idempotent: storing identical
/// bytes yields the same [`Hash`] and does not duplicate storage.
#[async_trait]
pub trait ContentStore: Send + Sync {
    /// Store `bytes` and return their content address.
    async fn put(&self, bytes: &[u8]) -> Result<Hash>;

    /// Store `bytes` under an explicit `key` rather than `Hash::of(bytes)`.
    ///
    /// This exists for transforming layers such as [`EncryptedStore`], which
    /// keep the plaintext hash as the address while storing ciphertext. The
    /// caller owns the addressing invariant; content-addressed backends simply
    /// write the bytes at `key`.
    ///
    /// [`EncryptedStore`]: crate::encrypt::EncryptedStore
    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()>;

    /// Fetch the full blob for `hash`.
    async fn get(&self, hash: &Hash) -> Result<Bytes>;

    /// Fetch the byte range `[off, off + len)`, clamped to the blob's end.
    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes>;

    /// Whether `hash` is present.
    async fn has(&self, hash: &Hash) -> Result<bool>;

    /// Enumerate every stored object's content address. Used by garbage
    /// collection to find unreachable objects.
    async fn list(&self) -> Result<Vec<Hash>>;

    /// Delete an object, returning the bytes freed. Idempotent: deleting an
    /// absent hash succeeds and frees `0`.
    async fn delete(&self, hash: &Hash) -> Result<u64>;

    /// Flush any buffered writes to durable storage. Most backends write
    /// immediately, so the default is a no-op; batching layers such as
    /// [`PackStore`] override it to seal the open pack.
    ///
    /// [`PackStore`]: crate::pack::PackStore
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Compact storage, reclaiming space held by deleted objects, and return the
    /// bytes reclaimed. A no-op for stores that delete in place; [`PackStore`]
    /// rewrites packs to drop dead chunks.
    ///
    /// [`PackStore`]: crate::pack::PackStore
    async fn repack(&self) -> Result<u64> {
        Ok(0)
    }

    /// A cheap liveness probe of the content backend, for the readiness endpoint
    /// (`/readyz`). The default is a no-op — an in-memory or always-present store
    /// is always ready. Remote backends override it with a cheap reachability
    /// check (an object store issues a single HEAD), and decorators forward to
    /// their inner store, so a probe on the outermost store reaches the real
    /// backend.
    async fn ping(&self) -> Result<()> {
        Ok(())
    }
}

/// A content-addressed store backed by a local directory.
///
/// Blobs live at `<root>/objects/<aa>/<rest-of-hex>`, sharded by the first byte
/// of the hash to keep directories small.
pub struct LocalCasStore {
    root: PathBuf,
    /// Number of `fsync`s performed by durable writes. A write fsyncs the
    /// object's bytes and its parent directory before returning; this counter
    /// lets tests assert the durability barrier actually ran (C3). Not part of
    /// the [`ContentStore`] contract.
    syncs: AtomicU64,
}

impl LocalCasStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(root.join("objects")).await?;
        Ok(Self {
            root,
            syncs: AtomicU64::new(0),
        })
    }

    fn path_for(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join("objects").join(&hex[0..2]).join(&hex[2..])
    }

    async fn exists(path: &Path) -> bool {
        tokio::fs::metadata(path).await.is_ok()
    }

    /// Number of `fsync` operations durable writes have performed so far.
    /// Exposed for tests to verify the durability barrier ran.
    pub fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Write `bytes` at `path` via a temp sibling + rename, so readers never
    /// observe a partial blob — and fsync so a crash can't lose it (C3).
    ///
    /// The temp file's contents are fsynced *before* the rename, so a crash can
    /// never leave the object durably named over unwritten (zero/torn) bytes;
    /// then the parent directory is fsynced so the rename entry itself survives.
    /// This establishes the invariant the metadata layer relies on: content is
    /// on disk before any metadata references it.
    async fn write_at(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = path.with_extension("tmp");
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(bytes).await?;
        f.sync_all().await?;
        drop(f);
        self.syncs.fetch_add(1, Ordering::Relaxed);
        tokio::fs::rename(&tmp, path).await?;
        // Directory fsync makes the rename durable. Unix-only: Windows has no
        // portable directory-fsync, and the temp-file fsync above still bounds
        // the exposure there.
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            let dir = tokio::fs::File::open(parent).await?;
            dir.sync_all().await?;
            self.syncs.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[async_trait]
impl ContentStore for LocalCasStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = Hash::of(bytes);
        let path = self.path_for(&hash);
        if Self::exists(&path).await {
            return Ok(hash); // already stored — content-addressed, so identical
        }
        self.write_at(&path, bytes).await?;
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        let path = self.path_for(key);
        if Self::exists(&path).await {
            return Ok(());
        }
        self.write_at(&path, bytes).await
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let path = self.path_for(hash);
        match tokio::fs::read(&path).await {
            Ok(v) => Ok(Bytes::from(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(OrigoFSError::ContentMissing(hash.to_hex()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        // M0 reads the whole blob then slices. M1's chunk manifests make this a
        // true ranged read that only fetches the covering chunks.
        let full = self.get(hash).await?;
        let start = (off as usize).min(full.len());
        let end = start.saturating_add(len as usize).min(full.len());
        Ok(full.slice(start..end))
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        Ok(Self::exists(&self.path_for(hash)).await)
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        let objects = self.root.join("objects");
        let mut out = Vec::new();
        let mut shards = match tokio::fs::read_dir(&objects).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(shard) = shards.next_entry().await? {
            if !shard.file_type().await?.is_dir() {
                continue;
            }
            let prefix = shard.file_name().to_string_lossy().into_owned();
            let mut entries = tokio::fs::read_dir(shard.path()).await?;
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".tmp") {
                    continue; // an in-flight write; not yet a committed object
                }
                if let Some(h) = Hash::from_hex(&format!("{prefix}{name}")) {
                    out.push(h);
                }
            }
        }
        Ok(out)
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        let path = self.path_for(hash);
        let size = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len(),
            Err(_) => return Ok(0),
        };
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(size),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    async fn ping(&self) -> Result<()> {
        // The object root must exist and be reachable on the local filesystem.
        tokio::fs::metadata(self.root.join("objects")).await?;
        Ok(())
    }
}

/// Delegating impl so `Arc<dyn ContentStore>` (and `Arc<ConcreteStore>`) is itself
/// a [`ContentStore`]. This lets the engine and [`TieredStore`] hold trait objects.
#[async_trait]
impl<T: ContentStore + ?Sized> ContentStore for Arc<T> {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        (**self).put(bytes).await
    }
    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        (**self).put_keyed(key, bytes).await
    }
    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        (**self).get(hash).await
    }
    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        (**self).get_range(hash, off, len).await
    }
    async fn has(&self, hash: &Hash) -> Result<bool> {
        (**self).has(hash).await
    }
    async fn list(&self) -> Result<Vec<Hash>> {
        (**self).list().await
    }
    async fn delete(&self, hash: &Hash) -> Result<u64> {
        (**self).delete(hash).await
    }
    async fn flush(&self) -> Result<()> {
        (**self).flush().await
    }
    async fn repack(&self) -> Result<u64> {
        (**self).repack().await
    }
    async fn ping(&self) -> Result<()> {
        (**self).ping().await
    }
}

/// An in-memory content store — for tests and ephemeral workspaces.
#[derive(Default)]
pub struct MemStore {
    map: Mutex<HashMap<Hash, Bytes>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct blobs stored (useful for dedup assertions in tests).
    pub fn len(&self) -> usize {
        self.map.lock().expect("mem store poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl ContentStore for MemStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = Hash::of(bytes);
        self.map
            .lock()
            .expect("mem store poisoned")
            .entry(hash)
            .or_insert_with(|| Bytes::copy_from_slice(bytes));
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.map
            .lock()
            .expect("mem store poisoned")
            .entry(*key)
            .or_insert_with(|| Bytes::copy_from_slice(bytes));
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.map
            .lock()
            .expect("mem store poisoned")
            .get(hash)
            .cloned()
            .ok_or_else(|| OrigoFSError::ContentMissing(hash.to_hex()))
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        let full = self.get(hash).await?;
        let start = (off as usize).min(full.len());
        let end = start.saturating_add(len as usize).min(full.len());
        Ok(full.slice(start..end))
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        Ok(self
            .map
            .lock()
            .expect("mem store poisoned")
            .contains_key(hash))
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        Ok(self
            .map
            .lock()
            .expect("mem store poisoned")
            .keys()
            .copied()
            .collect())
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        Ok(self
            .map
            .lock()
            .expect("mem store poisoned")
            .remove(hash)
            .map(|b| b.len() as u64)
            .unwrap_or(0))
    }
}

/// A two-tier store: a fast local `cache` in front of a (possibly remote)
/// `backend` (`docs/DESIGN.md` §4a). Reads are served from cache and populate it
/// on miss; writes are write-through to the backend and cached best-effort.
///
/// M1 is write-through for durability simplicity; write-back batching is a later
/// optimization. [`TieredStore::prefetch`] warms the cache for a file's chunks.
pub struct TieredStore {
    cache: Arc<dyn ContentStore>,
    backend: Arc<dyn ContentStore>,
}

impl TieredStore {
    pub fn new(cache: Arc<dyn ContentStore>, backend: Arc<dyn ContentStore>) -> Self {
        Self { cache, backend }
    }

    /// Warm the cache with `hashes` (e.g. a manifest's chunks, on open).
    pub async fn prefetch(&self, hashes: &[Hash]) -> Result<()> {
        for h in hashes {
            if !self.cache.has(h).await?
                && let Ok(bytes) = self.backend.get(h).await
            {
                let _ = self.cache.put(&bytes).await;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ContentStore for TieredStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = self.backend.put(bytes).await?;
        let _ = self.cache.put(bytes).await; // best-effort
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.backend.put_keyed(key, bytes).await?;
        let _ = self.cache.put_keyed(key, bytes).await; // best-effort
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        if let Ok(bytes) = self.cache.get(hash).await {
            return Ok(bytes);
        }
        let bytes = self.backend.get(hash).await?;
        let _ = self.cache.put(&bytes).await;
        Ok(bytes)
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        if self.cache.has(hash).await? {
            return self.cache.get_range(hash, off, len).await;
        }
        self.backend.get_range(hash, off, len).await
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        Ok(self.cache.has(hash).await? || self.backend.has(hash).await?)
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        // The backend is authoritative (writes are write-through); the cache
        // holds only a subset.
        self.backend.list().await
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        let freed = self.backend.delete(hash).await?;
        let _ = self.cache.delete(hash).await; // best-effort cache eviction
        Ok(freed)
    }

    async fn ping(&self) -> Result<()> {
        // The backend is authoritative for durability; the cache is best-effort.
        self.backend.ping().await
    }
}

/// A [`ContentStore`] decorator that **verifies integrity on read**: every
/// object fetched is re-hashed and checked against the address it was fetched by,
/// so a bit-rotted, truncated, or tampered object surfaces as
/// [`OrigoFSError::Corrupt`] instead of being served as authentic (audit M1).
///
/// Wrap the workspace's *outermost* content store with this. Content addressing
/// guarantees `Hash::of(get(h)) == h` at that boundary for every composition —
/// including packing (a chunk's address stays its hash) and encryption
/// (`EncryptedStore::get` returns the plaintext, whose hash is the address, and
/// its AEAD tag already rejects corrupt ciphertext underneath). It must **not**
/// wrap a `put_keyed`-addressed inner store (raw ciphertext, or a pack index),
/// where the address is deliberately not `Hash::of(value)`.
///
/// Recommended always-on for remote backends (S3 can bit-rot); opt-in for local,
/// where re-hashing every read trades a little CPU for the same guarantee.
pub struct VerifyingStore {
    inner: Arc<dyn ContentStore>,
}

impl VerifyingStore {
    pub fn new(inner: Arc<dyn ContentStore>) -> Self {
        Self { inner }
    }
}

/// Check that `bytes` hash to `expect`; otherwise the object is corrupt.
fn verify_integrity(expect: &Hash, bytes: &[u8]) -> Result<()> {
    let actual = Hash::of(bytes);
    if &actual == expect {
        Ok(())
    } else {
        Err(OrigoFSError::Corrupt(format!(
            "content {} failed its integrity check (got {})",
            expect.to_hex(),
            actual.to_hex()
        )))
    }
}

#[async_trait]
impl ContentStore for VerifyingStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        self.inner.put(bytes).await
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.inner.put_keyed(key, bytes).await
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let bytes = self.inner.get(hash).await?;
        verify_integrity(hash, &bytes)?;
        Ok(bytes)
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        // A partial slice can't be verified against the whole-object hash, so
        // fetch and verify the whole object (origofs objects are chunk-sized), then
        // slice. This keeps ranged reads honest at the cost of pulling the whole
        // chunk — the same tradeoff EncryptedStore already makes.
        let full = self.get(hash).await?;
        let start = (off as usize).min(full.len());
        let end = start.saturating_add(len as usize).min(full.len());
        Ok(full.slice(start..end))
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        self.inner.has(hash).await
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        self.inner.list().await
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        self.inner.delete(hash).await
    }

    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    async fn repack(&self) -> Result<u64> {
        self.inner.repack().await
    }

    async fn ping(&self) -> Result<()> {
        self.inner.ping().await
    }
}
