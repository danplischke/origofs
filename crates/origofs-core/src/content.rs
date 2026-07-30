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
// `parking_lot::Mutex` rather than `std::sync::Mutex`: it does not poison, so a
// panic under the lock cannot turn every later `MemStore` operation into a
// panic of its own. `SqliteMetadataStore` moved for the same reason.
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

/// How stale an already-stored object may be before a deduplicating `put`
/// refreshes its recency stamp (see [`ContentStore::touch`]).
///
/// The refresh exists to stop a garbage collection from sweeping content a writer
/// has just deduplicated onto. An object younger than this is already comfortably
/// inside any valid grace period, so refreshing it buys nothing — this threshold
/// is what keeps the refresh off the hot path, where deduping onto
/// recently-written content is the norm.
///
/// It is a *floor* on the grace period, not merely a tuning knob: a sweep whose
/// grace is shorter than this could reclaim an object in the band between the two.
/// [`Fs::gc_with_grace`](crate::Fs::gc_with_grace) rejects a shorter grace for
/// that reason.
pub const DEDUP_REFRESH_AFTER_SECS: u64 = 60;

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

    /// **Replace** the value stored under `key`, atomically where the backend can.
    ///
    /// [`put_keyed`](Self::put_keyed) is insert-if-absent in every backend — right
    /// for a content-addressed key, where the value is a function of the key and a
    /// second write would be redundant. But `put_keyed` also serves genuinely
    /// *mutable* keyed stores: a [`PackStore`](crate::PackStore)'s chunk→location
    /// index changes whenever a repack moves a chunk to a different pack. There,
    /// insert-if-absent silently drops the update.
    ///
    /// The workaround — delete, then put — leaves a window in which the key
    /// resolves to nothing at all. For the pack index that window is unrecoverable:
    /// a chunk with no index entry is invisible to `repack`, which then reads the
    /// pack holding it as fully dead and deletes it. Hence an explicit atomic
    /// replace.
    ///
    /// The default is that unsafe delete-then-put, for a custom backend that can do
    /// no better; every backend here overrides it with a genuinely atomic write
    /// (a rename, a map insert, an object PUT).
    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.delete(key).await?;
        self.put_keyed(key, bytes).await
    }

    /// Write a small **named slot** beside the store's objects, overwriting any
    /// previous value.
    ///
    /// Slots are *not* content-addressed and live **outside** the object
    /// namespace, so [`list`](Self::list) never returns them and `gc` can never
    /// sweep them. They exist for the handful of facts that must be findable
    /// without knowing a hash and must survive a metadata-DB loss — today only the
    /// store format descriptor stamped by [`Fs::init`](crate::Fs::init)
    /// (`crate::format`). Keep values tiny: a slot is one object/file, never
    /// chunked or deduplicated.
    ///
    /// `name` must be a single non-empty component of `[a-z0-9._-]` (no `/`, no
    /// `..`), so a slot can never escape the store's root.
    ///
    /// The default discards the write, paired with a `get_meta` that always
    /// reports "never written". A backend that doesn't override **both** forfeits
    /// the open-time format check — it cannot be stamped, so it cannot be
    /// verified. Every backend in this crate overrides them.
    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let _ = (name, bytes);
        Ok(())
    }

    /// Read a named slot, or `None` if it was never written. See
    /// [`put_meta`](Self::put_meta).
    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        let _ = name;
        Ok(None)
    }

    /// Fetch the full blob for `hash`.
    async fn get(&self, hash: &Hash) -> Result<Bytes>;

    /// Fetch the byte range `[off, off + len)`, clamped to the blob's end.
    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes>;

    /// Whether `hash` is present.
    async fn has(&self, hash: &Hash) -> Result<bool>;

    /// Enumerate every stored object's content address. Used by garbage
    /// collection to find unreachable objects.
    async fn list(&self) -> Result<Vec<Hash>>;

    /// Every stored object with its **age in seconds**, measured by the backend's
    /// own clock — `None` when the backend cannot tell.
    ///
    /// This is what makes garbage collection safe alongside writers. Content is
    /// written *before* the metadata that references it (the durability barrier),
    /// so every object is legitimately unreferenced for the window between its
    /// `put` and the commit of the transaction that names it. A mark-and-sweep
    /// that trusts reachability alone deletes exactly those objects — the write in
    /// flight — and the writer then fails with `ContentMissing` on content it had
    /// just stored. Skipping anything younger than a grace period longer than that
    /// window closes it without any per-write bookkeeping.
    ///
    /// The age is relative to the backend's own notion of now, not the engine's
    /// clock, so an injected test clock cannot make a real store look ancient.
    ///
    /// The default reports `None` for everything, which
    /// [`Fs::gc`](crate::Fs::gc) treats as "not safe to sweep" — a custom backend
    /// that cannot date its objects collects nothing rather than collecting
    /// something live.
    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        Ok(self.list().await?.into_iter().map(|h| (h, None)).collect())
    }

    /// Refresh `hash`'s recency stamp, so a concurrent sweep treats it as a write
    /// in flight rather than as long-dead garbage.
    ///
    /// **This is the other half of the age gate, and without it the gate leaks.**
    /// [`list_with_age`](Self::list_with_age) makes garbage collection safe by
    /// skipping anything younger than the grace period — the window between a
    /// `put` and the commit that references it. But `put` is *deduplicating*: it
    /// returns early when the object is already stored, and an object that already
    /// exists does not get a fresh timestamp. So a writer that dedups onto an old,
    /// currently-unreferenced object gets `Ok(hash)` for content the sweep is about
    /// to reclaim, and the commit that follows references a hash that no longer
    /// exists. That is not a rare shape: reverting a file, shared boilerplate, and
    /// checking out an older commit all produce exactly it.
    ///
    /// Each backend's `put`/`put_keyed` therefore refreshes on the dedup path.
    /// Backends age-gate the refresh themselves — an object younger than
    /// [`DEDUP_REFRESH_AFTER_SECS`] is already inside any valid grace period, so
    /// refreshing it would be pure cost. That keeps the common case (deduping onto
    /// recently-written content) free, and pays only where the race is real.
    ///
    /// The default is a no-op, which is correct **only** for a backend that also
    /// leaves `list_with_age` at its default — one that reports no ages collects
    /// nothing, so it has no sweep to race. A backend that overrides
    /// `list_with_age` **must** override this too; `tests/gc.rs::
    /// every_dateable_backend_can_refresh_recency` enforces that for the backends
    /// in this crate.
    async fn touch(&self, hash: &Hash) -> Result<()> {
        let _ = hash;
        Ok(())
    }

    /// Read a small named **sidecar** value, or `None` if it was never written.
    ///
    /// A sidecar is deliberately *outside* the content-addressed namespace: it is
    /// keyed by a name rather than a hash, and [`list`](Self::list) never
    /// enumerates it, so garbage collection cannot see it and therefore cannot
    /// sweep it. That last property is the whole point — the encryption salt lives
    /// here, and a GC pass that reclaimed it would make every encrypted object in
    /// the store permanently undecryptable.
    ///
    /// It also has to sit beside the *content*, not in the metadata database:
    /// losing the database is a survivable event that `fsck --rebuild` recovers
    /// from, and it must not also cost you the ability to read your bytes.
    ///
    /// Sidecars are for small, rarely-changing values only. The default has none.
    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let _ = name;
        Ok(None)
    }

    /// Write a sidecar **only if absent**, returning the value that is stored
    /// afterwards — the caller's bytes, or the existing ones if another writer got
    /// there first.
    ///
    /// Create-if-absent rather than plain write, because the salt is randomly
    /// generated: two processes opening a fresh store concurrently would otherwise
    /// each write a different salt, and whichever landed second would silently
    /// invalidate the key the first had already derived and started writing with.
    /// Returning the stored value makes both agree.
    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        let _ = (name, bytes);
        Err(OrigoFSError::Content(
            "this content backend does not support sidecar values (needed for \
             encryption-at-rest, which stores its key-derivation salt beside the content)"
                .into(),
        ))
    }

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

/// Validate a sidecar name so it can be used as a single path/key component.
///
/// Same rule as every other name that becomes a path: no traversal, no separator,
/// no NUL. Sidecar names are internal today, but this is the boundary where a
/// future caller-supplied one would escape.
fn sidecar_file(name: &str) -> Result<String> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
        || name == "objects"
    {
        return Err(OrigoFSError::InvalidPath(format!(
            "invalid sidecar name: {name:?}"
        )));
    }
    Ok(name.to_string())
}

/// Reject a slot name that isn't a single safe path component.
///
/// Backends turn a slot name into a path or object key, so this is the same
/// fail-closed rule `validate_component` applies to filenames in the metadata
/// layer: a poisoned name must never be *stored*, let alone escape the store root.
pub(crate) fn validate_slot_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
        && name != "."
        && name != "..";
    if ok {
        Ok(())
    } else {
        Err(OrigoFSError::InvalidArgument(format!(
            "invalid content-store slot name: {name:?}"
        )))
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
    /// Counter making each write's temp filename unique — see [`Self::write_at`].
    tmp_seq: AtomicU64,
}

impl LocalCasStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(root.join("objects")).await?;
        Ok(Self {
            root,
            syncs: AtomicU64::new(0),
            tmp_seq: AtomicU64::new(0),
        })
    }

    fn path_for(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join("objects").join(&hex[0..2]).join(&hex[2..])
    }

    async fn exists(path: &Path) -> bool {
        tokio::fs::metadata(path).await.is_ok()
    }

    /// Age of the object at `path` in seconds, or `None` if it isn't there (or
    /// the filesystem won't say). Mirrors what `list_with_age` reports, so the
    /// dedup refresh and the sweep agree on what "old" means.
    async fn age_secs(path: &Path) -> Option<u64> {
        let meta = tokio::fs::metadata(path).await.ok()?;
        let modified = meta.modified().ok()?;
        std::time::SystemTime::now()
            .duration_since(modified)
            .ok()
            .map(|d| d.as_secs())
    }

    /// Stamp `path`'s mtime to now, so a concurrent sweep sees a fresh object.
    /// `File::set_times` needs a writable handle and is a blocking syscall, hence
    /// the blocking pool. Best-effort at the call site: see `touch`.
    async fn set_mtime_now(path: &Path) -> Result<()> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let f = std::fs::File::options().write(true).open(&path)?;
            f.set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::now()))
        })
        .await
        .map_err(|e| OrigoFSError::Content(format!("touch join: {e}")))??;
        Ok(())
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
    ///
    /// The temp name is unique per write, not derived from the object's path.
    /// Deriving it from the path gives every concurrent writer of the *same*
    /// content one shared temp file — and that is the common case, because
    /// identical content is exactly what dedup produces. Since `File::create`
    /// truncates, one writer could zero another's partially-written file and the
    /// first would then fsync and rename the hole into place, returning `Ok(hash)`
    /// for bytes that do not hash to `hash`; the loser of the rename race would
    /// meanwhile see a spurious `ENOENT` for a write that had in fact succeeded.
    /// With distinct temps both writers rename their own complete copy, and rename
    /// is atomic, so the last one simply wins with identical bytes.
    async fn write_at(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // pid + counter is unique among *live* writers: within a process the
        // counter never repeats, and a recycled pid means the other process is
        // gone. Kept a sibling of `path` so the rename stays within one directory
        // (and so atomic).
        let tmp = path.with_extension(format!(
            "{}.{}.tmp",
            std::process::id(),
            self.tmp_seq.fetch_add(1, Ordering::Relaxed)
        ));
        let res = self.write_tmp_then_rename(&tmp, path, bytes).await;
        if res.is_err() {
            // Nothing will ever reuse this name, so a partial temp would just
            // accumulate. Best-effort: the write already failed.
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        res
    }

    async fn write_tmp_then_rename(&self, tmp: &Path, path: &Path, bytes: &[u8]) -> Result<()> {
        let mut f = tokio::fs::File::create(tmp).await?;
        f.write_all(bytes).await?;
        f.sync_all().await?;
        drop(f);
        self.syncs.fetch_add(1, Ordering::Relaxed);
        tokio::fs::rename(tmp, path).await?;
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
        // Already stored — content-addressed, so identical. Refresh its recency
        // first: a sweep racing this write must not reclaim the object we are
        // about to hand back as durable (see `ContentStore::touch`).
        if Self::exists(&path).await {
            self.touch(&hash).await?;
            return Ok(hash);
        }
        self.write_at(&path, bytes).await?;
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        let path = self.path_for(key);
        if Self::exists(&path).await {
            self.touch(key).await?;
            return Ok(());
        }
        self.write_at(&path, bytes).await
    }

    async fn touch(&self, hash: &Hash) -> Result<()> {
        let path = self.path_for(hash);
        match Self::age_secs(&path).await {
            Some(age) if age >= DEDUP_REFRESH_AFTER_SECS => Self::set_mtime_now(&path).await,
            // Young enough that any valid grace period already covers it, or gone
            // (a `has`/`put` race) — nothing useful to refresh either way.
            _ => Ok(()),
        }
    }

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        // `write_at` is temp-file + rename, so the key never resolves to a
        // half-written value and never to nothing.
        self.write_at(&self.path_for(key), bytes).await
    }

    /// Slots live in `<root>/meta/`, a sibling of `objects/` — so `list` (which
    /// walks `objects/` only) never sees them.
    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        validate_slot_name(name)?;
        let path = self.root.join("meta").join(name);
        // Not content-addressed, so unlike `put_keyed` this overwrites: the
        // durable temp-then-rename in `write_at` makes the replacement atomic.
        self.write_at(&path, bytes).await
    }

    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        validate_slot_name(name)?;
        match tokio::fs::read(self.root.join("meta").join(name)).await {
            Ok(v) => Ok(Some(Bytes::from(v))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
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

    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.root.join(sidecar_file(name)?)).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        let path = self.root.join(sidecar_file(name)?);
        tokio::fs::create_dir_all(&self.root).await?;
        // `create_new` is the exclusive-create: exactly one racing writer wins and
        // the loser re-reads the winner's value.
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut f) => {
                f.write_all(bytes).await?;
                f.flush().await?;
                Ok(bytes.to_vec())
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Ok(tokio::fs::read(&path).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        let now = std::time::SystemTime::now();
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
                let Some(h) = Hash::from_hex(&format!("{prefix}{name}")) else {
                    continue;
                };
                // An unreadable or future-dated mtime reports `None` (unknown),
                // which the sweep treats as "don't touch".
                let age = entry
                    .metadata()
                    .await
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| now.duration_since(t).ok())
                    .map(|d| d.as_secs());
                out.push((h, age));
            }
        }
        Ok(out)
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
    // Forwarding this is not optional. `replace_keyed` has a *default* body — the
    // unsafe delete-then-put the trait doc warns about — so omitting it here does
    // not fail to compile, it silently downgrades every backend reached through an
    // `Arc` to that default. `PackStore` holds its index as `Arc<dyn ContentStore>`
    // and calls `replace_keyed` on the repack path, so the omission cost exactly
    // the atomicity the method exists to provide. Covered by
    // `tests/pack.rs::arc_forwards_replace_keyed_atomically`.
    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        (**self).replace_keyed(key, bytes).await
    }
    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        (**self).put_meta(name, bytes).await
    }
    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        (**self).get_meta(name).await
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
    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        (**self).get_sidecar(name).await
    }
    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        (**self).put_sidecar_if_absent(name, bytes).await
    }
    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        (**self).list_with_age().await
    }
    async fn touch(&self, hash: &Hash) -> Result<()> {
        (**self).touch(hash).await
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
    /// When each object was first stored, so the store can report an age the way
    /// a real backend does (see [`ContentStore::list_with_age`]).
    born: Mutex<HashMap<Hash, std::time::Instant>>,
    /// Named sidecars, kept out of `map` so `list()` never enumerates them and GC
    /// therefore cannot sweep them (see [`ContentStore::get_sidecar`]).
    sidecars: Mutex<HashMap<String, Vec<u8>>>,
    /// Named slots, kept apart from `map` so they stay invisible to `list`/`gc`
    /// exactly as they are on a real backend.
    slots: Mutex<HashMap<String, Bytes>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `hash` as newly stored, unless it already was.
    fn touch_born(&self, hash: Hash) {
        self.born
            .lock()
            .entry(hash)
            .or_insert_with(std::time::Instant::now);
    }

    /// Number of distinct blobs stored (useful for dedup assertions in tests).
    pub fn len(&self) -> usize {
        self.map.lock().len()
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
            .entry(hash)
            .or_insert_with(|| Bytes::copy_from_slice(bytes));
        // Records a birth stamp for a new object and leaves an existing one
        // alone; `touch` is what refreshes a stale one on the dedup path.
        self.touch_born(hash);
        self.touch(&hash).await?;
        Ok(hash)
    }

    async fn touch(&self, hash: &Hash) -> Result<()> {
        let mut born = self.born.lock();
        if let Some(t) = born.get_mut(hash)
            && t.elapsed().as_secs() >= DEDUP_REFRESH_AFTER_SECS
        {
            *t = std::time::Instant::now();
        }
        Ok(())
    }

    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.sidecars.lock().get(name).cloned())
    }

    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(self
            .sidecars
            .lock()
            .entry(name.to_string())
            .or_insert_with(|| bytes.to_vec())
            .clone())
    }

    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        let born = self.born.lock();
        Ok(self
            .map
            .lock()
            .keys()
            .map(|h| (*h, born.get(h).map(|t| t.elapsed().as_secs())))
            .collect())
    }

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.map.lock().insert(*key, Bytes::copy_from_slice(bytes));
        self.touch_born(*key);
        Ok(())
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.map
            .lock()
            .entry(*key)
            .or_insert_with(|| Bytes::copy_from_slice(bytes));
        self.touch_born(*key);
        self.touch(key).await?;
        Ok(())
    }

    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        validate_slot_name(name)?;
        self.slots
            .lock()
            .insert(name.to_string(), Bytes::copy_from_slice(bytes));
        Ok(())
    }

    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        validate_slot_name(name)?;
        Ok(self.slots.lock().get(name).cloned())
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.map
            .lock()
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
        Ok(self.map.lock().contains_key(hash))
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        Ok(self.map.lock().keys().copied().collect())
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        self.born.lock().remove(hash);
        Ok(self
            .map
            .lock()
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

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.backend.replace_keyed(key, bytes).await?;
        // The cache holds the *old* value, which is now wrong — replace it too, and
        // drop it on failure rather than leave a stale read in front of the backend.
        if self.cache.replace_keyed(key, bytes).await.is_err() {
            let _ = self.cache.delete(key).await;
        }
        Ok(())
    }

    /// Slots go to the backend only. They are mutable, and a stale cached copy of
    /// "which format is this store" is worse than a round-trip on open.
    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.backend.put_meta(name, bytes).await
    }

    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        self.backend.get_meta(name).await
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

    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        self.backend.list_with_age().await
    }

    /// Forwarded, like `list_with_age`. A decorator that reports the inner
    /// store's ages must forward the refresh that keeps those ages honest,
    /// or a deduplicating write through this layer stays invisible to the
    /// sweep's grace period (`ContentStore::touch`).
    async fn touch(&self, hash: &Hash) -> Result<()> {
        self.backend.touch(hash).await
    }

    /// Forwarded to the backend, which is the authoritative store. Without this
    /// the trait's no-op default silently swallows a batching backend's seal — a
    /// `PackStore` behind a cache tier would never make its buffered chunks
    /// durable, while metadata already referenced them.
    async fn flush(&self) -> Result<()> {
        self.backend.flush().await
    }

    /// See [`flush`](Self::flush).
    async fn repack(&self) -> Result<u64> {
        self.backend.repack().await
    }

    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.backend.get_sidecar(name).await
    }

    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        self.backend.put_sidecar_if_absent(name, bytes).await
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

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.inner.replace_keyed(key, bytes).await
    }

    /// Pass through unverified: a slot is deliberately not content-addressed, so
    /// there is no address to check it against. Slot payloads carry their own
    /// tag+version header instead.
    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_meta(name, bytes).await
    }

    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        self.inner.get_meta(name).await
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let bytes = self.inner.get(hash).await?;
        if let Err(e) = verify_integrity(hash, &bytes) {
            // A bit-rotted / tampered object at the chunk-addressed boundary — the
            // operator wants to know immediately (it points at storage corruption).
            tracing::warn!(hash = %hash.to_hex(), "content failed integrity verification");
            return Err(e);
        }
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

    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        self.inner.list_with_age().await
    }

    /// Forwarded, like `list_with_age`. A decorator that reports the inner
    /// store's ages must forward the refresh that keeps those ages honest,
    /// or a deduplicating write through this layer stays invisible to the
    /// sweep's grace period (`ContentStore::touch`).
    async fn touch(&self, hash: &Hash) -> Result<()> {
        self.inner.touch(hash).await
    }

    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_sidecar(name).await
    }

    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        self.inner.put_sidecar_if_absent(name, bytes).await
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
