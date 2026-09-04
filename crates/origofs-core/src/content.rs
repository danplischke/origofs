//! The content store: a pluggable, content-addressed blob store (`docs/DESIGN.md` §4a).
//!
//! M0 ships one backend, [`LocalCasStore`], which keeps blobs in a sharded
//! directory. M1 adds FastCDC chunking + manifests and an S3 backend behind the
//! same [`ContentStore`] trait.

use crate::error::{OrigoFSError, Result};
use crate::types::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
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
///
/// # Consistency contract
///
/// A backend must be **strongly consistent per key**: a completed `put` is
/// immediately visible to `get`/`has`/`get_range`, a completed `delete` is
/// immediately reflected by `has`/`head`, and `put_sidecar_if_absent` is a
/// genuinely atomic create-if-absent. Every supported backend provides this —
/// S3 (strongly consistent since 2020), GCS, R2, Azure, MinIO, and the local
/// filesystem — so it is an assumption the engine leans on rather than a
/// property it defends:
///
/// * the durability barrier is put + flush **then** metadata commit, so a reader
///   resolving a fresh hash expects the object to be there — a stale 404 would
///   surface as a terminal [`ContentMissing`](crate::OrigoFSError::ContentMissing)
///   (deliberately not retryable, because on a conforming backend "missing"
///   means really missing);
/// * the deduplicating `put` skips the upload when the object exists, so a
///   stale positive after a delete would commit a reference to bytes that are
///   gone;
/// * garbage collection's age gate reads `last_modified` via HEAD/LIST and
///   re-checks it at deletion time, which only helps if that metadata is fresh.
///
/// What is deliberately **not** required: staleness of reads of an *existing*
/// object is harmless (a key has exactly one possible value, ever — the only
/// overwrites are identical-byte recency rewrites), and LIST completeness is
/// never load-bearing (nothing user-facing lists the store; the sweep, repack,
/// and rebuild all fail safe on omission). An eventually-consistent
/// S3-compatible endpoint — an async-replicated bucket read from the far
/// region, a CDN-fronted endpoint, an older gateway — is outside this contract
/// and can produce dangling references or unsound collection.
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

    /// The stored size of `hash` **without reading it**, or `None` when the
    /// backend cannot answer cheaply (issue #114).
    ///
    /// Exists so [`TieredStore`]'s bounded cache can seed its LRU index from a
    /// cache directory that already has contents, which it otherwise could not do:
    /// `list` gives hashes but no sizes, and reading every object to measure it
    /// would defeat the point of a cache.
    ///
    /// The default reports `None`, which is correct for any backend that would
    /// have to fetch the object to answer. A `TieredStore` over such a cache falls
    /// back to counting only what it observes during this process's lifetime, so
    /// the bound still holds going forward but does not account for what was
    /// already on disk.
    async fn size_of(&self, hash: &Hash) -> Result<Option<u64>> {
        let _ = hash;
        Ok(None)
    }

    /// Delete `hash` **only if** it is still at least `min_age_secs` old, by the
    /// same clock [`list_with_age`](Self::list_with_age) reports.
    ///
    /// `Some(bytes_freed)` when it deleted, `None` when it declined because the
    /// object had been refreshed. The two are distinguished by more than the byte
    /// count on purpose: a [`PackStore`](crate::PackStore) delete frees `0` bytes
    /// even on success (it drops an index entry; the bytes come back from
    /// `repack`), so `0` cannot mean "didn't delete".
    ///
    /// This is the second half of the age gate, and without it the gate is
    /// check-then-act. [`list_with_age`](Self::list_with_age) makes a sweep safe by
    /// skipping anything younger than the grace period, and
    /// [`touch`](Self::touch) keeps a deduplicated-onto object out of that band —
    /// but a sweep decides on the ages it read at the *start* of the pass and then
    /// deletes unconditionally, and a pass over a large store runs for minutes. A
    /// writer that dedups onto an object in that window refreshes its recency and
    /// commits a reference to it, and the sweep deletes it anyway: exactly the
    /// `ContentMissing`-after-commit the grace period exists to prevent, just with
    /// a wider window than the one that was closed.
    ///
    /// Re-reading the age at the moment of deletion closes it. The check is not
    /// atomic with the delete on every backend — a filesystem `stat`+`unlink` and
    /// an object-store `head`+`delete` both have a gap — but the gap shrinks from
    /// "the length of the sweep" to "two adjacent calls", far inside any valid
    /// grace period.
    ///
    /// The default implements the check generically; backends that can do better
    /// (a conditional request) may override it.
    async fn delete_if_older_than(&self, hash: &Hash, min_age_secs: u64) -> Result<Option<u64>> {
        // `0` means the caller opted out of the age gate entirely (a quiesced
        // store), so skip the re-read rather than paying for it.
        if min_age_secs > 0 {
            match self.age_of(hash).await? {
                Some(age) if age >= min_age_secs => {}
                // Refreshed under us, or the backend stopped being able to date it:
                // either way this is no longer something the gate says is safe.
                _ => return Ok(None),
            }
        }
        self.delete(hash).await.map(Some)
    }

    /// The age in seconds of a single object, by the same clock
    /// [`list_with_age`](Self::list_with_age) uses — `None` when the backend cannot
    /// tell or the object is absent.
    ///
    /// The default derives it from `list_with_age`, which is correct but scans the
    /// whole store; every backend here overrides it with a single stat/head.
    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        Ok(self
            .list_with_age()
            .await?
            .into_iter()
            .find(|(h, _)| h == hash)
            .and_then(|(_, age)| age))
    }

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

    /// Release the backend's resources — HTTP clients, connection pools, open
    /// handles — and make this store unusable (issue #154).
    ///
    /// The default is a no-op: a local directory or an in-memory map holds
    /// nothing that a drop would not already release. An object-store backend
    /// overrides it, and every decorator forwards, so a close on the outermost
    /// store reaches the real backend the same way [`ping`](ContentStore::ping)
    /// does.
    ///
    /// **This does not flush.** A [`PackStore`](crate::pack::PackStore) buffers
    /// chunks in memory until a pack is sealed, and deciding to write them is a
    /// durability call that belongs to the caller, not to a teardown path — so
    /// close releases what is open and says nothing about what is pending. A
    /// caller driving the trait directly should call
    /// [`flush`](ContentStore::flush) itself; the SDK's `Workspace::close` does.
    /// Idempotent, so a double close is not an error.
    async fn close(&self) -> Result<()> {
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
        // The two subdirectories `LocalCasStore` owns under its root: `objects/`
        // holds the content-addressed blobs and `meta/` holds the named slots. A
        // sidecar is a *file* at the root, so either name would collide with a
        // directory — `meta` was missing here even though `put_meta` has written to
        // `<root>/meta/` since slots were added.
        || name == "objects"
        || name == "meta"
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

    /// One `stat`, from the same mtime `list_with_age` reports.
    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        Ok(Self::age_secs(&self.path_for(hash)).await)
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
        // A sidecar holds the encryption salt, so it gets the same discipline as an
        // object: write a uniquely-named temp, fsync it, then link it into place.
        //
        // This used to be `create_new` + `write_all` + `flush` directly at `path`,
        // which is the one write in this store with neither durability nor atomic
        // visibility, and both gaps are reachable. A second process opening the same
        // fresh store can `read` the file between the create and the write and get a
        // *torn* (usually empty) salt, then derive a different key — a split-brain
        // store where neither writer can read the other's objects. And with no
        // fsync, a crash can leave an empty salt file behind after objects were
        // already written under the real one.
        //
        // `hard_link` rather than `rename`: rename would overwrite a salt another
        // writer had already established, which is exactly what create-if-absent
        // exists to prevent. Linking fails with `AlreadyExists` instead, and that
        // loser re-reads the winner's complete value.
        let tmp = path.with_extension(format!(
            "{}.{}.tmp",
            std::process::id(),
            self.tmp_seq.fetch_add(1, Ordering::Relaxed)
        ));
        let write_tmp = async {
            let mut f = tokio::fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            f.sync_all().await?;
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(e) = write_tmp {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e.into());
        }
        self.syncs.fetch_add(1, Ordering::Relaxed);
        let linked = tokio::fs::hard_link(&tmp, &path).await;
        let _ = tokio::fs::remove_file(&tmp).await;
        match linked {
            Ok(()) => {
                #[cfg(unix)]
                if let Some(parent) = path.parent()
                    && let Ok(dir) = tokio::fs::File::open(parent).await
                {
                    let _ = dir.sync_all().await;
                    self.syncs.fetch_add(1, Ordering::Relaxed);
                }
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

    /// One `stat`, no read — see [`ContentStore::size_of`].
    async fn size_of(&self, hash: &Hash) -> Result<Option<u64>> {
        Ok(tokio::fs::metadata(self.path_for(hash))
            .await
            .ok()
            .map(|m| m.len()))
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
    // Same trap as `replace_keyed` below, and it bit the same way: `size_of` has a
    // default body returning `None`, so omitting it here compiles fine and quietly
    // makes every backend reached through an `Arc` report "cannot answer". The
    // cache tier holds its cache as `Arc<dyn ContentStore>`, so the omission cost
    // `TieredStore::warm_index` its ability to see a pre-existing cache at all —
    // it silently accounted for nothing and evicted nothing. Covered by
    // `tests/cache_tier.rs::a_warmed_index_accounts_for_pre_existing_cache_contents`.
    async fn size_of(&self, hash: &Hash) -> Result<Option<u64>> {
        (**self).size_of(hash).await
    }
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
    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        (**self).age_of(hash).await
    }
    async fn delete(&self, hash: &Hash) -> Result<u64> {
        (**self).delete(hash).await
    }
    // Forwarded for the same reason `replace_keyed` is: it has a default body, so
    // omitting it silently downgrades every backend reached through an `Arc` to
    // the generic whole-store scan instead of its own single stat/head.
    async fn delete_if_older_than(&self, hash: &Hash, min_age_secs: u64) -> Result<Option<u64>> {
        (**self).delete_if_older_than(hash, min_age_secs).await
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
    async fn close(&self) -> Result<()> {
        (**self).close().await
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

    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        if !self.map.lock().contains_key(hash) {
            return Ok(None);
        }
        Ok(self.born.lock().get(hash).map(|t| t.elapsed().as_secs()))
    }

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.map.lock().insert(*key, Bytes::copy_from_slice(bytes));
        // A replaced value is a *new* write, so it gets a new birth stamp.
        // `touch_born` only fills in a missing one, so an existing key kept the old
        // entry's age — reporting a just-written value as ancient, where the local
        // and object backends both report it as fresh (their replace rewrites the
        // file/object, moving its mtime).
        self.born.lock().insert(*key, std::time::Instant::now());
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

    async fn size_of(&self, hash: &Hash) -> Result<Option<u64>> {
        Ok(self.map.lock().get(hash).map(|b| b.len() as u64))
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

/// Bounds on a [`TieredStore`]'s local cache tier (issue #114).
///
/// Default is unbounded, which is what `TieredStore` did before #114 — and is why
/// it was not safe to wire into the `open_*` constructors: a cache that grows
/// without limit eventually fills the user's disk.
#[derive(Clone, Debug, Default)]
pub struct CacheLimits {
    /// Evict least-recently-used entries to keep the cache at or below this many
    /// bytes. `None` = no size bound.
    pub max_bytes: Option<u64>,
    /// Keep at least this many bytes free on the filesystem holding [`dir`](Self::dir),
    /// evicting when it drops below. `None` = no free-space floor.
    ///
    /// This is the bound that matters on a shared machine: a cache sized for a
    /// laptop's disk is still ruinous on a container with a small writable layer,
    /// and the disk can also fill for reasons that have nothing to do with origofs.
    pub min_free_bytes: Option<u64>,
    /// The cache's directory. Required for [`min_free_bytes`](Self::min_free_bytes)
    /// to do anything — free space is a property of a filesystem, not of a
    /// `dyn ContentStore`.
    pub dir: Option<PathBuf>,
}

impl CacheLimits {
    /// A size-bounded cache with no free-space floor.
    pub fn bytes(max_bytes: u64) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            ..Default::default()
        }
    }

    /// The recommended shape: a size bound *and* a floor under the free space of
    /// the filesystem holding `dir`.
    pub fn bounded(dir: impl Into<PathBuf>, max_bytes: u64, min_free_bytes: u64) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            min_free_bytes: Some(min_free_bytes),
            dir: Some(dir.into()),
        }
    }

    fn is_unbounded(&self) -> bool {
        self.max_bytes.is_none() && self.min_free_bytes.is_none()
    }
}

/// LRU bookkeeping for the cache tier. Recency is a monotonic counter rather than
/// a clock: it needs only to order accesses, and a counter cannot go backwards
/// when the system clock does.
#[derive(Default)]
struct CacheIndex {
    /// hash -> (size, last-use tick)
    entries: HashMap<Hash, (u64, u64)>,
    /// (tick, hash) in tick order, so the LRU victim is the first entry.
    by_age: BTreeMap<(u64, Hash), ()>,
    bytes: u64,
    tick: u64,
}

impl CacheIndex {
    fn touch(&mut self, hash: &Hash, size: u64) {
        self.tick += 1;
        let tick = self.tick;
        if let Some((old_size, old_tick)) = self.entries.insert(*hash, (size, tick)) {
            self.by_age.remove(&(old_tick, *hash));
            self.bytes = self.bytes.saturating_sub(old_size);
        }
        self.by_age.insert((tick, *hash), ());
        self.bytes = self.bytes.saturating_add(size);
    }

    fn forget(&mut self, hash: &Hash) {
        if let Some((size, tick)) = self.entries.remove(hash) {
            self.by_age.remove(&(tick, *hash));
            self.bytes = self.bytes.saturating_sub(size);
        }
    }

    /// The least-recently-used entry, without removing it.
    fn lru(&self) -> Option<(Hash, u64)> {
        let (&(_, hash), ()) = self.by_age.iter().next()?;
        let size = self.entries.get(&hash).map(|(s, _)| *s).unwrap_or(0);
        Some((hash, size))
    }
}

/// Bytes free on the filesystem holding `dir`, or `None` where that cannot be
/// asked.
///
/// Unix-only: `statvfs` is the portable-enough answer there, and Windows would
/// need `GetDiskFreeSpaceEx`. A `None` result disables the free-space floor rather
/// than failing — a cache that cannot measure the disk still honours its size
/// bound, which is the bound a caller actually set a number for.
#[cfg(unix)]
fn free_bytes(dir: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: `c` is a valid NUL-terminated path for the duration of the call, and
    // `st` is a caller-owned, correctly-sized, zeroed `statvfs` the call fills in.
    unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut st) != 0 {
            return None;
        }
        // f_bavail is what an unprivileged process may actually use, which is the
        // honest figure here; f_bfree includes root's reserve.
        //
        // The casts look redundant on glibc/x86_64, where both fields are already
        // `u64`, and clippy says so — but `statvfs`'s field widths are libc- and
        // target-dependent (`fsblkcnt_t`/`c_ulong` are 32-bit on several 32-bit
        // targets, and musl differs from glibc), so dropping them breaks those
        // builds. Kept and silenced rather than removed.
        #[allow(clippy::unnecessary_cast)]
        Some((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
    }
}

#[cfg(not(unix))]
fn free_bytes(_dir: &Path) -> Option<u64> {
    None
}

/// A two-tier store: a fast local `cache` in front of a (possibly remote)
/// `backend` (`docs/DESIGN.md` §4a). Reads are served from cache and populate it
/// on miss; writes are write-through to the backend and cached best-effort.
///
/// M1 is write-through for durability simplicity; write-back batching is a later
/// optimization. [`TieredStore::prefetch`] warms the cache for a file's chunks.
///
/// # Bounds and eviction (issue #114)
///
/// The cache is bounded by [`CacheLimits`] and evicts least-recently-used entries
/// when it would exceed them. Before #114 it was unbounded, which is precisely why
/// it was complete, tested, and reachable from **no** `open_*` constructor: a
/// cache that grows forever cannot be turned on by default.
///
/// # A failed cache read is a miss, not an error
///
/// Every read falls through to the backend if the cache cannot serve it, and evicts
/// the offending entry on the way. A local cache entry can be truncated by a full
/// disk, corrupted by bit-rot, or removed by something else on the machine, and
/// none of those should fail a read that the authoritative backend can satisfy.
///
/// Whole reads are additionally re-hashed here, so a *corrupt* cache entry becomes
/// a refetch rather than an error. That has to happen at this layer: the
/// `VerifyingStore` in the `open_*` stacks sits **outside** the tier split, so it
/// sees only the bytes this store returns and cannot tell which tier produced them
/// — it would reject a corrupt cached copy of an object the backend still holds
/// intact. Ranged reads cannot be verified without fetching the whole object, so
/// they fall through on error but are not re-hashed; the outer `VerifyingStore`
/// remains the backstop there.
pub struct TieredStore {
    cache: Arc<dyn ContentStore>,
    backend: Arc<dyn ContentStore>,
    limits: CacheLimits,
    index: Mutex<CacheIndex>,
}

impl TieredStore {
    /// An **unbounded** cache tier — the pre-#114 behaviour.
    ///
    /// Suitable when the cache is itself bounded (a fixed-size `MemStore`, a
    /// tmpfs with its own limit) or in tests. For a local directory prefer
    /// [`with_limits`](Self::with_limits): an unbounded on-disk cache will fill
    /// the disk.
    pub fn new(cache: Arc<dyn ContentStore>, backend: Arc<dyn ContentStore>) -> Self {
        Self::with_limits(cache, backend, CacheLimits::default())
    }

    /// A cache tier bounded by `limits`.
    pub fn with_limits(
        cache: Arc<dyn ContentStore>,
        backend: Arc<dyn ContentStore>,
        limits: CacheLimits,
    ) -> Self {
        Self {
            cache,
            backend,
            limits,
            index: Mutex::new(CacheIndex::default()),
        }
    }

    /// Account for what a cache directory already holds, so the bound covers
    /// pre-existing contents rather than only what this process happens to touch.
    ///
    /// Costs one `list` plus a `size_of` per entry — cheap for a local directory
    /// (`size_of` is a `stat`), and skipped entirely for an unbounded cache. A
    /// cache backend that cannot answer `size_of` is left to lazy accounting,
    /// which still bounds future growth but cannot see what was already there.
    pub async fn warm_index(&self) -> Result<()> {
        if self.limits.is_unbounded() {
            return Ok(());
        }
        for h in self.cache.list().await? {
            if let Ok(Some(size)) = self.cache.size_of(&h).await {
                self.index.lock().touch(&h, size);
            }
        }
        self.enforce_limits().await;
        Ok(())
    }

    /// Current tracked cache size in bytes, for tests and diagnostics.
    pub fn cached_bytes(&self) -> u64 {
        self.index.lock().bytes
    }

    /// Note that `hash` (of `size` bytes) was just stored or read, then evict if
    /// that puts the cache over its limits.
    async fn record(&self, hash: &Hash, size: u64) {
        if self.limits.is_unbounded() {
            return;
        }
        self.index.lock().touch(hash, size);
        self.enforce_limits().await;
    }

    /// Drop the least-recently-used entries until the cache is back inside its
    /// limits.
    ///
    /// Bounded by the number of entries so a cache whose deletes are all failing
    /// cannot spin: each pass either removes an index entry or stops.
    async fn enforce_limits(&self) {
        loop {
            let over = {
                let idx = self.index.lock();
                let over_size = self.limits.max_bytes.is_some_and(|max| idx.bytes > max);
                let over_disk = match (&self.limits.min_free_bytes, &self.limits.dir) {
                    (Some(min), Some(dir)) => free_bytes(dir).is_some_and(|f| f < *min),
                    _ => false,
                };
                // Never evict the last entry on a free-space trigger alone: if the
                // disk is full because of something *else*, emptying the cache
                // entirely would not help and would throw away every warm read.
                (over_size || over_disk) && idx.entries.len() > 1
            };
            if !over {
                return;
            }
            let Some((victim, _)) = self.index.lock().lru() else {
                return;
            };
            // Forget it either way: a delete that failed leaves an entry this
            // store can no longer account for, and retrying it forever is worse
            // than under-counting by one object.
            let _ = self.cache.delete(&victim).await;
            self.index.lock().forget(&victim);
        }
    }

    /// Drop a cache entry that could not be served, so the next read refetches it
    /// from the backend rather than tripping over the same bad copy.
    async fn evict(&self, hash: &Hash) {
        let _ = self.cache.delete(hash).await;
        self.index.lock().forget(hash);
    }

    /// Warm the cache with `hashes` (e.g. a manifest's chunks, on open).
    ///
    /// Concurrent since #114 — it was a sequential `has`/`get`/`put` loop, so
    /// warming a file's chunks cost one full round trip *per chunk* against the
    /// very backend latency the cache exists to hide. Bounded by the same window
    /// the read path uses, and errors are per-chunk: a prefetch is an optimization,
    /// so one unfetchable chunk must not fail the warm-up of the rest.
    pub async fn prefetch(&self, hashes: &[Hash]) -> Result<()> {
        use futures::StreamExt;
        futures::stream::iter(hashes)
            .for_each_concurrent(PREFETCH_CONCURRENCY, |h| async move {
                if let Ok(false) = self.cache.has(h).await
                    && let Ok(bytes) = self.backend.get(h).await
                    && self.cache.put(&bytes).await.is_ok()
                {
                    self.record(h, bytes.len() as u64).await;
                }
            })
            .await;
        Ok(())
    }
}

/// How many chunks [`TieredStore::prefetch`] warms at once. Matches the read
/// path's default window; the point is to hide backend latency, which a
/// sequential loop cannot do.
const PREFETCH_CONCURRENCY: usize = 16;

#[async_trait]
impl ContentStore for TieredStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = self.backend.put(bytes).await?;
        if self.cache.put(bytes).await.is_ok() {
            // best-effort; only accounted for if it actually landed
            self.record(&hash, bytes.len() as u64).await;
        }
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.backend.put_keyed(key, bytes).await?;
        if self.cache.put_keyed(key, bytes).await.is_ok() {
            self.record(key, bytes.len() as u64).await;
        }
        Ok(())
    }

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.backend.replace_keyed(key, bytes).await?;
        // The cache holds the *old* value, which is now wrong — replace it too, and
        // drop it on failure rather than leave a stale read in front of the backend.
        if self.cache.replace_keyed(key, bytes).await.is_err() {
            self.evict(key).await;
        } else {
            self.record(key, bytes.len() as u64).await;
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
            // Re-hash here rather than trusting the cached copy. The
            // `VerifyingStore` in the `open_*` stacks sits *outside* the tier
            // split, so it cannot tell a corrupt cache entry from a corrupt
            // backend object and would reject a read the backend could still
            // serve. Checking at the tier boundary turns that into a refetch.
            if Hash::of(&bytes) == *hash {
                self.record(hash, bytes.len() as u64).await;
                return Ok(bytes);
            }
            tracing::warn!(
                hash = %hash.to_hex(),
                "cached object failed verification; evicting and refetching from the backend"
            );
            self.evict(hash).await;
        } else {
            // A cache entry can also be truncated by a full disk or removed by
            // something else on the machine. Either way it is a miss.
            self.evict(hash).await;
        }
        let bytes = self.backend.get(hash).await?;
        if self.cache.put(&bytes).await.is_ok() {
            self.record(hash, bytes.len() as u64).await;
        }
        Ok(bytes)
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        // A ranged read cannot be verified without fetching the whole object, so
        // this falls through on failure but does not re-hash; the outer
        // `VerifyingStore` stays the backstop for corruption here.
        if self.cache.has(hash).await.unwrap_or(false) {
            match self.cache.get_range(hash, off, len).await {
                Ok(bytes) => return Ok(bytes),
                Err(_) => self.evict(hash).await,
            }
        }
        self.backend.get_range(hash, off, len).await
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        // A cache that cannot answer is not evidence of absence — fall through to
        // the authoritative backend rather than failing the call.
        if self.cache.has(hash).await.unwrap_or(false) {
            return Ok(true);
        }
        self.backend.has(hash).await
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

    /// From the backend, which is where `list_with_age` reports ages from.
    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        self.backend.age_of(hash).await
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

    /// The conditional delete has to reach the backend's own check, and the cache
    /// entry has to go with it — but only if the backend actually deleted.
    async fn delete_if_older_than(&self, hash: &Hash, min_age_secs: u64) -> Result<Option<u64>> {
        let freed = self
            .backend
            .delete_if_older_than(hash, min_age_secs)
            .await?;
        if freed.is_some() {
            let _ = self.cache.delete(hash).await; // best-effort cache eviction
        }
        Ok(freed)
    }

    async fn ping(&self) -> Result<()> {
        // The backend is authoritative for durability; the cache is best-effort.
        self.backend.ping().await
    }

    /// Close both tiers. The cache is closed too — it is a real store holding
    /// real handles, and leaving it open would defeat the point of a shutdown.
    async fn close(&self) -> Result<()> {
        let cache = self.cache.close().await;
        let backend = self.backend.close().await;
        // Both are attempted before either error is returned: a cache that fails
        // to close must not leave the backend's sockets open. The backend's error
        // wins, since it is the tier that owns the durable resources.
        backend.and(cache)
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

    /// Forwarded alongside `list_with_age`/`touch`: all three have to agree on one
    /// clock, or the sweep's age gate is reading a different one than it acts on.
    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        self.inner.age_of(hash).await
    }

    async fn delete_if_older_than(&self, hash: &Hash, min_age_secs: u64) -> Result<Option<u64>> {
        self.inner.delete_if_older_than(hash, min_age_secs).await
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
    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}
