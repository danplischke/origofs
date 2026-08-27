//! The working-tree engine: POSIX-flavored operations over a [`MetadataStore`]
//! plus a [`ContentStore`].
//!
//! This is the mutable working tree of `docs/DESIGN.md` §3. In M0 it is the whole
//! story (no commits yet); later milestones layer commits/branches (M3), merge
//! (M4), and attribution (M6) on top without changing this surface.

use crate::chunk::{AVG_CHUNK, ChunkRef, MAX_CHUNK, MIN_CHUNK, Manifest, chunk_bounds};
use crate::clock::{Clock, SystemClock};
use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::metadata::{MetaTxn, MetadataStore};
use crate::types::{DirEntry, FileKind, Hash, INO_ROOT, Ino, Inode, InodeInit};
use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use std::sync::Arc;

const DIR_MODE: u32 = 0o040755;
const FILE_MODE: u32 = 0o100644;
const SYMLINK_MODE: u32 = 0o120777;

/// Bound on retries when a concurrent writer wins the create race for a new
/// path. One retry resolves it in practice (the loser then finds the inode and
/// updates it); the bound only guards against a pathological churn of
/// create/delete on the same name.
pub(crate) const CREATE_RETRIES: usize = 16;

/// How many chunk uploads may be in flight at once during a write.
///
/// Chunks used to be stored one at a time — `put().await` in a loop — so a write
/// cost one full round trip per chunk. That is invisible on a local store and
/// dominant on object storage: content-defined chunking turns 1 GiB of
/// incompressible data (media, archives, anything already compressed) into ~13,700
/// chunks, so at a 30 ms round trip a single gigabyte took about seven minutes of
/// pure latency, with the link nearly idle throughout.
///
/// The window is bounded rather than unlimited for three reasons: memory is
/// `window x MAX_CHUNK` (16 x 256 KiB = 4 MiB), object stores rate-limit, and an
/// unbounded window would let a fast reader queue the whole file. Override with
/// `ORIGOFS_UPLOAD_CONCURRENCY` — raise it for a high-latency bucket, drop it to 1
/// to recover the old sequential behaviour.
fn upload_concurrency() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("ORIGOFS_UPLOAD_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(16)
    })
}

/// How many chunk **fetches** a read keeps in flight (issue #113).
///
/// The mirror of [`upload_concurrency`], and it exists for the same reason: a read
/// walks its manifest and pulls each covering chunk, and doing that one `await` at
/// a time costs a full round trip per chunk. At the ~64 KiB average chunk size a
/// 1 MiB read is ~16 chunks, so on an S3-backed workspace at 30 ms RTT it spent
/// about half a second of pure latency per megabyte with the link idle throughout.
/// The write path has had bounded concurrency since M1; the read path had not.
///
/// Bounded for the same three reasons — memory is `window x MAX_CHUNK`, object
/// stores rate-limit, and an unbounded window would let one read queue the whole
/// file. On the streaming paths it doubles as the look-ahead window, which is why
/// those must not simply submit the entire plan.
///
/// Override with `ORIGOFS_FETCH_CONCURRENCY`; set it to 1 to recover the old
/// strictly-sequential behaviour.
fn fetch_concurrency() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("ORIGOFS_FETCH_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(16)
    })
}

/// The chunks covering `[off, end)` of `manifest`, each as `(hash, from, len)`
/// **relative to that chunk**.
///
/// Every ranged read — buffered or streaming, borrowed or owned — needs exactly
/// this walk, and it was open-coded at each of them. One helper, so the four call
/// sites cannot drift on the boundary arithmetic (the trimming of the first and
/// last chunk is the fiddly part) and so a fix lands everywhere at once.
///
/// `end` is expected to be clamped to `manifest.size` by the caller; entries are
/// returned in byte order, which is the order the results must be concatenated in.
fn covering_chunks(manifest: &Manifest, off: u64, end: u64) -> Vec<(Hash, u64, u64)> {
    let mut plan: Vec<(Hash, u64, u64)> = Vec::new();
    let mut pos: u64 = 0;
    for c in &manifest.chunks {
        let cstart = pos;
        let cend = pos + c.len as u64;
        pos = cend;
        if cend <= off {
            continue;
        }
        if cstart >= end {
            break;
        }
        let from = off.max(cstart) - cstart;
        let to = end.min(cend) - cstart;
        plan.push((c.hash, from, to - from));
    }
    plan
}

/// Reject a single path component that could escape the workspace tree or
/// corrupt the dentry graph: the traversal names `.`/`..`, an empty name, or a
/// name embedding a path separator or NUL. Enforced at every metadata boundary
/// (path resolution and the inode-oriented FUSE/NFS ops) so a poisoned name can
/// never be *stored* — which is what stops it from later escaping during a host
/// materialization such as the sandbox's `export_tree` (`host_dir.join("..")`).
pub(crate) fn validate_component(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(OrigoFSError::InvalidPath(format!(
            "invalid path component: {name:?}"
        )));
    }
    Ok(())
}

/// Reject a ref (branch) name that could escape a directory when a surface turns
/// it back into a host path, or that `git` itself would refuse.
///
/// Ref names are not just database keys: the git-interop layer writes
/// `refs/heads/<name>` and interpolates the name into `HEAD`, so an absolute or
/// `..`-bearing name would place a file outside the exported repository, and an
/// embedded newline would inject a second line into `HEAD`. This is the ref-level
/// counterpart of [`validate_component`] — enforced where a name enters the ref
/// table, so a hostile name can never be *stored*.
///
/// The rules follow `git check-ref-format`, which also keeps every name we accept
/// round-trippable through a real git repository. The internal refs (`HEAD`,
/// `MERGE_HEAD`) satisfy them, so nothing needs a carve-out.
pub fn validate_ref_name(name: &str) -> Result<()> {
    // Long enough for any real branch name; short enough that a name can't be used
    // to blow past a filesystem's path limit during export.
    const MAX_REF_NAME: usize = 255;

    let bad = |why: &str| {
        Err(OrigoFSError::InvalidArgument(format!(
            "invalid ref name {name:?}: {why}"
        )))
    };

    if name.is_empty() {
        return bad("empty");
    }
    if name.len() > MAX_REF_NAME {
        return bad("longer than 255 bytes");
    }
    // Control characters (incl. NUL and newline — the `HEAD` injection vector),
    // DEL, space, and the characters git reserves for refspecs and globbing.
    if let Some(c) = name
        .chars()
        .find(|c| c.is_control() || c.is_whitespace() || "~^:?*[\\".contains(*c))
    {
        return bad(&format!("contains {c:?}"));
    }
    // A leading `/` makes `Path::join` discard the directory it is joined onto; a
    // trailing or doubled `/` yields an empty component.
    if name.starts_with('/') || name.ends_with('/') || name.contains("//") {
        return bad("leading, trailing, or repeated '/'");
    }
    // `..` anywhere is a traversal; `.` as a component is at best meaningless.
    // Checked per component so a legitimate name like `fix.2` still passes.
    if name.split('/').any(|c| c == "." || c == "..") {
        return bad("'.' or '..' path component");
    }
    if name.contains("..") || name.contains("@{") || name == "@" {
        return bad("contains '..' or '@{', or is '@'");
    }
    // `-` leading would be read as a flag by any CLI that forwards the name.
    if name.starts_with('-') {
        return bad("starts with '-'");
    }
    if name.ends_with('.') || name.ends_with(".lock") {
        return bad("ends with '.' or '.lock'");
    }
    Ok(())
}

/// An owned [`Stream`] over a manifest's chunks, with up to
/// [`fetch_concurrency`] fetches in flight. The store handle is moved into the
/// stream, so it is self-contained (`'static` when `S` is) and can outlive the
/// [`Fs`] it came from — unlike [`Fs::content_stream`], which borrows. Powers
/// [`Fs::read_stream_owned`].
///
/// `buffered` (not `buffer_unordered`) so chunks are yielded in manifest order,
/// which *is* the file's byte order — the same reason `store_body` uses it on the
/// write side. The bound doubles as the read-ahead window: a consumer that stops
/// early leaves at most that many fetches wasted, rather than the whole file.
fn owned_chunk_stream<S: ContentStore + 'static>(
    store: S,
    manifest: Manifest,
) -> impl Stream<Item = Result<Bytes>> + Send + 'static {
    let store = std::sync::Arc::new(store);
    futures::stream::iter(manifest.chunks)
        .map(move |c| {
            let store = store.clone();
            async move { store.get(&c.hash).await }
        })
        .buffered(fetch_concurrency())
}

/// A filesystem over a metadata store and a content store.
#[derive(Clone)]
pub struct Fs<M: MetadataStore, C: ContentStore> {
    pub meta: M,
    pub content: C,
    /// The time source for engine-layer timestamps (commits, edit-ops, events,
    /// presence, locks, sessions). Injectable so a deterministic simulation can
    /// reproduce every timestamp — and thus every commit hash — from a seed.
    pub(crate) clock: Arc<dyn Clock>,
    /// The root directory inode this engine resolves paths from. `INO_ROOT` for a
    /// store's `default` workspace; a distinct inode for any other workspace in the
    /// same store, so many workspaces coexist (`docs/MULTI_TENANCY.md`). Every
    /// path walk starts here, so a workspace only ever reaches its own subtree.
    pub(crate) root_ino: Ino,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    pub fn new(meta: M, content: C) -> Self {
        Self {
            meta,
            content,
            clock: Arc::new(SystemClock),
            root_ino: INO_ROOT,
        }
    }

    /// Construct with an injected [`Clock`] instead of the wall clock — the entry
    /// point deterministic simulation (and any time-sensitive test) uses so a
    /// seed reproduces every engine-layer timestamp exactly.
    pub fn with_clock(meta: M, content: C, clock: Arc<dyn Clock>) -> Self {
        Self {
            meta,
            content,
            clock,
            root_ino: INO_ROOT,
        }
    }

    /// Build a sibling `Fs` bound to a different metadata handle and root inode,
    /// sharing this one's content store and clock — used to open another workspace
    /// living in the same stores (`docs/MULTI_TENANCY.md`). Pair with
    /// [`MetadataStore::with_workspace`] and [`MetadataStore::create_workspace`].
    pub fn rebind(&self, meta: M, root_ino: Ino) -> Self
    where
        C: Clone,
    {
        Self {
            meta,
            content: self.content.clone(),
            clock: self.clock.clone(),
            root_ino,
        }
    }

    /// The current time from the injected clock, in whole seconds since the Unix
    /// epoch. All engine-layer timestamps go through here (not `util::now_secs`)
    /// so simulation can control them.
    pub(crate) fn now_secs(&self) -> i64 {
        self.clock.now_secs()
    }

    /// Initialize the metadata schema, the root directory, and versioning state
    /// (HEAD → `main`, default `versioning = native`), after checking that this
    /// build can read the content store's object format.
    pub async fn init(&self) -> Result<()> {
        self.check_store_format().await?;
        self.check_schema_version().await?;
        self.meta.init().await?;
        self.init_versioning().await?;
        Ok(())
    }

    /// Refuse a metadata database written by a **newer** origofs, before any
    /// migration runs against it.
    ///
    /// The content store has had this guard from the start
    /// ([`check_store_format`](Self::check_store_format)); the metadata half did
    /// not, and the asymmetry was dangerous. `MetadataStore::init` applies every
    /// migration whose version is absent from `schema_meta` and never compares the
    /// database against `latest_schema_version()` — so a v15 binary opening a v16
    /// database reported no error at all and simply proceeded against a schema it
    /// does not know. Migrations here have changed primary keys (V11, V13), so
    /// "proceed anyway" is the shape that corrupts rather than the shape that
    /// fails.
    ///
    /// A fresh database reports version 0 and passes. The error is
    /// [`UnsupportedVersion`](OrigoFSError::UnsupportedVersion), the same one the
    /// content store raises, because the remedy is the same: upgrade the reader,
    /// do not restore from a backup.
    async fn check_schema_version(&self) -> Result<()> {
        let found = self.meta.schema_version().await?;
        let max = crate::migrations::latest_schema_version();
        if found > max {
            return Err(OrigoFSError::UnsupportedVersion {
                kind: "metadata schema",
                // Schema versions are small and monotonic; the cast is lossless in
                // any reachable range and saturates rather than wrapping if that
                // ever stops being true.
                found: u8::try_from(found).unwrap_or(u8::MAX),
                max_supported: u8::try_from(max).unwrap_or(u8::MAX),
            });
        }
        Ok(())
    }

    /// Verify — and on a fresh store, stamp — the content store's format
    /// descriptor (`crate::format`).
    ///
    /// Every `Workspace::open_*` funnels through [`init`](Self::init), so this is
    /// the one place a store written by a **newer** origofs is caught: once, at
    /// open, with a single actionable error. Without it the same condition
    /// surfaces object-by-object, deep in whatever operation happened to touch a
    /// v2 object first — and some of those paths (recovery's classification, the
    /// co-edit sidecar's "rebuild if unparseable" fallback) are designed to treat
    /// bytes they can't parse as *absent*, which turns "upgrade origofs" into
    /// silent data loss.
    ///
    /// A store with no descriptor is a fresh one — stamp it. A backend that doesn't
    /// implement named slots reports "never written" forever and is simply never
    /// checked — see [`ContentStore::put_meta`].
    async fn check_store_format(&self) -> Result<()> {
        use crate::format::{STORE_DESCRIPTOR_SLOT, StoreDescriptor};
        let current = StoreDescriptor::current();
        match self.content.get_meta(STORE_DESCRIPTOR_SLOT).await? {
            Some(bytes) => {
                let found = StoreDescriptor::decode(&bytes)?;
                found.check_readable()?;
                // We are about to write objects newer than the store advertises;
                // record that before any of them lands, so a reader that arrives
                // between the bump and the first v2 object is still warned.
                if current.format_version > found.format_version {
                    self.content
                        .put_meta(STORE_DESCRIPTOR_SLOT, &current.encode())
                        .await?;
                }
            }
            None => {
                self.content
                    .put_meta(STORE_DESCRIPTOR_SLOT, &current.encode())
                    .await?;
            }
        }
        Ok(())
    }

    /// Probe both backends for a readiness check: are the metadata and content
    /// stores reachable right now? Runs the two probes concurrently and returns
    /// their results; an unreachable backend is a classified
    /// [`OrigoFSError::Backend`](crate::OrigoFSError) of class `Unavailable`. This
    /// backs the SDK/HTTP `/readyz` endpoint (`docs/DESIGN.md` M9 — hardening).
    pub async fn probe(&self) -> (Result<()>, Result<()>) {
        futures::join!(self.meta.ping(), self.content.ping())
    }

    // --- path helpers -----------------------------------------------------

    /// Split an absolute path into its non-empty segments, rejecting any
    /// traversal component (`.`/`..`) so no path can escape the workspace root.
    fn split(path: &str) -> Result<Vec<&str>> {
        if !path.starts_with('/') {
            return Err(OrigoFSError::InvalidPath(format!(
                "path must be absolute: {path}"
            )));
        }
        let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        for seg in &segs {
            validate_component(seg)?;
        }
        Ok(segs)
    }

    /// Resolve an absolute path to its inode.
    pub(crate) async fn resolve(&self, path: &str) -> Result<Ino> {
        let mut ino = self.root_ino;
        for seg in Self::split(path)? {
            ino = self
                .meta
                .lookup(ino, seg)
                .await?
                .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        }
        Ok(ino)
    }

    /// Resolve a path's parent directory inode and return `(parent, basename)`.
    pub(crate) async fn resolve_parent<'a>(&self, path: &'a str) -> Result<(Ino, &'a str)> {
        let segs = Self::split(path)?;
        let (name, dirs) = segs
            .split_last()
            .ok_or_else(|| OrigoFSError::InvalidPath(format!("no basename in {path}")))?;
        let mut ino = self.root_ino;
        for &seg in dirs {
            ino = self
                .meta
                .lookup(ino, seg)
                .await?
                .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        }
        Ok((ino, *name))
    }

    pub(crate) async fn ensure_dir(&self, ino: Ino) -> Result<()> {
        let inode = self
            .meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("ino {ino}")))?;
        if inode.kind != FileKind::Dir {
            return Err(OrigoFSError::NotADirectory(format!("ino {ino}")));
        }
        Ok(())
    }

    // --- directory operations --------------------------------------------

    /// Create a single directory; its parent must already exist.
    pub async fn mkdir(&self, path: &str) -> Result<Ino> {
        crate::retry::retrying("mkdir", || self.mkdir_attempt(path)).await
    }

    /// One attempt at [`mkdir`](Self::mkdir); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn mkdir_attempt(&self, path: &str) -> Result<Ino> {
        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(path.to_string()));
        }
        // Inode + dentry commit together, so a failed link can't orphan the
        // inode (C1/M6).
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit::new(FileKind::Dir, DIR_MODE))
            .await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        Ok(ino)
    }

    /// Create a directory and any missing parents (like `mkdir -p`).
    /// Returns the inode of the final component (root for `/`).
    pub async fn mkdir_p(&self, path: &str) -> Result<Ino> {
        let mut ino = self.root_ino;
        for seg in Self::split(path)? {
            match self.meta.lookup(ino, seg).await? {
                Some(child) => {
                    let inode = self
                        .meta
                        .get_inode(child)
                        .await?
                        .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
                    if inode.kind != FileKind::Dir {
                        return Err(OrigoFSError::NotADirectory(path.to_string()));
                    }
                    ino = child;
                }
                None => {
                    // Create this segment atomically (inode + dentry). If a
                    // concurrent writer wins the race, `add_dentry` errors on the
                    // unique index; the transaction rolls back (no orphaned
                    // inode) and we adopt the directory they created, keeping
                    // `mkdir -p` idempotent under concurrency (C1/M6).
                    let mut tx = self.meta.begin().await?;
                    let child = tx
                        .create_inode(InodeInit::new(FileKind::Dir, DIR_MODE))
                        .await?;
                    match tx.add_dentry(ino, seg, child).await {
                        Ok(()) => {
                            tx.commit().await?;
                            ino = child;
                        }
                        Err(OrigoFSError::AlreadyExists(_)) => {
                            // Awaited, not dropped: the `lookup` immediately
                            // below must not race this rollback for the pooled
                            // connection. See `MetaTxn::rollback`.
                            tx.rollback().await?;
                            let existing = self
                                .meta
                                .lookup(ino, seg)
                                .await?
                                .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
                            let inode = self
                                .meta
                                .get_inode(existing)
                                .await?
                                .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
                            if inode.kind != FileKind::Dir {
                                return Err(OrigoFSError::NotADirectory(path.to_string()));
                            }
                            ino = existing;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Ok(ino)
    }

    /// Remove an empty directory.
    pub async fn rmdir(&self, path: &str) -> Result<()> {
        let (parent, name) = self.resolve_parent(path).await?;
        let ino = self
            .meta
            .lookup(parent, name)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        let inode = self
            .meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        if inode.kind != FileKind::Dir {
            return Err(OrigoFSError::NotADirectory(path.to_string()));
        }
        // A cheap early answer for the common case; the binding check is the
        // conditional delete below, which evaluates emptiness as part of the
        // statement rather than trusting this read from before the transaction.
        if self.meta.child_count(ino).await? > 0 {
            return Err(OrigoFSError::DirectoryNotEmpty(path.to_string()));
        }
        // Unlink + free the inode atomically (C1/L3).
        let mut tx = self.meta.begin().await?;
        tx.remove_dentry(parent, name).await?;
        if !tx.delete_inode_if_childless(ino).await? {
            return Err(OrigoFSError::DirectoryNotEmpty(path.to_string()));
        }
        tx.commit().await?;
        Ok(())
    }

    /// List a directory's entries, ordered by name.
    pub async fn ls(&self, path: &str) -> Result<Vec<DirEntry>> {
        let ino = self.resolve(path).await?;
        self.ensure_dir(ino).await?;
        self.meta.list_dir(ino).await
    }

    // --- file operations --------------------------------------------------

    /// Resolve the *existing* file inode for `(parent, name)`, or `None` if the
    /// name is free. Errors if the name exists but is a directory. Creating a
    /// missing file is deferred to the caller's transaction (via
    /// [`create_file_in`](Self::create_file_in)) so the new inode, its dentry,
    /// and its content all commit atomically (C1/M6).
    pub(crate) async fn lookup_file(
        &self,
        parent: Ino,
        name: &str,
        path: &str,
    ) -> Result<Option<Ino>> {
        match self.meta.lookup(parent, name).await? {
            Some(existing) => {
                let inode = self
                    .meta
                    .get_inode(existing)
                    .await?
                    .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
                if inode.kind == FileKind::Dir {
                    return Err(OrigoFSError::IsADirectory(path.to_string()));
                }
                Ok(Some(existing))
            }
            None => Ok(None),
        }
    }

    /// Create a fresh regular-file inode and link it under `(parent, name)`,
    /// inside `tx`. Pairs with [`lookup_file`](Self::lookup_file): if the name
    /// was taken by a concurrent writer, `add_dentry` errors on the unique index
    /// and the whole transaction rolls back rather than orphaning the inode.
    pub(crate) async fn create_file_in(
        tx: &mut dyn MetaTxn,
        parent: Ino,
        name: &str,
    ) -> Result<Ino> {
        let ino = tx
            .create_inode(InodeInit::new(FileKind::File, FILE_MODE))
            .await?;
        tx.add_dentry(parent, name, ino).await?;
        Ok(ino)
    }

    /// Chunk `data` (content-defined), store each chunk, and write a manifest.
    /// Returns `(manifest_hash, size)`; an empty body yields `(None, 0)`.
    pub(crate) async fn store_body(&self, data: &[u8]) -> Result<(Option<Hash>, u64)> {
        if data.is_empty() {
            return Ok((None, 0));
        }
        // The FastCDC scan is a CPU-bound pass over the whole buffer, and this is
        // the path behind `write`, `write_as`, `write_or_propose`, `vfs_write`, and
        // every merge. Run bare in an `async fn` it pins a runtime worker for the
        // duration — seconds, on a large body — starving every other task; that
        // matters most in the Python bindings, where one runtime serves the whole
        // process. `write_reader` already chunks off-runtime; this brings `write`
        // in line. (`block_in_place` rather than `spawn_blocking`, so `data` can
        // stay borrowed instead of being copied wholesale — see the helper.)
        let bounds = crate::util::blocking_section(|| chunk_bounds(data));
        // Bounded-concurrency upload, ordered: `buffered` keeps up to N puts in
        // flight but yields results in submission order, so the manifest's chunk
        // order — which *is* the file's byte order — is preserved without sorting.
        let chunks: Vec<ChunkRef> = futures::stream::iter(bounds)
            .map(|(off, len)| async move {
                self.content
                    .put(&data[off..off + len])
                    .await
                    .map(|hash| ChunkRef {
                        hash,
                        len: len as u32,
                    })
            })
            .buffered(upload_concurrency())
            .try_collect()
            .await?;
        let manifest = Manifest {
            size: data.len() as u64,
            chunks,
        };
        let mhash = self.content.put(&manifest.encode()?).await?;
        // Durability barrier (C4): make the content durable before the metadata
        // commit that will reference it. For LocalCasStore each `put` already
        // fsynced; for PackStore this seals the open pack so a crash can't lose
        // chunks that only lived in the in-memory buffer while metadata points
        // at them. Most backends flush immediately, so this is a cheap no-op.
        self.content.flush().await?;
        Ok((Some(mhash), manifest.size))
    }

    /// Store the canonical **empty** manifest and make it durable, returning its
    /// hash.
    ///
    /// [`store_body`](Self::store_body) returns `None` for an empty body — an inode
    /// with no content hash — so the callers that need an empty *file* to have a
    /// real manifest (a merge result, a suggestion's proposed bytes, a git-imported
    /// blob) put one themselves. Each of them used a bare `content.put`, which
    /// skips the durability barrier `store_body` pays on every non-empty body: on a
    /// batching backend the manifest lived only in `PackStore`'s in-memory buffer
    /// while the metadata referencing it committed, so a crash in that window left
    /// a row pointing at content that was never sealed (`ContentMissing`).
    ///
    /// One helper rather than four flushes, so a new empty-body caller inherits the
    /// barrier instead of having to remember it.
    pub(crate) async fn store_empty_manifest(&self) -> Result<Hash> {
        let hash = self.content.put(&Manifest::default().encode()?).await?;
        self.content.flush().await?;
        Ok(hash)
    }

    pub(crate) async fn load_manifest(&self, mhash: &Hash) -> Result<Manifest> {
        let bytes = self.content.get(mhash).await?;
        Manifest::decode(&bytes)
    }

    /// Write `data` as the entire contents of `path`, creating the file if needed.
    /// The body is content-defined-chunked; unchanged chunks are deduplicated.
    pub async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        crate::retry::retrying("write", || self.write_attempt(path, data)).await
    }

    /// One attempt at [`write`](Self::write); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn write_attempt(&self, path: &str, data: &[u8]) -> Result<()> {
        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;
        // Refuse before storing, not after: a quota that only rejected the metadata
        // commit would still have uploaded the body, leaving chunks for gc to sweep
        // and charging the user's bandwidth for a write that was never going to
        // land (issue #116).
        self.check_quota_for_path(path, data.len() as u64).await?;
        // Content is made durable first (store_body flushes), then the metadata
        // that references it commits atomically: for a new file the inode, its
        // dentry, and its content all land together or not at all (C1).
        let (mhash, size) = self.store_body(data).await?;
        // The lookup is *before* the transaction, so a concurrent writer can
        // create the same new path in between. On that unique-index failure we
        // roll back and retry, adopting their inode and applying this write as an
        // update — so racing create-or-update writes linearize instead of one
        // spuriously failing with `AlreadyExists` (mirrors `mkdir_p`).
        for _ in 0..CREATE_RETRIES {
            let existing = self.lookup_file(parent, name, path).await?;
            let mut tx = self.meta.begin().await?;
            let ino = match existing {
                Some(ino) => ino,
                None => match Self::create_file_in(tx.as_mut(), parent, name).await {
                    Ok(ino) => ino,
                    Err(OrigoFSError::AlreadyExists(_)) => {
                        // Awaited: the loop's next iteration re-reads immediately.
                        // See `MetaTxn::rollback`.
                        tx.rollback().await?;
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            tx.set_content(ino, mhash, size).await?;
            tx.commit().await?;
            return Ok(());
        }
        Err(OrigoFSError::Conflict(format!(
            "{path}: too many concurrent creators"
        )))
    }

    /// Stream a reader into the content store, returning `(manifest, size)`.
    ///
    /// The streaming half of a large write, shared by [`write_reader`](Self::write_reader)
    /// and [`write_reader_as`](Self::write_reader_as). It stores content and
    /// nothing else: no inode is resolved, no metadata is touched, so the caller
    /// owns the create-race and attribution decisions. Content is
    /// content-addressed and therefore idempotent, which is what lets the caller
    /// retry its metadata commit without re-reading the stream — and is why this
    /// is split out rather than duplicated.
    ///
    /// Memory is bounded by the channel depth plus the in-flight upload window —
    /// both `upload_concurrency()`, so <= 2 x 16 x `MAX_CHUNK` (8 MiB) at the
    /// default — plus the accumulating `Vec<ChunkRef>`: 36 bytes per chunk, about
    /// 0.055% of the file at the average chunk size. That manifest is the real
    /// ceiling on file size; see `docs/LIMITS.md`.
    pub(crate) async fn stream_body<R>(&self, path: &str, reader: R) -> Result<(Option<Hash>, u64)>
    where
        R: std::io::Read + Send + 'static,
    {
        // Chunk on the blocking pool, delivering to the async side over a queue as
        // deep as the upload window. Shallower and the window starves: `buffered(16)`
        // cannot hold 16 puts in flight if only 8 chunks are ever available to it,
        // which silently capped the effective concurrency at the old depth of 8.
        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Vec<u8>, String>>(
            upload_concurrency(),
        );
        let handle = tokio::task::spawn_blocking(move || {
            for item in fastcdc::v2020::StreamCDC::new(reader, MIN_CHUNK, AVG_CHUNK, MAX_CHUNK) {
                match item {
                    Ok(chunk) => {
                        if tx.blocking_send(Ok(chunk.data)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e.to_string()));
                        break;
                    }
                }
            }
        });

        // Same bounded-concurrency window as `store_body`, fed by the chunker
        // rather than a precomputed list.
        //
        // This must be one combinator over a *stream*, not a `recv()` loop that
        // pushes into a `FuturesOrdered`: an in-flight upload only makes progress
        // while something polls it, and a loop awaiting `rx.recv()` polls nothing
        // else. That shape overlapped only during the brief windows between
        // receives, yielding ~1.8x where the window should give ~16x — the first
        // version of this did exactly that, and `upload_concurrency.rs` caught it.
        // `buffered` polls the source and every in-flight put in the same poll, and
        // yields in submission order so the manifest stays in byte order.
        let chunks: Vec<ChunkRef> = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
        .map(|item| async move {
            let data = item.map_err(OrigoFSError::Content)?;
            let len = data.len() as u32;
            self.content
                .put(&data)
                .await
                .map(|hash| ChunkRef { hash, len })
        })
        .buffered(upload_concurrency())
        .try_collect()
        .await?;
        let size: u64 = chunks.iter().map(|c| c.len as u64).sum();
        // The `JoinError` matters and must not be discarded. `StreamCDC`'s own
        // errors arrive through the channel as `Err`, but a *panic* — most
        // plausibly from a caller-supplied `Read` impl — drops the sender instead:
        // `rx.recv()` returns `None`, the loop above exits perfectly normally, and
        // what follows would build a manifest from the chunks that happened to
        // arrive and commit it as the whole file. Silent truncation, reported as
        // `Ok(())`. Surfacing the join failure turns that into an error before any
        // metadata references the partial body.
        handle.await.map_err(|e| {
            OrigoFSError::Content(format!(
                "chunking {path} failed before the stream ended, so the file was \
                 only partially read: {e}"
            ))
        })?;

        let mhash = if size == 0 {
            None
        } else {
            let manifest = Manifest { size, chunks };
            Some(self.content.put(&manifest.encode()?).await?)
        };
        // Durability barrier (C4): seal/flush content before metadata references it.
        self.content.flush().await?;
        Ok((mhash, size))
    }

    /// Write a file by streaming from a blocking reader, chunking incrementally so
    /// large files never need to be fully resident. Creates the file if needed.
    ///
    /// **Unattributed** — records no blame and no edit-op, and is exempt from the
    /// §6 write policy by construction. Prefer
    /// [`write_reader_as`](Self::write_reader_as) wherever an actor is known;
    /// this exists for internal machinery and for genuinely actor-less imports.
    pub async fn write_reader<R>(&self, path: &str, reader: R) -> Result<()>
    where
        R: std::io::Read + Send + 'static,
    {
        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;

        let (mhash, size) = self.stream_body(path, reader).await?;

        // Commit the metadata atomically — the txn spans only this fast final
        // step, not the whole stream, so a large upload doesn't hold the write
        // lock while chunking.
        //
        // The lookup is inside the retry loop, and deliberately after the stream:
        // a concurrent writer can create the same new path while we were reading,
        // and the content is content-addressed (so already durable and idempotent)
        // by the time we get here. Retrying costs a metadata round trip, not a
        // re-read of the body.
        for _ in 0..CREATE_RETRIES {
            let existing = self.lookup_file(parent, name, path).await?;
            let mut txn = self.meta.begin().await?;
            let ino = match existing {
                Some(ino) => ino,
                None => match Self::create_file_in(txn.as_mut(), parent, name).await {
                    Ok(ino) => ino,
                    Err(OrigoFSError::AlreadyExists(_)) => {
                        txn.rollback().await?;
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            txn.set_content(ino, mhash, size).await?;
            txn.commit().await?;
            return Ok(());
        }
        Err(OrigoFSError::Conflict(format!(
            "{path}: too many concurrent creators"
        )))
    }

    /// Read the entire contents of a file.
    pub async fn read(&self, path: &str) -> Result<Bytes> {
        let ino = self.resolve(path).await?;
        let inode = self
            .meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        match inode.kind {
            FileKind::Dir => Err(OrigoFSError::IsADirectory(path.to_string())),
            FileKind::Symlink => Err(OrigoFSError::InvalidArgument(format!(
                "{path} is a symlink"
            ))),
            FileKind::File => match inode.content {
                None => Ok(Bytes::new()),
                Some(mhash) => self.content_bytes(&mhash).await,
            },
        }
    }

    /// Reassemble a file body from its manifest hash (the content address stored
    /// on a file inode / tree entry). Used by `read` and by the diff API to
    /// reconstruct a specific version's bytes.
    pub(crate) async fn content_bytes(&self, mhash: &Hash) -> Result<Bytes> {
        let manifest = self.load_manifest(mhash).await?;
        // This buffers the whole body in memory. That is fine for ordinary files,
        // but a caller that must stay bounded on an arbitrarily large file should
        // use [`Self::read_stream`] instead. The reservation is a capped hint
        // rather than the manifest's declared size — see `Manifest::capacity_hint`.
        let mut buf = BytesMut::with_capacity(manifest.capacity_hint());
        // Bounded-concurrency, ordered fetch (issue #113). This is what `read`
        // itself uses, so it is the single most-travelled read path in the engine.
        let mut parts = self.content_stream(manifest);
        while let Some(part) = parts.next().await {
            buf.extend_from_slice(&part?);
        }
        Ok(buf.freeze())
    }

    /// Resolve `path` for streaming: check it is a regular file and return its
    /// manifest (`None` if the file has no content, i.e. is empty). The manifest
    /// is loaded eagerly so its errors surface before any chunk is streamed.
    /// [`read_range_stream`](Self::read_range_stream) with a `'static` lifetime, so
    /// it can become an HTTP response body that outlives the handler — the same
    /// reason [`read_stream_owned`](Self::read_stream_owned) exists beside
    /// [`read_stream`](Self::read_stream).
    pub fn read_range_stream_owned(
        &self,
        manifest: Manifest,
        off: u64,
        len: u64,
    ) -> BoxStream<'static, Result<Bytes>>
    where
        C: Clone + 'static,
    {
        let end = off.saturating_add(len).min(manifest.size);
        let plan = covering_chunks(&manifest, off, end);
        let store = std::sync::Arc::new(self.content.clone());
        // Bounded look-ahead, in order — see `owned_chunk_stream`.
        futures::stream::iter(plan)
            .map(move |(hash, from, len)| {
                let store = store.clone();
                async move { store.get_range(&hash, from, len).await }
            })
            .buffered(fetch_concurrency())
            .boxed()
    }

    /// Open a file for streaming and report its size, so a caller can answer a
    /// ranged request without a second metadata round trip.
    ///
    /// `Content-Length` and `Content-Range` both need the size, and a `416` needs
    /// it *before* any bytes are read — so returning it alongside the manifest is
    /// what lets the HTTP surface answer a `Range` request in one pass.
    pub async fn open_for_range(&self, path: &str) -> Result<(Option<Manifest>, u64)> {
        let manifest = self.open_for_stream(path).await?;
        let size = manifest.as_ref().map(|m| m.size).unwrap_or(0);
        Ok((manifest, size))
    }

    /// Stream the byte range `[off, off+len)` of a file, fetching only the chunks
    /// that cover it.
    ///
    /// The streaming counterpart of [`read_range`](Self::read_range), which
    /// materializes the range in memory. That is fine for a small ranged read and
    /// wrong for serving media over HTTP, where a player may request a range of
    /// arbitrary size (including, quite legally, `bytes=0-` for the whole file) —
    /// buffering that would undo the reason `read_file` streams at all.
    ///
    /// Boundary chunks are trimmed with `get_range` so the store fetches only the
    /// needed slice of the first and last chunk, not the whole of either.
    pub fn read_range_stream(
        &self,
        manifest: Manifest,
        off: u64,
        len: u64,
    ) -> impl Stream<Item = Result<Bytes>> + Send + '_ {
        let end = off.saturating_add(len).min(manifest.size);
        // Precompute each covering chunk's (hash, from, to) so the stream itself
        // stays a simple fetch loop.
        let plan = covering_chunks(&manifest, off, end);
        let content = &self.content;
        // Bounded look-ahead, in order — see `owned_chunk_stream`.
        futures::stream::iter(plan)
            .map(move |(hash, from, len)| async move { content.get_range(&hash, from, len).await })
            .buffered(fetch_concurrency())
    }

    async fn open_for_stream(&self, path: &str) -> Result<Option<Manifest>> {
        let ino = self.resolve(path).await?;
        let inode = self
            .meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        match inode.kind {
            FileKind::Dir => return Err(OrigoFSError::IsADirectory(path.to_string())),
            FileKind::Symlink => {
                return Err(OrigoFSError::InvalidArgument(format!(
                    "{path} is a symlink"
                )));
            }
            FileKind::File => {}
        }
        match inode.content {
            None => Ok(None),
            Some(mhash) => Ok(Some(self.load_manifest(&mhash).await?)),
        }
    }

    /// Stream a file's body chunk-by-chunk, fetching one chunk at a time so an
    /// arbitrarily large file never has to be fully resident. Prefer this over
    /// [`Self::read`] whenever a file may be larger than you want to hold in
    /// memory (there is no fixed size ceiling on origofs files).
    ///
    /// The stream yields the body in order; a chunk that fails to fetch
    /// (missing/corrupt) surfaces as an `Err` item, after which the stream ends.
    /// An empty file, or one with no content, yields no items. The returned stream
    /// borrows `self`; for one that can outlive this handle (e.g. moved into an
    /// HTTP response body) use [`Self::read_stream_owned`].
    pub async fn read_stream(&self, path: &str) -> Result<BoxStream<'_, Result<Bytes>>> {
        match self.open_for_stream(path).await? {
            None => Ok(futures::stream::empty::<Result<Bytes>>().boxed()),
            Some(manifest) => Ok(self.content_stream(manifest).boxed()),
        }
    }

    /// A borrowed [`Stream`] over a manifest's chunks, with up to
    /// [`fetch_concurrency`] fetches in flight. Borrows `self`, so the stream
    /// cannot outlive this handle. See [`owned_chunk_stream`] for why `buffered`.
    pub(crate) fn content_stream(
        &self,
        manifest: Manifest,
    ) -> impl Stream<Item = Result<Bytes>> + Send + '_ {
        let content = &self.content;
        futures::stream::iter(manifest.chunks)
            .map(move |c| async move { content.get(&c.hash).await })
            .buffered(fetch_concurrency())
    }

    /// Like [`Self::read_stream`] but the returned stream owns its content handle,
    /// so it is `'static` and can be moved into a spawned task or a response body
    /// that outlives this borrow. Requires a cloneable content store — every real
    /// backend is `Arc`-based, so this holds in practice.
    pub async fn read_stream_owned(&self, path: &str) -> Result<BoxStream<'static, Result<Bytes>>>
    where
        C: Clone + 'static,
    {
        match self.open_for_stream(path).await? {
            None => Ok(futures::stream::empty::<Result<Bytes>>().boxed()),
            Some(manifest) => Ok(owned_chunk_stream(self.content.clone(), manifest).boxed()),
        }
    }

    /// Stream a file's body into an async writer without ever materializing it
    /// whole; returns the number of bytes written. The memory-bounded way to copy
    /// a large file out — to a socket, a temp file, or an HTTP response body.
    pub async fn read_to_writer<W>(&self, path: &str, mut writer: W) -> Result<u64>
    where
        W: tokio::io::AsyncWrite + Unpin + Send,
    {
        use tokio::io::AsyncWriteExt;
        let mut stream = self.read_stream(path).await?;
        let mut total: u64 = 0;
        while let Some(item) = stream.next().await {
            let bytes = item?;
            writer.write_all(&bytes).await?;
            total += bytes.len() as u64;
        }
        writer.flush().await?;
        Ok(total)
    }

    /// Read the byte range `[off, off + len)` of a file, fetching only the chunks
    /// that overlap the range.
    pub async fn read_range(&self, path: &str, off: u64, len: u64) -> Result<Bytes> {
        let ino = self.resolve(path).await?;
        let inode = self
            .meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        if inode.kind != FileKind::File {
            return Err(OrigoFSError::InvalidArgument(format!(
                "{path} is not a regular file"
            )));
        }
        let Some(mhash) = inode.content else {
            return Ok(Bytes::new());
        };
        let manifest = self.load_manifest(&mhash).await?;
        let end = off.saturating_add(len).min(manifest.size);
        if off >= end {
            return Ok(Bytes::new());
        }
        // Bounded by the same cap as a whole-body read: `end` derives from the
        // manifest's declared size, which is attacker-controlled on a corrupt
        // store (see `Manifest::capacity_hint`).
        let mut buf =
            BytesMut::with_capacity(((end - off) as usize).min(manifest.capacity_hint().max(1)));
        // Bounded-concurrency fetch, ordered: `buffered` keeps up to N `get_range`s
        // in flight but yields them in submission order, so appending as they
        // arrive still reconstructs the range in byte order (issue #113).
        let mut parts = futures::stream::iter(covering_chunks(&manifest, off, end))
            .map(|(hash, from, len)| async move { self.content.get_range(&hash, from, len).await })
            .buffered(fetch_concurrency());
        while let Some(part) = parts.next().await {
            buf.extend_from_slice(&part?);
        }
        Ok(buf.freeze())
    }

    /// Fetch inode metadata for a path.
    pub async fn stat(&self, path: &str) -> Result<Inode> {
        let ino = self.resolve(path).await?;
        self.meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))
    }

    /// Remove a file (decrementing link count; the inode is freed at nlink 0).
    pub async fn unlink(&self, path: &str) -> Result<()> {
        let (parent, name) = self.resolve_parent(path).await?;
        let ino = self
            .meta
            .lookup(parent, name)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        let inode = self
            .meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        if inode.kind == FileKind::Dir {
            return Err(OrigoFSError::IsADirectory(path.to_string()));
        }
        // Unlink and the inode's fate (free vs. decrement) commit together, so a
        // crash can't drop the name yet leave the inode dangling (C1/L3).
        let mut tx = self.meta.begin().await?;
        tx.remove_dentry(parent, name).await?;
        // Decremented by the database, not by us: the `nlink` we read above
        // happened before the transaction, so computing the new value here would
        // let two concurrent unlinks both write the same one (see
        // `MetaTxn::adjust_nlink`).
        if tx.adjust_nlink(ino, -1).await? <= 0 {
            tx.delete_inode(ino).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Remove a file or an empty directory.
    pub async fn remove(&self, path: &str) -> Result<()> {
        crate::retry::retrying("remove", || self.remove_attempt(path)).await
    }

    /// One attempt at [`remove`](Self::remove); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn remove_attempt(&self, path: &str) -> Result<()> {
        let inode = self.stat(path).await?;
        if inode.kind == FileKind::Dir {
            self.rmdir(path).await
        } else {
            self.unlink(path).await
        }
    }

    /// Refuse to move `sino` inside itself.
    ///
    /// `rename("/a", "/a/b/a2")` would make `a` a child of its own child. The
    /// subtree is then unreachable from the root, so it vanishes from `ls`, from
    /// `build_tree` — and from `mark_working`, which is what makes GC reclaim all
    /// of its content while the inode and dentry rows stay behind forever. An
    /// ordinary `mv` silently destroying data. POSIX `rename(2)` returns `EINVAL`.
    ///
    /// Walks up from the destination parent rather than scanning the source's
    /// subtree downward, so the cost is the destination's depth and not the size
    /// of what is being moved. The walk is bounded by `MAX_ANCESTOR_WALK` so a
    /// cycle already present in the store (from a build without this check)
    /// surfaces as an error instead of hanging.
    pub(crate) async fn ensure_not_own_descendant(&self, sino: Ino, dst_parent: Ino) -> Result<()> {
        /// Deeper than any real tree; only a pre-existing cycle reaches it.
        const MAX_ANCESTOR_WALK: usize = 4096;

        let mut cur = dst_parent;
        for _ in 0..MAX_ANCESTOR_WALK {
            if cur == sino {
                return Err(OrigoFSError::InvalidArgument(
                    "cannot move a directory inside itself".to_string(),
                ));
            }
            if cur == self.root_ino {
                return Ok(());
            }
            match self.meta.parent_of(cur).await? {
                Some(p) => cur = p,
                None => return Ok(()), // unlinked or the root: no cycle above it
            }
        }
        Err(OrigoFSError::Corrupt(format!(
            "directory ancestry from inode {dst_parent} exceeds {MAX_ANCESTOR_WALK} levels (cycle?)"
        )))
    }

    /// Rename/move `from` to `to`. Overwrites an existing regular file or an
    /// existing empty directory at `to`.
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        crate::retry::retrying("rename", || self.rename_attempt(from, to)).await
    }

    /// One attempt at [`rename`](Self::rename); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn rename_attempt(&self, from: &str, to: &str) -> Result<()> {
        let (sp, sn) = self.resolve_parent(from).await?;
        let sino = self
            .meta
            .lookup(sp, sn)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(from.to_string()))?;
        let (dp, dn) = self.resolve_parent(to).await?;
        self.ensure_dir(dp).await?;
        self.ensure_not_own_descendant(sino, dp).await?;

        // Read the destination's state before the txn; the mutations below all
        // commit together so a crash mid-rename can't leave the source unlinked
        // with the destination half-replaced, or orphan the overwritten inode.
        let overwrite = match self.meta.lookup(dp, dn).await? {
            Some(dino) if dino == sino => return Ok(()),
            Some(dino) => {
                let dinode = self
                    .meta
                    .get_inode(dino)
                    .await?
                    .ok_or_else(|| OrigoFSError::NotFound(to.to_string()))?;
                if dinode.kind == FileKind::Dir && self.meta.child_count(dino).await? > 0 {
                    return Err(OrigoFSError::DirectoryNotEmpty(to.to_string()));
                }
                Some((dino, dinode))
            }
            None => None,
        };

        let mut tx = self.meta.begin().await?;
        if let Some((dino, dinode)) = overwrite {
            tx.remove_dentry(dp, dn).await?;
            match dinode.kind {
                // Conditional, for the same reason `rmdir` is: the emptiness
                // check above ran before this transaction opened.
                FileKind::Dir => {
                    if !tx.delete_inode_if_childless(dino).await? {
                        return Err(OrigoFSError::DirectoryNotEmpty(to.to_string()));
                    }
                }
                _ => {
                    if tx.adjust_nlink(dino, -1).await? <= 0 {
                        tx.delete_inode(dino).await?;
                    }
                }
            }
        }
        tx.remove_dentry(sp, sn).await?;
        tx.add_dentry(dp, dn, sino).await?;
        tx.commit().await?;
        Ok(())
    }

    // --- symlinks ---------------------------------------------------------

    /// Create a symbolic link at `linkpath` pointing at `target`.
    pub async fn symlink(&self, target: &str, linkpath: &str) -> Result<Ino> {
        crate::retry::retrying("symlink", || self.symlink_attempt(target, linkpath)).await
    }

    /// One attempt at [`symlink`](Self::symlink); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn symlink_attempt(&self, target: &str, linkpath: &str) -> Result<Ino> {
        let (parent, name) = self.resolve_parent(linkpath).await?;
        self.ensure_dir(parent).await?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(linkpath.to_string()));
        }
        // Inode, its target, and its dentry commit together (C1/M6).
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit::new(FileKind::Symlink, SYMLINK_MODE))
            .await?;
        tx.set_symlink(ino, target).await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        Ok(ino)
    }

    /// Read a symlink's target.
    pub async fn readlink(&self, path: &str) -> Result<String> {
        let ino = self.resolve(path).await?;
        self.meta
            .get_symlink(ino)
            .await?
            .ok_or_else(|| OrigoFSError::InvalidArgument(format!("{path} is not a symlink")))
    }
}
