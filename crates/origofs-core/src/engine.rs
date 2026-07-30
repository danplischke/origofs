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
use futures::stream::{BoxStream, StreamExt};
use std::sync::Arc;

const DIR_MODE: u32 = 0o040755;
const FILE_MODE: u32 = 0o100644;
const SYMLINK_MODE: u32 = 0o120777;

/// Bound on retries when a concurrent writer wins the create race for a new
/// path. One retry resolves it in practice (the loser then finds the inode and
/// updates it); the bound only guards against a pathological churn of
/// create/delete on the same name.
pub(crate) const CREATE_RETRIES: usize = 16;

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

/// An owned [`Stream`] over a manifest's chunks, one `store.get` at a time. The
/// store handle is moved into the stream state, so the stream is self-contained
/// (`'static` when `S` is) and can outlive the [`Fs`] it came from — unlike
/// [`Fs::content_stream`], which borrows. Powers [`Fs::read_stream_owned`].
fn owned_chunk_stream<S: ContentStore + 'static>(
    store: S,
    manifest: Manifest,
) -> impl Stream<Item = Result<Bytes>> + Send + 'static {
    futures::stream::unfold(
        Some((store, manifest.chunks.into_iter())),
        |state| async move {
            let (store, mut chunks) = state?;
            let c = chunks.next()?;
            match store.get(&c.hash).await {
                Ok(bytes) => Some((Ok(bytes), Some((store, chunks)))),
                // Surface the error once, then end the stream (state -> None).
                Err(e) => Some((Err(e), None)),
            }
        },
    )
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
    /// (HEAD → `main`, default `versioning = native`).
    pub async fn init(&self) -> Result<()> {
        self.meta.init().await?;
        self.init_versioning().await?;
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
        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(path.to_string()));
        }
        // Inode + dentry commit together, so a failed link can't orphan the
        // inode (C1/M6).
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit {
                kind: FileKind::Dir,
                mode: DIR_MODE,
            })
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
                        .create_inode(InodeInit {
                            kind: FileKind::Dir,
                            mode: DIR_MODE,
                        })
                        .await?;
                    match tx.add_dentry(ino, seg, child).await {
                        Ok(()) => {
                            tx.commit().await?;
                            ino = child;
                        }
                        Err(OrigoFSError::AlreadyExists(_)) => {
                            drop(tx); // roll back the just-created inode
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
        if self.meta.child_count(ino).await? > 0 {
            return Err(OrigoFSError::DirectoryNotEmpty(path.to_string()));
        }
        // Unlink + free the inode atomically (C1/L3).
        let mut tx = self.meta.begin().await?;
        tx.remove_dentry(parent, name).await?;
        tx.delete_inode(ino).await?;
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
            .create_inode(InodeInit {
                kind: FileKind::File,
                mode: FILE_MODE,
            })
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
        let mut chunks = Vec::new();
        for (off, len) in chunk_bounds(data) {
            let hash = self.content.put(&data[off..off + len]).await?;
            chunks.push(ChunkRef {
                hash,
                len: len as u32,
            });
        }
        let manifest = Manifest {
            size: data.len() as u64,
            chunks,
        };
        let mhash = self.content.put(&manifest.encode()).await?;
        // Durability barrier (C4): make the content durable before the metadata
        // commit that will reference it. For LocalCasStore each `put` already
        // fsynced; for PackStore this seals the open pack so a crash can't lose
        // chunks that only lived in the in-memory buffer while metadata points
        // at them. Most backends flush immediately, so this is a cheap no-op.
        self.content.flush().await?;
        Ok((Some(mhash), manifest.size))
    }

    pub(crate) async fn load_manifest(&self, mhash: &Hash) -> Result<Manifest> {
        let bytes = self.content.get(mhash).await?;
        Manifest::decode(&bytes)
    }

    /// Write `data` as the entire contents of `path`, creating the file if needed.
    /// The body is content-defined-chunked; unchanged chunks are deduplicated.
    pub async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;
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
                        drop(tx);
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

    /// Write a file by streaming from a blocking reader, chunking incrementally so
    /// large files never need to be fully resident. Creates the file if needed.
    pub async fn write_reader<R>(&self, path: &str, reader: R) -> Result<()>
    where
        R: std::io::Read + Send + 'static,
    {
        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;
        let existing = self.lookup_file(parent, name, path).await?;

        // Chunk on the blocking pool; deliver one chunk at a time to the async side.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<std::result::Result<Vec<u8>, String>>(8);
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

        let mut chunks = Vec::new();
        let mut size: u64 = 0;
        while let Some(item) = rx.recv().await {
            let data = item.map_err(OrigoFSError::Content)?;
            size += data.len() as u64;
            let hash = self.content.put(&data).await?;
            chunks.push(ChunkRef {
                hash,
                len: data.len() as u32,
            });
        }
        let _ = handle.await;

        let mhash = if size == 0 {
            None
        } else {
            let manifest = Manifest { size, chunks };
            Some(self.content.put(&manifest.encode()).await?)
        };
        // Durability barrier (C4): seal/flush content before metadata references it.
        self.content.flush().await?;
        // Commit the metadata atomically — the txn spans only this fast final
        // step, not the whole stream, so a large upload doesn't hold the write
        // lock while chunking.
        let mut txn = self.meta.begin().await?;
        let ino = match existing {
            Some(ino) => ino,
            None => Self::create_file_in(txn.as_mut(), parent, name).await?,
        };
        txn.set_content(ino, mhash, size).await?;
        txn.commit().await?;
        Ok(())
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
        // use [`Self::read_stream`] instead. We still refuse to *pre-allocate* from
        // the manifest's declared size: even though `Manifest::decode` checks
        // `size == Σ chunk.len`, a crafted manifest can declare many oversized
        // chunk lengths, so we reserve a bounded amount up front and let the buffer
        // grow as real chunk bytes arrive.
        const INITIAL_HINT: usize = 8 * 1024 * 1024;
        let hint = manifest
            .chunks
            .iter()
            .fold(0usize, |a, c| a.saturating_add(c.len as usize))
            .min(INITIAL_HINT);
        let mut buf = BytesMut::with_capacity(hint);
        for c in &manifest.chunks {
            buf.extend_from_slice(&self.content.get(&c.hash).await?);
        }
        Ok(buf.freeze())
    }

    /// Resolve `path` for streaming: check it is a regular file and return its
    /// manifest (`None` if the file has no content, i.e. is empty). The manifest
    /// is loaded eagerly so its errors surface before any chunk is streamed.
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

    /// A borrowed [`Stream`] over a manifest's chunks, one `content.get` at a
    /// time. Borrows `self`, so the stream cannot outlive this handle.
    fn content_stream(&self, manifest: Manifest) -> impl Stream<Item = Result<Bytes>> + Send + '_ {
        futures::stream::unfold(
            Some((&self.content, manifest.chunks.into_iter())),
            |state| async move {
                let (content, mut chunks) = state?;
                let c = chunks.next()?;
                match content.get(&c.hash).await {
                    Ok(bytes) => Some((Ok(bytes), Some((content, chunks)))),
                    // Surface the error once, then end the stream (state -> None).
                    Err(e) => Some((Err(e), None)),
                }
            },
        )
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
        let mut buf = BytesMut::with_capacity((end - off) as usize);
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
            let part = self.content.get_range(&c.hash, from, to - from).await?;
            buf.extend_from_slice(&part);
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
        let nlink = inode.nlink - 1;
        if nlink <= 0 {
            tx.delete_inode(ino).await?;
        } else {
            tx.set_nlink(ino, nlink).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Remove a file or an empty directory.
    pub async fn remove(&self, path: &str) -> Result<()> {
        let inode = self.stat(path).await?;
        if inode.kind == FileKind::Dir {
            self.rmdir(path).await
        } else {
            self.unlink(path).await
        }
    }

    /// Rename/move `from` to `to`. Overwrites an existing regular file or an
    /// existing empty directory at `to`.
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let (sp, sn) = self.resolve_parent(from).await?;
        let sino = self
            .meta
            .lookup(sp, sn)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(from.to_string()))?;
        let (dp, dn) = self.resolve_parent(to).await?;
        self.ensure_dir(dp).await?;

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
                FileKind::Dir => tx.delete_inode(dino).await?,
                _ => {
                    let nlink = dinode.nlink - 1;
                    if nlink <= 0 {
                        tx.delete_inode(dino).await?;
                    } else {
                        tx.set_nlink(dino, nlink).await?;
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
        let (parent, name) = self.resolve_parent(linkpath).await?;
        self.ensure_dir(parent).await?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(linkpath.to_string()));
        }
        // Inode, its target, and its dentry commit together (C1/M6).
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit {
                kind: FileKind::Symlink,
                mode: SYMLINK_MODE,
            })
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
