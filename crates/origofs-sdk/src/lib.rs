//! origofs-sdk — an ergonomic front door to an origofs workspace.
//!
//! A workspace pairs a metadata store (SQLite or Postgres) with a pluggable
//! content backend (local dir, S3-compatible object store, in-memory, or a cached
//! tier). Both sides are `Arc<dyn …>`, so the backend is chosen at runtime. Later
//! milestones add commits and attribution behind the same façade.

use origofs_core::{
    ContentStore, Fs, LocalCasStore, MetadataStore, PostgresMetadataStore, Result,
    SqliteMetadataStore,
};
use std::path::Path;
use std::sync::Arc;

pub use bytes::Bytes;
pub use futures::stream::BoxStream;
pub use origofs_core::{
    Actor, ActorInit, ActorKind, BlameRange, CommitInfo, Conflict, DiffEntry, DiffStatus, DirEntry,
    EditOp, EncryptedStore, Event, EventInit, EventSubscription, FileKind, GcStats, GcsConfig,
    Hash, Inode, MemStore, MergeOutcome, ObjectContentStore, OrigoFSError, PackStore, Presence,
    RebuildReport, S3Config, Suggestion, SuggestionContent, SuggestionInit, SuggestionStatus,
    TieredStore, ToolCallInit, VerifyingStore, VersioningMode, WriteCtx, WriteOutcome, WritePolicy,
};
#[cfg(feature = "coedit")]
pub use origofs_core::{CoeditDoc, CoeditRelayNote, CoeditRelaySub};

// ── Access surfaces ─────────────────────────────────────────────────────────
// Each surface that was formerly its own crate is now an opt-in, feature-gated
// module over this same `Workspace`. A default build pulls none of their
// dependencies (axum, fuser, nfsserve, …); enable the ones you need, or `full`.
// FUSE/NFS are Unix-only. See `Cargo.toml` `[features]`.
#[cfg(feature = "api")]
pub mod api;
#[cfg(all(unix, feature = "fuse"))]
pub mod fuse;
#[cfg(feature = "git")]
pub mod git;
#[cfg(feature = "mcp")]
pub mod mcp;
#[cfg(all(unix, feature = "nfs"))]
pub mod nfs;
#[cfg(feature = "sandbox")]
pub mod sandbox;

type Meta = Arc<dyn MetadataStore>;
type Content = Arc<dyn ContentStore>;

/// The outcome of a [`Workspace::ready`] readiness probe: whether each backend
/// answered. `None` for a store means it is reachable; `Some(msg)` carries why
/// the probe failed. Backs the HTTP `/readyz` endpoint.
#[derive(Debug, Clone)]
pub struct ReadyReport {
    /// `None` if the metadata store answered its probe; the error otherwise.
    pub metadata: Option<String>,
    /// `None` if the content store answered its probe; the error otherwise.
    pub content: Option<String>,
}

impl ReadyReport {
    /// Whether both backends are reachable — the service is ready to serve.
    pub fn is_ready(&self) -> bool {
        self.metadata.is_none() && self.content.is_none()
    }
}

/// A workspace: a metadata store over a content store.
///
/// Cheap to clone — it's a pair of `Arc` handles to the shared backends, so
/// clones share the same underlying store (useful for handing an owned
/// `Workspace` to a mount/serve call while keeping one for the API).
#[derive(Clone)]
pub struct Workspace {
    fs: Fs<Meta, Content>,
    /// The concrete Postgres store, kept when opened on Postgres so the
    /// `LISTEN/NOTIFY` change-feed subscription is reachable (it is PG-specific
    /// and not on the object-safe `MetadataStore` trait). `None` otherwise.
    pg: Option<Arc<PostgresMetadataStore>>,
}

impl Workspace {
    /// Open (creating if needed) a workspace from explicit metadata + content
    /// backends.
    pub async fn open(meta: Meta, content: Content) -> Result<Self> {
        let fs = Fs::new(meta, content);
        fs.init().await?;
        Ok(Self { fs, pg: None })
    }

    /// SQLite metadata + content-addressed blobs under a local directory.
    pub async fn open_local(db_path: impl AsRef<Path>, cas_dir: impl AsRef<Path>) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let content: Content = Arc::new(LocalCasStore::open(cas_dir).await?);
        Self::open(meta, content).await
    }

    /// SQLite metadata + a local content store **encrypted at rest** with a key
    /// derived from `passphrase` (Argon2id) and a per-store random salt kept in
    /// `cas_dir/keysalt`. The same passphrase must be used on reopen; the wrong
    /// one fails loudly rather than returning garbage. The salt is created on
    /// first open and is not secret, but it must persist — it lives beside the
    /// content store so it survives a metadata-DB loss (recovery-safe).
    pub async fn open_local_encrypted(
        db_path: impl AsRef<Path>,
        cas_dir: impl AsRef<Path>,
        passphrase: &str,
    ) -> Result<Self> {
        let cas_dir = cas_dir.as_ref();
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        // Open the CAS first so the directory exists, then place the salt in it.
        let backend: Content = Arc::new(LocalCasStore::open(cas_dir).await?);
        let salt = read_or_create_salt(cas_dir).await?;
        let content: Content =
            Arc::new(EncryptedStore::from_passphrase(backend, passphrase, &salt)?);
        Self::open(meta, content).await
    }

    /// SQLite metadata + an S3-compatible object store for content.
    pub async fn open_s3(db_path: impl AsRef<Path>, cfg: S3Config) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        // Verify integrity on read: object storage can bit-rot, so a corrupt
        // object surfaces as `Corrupt` rather than being served as authentic (M1).
        let content: Content =
            Arc::new(VerifyingStore::new(Arc::new(ObjectContentStore::s3(cfg)?)));
        Self::open(meta, content).await
    }

    /// SQLite metadata + a **packed** local content store: chunks batched into
    /// pack objects under `data_dir`, with the per-chunk index under `index_dir`.
    /// The local mirror of [`Workspace::open_s3_packed`].
    pub async fn open_local_packed(
        db_path: impl AsRef<Path>,
        data_dir: impl AsRef<Path>,
        index_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let data: Content = Arc::new(LocalCasStore::open(data_dir).await?);
        let index: Content = Arc::new(LocalCasStore::open(index_dir).await?);
        let content: Content = Arc::new(PackStore::new(data, index));
        Self::open(meta, content).await
    }

    /// SQLite metadata + an S3-compatible object store whose chunks are batched
    /// into **pack objects** (few large PUTs instead of many tiny ones), with the
    /// per-chunk index kept in a local directory. This is the recommended layout
    /// for object storage; call [`Workspace::flush`] (or `commit`) to seal the
    /// open pack and [`Workspace::repack`] to reclaim deleted space.
    pub async fn open_s3_packed(
        db_path: impl AsRef<Path>,
        cfg: S3Config,
        index_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let data: Content = Arc::new(ObjectContentStore::s3(cfg)?);
        let index: Content = Arc::new(LocalCasStore::open(index_dir).await?);
        // Verify integrity on read at the outermost (chunk-addressed) layer, so a
        // bit-rotted pack surfaces as `Corrupt` on the affected chunk (M1).
        let content: Content = Arc::new(VerifyingStore::new(Arc::new(PackStore::new(data, index))));
        Self::open(meta, content).await
    }

    /// Postgres metadata (multi-writer) over the given content backend.
    pub async fn open_pg(dsn: &str, content: Content) -> Result<Self> {
        let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let mut ws = Self::open(pg.clone(), content).await?;
        ws.pg = Some(pg);
        Ok(ws)
    }

    /// Postgres metadata (multi-writer) + an S3-compatible object store for
    /// content — the production pairing for a shared human+agent workspace: many
    /// writers on one database, one shared content store. Reads are integrity-
    /// verified (a bit-rotted object surfaces as `Corrupt`, not as authentic).
    pub async fn open_pg_s3(dsn: &str, cfg: S3Config) -> Result<Self> {
        let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let content: Content =
            Arc::new(VerifyingStore::new(Arc::new(ObjectContentStore::s3(cfg)?)));
        let mut ws = Self::open(pg.clone(), content).await?;
        ws.pg = Some(pg);
        Ok(ws)
    }

    /// Postgres metadata + a **packed** S3 object store (few large PUTs instead of
    /// many tiny ones), with the per-chunk index in a local directory. The
    /// recommended object-storage layout; seal the open pack with [`Workspace::flush`]
    /// (or `commit`) and reclaim deleted space with [`Workspace::repack`].
    pub async fn open_pg_s3_packed(
        dsn: &str,
        cfg: S3Config,
        index_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let data: Content = Arc::new(ObjectContentStore::s3(cfg)?);
        let index: Content = Arc::new(LocalCasStore::open(index_dir).await?);
        let content: Content = Arc::new(VerifyingStore::new(Arc::new(PackStore::new(data, index))));
        let mut ws = Self::open(pg.clone(), content).await?;
        ws.pg = Some(pg);
        Ok(ws)
    }

    /// SQLite metadata + a **native** GCS object store for content (GCS JSON API +
    /// OAuth2, so service-account / ADC / workload-identity credentials work; see
    /// [`GcsConfig`]). Reads are integrity-verified (a bit-rotted object surfaces
    /// as `Corrupt` rather than being served as authentic).
    pub async fn open_gcs(db_path: impl AsRef<Path>, cfg: GcsConfig) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let content: Content =
            Arc::new(VerifyingStore::new(Arc::new(ObjectContentStore::gcs(cfg)?)));
        Self::open(meta, content).await
    }

    /// SQLite metadata + a **packed** native GCS object store (few large PUTs
    /// instead of many tiny ones), with the per-chunk index under `index_dir`. The
    /// recommended object-storage layout; seal the open pack with [`Workspace::flush`]
    /// (or `commit`) and reclaim deleted space with [`Workspace::repack`].
    pub async fn open_gcs_packed(
        db_path: impl AsRef<Path>,
        cfg: GcsConfig,
        index_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let data: Content = Arc::new(ObjectContentStore::gcs(cfg)?);
        let index: Content = Arc::new(LocalCasStore::open(index_dir).await?);
        let content: Content = Arc::new(VerifyingStore::new(Arc::new(PackStore::new(data, index))));
        Self::open(meta, content).await
    }

    /// Postgres metadata (multi-writer) + a **native** GCS object store — the
    /// production pairing for a shared human+agent workspace on Google Cloud: many
    /// writers on one database, one shared content store. Reads are integrity-
    /// verified (a bit-rotted object surfaces as `Corrupt`, not as authentic).
    pub async fn open_pg_gcs(dsn: &str, cfg: GcsConfig) -> Result<Self> {
        let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let content: Content =
            Arc::new(VerifyingStore::new(Arc::new(ObjectContentStore::gcs(cfg)?)));
        let mut ws = Self::open(pg.clone(), content).await?;
        ws.pg = Some(pg);
        Ok(ws)
    }

    /// Postgres metadata + a **packed** native GCS object store, with the per-chunk
    /// index in a local directory. The recommended object-storage layout for a team
    /// on Google Cloud; seal the open pack with [`Workspace::flush`] (or `commit`)
    /// and reclaim deleted space with [`Workspace::repack`].
    pub async fn open_pg_gcs_packed(
        dsn: &str,
        cfg: GcsConfig,
        index_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let data: Content = Arc::new(ObjectContentStore::gcs(cfg)?);
        let index: Content = Arc::new(LocalCasStore::open(index_dir).await?);
        let content: Content = Arc::new(VerifyingStore::new(Arc::new(PackStore::new(data, index))));
        let mut ws = Self::open(pg.clone(), content).await?;
        ws.pg = Some(pg);
        Ok(ws)
    }

    /// SQLite metadata + an **in-memory** object store — the same object-store
    /// adapter as [`Workspace::open_s3`] minus the network, so it exercises the
    /// real object-storage content path (integrity verification included). For
    /// local development and tests without a live bucket; content is not durable.
    pub async fn open_object_memory(db_path: impl AsRef<Path>) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let content: Content = Arc::new(VerifyingStore::new(Arc::new(
            ObjectContentStore::in_memory(),
        )));
        Self::open(meta, content).await
    }

    /// Access the underlying engine for operations not surfaced here.
    pub fn fs(&self) -> &Fs<Meta, Content> {
        &self.fs
    }

    /// Probe the metadata and content backends for a readiness check (the HTTP
    /// `/readyz` endpoint). Both probes run concurrently; an unreachable backend
    /// is reported per-store rather than collapsing the whole check. This is
    /// distinct from liveness (`/health`), which only says the process is up.
    pub async fn ready(&self) -> ReadyReport {
        let (meta, content) = self.fs.probe().await;
        ReadyReport {
            metadata: meta.err().map(|e| e.to_string()),
            content: content.err().map(|e| e.to_string()),
        }
    }

    // --- workspaces (multi-workspace in one store) -----------------------

    /// Open (creating on first use) another **workspace** inside this same store,
    /// returning a `Workspace` bound to it. Workspaces share the store's content
    /// and identity (actors/blame/audit) and are separated by a `workspace_id`;
    /// each has its own root, refs, and working tree (`docs/MULTI_TENANCY.md`).
    /// The metadata connection/pool, content store, and any Postgres push-feed
    /// handle are shared with `self`, so this is cheap.
    pub async fn workspace(&self, name: &str) -> Result<Self> {
        // Validate the name at the user entry point: it becomes a registry key and
        // the recovery mirror's tag, and surfaces in listings/URLs. Reject the same
        // set `validate_component` rejects for path components (empty, `.`/`..`,
        // path separators, NUL) so a workspace name can't be empty or path-like.
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.contains('\0')
        {
            return Err(OrigoFSError::InvalidArgument(format!(
                "invalid workspace name: {name:?}"
            )));
        }
        let (id, root) =
            match self.fs.meta.lookup_workspace(name).await? {
                Some(existing) => existing,
                None => match self.fs.meta.create_workspace(name).await {
                    Ok(created) => created,
                    // Lost a concurrent first-time create race: the other caller's row
                    // is now committed, so adopt it instead of surfacing AlreadyExists
                    // (matches how `mkdir_p`/`write` adopt the winner). UNIQUE(name)
                    // guarantees there is exactly one row to find.
                    Err(OrigoFSError::AlreadyExists(_)) => {
                        self.fs.meta.lookup_workspace(name).await?.ok_or_else(|| {
                            OrigoFSError::AlreadyExists(format!("workspace {name}"))
                        })?
                    }
                    Err(e) => return Err(e),
                },
            };
        let scoped = self.fs.meta.with_workspace(id);
        let fs = self.fs.rebind(scoped, root);
        // Give a freshly created workspace its versioning refs/config; idempotent
        // for one that already exists.
        fs.init().await?;
        Ok(Self {
            fs,
            // Re-scope the Postgres push-feed handle to this workspace, so
            // `subscribe` tails only this workspace's change feed.
            pg: self.pg.as_ref().map(|p| p.for_workspace(id)),
        })
    }

    /// The names of every workspace in this store — `default` plus any opened via
    /// [`Self::workspace`], oldest first.
    pub async fn workspaces(&self) -> Result<Vec<String>> {
        Ok(self
            .fs
            .meta
            .list_workspaces()
            .await?
            .into_iter()
            .map(|(_id, name, _root)| name)
            .collect())
    }

    /// Record a collaboration event (best-effort: a feed hiccup never fails the
    /// underlying operation, which has already succeeded).
    async fn emit(
        &self,
        kind: &str,
        path: &str,
        detail: Option<String>,
        actor: Option<i64>,
        session: Option<i64>,
    ) {
        // Tag the event with the branch it happened on so a per-branch UI can
        // filter the feed. Best-effort, like the emit itself.
        let branch = self.fs.current_branch().await.ok().flatten();
        let _ = self
            .fs
            .record_event(EventInit {
                actor_id: actor,
                session_id: session,
                kind: kind.to_string(),
                path: path.to_string(),
                detail,
                branch,
            })
            .await;
    }

    #[tracing::instrument(level = "debug", skip_all, fields(path = %path, bytes = data.len()))]
    pub async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        self.fs.write(path, data).await?;
        self.emit("write", path, None, None, None).await;
        Ok(())
    }

    /// Write a file by streaming from a blocking reader (for large files).
    pub async fn write_reader<R: std::io::Read + Send + 'static>(
        &self,
        path: &str,
        reader: R,
    ) -> Result<()> {
        self.fs.write_reader(path, reader).await?;
        self.emit("write", path, None, None, None).await;
        Ok(())
    }

    pub async fn read(&self, path: &str) -> Result<Bytes> {
        self.fs.read(path).await
    }

    pub async fn read_range(&self, path: &str, off: u64, len: u64) -> Result<Bytes> {
        self.fs.read_range(path, off, len).await
    }

    /// Stream a file's body chunk-by-chunk without holding it all in memory — the
    /// memory-bounded counterpart to [`Self::read`] for large files (origofs imposes
    /// no fixed file-size ceiling). The stream is `'static`, so it can be moved
    /// into a spawned task or an HTTP response body.
    pub async fn read_stream(&self, path: &str) -> Result<BoxStream<'static, Result<Bytes>>> {
        self.fs.read_stream_owned(path).await
    }

    /// Stream a file's body into an async writer without ever materializing it
    /// whole; returns the number of bytes written.
    pub async fn read_to_writer<W>(&self, path: &str, writer: W) -> Result<u64>
    where
        W: tokio::io::AsyncWrite + Unpin + Send,
    {
        self.fs.read_to_writer(path, writer).await
    }

    pub async fn mkdir_p(&self, path: &str) -> Result<()> {
        self.fs.mkdir_p(path).await?;
        self.emit("mkdir", path, None, None, None).await;
        Ok(())
    }

    pub async fn ls(&self, path: &str) -> Result<Vec<DirEntry>> {
        self.fs.ls(path).await
    }

    pub async fn stat(&self, path: &str) -> Result<Inode> {
        self.fs.stat(path).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(path = %path))]
    pub async fn remove(&self, path: &str) -> Result<()> {
        self.fs.remove(path).await?;
        self.emit("remove", path, None, None, None).await;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(from = %from, to = %to))]
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.fs.rename(from, to).await?;
        self.emit("rename", from, Some(to.to_string()), None, None)
            .await;
        Ok(())
    }

    pub async fn readlink(&self, path: &str) -> Result<String> {
        self.fs.readlink(path).await
    }

    pub async fn symlink(&self, target: &str, linkpath: &str) -> Result<()> {
        self.fs.symlink(target, linkpath).await?;
        self.emit("symlink", linkpath, Some(target.to_string()), None, None)
            .await;
        Ok(())
    }

    // --- versioning ------------------------------------------------------

    /// Snapshot the working tree into a commit on the current branch.
    #[tracing::instrument(skip_all, fields(author = %author))]
    pub async fn commit(&self, author: &str, message: &str) -> Result<Hash> {
        let hash = self.fs.commit(author, message).await?;
        self.emit("commit", "/", Some(message.to_string()), None, None)
            .await;
        Ok(hash)
    }

    /// Commit history from HEAD (first-parent).
    pub async fn log(&self) -> Result<Vec<CommitInfo>> {
        self.fs.log().await
    }

    /// Working-tree changes relative to HEAD.
    pub async fn status(&self) -> Result<Vec<DiffEntry>> {
        self.fs.status().await
    }

    /// Paths that differ between two refs/commits (`from` → `to`), compared by
    /// content address — the cheap file-list half of a branch comparison.
    pub async fn diff(&self, from: &str, to: &str) -> Result<Vec<DiffEntry>> {
        self.fs.diff(from, to).await
    }

    /// A unified line diff of one path between two refs/commits (empty when
    /// unchanged on both sides).
    pub async fn diff_file(&self, from: &str, to: &str, path: &str) -> Result<String> {
        self.fs.diff_file(from, to, path).await
    }

    /// The current branch name (or `None` if detached).
    pub async fn current_branch(&self) -> Result<Option<String>> {
        self.fs.current_branch().await
    }

    /// Create a branch at the current HEAD commit.
    pub async fn create_branch(&self, name: &str) -> Result<()> {
        self.fs.create_branch(name).await
    }

    /// Switch the working tree to `branch`.
    pub async fn checkout(&self, branch: &str) -> Result<()> {
        self.fs.checkout(branch).await
    }

    /// All branches with their commit hashes.
    pub async fn list_branches(&self) -> Result<Vec<(String, Hash)>> {
        self.fs.list_branches().await
    }

    pub async fn versioning_mode(&self) -> Result<VersioningMode> {
        self.fs.versioning_mode().await
    }

    pub async fn set_versioning_mode(&self, mode: VersioningMode) -> Result<()> {
        self.fs.set_versioning_mode(mode).await
    }

    // --- maintenance -----------------------------------------------------

    /// Reclaim content-store objects unreachable from any ref or the live
    /// working tree. Run when the workspace is idle.
    #[tracing::instrument(skip_all)]
    pub async fn gc(&self) -> Result<GcStats> {
        self.fs.gc().await
    }

    /// Rebuild refs and the working tree from the content store's object graph,
    /// for disaster recovery after the metadata DB is lost. Open a workspace with
    /// a **fresh** metadata DB pointed at the surviving content store, then call
    /// this: it scans the store, recovers branch names + tips (from the ref
    /// mirror, or by inferring heads), and materializes the checked-out tree.
    ///
    /// Recovers committed files, directories, symlinks, and branches — **not**
    /// attribution (blame/audit/actors) or uncommitted edits, which live only in
    /// the DB. Reading every object also integrity-checks it.
    pub async fn rebuild(&self) -> Result<RebuildReport> {
        self.fs.rebuild_from_content().await
    }

    /// Read-only companion to [`Self::rebuild`]: scan the content store and
    /// report what a rebuild would recover (commits, branches, the branch that
    /// would be checked out), without modifying the workspace.
    pub async fn scan(&self) -> Result<RebuildReport> {
        self.fs.scan_content().await
    }

    /// Seal any buffered writes to durable storage (a no-op unless the content
    /// backend batches, e.g. a packed store). `commit` flushes automatically.
    pub async fn flush(&self) -> Result<()> {
        self.fs.content.flush().await
    }

    /// Compact the content store, reclaiming space held by deleted objects;
    /// returns the bytes reclaimed. Meaningful for a packed store; run after
    /// `gc`. A no-op for in-place backends.
    pub async fn repack(&self) -> Result<u64> {
        self.fs.content.repack().await
    }

    // --- merge + locks ---------------------------------------------------

    /// Merge commit `theirs` into the current branch.
    pub async fn merge(&self, theirs: Hash, author: &str, message: &str) -> Result<MergeOutcome> {
        self.fs.merge(theirs, author, message).await
    }

    /// Merge branch `name` into the current branch.
    #[tracing::instrument(skip_all)]
    pub async fn merge_branch(
        &self,
        name: &str,
        author: &str,
        message: &str,
    ) -> Result<MergeOutcome> {
        let target = self
            .fs
            .list_branches()
            .await?
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, h)| h)
            .ok_or_else(|| OrigoFSError::NotFound(format!("branch {name}")))?;
        self.fs.merge(target, author, message).await
    }

    /// Unresolved merge conflicts as `(path, kind)`.
    pub async fn conflicts(&self) -> Result<Vec<(String, String)>> {
        self.fs.conflicts().await
    }

    pub async fn lock(&self, path: &str, owner: &str) -> Result<bool> {
        let acquired = self.fs.lock(path, owner).await?;
        if acquired {
            self.emit("lock", path, Some(owner.to_string()), None, None)
                .await;
        }
        Ok(acquired)
    }

    pub async fn unlock(&self, path: &str, owner: &str) -> Result<bool> {
        let released = self.fs.unlock(path, owner).await?;
        if released {
            self.emit("unlock", path, Some(owner.to_string()), None, None)
                .await;
        }
        Ok(released)
    }

    pub async fn locks(&self) -> Result<Vec<(String, String, i64)>> {
        self.fs.locks().await
    }

    // --- attribution -----------------------------------------------------

    /// Register a human actor.
    pub async fn create_human(&self, name: &str, auth_subject: Option<&str>) -> Result<i64> {
        self.fs.create_human(name, auth_subject).await
    }

    /// Register an agent actor, optionally with the human that launched it.
    pub async fn create_agent(
        &self,
        name: &str,
        model: &str,
        controller: Option<i64>,
    ) -> Result<i64> {
        self.fs.create_agent(name, model, controller).await
    }

    pub async fn get_actor(&self, id: i64) -> Result<Option<Actor>> {
        self.fs.get_actor(id).await
    }

    // --- schema / migrations -------------------------------------------------

    /// The migration version currently applied to this workspace's metadata DB.
    /// A normal open already brings this to [`latest_schema_version`](Self::latest_schema_version);
    /// this is here for operators who want to introspect or gate on it.
    pub async fn schema_version(&self) -> Result<i64> {
        self.fs.meta.schema_version().await
    }

    /// The highest schema version this build knows about.
    pub fn latest_schema_version(&self) -> i64 {
        origofs_core::latest_schema_version()
    }

    /// Apply any pending metadata migrations, returning `(from, to)` versions.
    /// Idempotent and forward-only — a normal open runs the same migration path,
    /// so this is mainly for explicitly upgrading a shared DB after deploying a
    /// build with new migrations, or verifying that one is current.
    pub async fn migrate(&self) -> Result<(i64, i64)> {
        let before = self.fs.meta.schema_version().await?;
        // `MetadataStore::init` is exactly the (idempotent) migration runner — it
        // applies unrecorded steps and touches nothing else (no ref/HEAD reset).
        self.fs.meta.init().await?;
        let after = self.fs.meta.schema_version().await?;
        Ok((before, after))
    }

    /// Look up an actor by external identity (`auth_subject`), if registered.
    pub async fn actor_by_subject(&self, subject: &str) -> Result<Option<Actor>> {
        self.fs.actor_by_subject(subject).await
    }

    /// Every registered actor, oldest first. Use this to resolve the bare
    /// `actor_id` carried by events, suggestions (`resolved_by` too), and
    /// presence to a name + kind — no app-side actor directory needed.
    pub async fn list_actors(&self) -> Result<Vec<Actor>> {
        self.fs.list_actors().await
    }

    /// Idempotently map your app's user id (`auth_subject`) to a **human** actor:
    /// returns the existing actor for that subject, or creates one. Race-safe, so
    /// you don't need to keep a user→actor side table.
    pub async fn find_or_create_human(&self, auth_subject: &str, name: &str) -> Result<i64> {
        self.fs.find_or_create_human(auth_subject, name).await
    }

    /// Idempotently map an external identity to an **agent** actor.
    pub async fn find_or_create_agent(
        &self,
        auth_subject: &str,
        name: &str,
        model: &str,
        controller: Option<i64>,
    ) -> Result<i64> {
        self.fs
            .find_or_create_agent(auth_subject, name, model, controller)
            .await
    }

    pub async fn create_session(&self, actor_id: i64, client: Option<&str>) -> Result<i64> {
        self.fs.create_session(actor_id, client).await
    }

    /// Attributed write: records the actor and updates per-line authorship.
    #[tracing::instrument(level = "debug", skip_all, fields(path = %path, bytes = data.len()))]
    pub async fn write_as(&self, ctx: WriteCtx, path: &str, data: &[u8]) -> Result<()> {
        self.fs.write_as(ctx, path, data).await?;
        self.emit("write", path, None, Some(ctx.actor), ctx.session)
            .await;
        Ok(())
    }

    /// Attributed write with **explicit** byte-range authorship — the CRDT/editor
    /// checkpoint path (roadmap M8). `spans` holds `(actor_id, session_id,
    /// byte_len)` runs summing to `data.len()`, so co-edited content lands with
    /// each collaborator's character-level spans attributed exactly (sub-line,
    /// interleaved), bypassing the line-diff heuristic. `ctx` is the actor
    /// performing the checkpoint (recorded on the op-log and the feed).
    pub async fn write_as_blamed(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        spans: &[(i64, i64, u64)],
    ) -> Result<()> {
        self.fs.write_as_blamed(ctx, path, data, spans).await?;
        self.emit("write", path, None, Some(ctx.actor), ctx.session)
            .await;
        Ok(())
    }

    /// Propose an edit to `path` for human review instead of applying it. The
    /// bytes are stored now; the working tree changes only on accept. Returns
    /// the suggestion id. (Records a `suggest` event on the feed.)
    pub async fn suggest(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        summary: Option<&str>,
    ) -> Result<i64> {
        self.fs.suggest(ctx, path, data, summary).await
    }

    /// Propose deleting `path`.
    pub async fn suggest_delete(
        &self,
        ctx: WriteCtx,
        path: &str,
        summary: Option<&str>,
    ) -> Result<i64> {
        self.fs.suggest_delete(ctx, path, summary).await
    }

    /// Set an actor's write policy — `Direct` (may write straight to the tree) or
    /// `Propose` (writes are routed through the suggestion queue). A bounded,
    /// actor-agnostic trust gate; the default is `Direct`.
    pub async fn set_write_policy(&self, actor_id: i64, policy: WritePolicy) -> Result<()> {
        self.fs.set_write_policy(actor_id, policy).await
    }

    /// Submit an edit to `path` governed by the actor's write policy: a `Direct`
    /// actor writes straight to the working tree ([`WriteOutcome::Wrote`]); a
    /// `Propose` actor's edit is queued as a suggestion for review
    /// ([`WriteOutcome::Proposed`]). The entry point an untrusted surface routes
    /// writes through so a propose-only actor can't land an unreviewed edit.
    pub async fn write_or_propose(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        summary: Option<&str>,
    ) -> Result<WriteOutcome> {
        let outcome = self.fs.write_or_propose(ctx, path, data, summary).await?;
        // Emit the change-feed event for a direct write, exactly as `write_as`
        // does. The propose path emits its own `suggest` event in the engine, so
        // don't double-emit here.
        if matches!(outcome, WriteOutcome::Wrote) {
            self.emit("write", path, None, Some(ctx.actor), ctx.session)
                .await;
        }
        Ok(outcome)
    }

    /// Suggestions, optionally filtered by status and/or path, newest first.
    pub async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        path: Option<&str>,
    ) -> Result<Vec<Suggestion>> {
        self.fs.list_suggestions(status, path).await
    }

    /// A single suggestion by id.
    pub async fn get_suggestion(&self, id: i64) -> Result<Option<Suggestion>> {
        self.fs.get_suggestion(id).await
    }

    /// Render a suggestion as a unified line diff (`base` → `proposed`).
    pub async fn suggestion_diff(&self, id: i64) -> Result<String> {
        self.fs.suggestion_diff(id).await
    }

    /// A suggestion's base and proposed **content** (read from the store), so a
    /// reviewer UI can render an inline diff without stashing the proposed bytes
    /// itself. `proposed` is `None` when the suggestion proposes a deletion.
    pub async fn suggestion_content(&self, id: i64) -> Result<SuggestionContent> {
        self.fs.suggestion_content(id).await
    }

    /// Accept a pending suggestion: apply it (attributed to the original author)
    /// and mark it accepted. Errors if the file changed since it was proposed.
    pub async fn accept_suggestion(&self, id: i64, approver: WriteCtx) -> Result<()> {
        self.fs.accept_suggestion(id, approver).await
    }

    /// Reject a pending suggestion without applying it.
    pub async fn reject_suggestion(&self, id: i64, approver: WriteCtx) -> Result<()> {
        self.fs.reject_suggestion(id, approver).await
    }

    /// Per-line-range authorship for a path (human vs agent).
    pub async fn blame(&self, path: &str) -> Result<Vec<BlameRange>> {
        self.fs.blame(path).await
    }

    /// The edit-op log for an actor (optionally one session).
    pub async fn edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        self.fs.edit_ops(actor_id, session_id).await
    }

    /// Revert every line an actor wrote in a session. Returns files changed.
    #[tracing::instrument(skip_all, fields(actor = actor_id, session = session_id))]
    pub async fn revert_session(&self, actor_id: i64, session_id: i64) -> Result<usize> {
        self.fs.revert_session(actor_id, session_id).await
    }

    // --- live collaboration ----------------------------------------------

    /// Tail the change feed: events strictly after `after_seq`, oldest first.
    /// Poll with the last seen `seq` as the cursor (Postgres also fires
    /// `NOTIFY origofs_events` so consumers can be pushed instead of polling).
    pub async fn watch(&self, after_seq: i64) -> Result<Vec<Event>> {
        self.fs.events_since(after_seq, 1000).await
    }

    /// A **push** subscription to the change feed, backed by Postgres
    /// `LISTEN/NOTIFY` — call [`EventSubscription::recv`] to block until the next
    /// batch of events instead of polling [`watch`](Self::watch). Optionally
    /// branch-scoped. Errors on non-Postgres backends (use `watch` there).
    pub async fn subscribe(
        &self,
        after_seq: i64,
        branch: Option<&str>,
    ) -> Result<EventSubscription> {
        match &self.pg {
            Some(pg) => pg.subscribe(after_seq, branch.map(str::to_string)).await,
            None => Err(OrigoFSError::InvalidArgument(
                "subscribe requires the Postgres backend; use watch() to poll".into(),
            )),
        }
    }

    /// Whether this workspace is backed by Postgres (multi-writer). The
    /// Postgres-only features — the push `subscribe` feed and the cross-worker
    /// co-edit relay — are available exactly when this is true.
    pub fn is_postgres(&self) -> bool {
        self.pg.is_some()
    }

    /// The Postgres store, or an error naming `op` as Postgres-only — the shared
    /// gate for the multi-writer/multi-worker features (the co-edit relay).
    #[cfg(feature = "coedit")]
    fn require_pg(&self, op: &str) -> Result<&Arc<origofs_core::PostgresMetadataStore>> {
        self.pg.as_ref().ok_or_else(|| {
            OrigoFSError::InvalidArgument(format!(
                "{op} requires the Postgres backend (multi-worker); a single-worker \
                 deployment needs no cross-worker relay"
            ))
        })
    }

    /// Open a live co-editing document for `path` (roadmap M8): resume the CRDT
    /// from its persisted sidecar if one exists, else promote the file's current
    /// text into a fresh document attributed to `ctx`. Drive it over the Yjs wire
    /// protocol with [`CoeditDoc::handle_sync`], then land it with
    /// [`checkpoint_coedit`](Self::checkpoint_coedit). Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn open_coedit(&self, ctx: WriteCtx, path: &str) -> Result<CoeditDoc> {
        self.fs.open_coedit(ctx, path).await
    }

    /// Checkpoint a live co-editing document into `path`, landing each
    /// collaborator's exact character spans in the byte-range blame index and
    /// persisting the CRDT sidecar so the session is durable and resumable. `ctx`
    /// is the actor performing the checkpoint. Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn checkpoint_coedit(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
    ) -> Result<()> {
        self.fs.checkpoint_coedit(ctx, path, doc).await
    }

    /// Ensure the cross-worker relay's backing table exists (idempotent). Call it
    /// before a room starts accepting edits, so the first publish can't race the
    /// table into existence. Requires the Postgres backend + the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn coedit_relay_init(&self) -> Result<()> {
        self.require_pg("coedit_relay_init")?
            .coedit_relay_init()
            .await
    }

    /// Publish a co-editing update `delta` for `path` to the cross-worker relay,
    /// tagged with this worker's `origin` id (so it can skip its own echo). Every
    /// other worker hosting `path` applies it and fans it out to its sockets, so
    /// replicas across workers converge. Requires the Postgres backend (a
    /// single-worker deployment needs no relay); errors otherwise, like
    /// [`subscribe`](Self::subscribe). Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn coedit_publish(&self, path: &str, origin: &str, delta: &[u8]) -> Result<()> {
        self.require_pg("coedit_publish")?
            .coedit_publish(path, origin, delta)
            .await
    }

    /// Every relayed op currently held for `path` — for a worker that has just
    /// started hosting `path` to replay and catch up to its peers' state (applying
    /// is idempotent). Requires the Postgres backend + the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn coedit_replay(&self, path: &str) -> Result<Vec<CoeditRelayNote>> {
        self.require_pg("coedit_replay")?.coedit_replay(path).await
    }

    /// Subscribe to the cross-worker co-editing relay: `recv()` on the returned
    /// [`CoeditRelaySub`] yields every worker's update deltas in order. Requires
    /// the Postgres backend + the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn coedit_subscribe(&self) -> Result<CoeditRelaySub> {
        self.require_pg("coedit_subscribe")?
            .coedit_subscribe()
            .await
    }

    /// Record an arbitrary event on the change feed.
    pub async fn record_event(&self, ev: EventInit) -> Result<i64> {
        self.fs.record_event(ev).await
    }

    /// Heartbeat a session's presence (and the path it is working on).
    pub async fn touch(&self, actor_id: i64, session_id: i64, path: Option<&str>) -> Result<()> {
        self.fs.touch_presence(session_id, actor_id, path).await
    }

    /// Sessions active within the last `window_secs` seconds.
    pub async fn presence(&self, window_secs: i64) -> Result<Vec<Presence>> {
        self.fs.presence(window_secs).await
    }

    /// Reap presence rows older than `grace_secs` (keeps the table bounded).
    /// Call periodically with a grace comfortably larger than your presence
    /// window. Returns the number of rows reaped.
    pub async fn reap_presence(&self, grace_secs: i64) -> Result<u64> {
        self.fs.reap_presence(grace_secs).await
    }
}

/// Read the per-store encryption salt from `cas_dir/keysalt`, creating it with 16
/// fresh random bytes on first open.
///
/// The salt is not secret, but the Argon2id key is derived from
/// `passphrase + salt`, so it must stay stable for the life of the store and must
/// survive a metadata-DB loss — hence it lives beside the content store, not in
/// the DB. It is written with an exclusive `create_new`, so two processes opening
/// the same fresh store concurrently can't settle on different salts: exactly one
/// wins the create and the other re-reads the winner's file.
async fn read_or_create_salt(cas_dir: &Path) -> Result<Vec<u8>> {
    use tokio::io::AsyncWriteExt;
    let path = cas_dir.join("keysalt");
    match tokio::fs::read(&path).await {
        Ok(salt) if !salt.is_empty() => return Ok(salt),
        Ok(_) => {
            return Err(OrigoFSError::Content(format!(
                "encryption salt {} is empty (refusing to derive a key from it)",
                path.display()
            )));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut salt = [0u8; 16];
    getrandom::getrandom(&mut salt)
        .map_err(|e| OrigoFSError::Content(format!("failed to generate encryption salt: {e}")))?;
    match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
    {
        Ok(mut f) => {
            f.write_all(&salt).await?;
            f.flush().await?;
            Ok(salt.to_vec())
        }
        // Lost the create race with a concurrent open: adopt the salt they wrote.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let salt = tokio::fs::read(&path).await?;
            if salt.is_empty() {
                return Err(OrigoFSError::Content(format!(
                    "encryption salt {} is empty (refusing to derive a key from it)",
                    path.display()
                )));
            }
            Ok(salt)
        }
        Err(e) => Err(e.into()),
    }
}
