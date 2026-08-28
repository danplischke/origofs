//! origofs-sdk — an ergonomic front door to an origofs workspace.
//!
//! A workspace pairs a metadata store (SQLite or Postgres) with a pluggable
//! content backend (local dir, S3-compatible object store, in-memory, or a cached
//! tier). Both sides are `Arc<dyn …>`, so the backend is chosen at runtime. Later
//! milestones add commits and attribution behind the same façade.

use origofs_core::{Fs, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use bytes::Bytes;
pub use futures::stream::BoxStream;
/// The emit-only metrics facade (`metrics` feature). A library only *records*;
/// the binary installs an exporter and hands its renderer to
/// [`api::set_metrics_renderer`], exactly as it installs a tracing subscriber.
#[cfg(feature = "metrics")]
pub use origofs_core::metrics;
// The building blocks a caller needs to assemble a backend stack by hand, which
// `Workspace::open_encrypted` takes. Previously private, so an embedder could not
// compose one.
pub use origofs_core::{
    AclGrant, Actor, ActorInit, ActorKind, BlameRange, CacheLimits, CommitInfo, Conflict,
    DEFAULT_GC_GRACE_SECS, DiffEntry, DiffStatus, DirEntry, DirEntryAttr, DirPage, EditOp, Event,
    EventInit, FileKind, FsStat, GcStats, Hash, Inode, LiveDoc, LoadReport, MemStore, MergeOutcome,
    OrigoFSError, Owner, PackStore, Passage, PassageOptions, Perms, Presence, Quota, RebuildReport,
    ResyncOutcome, ResyncReport, Scope, ScopeError, Segmentation, Suggestion, SuggestionContent,
    SuggestionInit, SuggestionKind, SuggestionStatus, TieredStore, ToolCallInit, TransferStats,
    TrashEntry, Usage, VerifyingStore, VersioningMode, WriteCtx, WriteOutcome, WritePolicy,
};
// Backend-specific re-exports, gated to match `origofs-core`'s own features.
#[cfg(feature = "encryption")]
pub use origofs_core::EncryptedStore;
#[cfg(feature = "coedit")]
pub use origofs_core::{
    COEDIT_SIDECAR_DIR, CoeditDoc, CoeditTreeDoc, DEFAULT_TREE_ROOT, SyncReply, TreeRun, TreeSpan,
};
// The cross-worker co-editing relay rides on Postgres `LISTEN`/`NOTIFY`, so
// these types exist only when both features are on.
#[cfg(all(feature = "coedit", feature = "postgres"))]
pub use origofs_core::{CoeditRelayNote, CoeditRelaySub};
pub use origofs_core::{ContentStore, LocalCasStore, MetadataStore, SqliteMetadataStore};
// The chunk manifest, needed by callers doing ranged streaming reads.
pub use origofs_core::Manifest;
#[cfg(feature = "postgres")]
pub use origofs_core::{EventSubscription, PostgresMetadataStore};
#[cfg(feature = "object-store")]
pub use origofs_core::{GcsConfig, ObjectContentStore, S3Config};

// ── Access surfaces ─────────────────────────────────────────────────────────
// Each surface that was formerly its own crate is now an opt-in, feature-gated
// module over this same `Workspace`. A default build pulls none of their
// dependencies (axum, fuser, nfsserve, …); enable the ones you need, or `full`.
// FUSE/NFS/sandbox are Unix-only. See `Cargo.toml` `[features]`.
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
// Unix-only, for the same reason FUSE and NFS are — not an oversight. The whole
// surface is built on a kernel overlay: an unprivileged `unshare -U -r -m`
// overlayfs mount, a delta read back out of `upper/`, and deletions encoded as
// overlayfs whiteouts — which are *character devices* (`rdev` 0:0) and opaque-dir
// xattrs. Windows has no overlayfs, no user namespaces, and no char-device
// whiteout to detect, so there is nothing here to port: it would be a different
// implementation, not this one compiled elsewhere.
//
// This was previously gated on the feature alone, which is what made `full` — and
// therefore `origofs-cli` — fail to compile for `*-pc-windows-*` (#107).
#[cfg(all(unix, feature = "sandbox"))]
pub mod sandbox;

/// Resolves when the process is asked to stop: `SIGTERM` or `SIGINT` on Unix,
/// Ctrl-C elsewhere.
///
/// `SIGTERM` is the one that matters in production — it is what Kubernetes and
/// `docker stop` send, and what a long-running server previously had no handler
/// for at all, so every in-flight request was severed at whatever point it had
/// reached. `SIGINT` is here so an interactive Ctrl-C drains the same way.
///
/// Returning a future rather than installing a handler keeps the choice with the
/// caller: an embedder that already has its own shutdown plumbing passes that to
/// [`api::serve_until`] instead.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        // If a handler can't be installed, wait forever rather than shutting down
        // immediately — a spurious instant shutdown is far worse than no handler.
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGTERM; shutdown will not be graceful");
                std::future::pending::<()>().await;
                unreachable!()
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGINT");
                std::future::pending::<()>().await;
                unreachable!()
            }
        };
        tokio::select! {
            _ = term.recv() => tracing::info!("received SIGTERM"),
            _ = int.recv() => tracing::info!("received SIGINT"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("received Ctrl-C");
    }
}

type Meta = Arc<dyn MetadataStore>;
type Content = Arc<dyn ContentStore>;

/// The outcome of a [`Workspace::ready`] readiness probe: whether each backend
/// answered. `None` for a store means it is reachable; `Some(msg)` carries why
/// the probe failed. Backs the HTTP `/readyz` endpoint.
/// `#[non_exhaustive]`: callers read this, they never construct it, so adding a
/// counter should not be a breaking change. (Config structs like `S3Config` are
/// deliberately left constructible — a caller has to be able to build those, and
/// `..Default::default()` already absorbs new fields there.)
#[non_exhaustive]
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
    #[cfg(feature = "postgres")]
    pg: Option<Arc<PostgresMetadataStore>>,
}

/// Where and how large a local read cache may be (issue #114).
///
/// Caching is **opt-in** rather than a default, and deliberately so: turning it on
/// means writing to the user's disk, and a library that starts doing that without
/// being asked is a library that fills someone's laptop. The cost of opt-in is that
/// it gets used less, which is a fair trade against surprising a caller who chose a
/// remote backend precisely so that nothing landed locally.
///
/// The defaults from [`new`](Self::new) are sized for a developer machine. Pick
/// them explicitly for a container, where the writable layer is usually far smaller
/// than the disk the numbers below assume.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Directory for the cache tier. Created if absent.
    pub dir: PathBuf,
    /// Evict least-recently-used chunks to stay at or below this.
    pub max_bytes: u64,
    /// Also evict whenever the filesystem holding `dir` has less than this free,
    /// so the cache yields to the rest of the machine rather than competing with
    /// it. Enforced on Unix; on Windows there is no `statvfs` and only `max_bytes`
    /// applies.
    pub min_free_bytes: u64,
}

impl CacheConfig {
    /// 8 GiB of cache, yielding whenever the disk drops under 2 GiB free.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_bytes: 8 << 30,
            min_free_bytes: 2 << 30,
        }
    }

    /// Set the size bound.
    pub fn max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = n;
        self
    }

    /// Set the free-space floor.
    pub fn min_free_bytes(mut self, n: u64) -> Self {
        self.min_free_bytes = n;
        self
    }
}

/// Split an absolute path into `(parent, name)`.
///
/// The façade's path-addressed wrappers over the inode-oriented engine ops need
/// this; the engine's own `resolve_parent` is `pub(crate)`, and an inode number is
/// a mount implementation detail no `Workspace` caller should have to hold.
fn split_parent(path: &str) -> Result<(String, String)> {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rsplit_once('/') {
        Some(("", name)) if !name.is_empty() => Ok(("/".to_string(), name.to_string())),
        Some((parent, name)) if !name.is_empty() => Ok((parent.to_string(), name.to_string())),
        _ => Err(OrigoFSError::InvalidPath(format!(
            "{path:?} names no parent directory"
        ))),
    }
}

impl Workspace {
    /// Open (creating if needed) a workspace from explicit metadata + content
    /// backends.
    pub async fn open(meta: Meta, content: Content) -> Result<Self> {
        let fs = Fs::new(meta, content);
        fs.init().await?;
        Ok(Self {
            fs,
            #[cfg(feature = "postgres")]
            pg: None,
        })
    }

    /// SQLite metadata + content-addressed blobs under a local directory.
    pub async fn open_local(db_path: impl AsRef<Path>, cas_dir: impl AsRef<Path>) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let content: Content = Arc::new(LocalCasStore::open(cas_dir).await?);
        Self::open(meta, content).await
    }

    #[cfg(feature = "encryption")]
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
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let backend: Content = Arc::new(LocalCasStore::open(cas_dir).await?);
        Self::open_encrypted(meta, backend, passphrase).await
    }

    #[cfg(feature = "encryption")]
    /// Any metadata store + any content backend, **encrypted at rest** with a key
    /// derived from `passphrase` (Argon2id) over a per-store random salt.
    ///
    /// Encryption used to be wired only for SQLite + a local directory, and the
    /// CLI refused outright for anything else — so the deployment
    /// `deploy/config.example.toml` recommends, Postgres over an object store,
    /// could not have encryption at rest at all. `EncryptedStore` always composed
    /// over any `ContentStore`; what was missing was somewhere to keep the salt
    /// that (a) survives losing the metadata database and (b) garbage collection
    /// cannot sweep. That is now a content-store *sidecar* — see
    /// [`ContentStore::get_sidecar`](origofs_core::ContentStore::get_sidecar).
    ///
    /// The same passphrase must be used on every open; a wrong one fails loudly
    /// rather than returning garbage.
    ///
    /// `VerifyingStore` belongs *outside* this, as the `open_*` recipes arrange:
    /// integrity is checked at the chunk-addressed boundary the caller reads by.
    pub async fn open_encrypted(meta: Meta, backend: Content, passphrase: &str) -> Result<Self> {
        let salt = read_or_create_salt(&backend).await?;
        let content: Content =
            Arc::new(EncryptedStore::from_passphrase(backend, passphrase, &salt)?);
        Self::open(meta, content).await
    }

    /// Wrap a remote content backend in a **bounded local read cache** (issue
    /// #114), returning the full stack a workspace should use.
    ///
    /// The layering is the point, and it is not the obvious one:
    ///
    /// ```text
    /// VerifyingStore( TieredStore( cache: LocalCasStore, backend: <remote> ) )
    /// ```
    ///
    /// `VerifyingStore` stays **outside**, as every other `open_*` recipe arranges,
    /// so integrity is still checked at the chunk-addressed boundary the caller
    /// reads by. The cache goes *inside* it, which is why a cache cannot simply be
    /// bolted onto an already-verified stack — and why the cached constructors are
    /// separate rather than a decorator a caller applies afterwards.
    /// `TieredStore` additionally re-verifies its own cache hits, so a corrupt
    /// cached copy becomes a refetch instead of the hard `Corrupt` the outer layer
    /// would raise.
    #[cfg(feature = "object-store")]
    async fn cached_remote(backend: Content, cache: &CacheConfig) -> Result<Content> {
        let local: Content = Arc::new(LocalCasStore::open(&cache.dir).await?);
        let tier = TieredStore::with_limits(
            local,
            backend,
            CacheLimits::bounded(&cache.dir, cache.max_bytes, cache.min_free_bytes),
        );
        // Account for whatever the directory already holds, so the bound covers a
        // cache that survived a restart rather than only this process's own reads.
        tier.warm_index().await?;
        Ok(Arc::new(VerifyingStore::new(Arc::new(tier))))
    }

    /// [`open_s3`](Self::open_s3) with a bounded local read cache (issue #114).
    #[cfg(feature = "object-store")]
    pub async fn open_s3_cached(
        db_path: impl AsRef<Path>,
        cfg: S3Config,
        cache: CacheConfig,
    ) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let backend: Content = Arc::new(ObjectContentStore::s3(cfg)?);
        let content = Self::cached_remote(backend, &cache).await?;
        Self::open(meta, content).await
    }

    /// [`open_pg_s3`](Self::open_pg_s3) with a bounded local read cache (#114).
    #[cfg(all(feature = "object-store", feature = "postgres"))]
    pub async fn open_pg_s3_cached(dsn: &str, cfg: S3Config, cache: CacheConfig) -> Result<Self> {
        let meta: Meta = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let backend: Content = Arc::new(ObjectContentStore::s3(cfg)?);
        let content = Self::cached_remote(backend, &cache).await?;
        Self::open(meta, content).await
    }

    /// [`open_gcs`](Self::open_gcs) with a bounded local read cache (#114).
    #[cfg(feature = "object-store")]
    pub async fn open_gcs_cached(
        db_path: impl AsRef<Path>,
        cfg: GcsConfig,
        cache: CacheConfig,
    ) -> Result<Self> {
        let meta: Meta = Arc::new(SqliteMetadataStore::open(db_path)?);
        let backend: Content = Arc::new(ObjectContentStore::gcs(cfg)?);
        let content = Self::cached_remote(backend, &cache).await?;
        Self::open(meta, content).await
    }

    /// [`open_pg_gcs`](Self::open_pg_gcs) with a bounded local read cache (#114).
    #[cfg(all(feature = "object-store", feature = "postgres"))]
    pub async fn open_pg_gcs_cached(dsn: &str, cfg: GcsConfig, cache: CacheConfig) -> Result<Self> {
        let meta: Meta = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let backend: Content = Arc::new(ObjectContentStore::gcs(cfg)?);
        let content = Self::cached_remote(backend, &cache).await?;
        Self::open(meta, content).await
    }

    #[cfg(feature = "object-store")]
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

    #[cfg(feature = "object-store")]
    /// SQLite metadata + an S3-compatible object store whose chunks are batched
    /// into **pack objects** (few large PUTs instead of many tiny ones — batched *within* a write; see
    /// [`PackStore`] for where that stops), with the
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

    #[cfg(feature = "postgres")]
    /// Postgres metadata (multi-writer) over the given content backend.
    pub async fn open_pg(dsn: &str, content: Content) -> Result<Self> {
        let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
        let mut ws = Self::open(pg.clone(), content).await?;
        ws.pg = Some(pg);
        Ok(ws)
    }

    #[cfg(feature = "postgres")]
    /// Postgres metadata (multi-writer) + a local content-addressed store — the
    /// single-host production pairing (one shared database, content on local disk).
    /// For content shared across hosts use [`Workspace::open_pg_s3`] /
    /// [`Workspace::open_pg_gcs`] instead.
    pub async fn open_pg_local(dsn: &str, cas_dir: impl AsRef<Path>) -> Result<Self> {
        let content: Content = Arc::new(LocalCasStore::open(cas_dir).await?);
        Self::open_pg(dsn, content).await
    }

    // Needs both halves: a Postgres metadata store and an object content store.
    #[cfg(all(feature = "postgres", feature = "object-store"))]
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

    // Needs both halves: a Postgres metadata store and an object content store.
    #[cfg(all(feature = "postgres", feature = "object-store"))]
    /// Postgres metadata + a **packed** S3 object store (few large PUTs instead of
    /// many tiny ones — batched *within* a write; see [`PackStore`] for where that
    /// stops), with the per-chunk index in a local directory. The
    /// recommended object-storage layout; seal the open pack with [`Workspace::flush`]
    /// (or `commit`) and reclaim deleted space with [`Workspace::repack`].
    ///
    /// **`index_dir` is node-local, so this is single-writer-per-index.** The
    /// per-chunk index lives in a `LocalCasStore`, not in the object store — two
    /// processes with separate index directories cannot see each other's chunks,
    /// even though they share one database and one bucket. For a multi-container
    /// deployment either put `index_dir` on a shared volume or keep one writer.
    /// (This constraint was only recorded in `docker-compose.yml`, where nobody
    /// reading the API would find it.)
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

    #[cfg(feature = "object-store")]
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

    #[cfg(feature = "object-store")]
    /// SQLite metadata + a **packed** native GCS object store (few large PUTs
    /// instead of many tiny ones — batched *within* a write; see [`PackStore`] for
    /// where that stops), with the per-chunk index under `index_dir`. The
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

    // Needs both halves: a Postgres metadata store and an object content store.
    #[cfg(all(feature = "postgres", feature = "object-store"))]
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

    // Needs both halves: a Postgres metadata store and an object content store.
    #[cfg(all(feature = "postgres", feature = "object-store"))]
    /// Postgres metadata + a **packed** native GCS object store, with the per-chunk
    /// index in a local directory. The recommended object-storage layout for a team
    /// on Google Cloud; seal the open pack with [`Workspace::flush`] (or `commit`)
    /// and reclaim deleted space with [`Workspace::repack`].
    ///
    /// **`index_dir` is node-local, so this is single-writer-per-index.** The
    /// per-chunk index lives in a `LocalCasStore`, not in the object store — two
    /// processes with separate index directories cannot see each other's chunks,
    /// even though they share one database and one bucket. For a multi-container
    /// deployment either put `index_dir` on a shared volume or keep one writer.
    /// (This constraint was only recorded in `docker-compose.yml`, where nobody
    /// reading the API would find it.)
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

    #[cfg(feature = "object-store")]
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
            #[cfg(feature = "postgres")]
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
    ///
    /// **Unattributed.** Prefer [`write_reader_as`](Self::write_reader_as) wherever
    /// an actor is known: this records no blame, no edit-op, and is exempt from the
    /// write policy.
    pub async fn write_reader<R: std::io::Read + Send + 'static>(
        &self,
        path: &str,
        reader: R,
    ) -> Result<()> {
        self.fs.write_reader(path, reader).await?;
        self.emit("write", path, None, None, None).await;
        Ok(())
    }

    /// Write a file by streaming from a blocking reader, **attributed** to `ctx`.
    ///
    /// The way to write a file larger than memory without giving up attribution.
    /// Subject to the write policy (a propose-only actor is refused), and blame
    /// covers the whole file rather than being diffed line-by-line against the
    /// previous body — a streamed write is a wholesale replacement, and the
    /// previous body is exactly what is not resident. See
    /// [`Fs::write_reader_as`](origofs_core::Fs::write_reader_as).
    #[tracing::instrument(skip(self, reader), fields(path = %path, actor = ctx.actor))]
    pub async fn write_reader_as<R: std::io::Read + Send + 'static>(
        &self,
        ctx: WriteCtx,
        path: &str,
        reader: R,
    ) -> Result<()> {
        self.fs.write_reader_as(ctx, path, reader).await?;
        self.emit("write", path, None, Some(ctx.actor), ctx.session)
            .await;
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

    /// Open a file for a ranged read: the manifest to stream from, and its size.
    ///
    /// The size is what a `Content-Length`, a `Content-Range`, or a `416` all need
    /// *before* any bytes are read, so returning it here lets a surface answer a
    /// `Range` request in one metadata round trip.
    pub async fn open_for_range(&self, path: &str) -> Result<(Option<Manifest>, u64)> {
        self.fs.open_for_range(path).await
    }

    /// Stream the byte range `[off, off+len)` of a file, fetching only the chunks
    /// that cover it and trimming the boundary chunks.
    ///
    /// The streaming counterpart of [`read_range`](Self::read_range), which
    /// materializes the range. Use this to serve media: a player may request a
    /// range of any size — including `bytes=0-` for the whole file — and buffering
    /// that would defeat the point of streaming reads.
    pub fn read_range_stream(
        &self,
        manifest: Manifest,
        off: u64,
        len: u64,
    ) -> BoxStream<'static, Result<Bytes>> {
        self.fs.read_range_stream_owned(manifest, off, len)
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
    ///
    /// Unattributed. A surface that has resolved an actor must call
    /// [`create_branch_as`](Self::create_branch_as) instead — see it for why.
    pub async fn create_branch(&self, name: &str) -> Result<()> {
        self.fs.create_branch(name).await
    }

    /// [`create_branch`](Self::create_branch), attributed and policy-gated.
    pub async fn create_branch_as(&self, ctx: WriteCtx, name: &str) -> Result<()> {
        self.fs.create_branch_as(ctx, name).await?;
        self.emit(
            "branch",
            "/",
            Some(name.to_string()),
            Some(ctx.actor),
            ctx.session,
        )
        .await;
        Ok(())
    }

    /// Switch the working tree to `branch`.
    ///
    /// Unattributed, and **destructive**: it truncates and rematerializes the
    /// whole working tree, discarding every uncommitted edit. A surface that has
    /// resolved an actor must call [`checkout_as`](Self::checkout_as) instead —
    /// otherwise a propose-only actor, barred from overwriting a single file, can
    /// discard the entire workspace.
    pub async fn checkout(&self, branch: &str) -> Result<()> {
        self.fs.checkout(branch).await
    }

    /// [`checkout`](Self::checkout), attributed and policy-gated.
    pub async fn checkout_as(&self, ctx: WriteCtx, branch: &str) -> Result<()> {
        self.fs.checkout_as(ctx, branch).await?;
        self.emit(
            "checkout",
            "/",
            Some(branch.to_string()),
            Some(ctx.actor),
            ctx.session,
        )
        .await;
        Ok(())
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
    /// working tree.
    ///
    /// **Safe to run alongside active writers**, which is the only option in a
    /// workspace agents are always writing to. Reachability alone would not be:
    /// content is written *before* the metadata that references it, so every write
    /// has a window where its bytes are stored and nothing points at them. Two
    /// things close that — the sweep skips anything younger than
    /// [`DEFAULT_GC_GRACE_SECS`], and a deduplicating write refreshes content that
    /// has gone stale (`ContentStore::touch`), which is what covers a write that
    /// dedups onto old unreferenced bytes rather than storing new ones.
    ///
    /// Still cheapest on a quiet workspace, and a packed content store needs
    /// [`repack`](Self::repack) afterwards to actually give the space back.
    #[tracing::instrument(skip_all)]
    pub async fn gc(&self) -> Result<GcStats> {
        self.fs.gc().await
    }

    // --- usage, quotas, statfs (issues #116, #119) --------------------------

    /// Usage of the whole workspace — one aggregate query.
    pub async fn usage(&self) -> Result<Usage> {
        self.fs.usage().await
    }

    /// Recursive usage of a subtree: the `du` primitive.
    pub async fn du(&self, path: &str) -> Result<Usage> {
        self.fs.du(path).await
    }

    /// The workspace's capacity limits, all-`None` when unset.
    pub async fn quota(&self) -> Result<Quota> {
        self.fs.quota().await
    }

    /// Set (or clear) the workspace's capacity limits.
    pub async fn set_quota(&self, quota: Quota) -> Result<()> {
        self.fs.set_quota(quota).await
    }

    /// Answer a `statfs(2)`.
    pub async fn statfs(&self) -> Result<FsStat> {
        self.fs.statfs().await
    }

    // --- ownership, mode, links, xattrs (issues #119, #121, #122) -----------

    /// Change a path's permission bits.
    pub async fn chmod(&self, path: &str, mode: u32) -> Result<Inode> {
        let ino = self.fs.stat(path).await?.ino;
        self.fs.vfs_chmod(ino, mode).await
    }

    /// Change a path's owning uid/gid. `None` leaves that half alone, as
    /// `chown(2)`'s `-1` does.
    pub async fn chown(&self, path: &str, uid: Option<u32>, gid: Option<u32>) -> Result<Inode> {
        let ino = self.fs.stat(path).await?.ino;
        self.fs.vfs_chown(ino, uid, gid).await
    }

    /// Hard-link `existing` as `link_path`.
    pub async fn link(&self, existing: &str, link_path: &str) -> Result<Inode> {
        let ino = self.fs.stat(existing).await?.ino;
        let (parent, name) = split_parent(link_path)?;
        let parent_ino = self.fs.stat(&parent).await?.ino;
        self.fs.vfs_link(ino, parent_ino, &name).await
    }

    /// Read one extended attribute.
    pub async fn getxattr(&self, path: &str, name: &str) -> Result<Option<Vec<u8>>> {
        let ino = self.fs.stat(path).await?.ino;
        self.fs.vfs_getxattr(ino, name).await
    }

    /// Set one extended attribute. Values are capped at
    /// [`MAX_XATTR_LEN`](origofs_core::MAX_XATTR_LEN) — an xattr lives in the
    /// metadata store, which never holds large bytes.
    pub async fn setxattr(&self, path: &str, name: &str, value: &[u8]) -> Result<()> {
        let ino = self.fs.stat(path).await?.ino;
        self.fs.vfs_setxattr(ino, name, value).await
    }

    /// Remove one extended attribute, reporting whether it was set.
    pub async fn removexattr(&self, path: &str, name: &str) -> Result<bool> {
        let ino = self.fs.stat(path).await?.ino;
        self.fs.vfs_removexattr(ino, name).await
    }

    /// Every extended-attribute name on a path, in name order.
    pub async fn listxattr(&self, path: &str) -> Result<Vec<String>> {
        let ino = self.fs.stat(path).await?.ino;
        self.fs.vfs_listxattr(ino).await
    }

    // --- attribution completeness (issue #128) ------------------------------

    /// Whether this workspace requires every surface-initiated mutation to name
    /// an actor. Off by default; see
    /// [`Fs::require_attribution`](origofs_core::Fs::require_attribution) for why
    /// this is an attribution-completeness switch and **not** a security boundary.
    pub async fn require_attribution(&self) -> Result<bool> {
        self.fs.require_attribution().await
    }

    /// Turn the attribution requirement on or off.
    pub async fn set_require_attribution(&self, required: bool) -> Result<()> {
        self.fs.set_require_attribution(required).await
    }

    /// Refuse an unattributed mutation when this workspace requires attribution.
    /// Surfaces call this on the path where no actor was named.
    pub async fn ensure_attributed(&self, op: &str) -> Result<()> {
        self.fs.ensure_attributed(op).await
    }

    // --- path-scoped ACLs (issue #123) --------------------------------------

    /// Grant `perms` to an actor under `path_prefix`.
    pub async fn grant(
        &self,
        actor_id: i64,
        path_prefix: &str,
        perms: Perms,
        granted_by: Option<i64>,
    ) -> Result<()> {
        self.fs
            .grant(actor_id, path_prefix, perms, granted_by)
            .await
    }

    /// Remove a grant, reporting whether one was there.
    pub async fn revoke(
        &self,
        actor_id: i64,
        path_prefix: &str,
        revoked_by: Option<i64>,
    ) -> Result<bool> {
        self.fs.revoke(actor_id, path_prefix, revoked_by).await
    }

    /// Every grant in this workspace, or just one actor's.
    pub async fn list_grants(&self, actor_id: Option<i64>) -> Result<Vec<AclGrant>> {
        self.fs.list_grants(actor_id).await
    }

    /// The permissions an actor has at a path — longest matching prefix wins,
    /// falling back to its `write_policy` when it has no grant.
    pub async fn effective_perms(&self, actor_id: i64, path: &str) -> Result<Perms> {
        self.fs.effective_perms(actor_id, path).await
    }

    /// Whether an ungranted actor is denied rather than falling back.
    pub async fn acl_default_deny(&self) -> Result<bool> {
        self.fs.acl_default_deny().await
    }

    /// Switch between fallback (the default) and deny-by-default.
    pub async fn set_acl_default_deny(&self, deny: bool) -> Result<()> {
        self.fs.set_acl_default_deny(deny).await
    }

    /// Refuse an op at a path for an actor without `WRITE` there.
    pub async fn ensure_may_write_at(&self, ctx: WriteCtx, op: &str, path: &str) -> Result<()> {
        self.fs.ensure_may_write_at(ctx, op, path).await
    }

    /// Whether reads are checked against `READ` (issue #124). Off by default.
    pub async fn acl_enforce_reads(&self) -> Result<bool> {
        self.fs.acl_enforce_reads().await
    }

    /// Turn read enforcement on or off for this workspace.
    ///
    /// Off by default: reads have never been checked, so switching this on without
    /// writing read grants first stops every actor at once — the same hazard, and
    /// the same deliberate switch, as `set_acl_default_deny`.
    pub async fn set_acl_enforce_reads(&self, on: bool) -> Result<()> {
        self.fs.set_acl_enforce_reads(on).await
    }

    /// Refuse a read of a path for an actor without `READ` there. A no-op unless
    /// the workspace has read enforcement on.
    pub async fn ensure_may_read_at(&self, ctx: WriteCtx, op: &str, path: &str) -> Result<()> {
        self.fs.ensure_may_read_at(ctx, op, path).await
    }

    /// [`read`](Self::read), checked against `READ` at the path.
    pub async fn read_as(&self, ctx: WriteCtx, path: &str) -> Result<Bytes> {
        self.fs.read_as(ctx, path).await
    }

    /// [`read_range`](Self::read_range), checked against `READ` at the path.
    pub async fn read_range_as(
        &self,
        ctx: WriteCtx,
        path: &str,
        off: u64,
        len: u64,
    ) -> Result<Bytes> {
        self.fs.read_range_as(ctx, path, off, len).await
    }

    /// [`stat`](Self::stat), checked against `READ` at the path.
    pub async fn stat_as(&self, ctx: WriteCtx, path: &str) -> Result<Inode> {
        self.fs.stat_as(ctx, path).await
    }

    /// [`readlink`](Self::readlink), checked against `READ` at the path.
    pub async fn readlink_as(&self, ctx: WriteCtx, path: &str) -> Result<String> {
        self.fs.readlink_as(ctx, path).await
    }

    /// [`blame`](Self::blame), checked against `READ` at the path — blame returns
    /// who wrote which bytes, so it is a read of the file by another name.
    pub async fn blame_as(&self, ctx: WriteCtx, path: &str) -> Result<Vec<BlameRange>> {
        self.fs.blame_as(ctx, path).await
    }

    /// [`ls`](Self::ls), checked against `READ` at the directory. Checks the
    /// directory, not its entries — see `Fs::ls_as` on why per-entry filtering is
    /// not here yet.
    pub async fn ls_as(&self, ctx: WriteCtx, path: &str) -> Result<Vec<DirEntry>> {
        self.fs.ls_as(ctx, path).await
    }

    // --- portable dump/load (issue #117) ------------------------------------

    /// Write an engine-independent dump of the whole metadata store.
    ///
    /// The metadata DB is the half the content store cannot rebuild: `fsck
    /// --rebuild` recovers committed files, dirs, symlinks and branches from the
    /// bucket alone, and none of the attribution. This is how that half moves —
    /// as a backup, or as the SQLite → Postgres migration path that did not
    /// previously exist.
    pub async fn dump<W: std::io::Write>(&self, out: W) -> Result<usize> {
        self.fs.dump(out).await
    }

    /// [`dump`](Self::dump), authorized as `ctx` — checked as `WRITE` at `/`,
    /// because a dump is whole-store and carries every actor's `auth_subject` and
    /// every ACL grant. See [`Fs::dump_as`](origofs_core::Fs::dump_as) for why a
    /// write permission gates a read here.
    ///
    /// **A surface serving callers it did not authenticate wants this one.**
    pub async fn dump_as<W: std::io::Write>(&self, ctx: WriteCtx, out: W) -> Result<usize> {
        self.fs.dump_as(ctx, out).await
    }

    /// Restore a dump into a pristine store. Refuses to merge — see
    /// [`Fs::load`](origofs_core::Fs::load).
    pub async fn load<R: std::io::BufRead>(&self, input: R) -> Result<LoadReport> {
        self.fs.load(input).await
    }

    // --- trash (issue #115) ------------------------------------------------

    /// The workspace's trash retention in seconds, or `None` when trash is off.
    ///
    /// Off is the default: enabling it by default would silently change *when
    /// space is reclaimed* for every existing deployment, and the first anyone
    /// would learn of it is a storage bill.
    pub async fn trash_retention(&self) -> Result<Option<i64>> {
        self.fs.trash_retention().await
    }

    /// Enable trash with `secs` of retention, or disable it with `None`.
    ///
    /// Disabling does not purge what is already there — see
    /// [`Fs::set_trash_retention`](origofs_core::Fs::set_trash_retention).
    pub async fn set_trash_retention(&self, secs: Option<i64>) -> Result<()> {
        self.fs.set_trash_retention(secs).await
    }

    /// Everything currently recoverable, newest deletion first.
    pub async fn list_trash(&self) -> Result<Vec<TrashEntry>> {
        self.fs.list_trash().await
    }

    /// Put a trashed entry back at its original path, attributed to `ctx`.
    #[tracing::instrument(skip(self, ctx))]
    pub async fn restore_trash(&self, id: i64, ctx: WriteCtx) -> Result<String> {
        self.fs.restore_trash(id, ctx).await
    }

    /// Permanently drop one trash entry.
    pub async fn purge_trash(&self, id: i64) -> Result<bool> {
        self.fs.purge_trash(id).await
    }

    /// Permanently drop every trash entry, whatever its age.
    pub async fn empty_trash(&self) -> Result<usize> {
        self.fs.empty_trash().await
    }

    /// Remove a path, capturing it into the trash first when retention is on.
    ///
    /// The unattributed counterpart of `remove_as` for a surface with no actor
    /// context. Prefer `remove_as`/`remove_or_propose` wherever an actor is known.
    pub async fn remove_trashing(&self, path: &str) -> Result<()> {
        self.fs.remove_trashing(path).await
    }

    /// [`gc`](Self::gc) with an explicit grace period, in seconds. Only content
    /// unreferenced *and* untouched for at least that long is reclaimed.
    ///
    /// `0` disables the age gate entirely and is only safe on a quiesced store.
    /// Any other value below `DEDUP_REFRESH_AFTER_SECS` is **refused**: the
    /// dedup-side refresh only fires past that threshold, so a shorter grace
    /// leaves a band where content is sweepable but was never refreshed — the
    /// exact race the gate exists to close.
    pub async fn gc_with_grace(&self, grace_secs: u64) -> Result<GcStats> {
        self.fs.gc_with_grace(grace_secs).await
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

    // --- offline → reconnect resync --------------------------------------

    /// Reconcile this (offline/solo) workspace with `remote` over `branch`, using
    /// the ordinary three-way merge engine for any divergence — the reconnect path
    /// `docs/DESIGN.md` §4b promises for SQLite solo mode.
    ///
    /// The two workspaces need not share **either** backend: a laptop's SQLite +
    /// local CAS reconciles with a team's Postgres + S3. Commits, trees, manifests
    /// and chunks are copied both ways as needed; the remote branch only ever moves
    /// by compare-and-swap, retried against a fresh head if a concurrent writer
    /// wins, never forced.
    ///
    /// Per-byte-range **blame travels with the content** in both directions, with
    /// actors matched on `auth_subject` (so the same person resolves to one actor
    /// across resyncs) — the op-log, audit log, change feed and pending suggestions
    /// do not. Both working trees must be clean, both workspaces must have
    /// versioning enabled, and `branch` must be the local current branch. See
    /// [`origofs_core::resync`] for the full contract and the reasoning.
    ///
    /// A conflicted merge leaves the conflicts in *this* workspace's working tree
    /// (with `MERGE_HEAD` set, exactly like [`merge`](Self::merge)) and does not
    /// advance the remote branch: resolve, commit, and resync again.
    #[tracing::instrument(skip_all, fields(branch = %branch))]
    pub async fn resync(
        &self,
        remote: &Workspace,
        branch: &str,
        author: &str,
        message: &str,
    ) -> Result<ResyncReport> {
        let report = origofs_core::resync(&self.fs, &remote.fs, branch, author, message).await?;
        if let Some(head) = report.outcome.head() {
            self.emit(
                "resync",
                "/",
                Some(format!("{} {}", report.outcome.as_str(), head.to_hex())),
                None,
                None,
            )
            .await;
        }
        Ok(report)
    }

    /// Copy the commit closure reachable from `head` into `remote`'s content store,
    /// stopping at objects it already has. The push half of [`resync`](Self::resync)
    /// on its own — it moves objects only and never touches a ref, so it is safe to
    /// run ahead of time to make a later resync cheap.
    pub async fn push_objects(&self, remote: &Workspace, head: Hash) -> Result<TransferStats> {
        origofs_core::transfer(&self.fs, &remote.fs, head).await
    }

    /// The fetch half: copy the closure of `head` **from** `remote` into this
    /// workspace's content store. Refs are untouched.
    pub async fn fetch_objects(&self, remote: &Workspace, head: Hash) -> Result<TransferStats> {
        origofs_core::transfer(&remote.fs, &self.fs, head).await
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

    /// Write a consistent snapshot of the **metadata** store to `dest`.
    ///
    /// This is the half of a workspace that cannot be reconstructed: `fsck
    /// --rebuild` recovers committed files, directories, symlinks, and branches
    /// from the content store alone, but blame, the audit log, the actor
    /// registry, and every uncommitted edit live only in the database. Content is
    /// already durable and replicated wherever the object store puts it; this is
    /// what actually needs backing up.
    ///
    /// SQLite uses the online backup API, so a live workspace can be snapshotted
    /// without stopping writers. Postgres has no built-in equivalent here and
    /// refuses with a pointer to `pg_dump`/PITR rather than producing something
    /// that only resembles a backup.
    pub async fn backup_metadata(&self, dest: impl AsRef<Path>) -> Result<String> {
        self.fs.meta.backup_to(dest.as_ref()).await
    }

    /// The migration version currently applied to this workspace's metadata DB.
    /// A normal open already brings this to
    /// [`latest_schema_version`](Self::latest_schema_version); this is here for
    /// operators who want to introspect or gate on it.
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

    /// Refuse `op` with [`OrigoFSError::Denied`] if `ctx`'s actor is
    /// [`WritePolicy::Propose`] (§6).
    ///
    /// Every attributed engine method applies this itself, so an ordinary mutation
    /// needs no explicit call. It is exposed for the *administrative* operations
    /// that have no attributed variant — registering an actor, setting a policy —
    /// which mutate the identity registry rather than the working tree. There is
    /// nothing to attribute there, but they must still not be open to an actor the
    /// operator has deliberately restricted, and a surface has no other way to ask.
    pub async fn ensure_may_write(&self, ctx: WriteCtx, op: &str) -> Result<()> {
        self.fs.ensure_may_write(ctx, op).await
    }

    /// Refuse a **workspace-wide** `op` for an actor without `WRITE` at the root.
    ///
    /// For an operation that has no single path but reaches every one of them.
    /// Unlike [`ensure_may_write`](Self::ensure_may_write) this consults the ACL
    /// grants, so deny-by-default and subtree grants actually contain it.
    pub async fn ensure_may_write_workspace(&self, ctx: WriteCtx, op: &str) -> Result<()> {
        self.fs.ensure_may_write_workspace(ctx, op).await
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

    /// Submit a removal of `path` governed by the actor's write policy — the
    /// deletion counterpart of [`write_or_propose`](Self::write_or_propose). A
    /// `Direct` actor's removal happens now; a `Propose` actor's becomes a pending
    /// deletion suggestion. An untrusted surface must route deletes through this
    /// rather than [`remove`](Self::remove), or a propose-only actor can destroy
    /// what it is forbidden to overwrite (issue #78).
    #[tracing::instrument(level = "debug", skip_all, fields(path = %path))]
    pub async fn remove_or_propose(
        &self,
        ctx: WriteCtx,
        path: &str,
        summary: Option<&str>,
    ) -> Result<WriteOutcome> {
        let outcome = self.fs.remove_or_propose(ctx, path, summary).await?;
        // As in `write_or_propose`: the propose path emits its own `suggest` event
        // in the engine, so only the direct removal is emitted here.
        if matches!(outcome, WriteOutcome::Wrote) {
            self.emit("remove", path, None, Some(ctx.actor), ctx.session)
                .await;
        }
        Ok(outcome)
    }

    /// Rename `from` to `to`, attributed to `ctx` and refused for a propose-only
    /// actor. There is no propose-shaped equivalent for a rename, so this is a
    /// gate rather than a queue.
    #[tracing::instrument(level = "debug", skip_all, fields(from = %from, to = %to))]
    pub async fn rename_as(&self, ctx: WriteCtx, from: &str, to: &str) -> Result<()> {
        self.fs.rename_as(ctx, from, to).await?;
        self.emit(
            "rename",
            from,
            Some(to.to_string()),
            Some(ctx.actor),
            ctx.session,
        )
        .await;
        Ok(())
    }

    /// Create a directory (and missing parents), attributed to `ctx` and refused
    /// for a propose-only actor.
    pub async fn mkdir_as(&self, ctx: WriteCtx, path: &str) -> Result<()> {
        self.fs.mkdir_as(ctx, path).await?;
        self.emit("mkdir", path, None, Some(ctx.actor), ctx.session)
            .await;
        Ok(())
    }

    /// Create a symlink, attributed to `ctx` and refused for a propose-only actor.
    pub async fn symlink_as(&self, ctx: WriteCtx, target: &str, linkpath: &str) -> Result<()> {
        self.fs.symlink_as(ctx, target, linkpath).await?;
        self.emit(
            "symlink",
            linkpath,
            Some(target.to_string()),
            Some(ctx.actor),
            ctx.session,
        )
        .await;
        Ok(())
    }

    /// Snapshot the working tree into a commit, attributed to `ctx` and refused
    /// for a propose-only actor — committing crystallizes the working tree into
    /// history (and resolves a merge in progress), which is a trusted act.
    #[tracing::instrument(skip_all, fields(author = %author))]
    pub async fn commit_as(&self, ctx: WriteCtx, author: &str, message: &str) -> Result<Hash> {
        let hash = self.fs.commit_as(ctx, author, message).await?;
        self.emit(
            "commit",
            "/",
            Some(message.to_string()),
            Some(ctx.actor),
            ctx.session,
        )
        .await;
        Ok(hash)
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

    /// Extract retrieval [`Passage`]s from the working tree — the
    /// technology-agnostic half of RAG. Each passage carries its path, byte range,
    /// a content hash (dedup / incremental-embedding key), and per-passage blame.
    /// No embeddings or vectors: those live in userland (see the Python
    /// `SimpleWorkspaceReader`). See [`PassageOptions`] / [`Segmentation`].
    pub async fn passages(&self, opts: &PassageOptions) -> Result<Vec<Passage>> {
        self.fs.passages(opts).await
    }

    /// The edit-op log for an actor (optionally one session).
    pub async fn edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        self.fs.edit_ops(actor_id, session_id).await
    }

    /// Revert every line an actor wrote in a session. Returns the changed paths.
    ///
    /// `path_prefix` bounds the revert to one subtree, matched on directory
    /// boundaries — what a multi-tenant host needs so an "undo the agent's work"
    /// button can't reach outside the tenant that offered it (#94). `None`
    /// reverts everywhere the session wrote. Returning the paths rather than a
    /// count also lets a caller invalidate exactly the caches that went stale.
    #[tracing::instrument(skip_all, fields(actor = actor_id, session = session_id))]
    pub async fn revert_session(
        &self,
        actor_id: i64,
        session_id: i64,
        path_prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        self.fs
            .revert_session(actor_id, session_id, path_prefix)
            .await
    }

    /// [`revert_session`](Self::revert_session), authorized against `ctx`.
    ///
    /// The target actor/session stay parameters — a revert is a review action
    /// performed on someone else's work — while `ctx` decides whether the caller
    /// may do it, and over which subtree. **A surface accepting requests from
    /// possibly-untrusted actors must call this**, never the unauthorized form: a
    /// revert writes to every file the named session touched, and the path-less
    /// policy check the surfaces used before never consulted an ACL grant at all.
    #[tracing::instrument(skip_all, fields(actor = actor_id, session = session_id))]
    pub async fn revert_session_as(
        &self,
        ctx: WriteCtx,
        actor_id: i64,
        session_id: i64,
        path_prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        self.fs
            .revert_session_as(ctx, actor_id, session_id, path_prefix)
            .await
    }

    // --- live collaboration ----------------------------------------------

    /// Tail the change feed: events strictly after `after_seq`, oldest first.
    /// Poll with the last seen `seq` as the cursor (Postgres also fires
    /// `NOTIFY origofs_events` so consumers can be pushed instead of polling).
    pub async fn watch(&self, after_seq: i64) -> Result<Vec<Event>> {
        self.fs.events_since(after_seq, 1000).await
    }

    #[cfg(feature = "postgres")]
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

    #[cfg(feature = "postgres")]
    /// Whether this workspace is backed by Postgres (multi-writer). The
    /// Postgres-only features — the push `subscribe` feed and the cross-worker
    /// co-edit relay — are available exactly when this is true.
    pub fn is_postgres(&self) -> bool {
        self.pg.is_some()
    }

    #[cfg(feature = "postgres")]
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

    /// Load a co-edited document to **propose** against — no write rights needed
    /// and no live marker claimed, unlike [`open_coedit`](Self::open_coedit),
    /// which is a co-editing session. Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn load_coedit_as(&self, ctx: WriteCtx, path: &str) -> Result<CoeditDoc> {
        self.fs.load_coedit_as(ctx, path).await
    }

    /// Resume a tree document to **check point** against without opening a session
    /// on it — the write check, without the live marker. This is what a checkpoint
    /// route uses when no socket is attached. Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn load_coedit_tree_as(
        &self,
        ctx: WriteCtx,
        path: &str,
        root: &str,
    ) -> Result<CoeditTreeDoc> {
        self.fs.load_coedit_tree_as(ctx, path, root).await
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

    /// Propose a change to a co-edited `path` as a **CRDT merge** rather than a
    /// whole file body (issue #75 §3.2): the review row records the document's Yjs
    /// state vector as its base and `doc`'s opaque `encodeStateAsUpdate` blob as the
    /// proposal, both in the content store. Accepting it merges (`applyUpdate`)
    /// instead of overwriting, so a concurrent disjoint edit is never clobbered and
    /// never false-rejected as stale. Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn suggest_coedit(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
        summary: Option<&str>,
    ) -> Result<i64> {
        self.fs.suggest_coedit(ctx, path, doc, summary).await
    }

    /// The primitive behind [`suggest_coedit`](Self::suggest_coedit), for a client
    /// that already holds the Yjs blobs (a browser editor sends
    /// `encodeStateVector` + `encodeStateAsUpdate`). Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn suggest_coedit_update(
        &self,
        ctx: WriteCtx,
        path: &str,
        base_sv: &[u8],
        update: &[u8],
        summary: Option<&str>,
    ) -> Result<i64> {
        self.fs
            .suggest_coedit_update(ctx, path, base_sv, update, summary)
            .await
    }

    /// Open a **tree-shaped** live co-editing document for `path` (issue #92),
    /// rooted at the `XmlFragment` named `root` — the shape `@platejs/yjs`,
    /// `y-prosemirror` and `y-slate` bind to natively, as opposed to
    /// [`open_coedit`](Self::open_coedit)'s flat `Y.Text`.
    ///
    /// Resumes from the sidecar when it is still coherent with the file. Otherwise
    /// the document opens **empty** with
    /// [`resumed`](CoeditTreeDoc::resumed) false — origofs cannot rebuild a tree
    /// from flat bytes, because that needs the host's schema — and the host must
    /// seed it from [`read`](Self::read) before binding an editor. Requires the
    /// `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn open_coedit_tree(
        &self,
        ctx: WriteCtx,
        path: &str,
        root: &str,
    ) -> Result<CoeditTreeDoc> {
        self.fs.open_coedit_tree(ctx, path, root).await
    }

    /// Checkpoint a tree-shaped co-editing document into `path`, landing the host's
    /// serialized `body` with per-node authorship resolved from `spans` (see
    /// [`TreeSpan`]).
    ///
    /// origofs does not own the document schema, so the *host* serializes and says
    /// which bytes came from which co-edit node; origofs resolves each node to the
    /// author it stamped itself and lands the result in the byte-range blame index.
    /// A write that landed outside the session since the last checkpoint is refused
    /// with [`OrigoFSError::Conflict`] rather than clobbered. Requires the `coedit`
    /// feature.
    #[cfg(feature = "coedit")]
    pub async fn checkpoint_coedit_tree(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditTreeDoc,
        body: &[u8],
        spans: &[TreeSpan],
    ) -> Result<()> {
        self.fs
            .checkpoint_coedit_tree(ctx, path, doc, body, spans)
            .await
    }

    /// Persist a tree document's CRDT sidecar **without** landing a body — the
    /// server-side half of durability for a shape only the host can serialize. A
    /// crashed worker then loses no editing history, while the file and its blame
    /// stay where the last real checkpoint left them. Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn persist_coedit_tree(&self, path: &str, doc: &CoeditTreeDoc) -> Result<()> {
        self.fs.persist_coedit_tree(path, doc).await
    }

    /// End a live co-editing session for `path`: clear its live marker so byte
    /// readers stop being told the durable blob may lag. Checkpoint *first* — this
    /// only drops the flag. Requires the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn end_coedit(&self, path: &str) -> Result<()> {
        self.fs.end_coedit(path).await
    }

    #[cfg(feature = "postgres")]
    /// Ensure the cross-worker relay's backing table exists (idempotent). Call it
    /// before a room starts accepting edits, so the first publish can't race the
    /// table into existence. Requires the Postgres backend + the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn coedit_relay_init(&self) -> Result<()> {
        self.require_pg("coedit_relay_init")?
            .coedit_relay_init()
            .await
    }

    #[cfg(feature = "postgres")]
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

    #[cfg(feature = "postgres")]
    /// Every relayed op currently held for `path` — for a worker that has just
    /// started hosting `path` to replay and catch up to its peers' state (applying
    /// is idempotent). Requires the Postgres backend + the `coedit` feature.
    #[cfg(feature = "coedit")]
    pub async fn coedit_replay(&self, path: &str) -> Result<Vec<CoeditRelayNote>> {
        self.require_pg("coedit_replay")?.coedit_replay(path).await
    }

    #[cfg(feature = "postgres")]
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

    /// The live-document marker for `path`, or `None` when nothing has it open.
    ///
    /// A byte reader (`read`, three-way merge, git export) consults this to tell
    /// "these bytes are the whole truth" from "these bytes may lag an open
    /// `Y.Doc`". See [`read_live`](Self::read_live) for what to do about it.
    pub async fn live_doc(&self, path: &str) -> Result<Option<LiveDoc>> {
        self.fs.live_doc(path).await
    }

    /// Every path currently open in a live co-editing session.
    ///
    /// A caller that needs the freshest bytes for *all* of them — the git export
    /// path, a release build, a full-tree merge — checkpoints the co-editing
    /// coordinator (`api::Coordinator::checkpoint_all`) before reading, then reads
    /// normally.
    pub async fn live_paths(&self) -> Result<Vec<LiveDoc>> {
        self.fs.live_paths().await
    }

    /// Read `path` and report whether it is live — the staleness-aware read. The
    /// bytes are exactly what [`read`](Self::read) returns; the second element is
    /// `Some` when an open CRDT document may be ahead of them. Reading never
    /// blocks, fails, or forces a checkpoint on account of a live path; see
    /// `origofs_core::Fs::read_live` for why that is the least surprising rule.
    pub async fn read_live(&self, path: &str) -> Result<(Bytes, Option<LiveDoc>)> {
        self.fs.read_live(path).await
    }

    /// Retire every pending **byte** suggestion on `path` whose base no longer
    /// matches the file, resolving them to
    /// [`Superseded`](SuggestionStatus::Superseded). Returns how many were retired.
    /// CRDT suggestions are untouched — they merge into whatever the document has
    /// become, so a moved file does not invalidate them.
    pub async fn supersede_stale_suggestions(&self, path: &str) -> Result<usize> {
        self.fs.supersede_stale_byte_suggestions(path, None).await
    }

    /// Reap presence rows older than `grace_secs` (keeps the table bounded).
    /// Call periodically with a grace comfortably larger than your presence
    /// window. Returns the number of rows reaped.
    pub async fn reap_presence(&self, grace_secs: i64) -> Result<u64> {
        self.fs.reap_presence(grace_secs).await
    }

    /// Report what one file costs to read — chunk count, size distribution,
    /// self-dedup, and whether the store still holds the chunks (issue #118).
    ///
    /// `probe_residency` is the only part that touches the content backend, at one
    /// `has` per distinct chunk; everything else comes from the manifest. See
    /// [`origofs_core::perf`] for what the numbers do and do not claim — in
    /// particular that "dedup" here is repetition *within this file*, and that
    /// presence is not cache residency.
    pub async fn file_layout(&self, path: &str, probe_residency: bool) -> Result<FileLayout> {
        self.fs.file_layout(path, probe_residency).await
    }

    /// Run an end-to-end write/read benchmark against **this** workspace's
    /// backends (issue #118).
    ///
    /// It writes and then deletes files under [`BenchOpts::dir`], so it is a
    /// mutating call; it refuses to start in a directory that already holds
    /// anything unless [`BenchOpts::force`] is set. [`Fs::bench`] documents the
    /// destructive surface and what the phases do and do not measure.
    pub async fn bench(&self, opts: &BenchOpts) -> Result<BenchReport> {
        self.fs.bench(opts).await
    }
}

/// The per-store encryption salt, created with 16 fresh random bytes on first use.
///
/// The salt is not secret — an Argon2id salt exists to make the *same* passphrase
/// derive a *different* key in every store, which is what stops one cracked
/// passphrase from unlocking all of them. But it must stay stable for the life of
/// the store and must survive a metadata-database loss, so it lives beside the
/// **content**, as a sidecar: outside the content-addressed namespace, so garbage
/// collection never enumerates it and therefore cannot sweep it. Reclaiming the
/// salt would render every object in the store permanently undecryptable.
///
/// Written create-if-absent, so two processes opening the same fresh store cannot
/// each generate a salt and have the second silently invalidate the key the first
/// already started writing with.
///
/// For a local store the sidecar is `<cas_dir>/keysalt`, which is exactly where
/// the salt lived before — existing encrypted workspaces keep working unchanged.
#[cfg(feature = "encryption")]
const KEYSALT: &str = "keysalt";

#[cfg(feature = "encryption")]
async fn read_or_create_salt(content: &Content) -> Result<Vec<u8>> {
    if let Some(salt) = content.get_sidecar(KEYSALT).await? {
        if salt.is_empty() {
            return Err(OrigoFSError::Content(
                "the stored encryption salt is empty (refusing to derive a key from it)".into(),
            ));
        }
        return Ok(salt);
    }
    let mut fresh = [0u8; 16];
    getrandom::getrandom(&mut fresh)
        .map_err(|e| OrigoFSError::Content(format!("failed to generate encryption salt: {e}")))?;
    let stored = content.put_sidecar_if_absent(KEYSALT, &fresh).await?;
    if stored.is_empty() {
        return Err(OrigoFSError::Content(
            "the stored encryption salt is empty (refusing to derive a key from it)".into(),
        ));
    }
    Ok(stored)
}

// The performance-introspection types behind `origofs info` and `origofs bench`
// (issue #118). Re-exported here because the CLI — like every other consumer —
// depends on the sdk alone and never on `origofs-core` directly.
pub use origofs_core::perf::{BenchOpts, BenchReport, BenchStage, FileLayout, Residency, Tunable};
