//! The `open_*` constructors: every metadata + content backend pairing, plus the probe for which metadata backend a handle actually has.

use super::super::*;

#[pymethods]
impl Workspace {
    /// Open (creating if needed) a local workspace: SQLite metadata at
    /// `db_path`, content-addressed chunks under `cas_dir`.
    #[staticmethod]
    fn open_local<'py>(
        py: Python<'py>,
        db_path: String,
        cas_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_local(&db_path, &cas_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a local workspace **encrypted at rest**: SQLite metadata at `db_path`,
    /// content under `cas_dir` sealed with XChaCha20-Poly1305 under a key derived
    /// from `passphrase` (Argon2id) over a per-store random salt.
    ///
    /// Encryption was reachable only from Rust — no binding existed at all — so a
    /// Python deployment could not have encryption at rest, whatever the docs
    /// implied.
    ///
    /// The same passphrase must be given on every open; a wrong one fails loudly
    /// rather than returning garbage. The salt is created on first open, is not
    /// secret, and lives beside the content store so it survives losing the
    /// metadata database.
    ///
    /// Two things to know. Key derivation is Argon2id and deliberately slow, and it
    /// runs on the calling thread — call this at startup, not per request. And
    /// addresses stay the *plaintext* hash (convergent encryption) so dedup still
    /// works, which makes a shared encrypted store an existence oracle: use
    /// per-tenant keys if that matters.
    #[staticmethod]
    fn open_local_encrypted<'py>(
        py: Python<'py>,
        db_path: String,
        cas_dir: String,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_local_encrypted(&db_path, &cas_dir, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open an S3-backed workspace **encrypted at rest**. See
    /// [`open_local_encrypted`] for the key-derivation and dedup caveats.
    #[staticmethod]
    fn open_s3_encrypted<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> =
                Arc::new(origofs_sdk::SqliteMetadataStore::open(&db_path).map_err(to_pyerr)?);
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::s3(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a Postgres + S3 workspace **encrypted at rest** — the production
    /// pairing with encryption on. See [`open_local_encrypted`] for the caveats.
    #[staticmethod]
    fn open_pg_s3_encrypted<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: S3Config,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> = Arc::new(
                origofs_sdk::PostgresMetadataStore::connect(&dsn)
                    .await
                    .map_err(to_pyerr)?,
            );
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::s3(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a GCS-backed workspace **encrypted at rest**. See
    /// [`open_local_encrypted`] for the key-derivation and dedup caveats.
    #[staticmethod]
    fn open_gcs_encrypted<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> =
                Arc::new(origofs_sdk::SqliteMetadataStore::open(&db_path).map_err(to_pyerr)?);
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::gcs(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a Postgres + native-GCS workspace **encrypted at rest** — the
    /// production pairing on Google Cloud with encryption on. See
    /// [`open_local_encrypted`] for the caveats.
    #[staticmethod]
    fn open_pg_gcs_encrypted<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> = Arc::new(
                origofs_sdk::PostgresMetadataStore::connect(&dsn)
                    .await
                    .map_err(to_pyerr)?,
            );
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::gcs(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a local workspace whose chunks are batched into pack objects
    /// (`data_dir`), with the pack index under `index_dir`.
    #[staticmethod]
    fn open_local_packed<'py>(
        py: Python<'py>,
        db_path: String,
        data_dir: String,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_local_packed(&db_path, &data_dir, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a workspace with Postgres metadata (multi-writer) over a local CAS.
    /// `dsn` is a libpq URL/DSN, e.g. `postgres://user:pass@host/db`.
    #[staticmethod]
    fn open_pg<'py>(py: Python<'py>, dsn: String, cas_dir: String) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let content = Arc::new(LocalCasStore::open(&cas_dir).await.map_err(to_pyerr)?);
            // Via the SDK constructor so the workspace retains its Postgres handle
            // (needed for the `subscribe` LISTEN/NOTIFY push feed).
            let ws = CoreWorkspace::open_pg(&dsn, content)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + an S3-compatible object store for content. Reads are
    /// integrity-verified (a bit-rotted object errors instead of being served).
    #[staticmethod]
    fn open_s3<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_s3(&db_path, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + a **packed** S3 object store (few large PUTs instead of
    /// many tiny ones) with the per-chunk index under `index_dir`. Call
    /// `commit`/`flush` to seal the open pack and `repack` to reclaim space.
    #[staticmethod]
    fn open_s3_packed<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_s3_packed(&db_path, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// `open_s3` with a **bounded local read cache** (issue #114).
    ///
    /// Every read of an uncached chunk otherwise costs a network round trip, which
    /// is what makes a mount or a repeated ranged read over an object store slow.
    /// The tier keeps recently-read chunks on local disk inside `cache`'s bounds.
    ///
    /// The packed and encrypted variants were bound and this one was not, so a
    /// Python caller could compose every remote stack except the one that makes a
    /// remote stack fast.
    #[staticmethod]
    fn open_s3_cached<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_s3_cached(&db_path, cfg.inner, cache.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// `open_gcs` with a bounded local read cache. See `open_s3_cached`.
    #[staticmethod]
    fn open_gcs_cached<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_gcs_cached(&db_path, cfg.inner, cache.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// `open_pg_s3` with a bounded local read cache — the shape a multi-writer
    /// deployment actually wants: one shared database, one shared bucket, and each
    /// host keeping its own hot chunks locally. See `open_s3_cached`.
    #[staticmethod]
    fn open_pg_s3_cached<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: S3Config,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_s3_cached(&dsn, cfg.inner, cache.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// `open_pg_gcs` with a bounded local read cache. See `open_pg_s3_cached`.
    #[staticmethod]
    fn open_pg_gcs_cached<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_gcs_cached(&dsn, cfg.inner, cache.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata (multi-writer) + an S3-compatible object store — the
    /// production pairing for a shared human+agent workspace: many writers on one
    /// database, one shared content store.
    #[staticmethod]
    fn open_pg_s3<'py>(py: Python<'py>, dsn: String, cfg: S3Config) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_s3(&dsn, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata + a **packed** S3 object store with the per-chunk index
    /// under `index_dir`. The recommended object-storage layout for a team.
    #[staticmethod]
    fn open_pg_s3_packed<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: S3Config,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_s3_packed(&dsn, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + a **native** GCS object store (GCS JSON API + OAuth2;
    /// service-account / ADC / workload-identity credentials — see `GcsConfig`).
    /// Reads are integrity-verified (a bit-rotted object errors instead of being
    /// served).
    #[staticmethod]
    fn open_gcs<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_gcs(&db_path, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + a **packed** native GCS object store with the per-chunk
    /// index under `index_dir`. Call `commit`/`flush` to seal the open pack and
    /// `repack` to reclaim space.
    #[staticmethod]
    fn open_gcs_packed<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_gcs_packed(&db_path, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata (multi-writer) + a **native** GCS object store — the
    /// production pairing for a shared human+agent workspace on Google Cloud.
    #[staticmethod]
    fn open_pg_gcs<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_gcs(&dsn, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata + a **packed** native GCS object store with the per-chunk
    /// index under `index_dir`. The recommended object-storage layout for a team on
    /// Google Cloud.
    #[staticmethod]
    fn open_pg_gcs_packed<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_gcs_packed(&dsn, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + an **in-memory** object store — the same object-store
    /// adapter as `open_s3` minus the network, for local dev and tests without a
    /// live bucket. Content is not durable.
    #[staticmethod]
    fn open_object_memory<'py>(py: Python<'py>, db_path: String) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_object_memory(&db_path)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    // --- files --------------------------------------------------------------

    /// Whether this workspace is Postgres-backed (multi-writer). The cross-worker
    /// co-editing relay is available exactly when this is true; on SQLite a single
    /// worker holds every room, so no relay is needed.
    fn is_postgres(&self) -> bool {
        self.inner.is_postgres()
    }
}
