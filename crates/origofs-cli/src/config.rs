//! Backend selection for the shipped daemons (`--config`).
//!
//! Without a config the `origofs` CLI opens a local SQLite + local-CAS workspace
//! under `--workspace` (unchanged). A `--config <file>` (TOML) instead selects the
//! metadata store (SQLite or Postgres) and the content store (local, S3-compatible,
//! or GCS, optionally packed), so `origofs serve`/`nfs`/`mcp`/`mount` can front the
//! same Postgres/object-store backends the SDK exposes — with no custom host
//! program. Each variant maps to a `Workspace::open_*` constructor.
//!
//! ```toml
//! [metadata]
//! backend = "postgres"
//! dsn = "host=db.internal user=origofs dbname=origofs"
//!
//! [content]
//! backend = "s3"
//! bucket = "origofs-content"
//! region = "us-east-1"
//! packed = true
//!
//! [cache]
//! max_bytes = 8589934592
//! ```
//!
//! `[cache]` is optional and puts a bounded local LRU read cache in front of a
//! remote content store. It is the reason `TieredStore` is reachable from the
//! shipped binary at all: bounding it (#114) made the `open_*_cached` recipes
//! possible, and nothing exposed them, so `origofs serve` against S3 fetched
//! every covering chunk over the network on every read with no way to say
//! otherwise.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use origofs_sdk::{
    ContentStore, GcsConfig, LocalCasStore, MetadataStore, ObjectContentStore, PackStore,
    PostgresMetadataStore, S3Config, SqliteMetadataStore, Workspace,
};
use serde::Deserialize;

/// A workspace's backend configuration, loaded from a TOML file.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    metadata: Metadata,
    #[serde(default)]
    content: Content,
    /// A bounded local read cache in front of a remote content store (#114).
    /// Omit for no cache, which is the previous behaviour.
    #[serde(default)]
    cache: Option<Cache>,
}

// `deny_unknown_fields` on every level, not just the outer `Config`. A
// misspelled key that parses and is silently dropped is the worst outcome
// here: the daemon starts, reports no error, and runs against the *default*
// backend while the operator's file says otherwise.
#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase", deny_unknown_fields)]
enum Metadata {
    /// SQLite (solo/offline). `path` defaults to `<workspace>/meta.db`.
    Sqlite { path: Option<PathBuf> },
    /// Postgres (multi-writer/production). `dsn` is a libpq DSN or URL.
    Postgres { dsn: String },
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata::Sqlite { path: None }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase", deny_unknown_fields)]
enum Content {
    /// A local sharded directory. `path` defaults to `<workspace>/cas`.
    Local { path: Option<PathBuf> },
    /// An S3-compatible object store (S3 / R2 / MinIO, or GCS via S3 interop).
    S3(S3Content),
    /// A native Google Cloud Storage bucket (JSON API + OAuth2).
    Gcs(GcsContent),
}

impl Default for Content {
    fn default() -> Self {
        Content::Local { path: None }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct S3Content {
    bucket: String,
    region: String,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    allow_http: bool,
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    /// STS session token for temporary credentials (AWS SSO / SAML federation).
    #[serde(default)]
    session_token: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    /// Batch chunks into pack objects (recommended for object storage); the
    /// per-chunk index lives in `index_dir` (default `<workspace>/index`).
    #[serde(default)]
    packed: bool,
    #[serde(default)]
    index_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GcsContent {
    bucket: String,
    #[serde(default)]
    service_account_path: Option<String>,
    #[serde(default)]
    service_account_key: Option<String>,
    #[serde(default)]
    application_credentials: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    /// Allow a plaintext (`http://`) endpoint — for a local GCS emulator only.
    /// Real GCS is always https, so leave this unset in production.
    #[serde(default)]
    allow_http: bool,
    #[serde(default)]
    packed: bool,
    #[serde(default)]
    index_dir: Option<PathBuf>,
}

/// A bounded local read cache in front of a remote content store.
///
/// `TieredStore` was complete, tested, and reachable from no `open_*`
/// constructor while it was unbounded — a cache that grows forever cannot be
/// turned on. Bounding it (#114) made the `open_*_cached` recipes possible, and
/// this is what makes them reachable from the shipped binary: `origofs serve`
/// against S3 read every chunk over the network on every read, and no
/// configuration file could say otherwise.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cache {
    /// Cache directory. Defaults to `<workspace>/cache`.
    #[serde(default)]
    dir: Option<PathBuf>,
    /// Evict least-recently-used chunks to stay at or below this. Default 8 GiB.
    #[serde(default)]
    max_bytes: Option<u64>,
    /// Also evict whenever the filesystem holding `dir` drops below this free,
    /// so the cache yields to the rest of the machine. Default 2 GiB. Enforced
    /// on Unix; Windows has no `statvfs`, so only `max_bytes` applies there.
    #[serde(default)]
    min_free_bytes: Option<u64>,
}

impl S3Content {
    fn to_cfg(&self) -> S3Config {
        S3Config {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            allow_http: self.allow_http,
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            session_token: self.session_token.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

impl GcsContent {
    fn to_cfg(&self) -> GcsConfig {
        GcsConfig {
            bucket: self.bucket.clone(),
            service_account_path: self.service_account_path.clone(),
            service_account_key: self.service_account_key.clone(),
            application_credentials: self.application_credentials.clone(),
            prefix: self.prefix.clone(),
            allow_http: self.allow_http,
        }
    }
}

impl Config {
    /// Load a configuration from a TOML file.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing config file {}", path.display()))
    }

    /// Open the workspace this configuration describes, routing to the matching
    /// `Workspace::open_*` constructor. `workspace` is the `--workspace` directory:
    /// it roots any defaulted path and homes local sidecars (the pack index, the
    /// read cache).
    pub async fn open(&self, workspace: &Path) -> Result<Workspace> {
        let sqlite_db = || match &self.metadata {
            Metadata::Sqlite { path } => path.clone().unwrap_or_else(|| workspace.join("meta.db")),
            Metadata::Postgres { .. } => workspace.join("meta.db"),
        };
        let index_dir = |d: &Option<PathBuf>| d.clone().unwrap_or_else(|| workspace.join("index"));

        let enc = std::env::var("ORIGOFS_ENCRYPTION_KEY")
            .ok()
            .filter(|k| !k.is_empty());

        self.check_cache_is_usable(enc.is_some())?;

        // Encryption at rest, for **any** backend combination.
        //
        // This used to refuse everything but sqlite + a local directory, which
        // meant the deployment `deploy/config.example.toml` recommends — Postgres
        // over an object store — could not have encryption at all. `EncryptedStore`
        // always composed over any `ContentStore`; the missing piece was somewhere
        // to keep the key-derivation salt that survives losing the metadata
        // database and that GC cannot sweep. That is now a content-store sidecar,
        // so the stack is assembled here instead of going through the
        // per-combination `open_*` recipes.
        if let Some(key) = &enc {
            let backend = self.raw_backend(workspace, &index_dir).await?;
            return Ok(match &self.metadata {
                Metadata::Sqlite { .. } => {
                    let meta: Arc<dyn MetadataStore> =
                        Arc::new(SqliteMetadataStore::open(sqlite_db())?);
                    Workspace::open_encrypted(meta, backend, key).await?
                }
                // Through the Postgres-aware constructor, not the generic one.
                // `Workspace::open` takes an `Arc<dyn MetadataStore>` and cannot
                // tell it was handed Postgres, so this path used to produce a
                // workspace whose `subscribe` refused with "requires the Postgres
                // backend" — on a Postgres workspace — and whose cross-worker
                // co-edit relay silently degraded to single-worker behaviour.
                Metadata::Postgres { dsn } => {
                    let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
                    let salted = Workspace::encrypted_content(backend, key).await?;
                    Workspace::open_pg_store(pg, salted).await?
                }
            });
        }

        // A cache tier is likewise assembled here rather than routed through the
        // `open_*_cached` recipes: those cover four of the eight metadata × content
        // combinations and none of the packed ones, and adding four more recipes
        // per decorator is how the combinations get out of hand.
        if let Some(cache) = &self.cache {
            let backend = self.raw_backend(workspace, &index_dir).await?;
            let content = Workspace::cached_content(backend, &cache.to_cfg(workspace)).await?;
            return Ok(match &self.metadata {
                Metadata::Sqlite { .. } => {
                    let meta: Arc<dyn MetadataStore> =
                        Arc::new(SqliteMetadataStore::open(sqlite_db())?);
                    Workspace::open(meta, content).await?
                }
                Metadata::Postgres { dsn } => {
                    let pg = Arc::new(PostgresMetadataStore::connect(dsn).await?);
                    Workspace::open_pg_store(pg, content).await?
                }
            });
        }

        let ws = match (&self.metadata, &self.content) {
            // Unencrypted and uncached from here down: both branches above return
            // for every backend combination, so re-checking them here would be
            // dead code that reads as if this arm still handled them.
            (Metadata::Sqlite { path }, Content::Local { path: cas }) => {
                let db = path.clone().unwrap_or_else(|| workspace.join("meta.db"));
                let cas = cas.clone().unwrap_or_else(|| workspace.join("cas"));
                Workspace::open_local(&db, &cas).await?
            }
            (Metadata::Sqlite { .. }, Content::S3(s3)) => {
                if s3.packed {
                    Workspace::open_s3_packed(sqlite_db(), s3.to_cfg(), index_dir(&s3.index_dir))
                        .await?
                } else {
                    Workspace::open_s3(sqlite_db(), s3.to_cfg()).await?
                }
            }
            (Metadata::Sqlite { .. }, Content::Gcs(gcs)) => {
                if gcs.packed {
                    Workspace::open_gcs_packed(sqlite_db(), gcs.to_cfg(), index_dir(&gcs.index_dir))
                        .await?
                } else {
                    Workspace::open_gcs(sqlite_db(), gcs.to_cfg()).await?
                }
            }
            (Metadata::Postgres { dsn }, Content::Local { path: cas }) => {
                let cas = cas.clone().unwrap_or_else(|| workspace.join("cas"));
                Workspace::open_pg_local(dsn, &cas).await?
            }
            (Metadata::Postgres { dsn }, Content::S3(s3)) => {
                if s3.packed {
                    Workspace::open_pg_s3_packed(dsn, s3.to_cfg(), index_dir(&s3.index_dir)).await?
                } else {
                    Workspace::open_pg_s3(dsn, s3.to_cfg()).await?
                }
            }
            (Metadata::Postgres { dsn }, Content::Gcs(gcs)) => {
                if gcs.packed {
                    Workspace::open_pg_gcs_packed(dsn, gcs.to_cfg(), index_dir(&gcs.index_dir))
                        .await?
                } else {
                    Workspace::open_pg_gcs(dsn, gcs.to_cfg()).await?
                }
            }
        };
        Ok(ws)
    }

    /// The content backend with no cache, verification or encryption on it: a
    /// local directory, or an object store optionally batched into packs.
    ///
    /// The raw layer both decorated paths start from. `VerifyingStore` is
    /// deliberately absent — it belongs on the *outside* of whatever is layered
    /// next, which is the one thing about this stack that is easy to get wrong.
    async fn raw_backend(
        &self,
        workspace: &Path,
        index_dir: &dyn Fn(&Option<PathBuf>) -> PathBuf,
    ) -> Result<Arc<dyn ContentStore>> {
        Ok(match &self.content {
            Content::Local { path } => {
                let cas = path.clone().unwrap_or_else(|| workspace.join("cas"));
                Arc::new(LocalCasStore::open(&cas).await?)
            }
            Content::S3(s3) => {
                let data: Arc<dyn ContentStore> = Arc::new(ObjectContentStore::s3(s3.to_cfg())?);
                if s3.packed {
                    let index: Arc<dyn ContentStore> =
                        Arc::new(LocalCasStore::open(index_dir(&s3.index_dir)).await?);
                    Arc::new(PackStore::new(data, index))
                } else {
                    data
                }
            }
            Content::Gcs(gcs) => {
                let data: Arc<dyn ContentStore> = Arc::new(ObjectContentStore::gcs(gcs.to_cfg())?);
                if gcs.packed {
                    let index: Arc<dyn ContentStore> =
                        Arc::new(LocalCasStore::open(index_dir(&gcs.index_dir)).await?);
                    Arc::new(PackStore::new(data, index))
                } else {
                    data
                }
            }
        })
    }

    /// Refuse a `[cache]` that cannot mean what it says, rather than composing
    /// something subtly wrong.
    ///
    /// Two combinations are rejected, and both are rejected because the quiet
    /// alternative is worse than an error at startup:
    ///
    /// * **A local content store.** Caching a directory into another directory on
    ///   the same machine buys nothing and costs a second copy of every chunk.
    ///   Silently ignoring the section would leave an operator believing a cache
    ///   is doing something.
    /// * **Encryption at rest.** `EncryptedStore` addresses ciphertext by its
    ///   *plaintext* hash (convergent encryption, so dedup survives). A cache
    ///   below it would hold ciphertext that `TieredStore`'s own integrity
    ///   re-hash — the thing that turns a corrupt cache entry into a refetch
    ///   rather than an error — would reject as corrupt on every hit; a cache
    ///   above it would write plaintext to local disk, which is the one thing
    ///   encryption at rest exists to prevent. There is a correct composition
    ///   here, but it is not either of the obvious ones and it is not this
    ///   change.
    fn check_cache_is_usable(&self, encrypted: bool) -> Result<()> {
        let Some(_) = &self.cache else { return Ok(()) };
        if matches!(self.content, Content::Local { .. }) {
            anyhow::bail!(
                "[cache] is set but the content backend is a local directory; a read \
                 cache in front of local disk copies every chunk twice and speeds up \
                 nothing. Remove [cache], or point [content] at an object store."
            );
        }
        if encrypted {
            anyhow::bail!(
                "[cache] cannot be combined with ORIGOFS_ENCRYPTION_KEY: encrypted \
                 objects are addressed by their plaintext hash, so a cache below the \
                 encryption layer fails its own integrity check on every hit and one \
                 above it writes plaintext to local disk. Remove [cache], or unset \
                 ORIGOFS_ENCRYPTION_KEY."
            );
        }
        Ok(())
    }
}

impl Cache {
    fn to_cfg(&self, workspace: &Path) -> origofs_sdk::CacheConfig {
        let dir = self.dir.clone().unwrap_or_else(|| workspace.join("cache"));
        let mut cfg = origofs_sdk::CacheConfig::new(dir);
        if let Some(n) = self.max_bytes {
            cfg = cfg.max_bytes(n);
        }
        if let Some(n) = self.min_free_bytes {
            cfg = cfg.min_free_bytes(n);
        }
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `--config` is what points the shipped daemons at Postgres and an object
    /// store, so a parsing slip silently changes which backend production runs on.
    /// This file had no tests; the Docker job only exercises the compose file's
    /// one configuration.
    fn parse(toml_src: &str) -> Result<Config> {
        toml::from_str(toml_src).map_err(Into::into)
    }

    #[test]
    fn an_empty_config_is_the_local_default() {
        // `origofs --config empty.toml` must behave like no config at all, because
        // every field has a documented default.
        let c = parse("").expect("empty config");
        assert!(matches!(c.metadata, Metadata::Sqlite { path: None }));
        assert!(matches!(c.content, Content::Local { path: None }));
    }

    #[test]
    fn the_documented_example_parses() {
        // The exact block from this module's own doc comment. A doc example that
        // does not parse is worse than none.
        let c = parse(
            r#"
            [metadata]
            backend = "postgres"
            dsn = "host=db.internal user=origofs dbname=origofs"

            [content]
            backend = "s3"
            bucket = "origofs-content"
            region = "us-east-1"
            packed = true
            "#,
        )
        .expect("the documented example");
        match c.metadata {
            Metadata::Postgres { dsn } => assert!(dsn.contains("db.internal")),
            other => panic!("expected postgres, got {other:?}"),
        }
        match c.content {
            Content::S3(s3) => {
                assert_eq!(s3.bucket, "origofs-content");
                assert_eq!(s3.region, "us-east-1");
                assert!(s3.packed);
            }
            other => panic!("expected s3, got {other:?}"),
        }
    }

    #[test]
    fn the_shipped_example_config_parses() {
        // `deploy/config.example.toml` is what an operator copies. If it stops
        // parsing, the first person to find out is them.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/config.example.toml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));

        // The file documents several alternatives with most commented out; each
        // uncommented `[metadata]`/`[content]` pair must be a valid config.
        parse(&text).unwrap_or_else(|e| panic!("{} does not parse: {e}", path.display()));
    }

    #[test]
    fn an_unknown_backend_is_rejected() {
        assert!(parse("[metadata]\nbackend = \"mysql\"").is_err());
        assert!(parse("[content]\nbackend = \"azure\"").is_err());
    }

    #[test]
    fn a_typo_is_rejected_rather_than_ignored() {
        // `deny_unknown_fields` is the point: a misspelled key that parsed and was
        // silently dropped would hand someone the *default* backend while their
        // config said otherwise.
        assert!(
            parse("[metadata]\nbackend = \"postgres\"\ndns = \"host=x\"").is_err(),
            "a misspelled `dsn` must not be silently ignored"
        );
        assert!(
            parse("[content]\nbackend = \"local\"\npth = \"/tmp/cas\"").is_err(),
            "a misspelled `path` must not be silently ignored"
        );
    }

    #[test]
    fn postgres_requires_a_dsn() {
        assert!(
            parse("[metadata]\nbackend = \"postgres\"").is_err(),
            "postgres with no dsn has nothing to connect to"
        );
    }

    // --- the read cache (#114) ----------------------------------------------

    #[test]
    fn a_cache_section_parses_with_every_field_defaulted() {
        let c = parse(
            r#"
            [content]
            backend = "s3"
            bucket = "b"
            region = "r"

            [cache]
            "#,
        )
        .expect("bare [cache]");
        let cache = c.cache.expect("cache section");
        assert!(cache.dir.is_none() && cache.max_bytes.is_none());
        // Defaults come from `CacheConfig::new`, not from this file, so there is
        // one place that decides what "8 GiB" means.
        let cfg = cache.to_cfg(std::path::Path::new("/ws"));
        assert_eq!(cfg.dir, std::path::Path::new("/ws/cache"));
        assert_eq!(cfg.max_bytes, 8 << 30);
    }

    #[test]
    fn cache_fields_override_the_defaults() {
        let c = parse(
            r#"
            [content]
            backend = "s3"
            bucket = "b"
            region = "r"

            [cache]
            dir = "/var/cache/origofs"
            max_bytes = 1024
            min_free_bytes = 512
            "#,
        )
        .expect("full [cache]");
        let cfg = c.cache.unwrap().to_cfg(std::path::Path::new("/ws"));
        assert_eq!(cfg.dir, std::path::Path::new("/var/cache/origofs"));
        assert_eq!(cfg.max_bytes, 1024);
        assert_eq!(cfg.min_free_bytes, 512);
    }

    #[test]
    fn a_cache_typo_is_rejected_like_every_other() {
        assert!(parse("[cache]\nmax_byte = 1").is_err());
    }

    #[test]
    fn a_cache_over_local_content_is_refused_rather_than_ignored() {
        // Caching a directory into another directory on the same machine copies
        // every chunk twice and speeds up nothing. Ignoring the section would
        // leave an operator believing a cache is doing something.
        let c = parse("[cache]\n").expect("parses");
        let err = c.check_cache_is_usable(false).unwrap_err().to_string();
        assert!(err.contains("local directory"), "{err}");
    }

    #[test]
    fn a_cache_with_encryption_is_refused_rather_than_composed_wrongly() {
        // Encrypted objects are addressed by their *plaintext* hash, so a cache
        // below the encryption layer fails `TieredStore`'s own integrity re-hash
        // on every hit, and one above it writes plaintext to local disk — the one
        // thing encryption at rest exists to prevent.
        let c = parse(
            r#"
            [content]
            backend = "s3"
            bucket = "b"
            region = "r"

            [cache]
            "#,
        )
        .expect("parses");
        assert!(c.check_cache_is_usable(false).is_ok(), "fine unencrypted");
        let err = c.check_cache_is_usable(true).unwrap_err().to_string();
        assert!(err.contains("ORIGOFS_ENCRYPTION_KEY"), "{err}");
    }

    #[test]
    fn no_cache_section_means_no_cache() {
        let c = parse("").expect("empty");
        assert!(c.cache.is_none());
        assert!(c.check_cache_is_usable(true).is_ok());
    }

    #[test]
    fn load_reports_the_offending_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.toml");
        std::fs::write(&path, "[metadata]\nbackend = \"nope\"").unwrap();
        let err = Config::load(&path).unwrap_err().to_string();
        assert!(err.contains("broken.toml"), "unhelpful error: {err}");

        let missing = dir.path().join("absent.toml");
        let err = Config::load(&missing).unwrap_err().to_string();
        assert!(err.contains("absent.toml"), "unhelpful error: {err}");
    }
}
