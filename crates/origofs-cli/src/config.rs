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
//! ```

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
    #[serde(default)]
    packed: bool,
    #[serde(default)]
    index_dir: Option<PathBuf>,
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
    /// it roots any defaulted path and homes local sidecars (the pack index).
    pub async fn open(&self, workspace: &Path) -> Result<Workspace> {
        let sqlite_db = || match &self.metadata {
            Metadata::Sqlite { path } => path.clone().unwrap_or_else(|| workspace.join("meta.db")),
            Metadata::Postgres { .. } => workspace.join("meta.db"),
        };
        let index_dir = |d: &Option<PathBuf>| d.clone().unwrap_or_else(|| workspace.join("index"));

        let enc = std::env::var("ORIGOFS_ENCRYPTION_KEY")
            .ok()
            .filter(|k| !k.is_empty());

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
            let meta: Arc<dyn MetadataStore> = match &self.metadata {
                Metadata::Sqlite { .. } => Arc::new(SqliteMetadataStore::open(sqlite_db())?),
                Metadata::Postgres { dsn } => Arc::new(PostgresMetadataStore::connect(dsn).await?),
            };
            // The raw backend the salt sidecar lives on, packed if configured.
            let backend: Arc<dyn ContentStore> = match &self.content {
                Content::Local { path } => {
                    let cas = path.clone().unwrap_or_else(|| workspace.join("cas"));
                    Arc::new(LocalCasStore::open(&cas).await?)
                }
                Content::S3(s3) => {
                    let data: Arc<dyn ContentStore> =
                        Arc::new(ObjectContentStore::s3(s3.to_cfg())?);
                    if s3.packed {
                        let index: Arc<dyn ContentStore> =
                            Arc::new(LocalCasStore::open(index_dir(&s3.index_dir)).await?);
                        Arc::new(PackStore::new(data, index))
                    } else {
                        data
                    }
                }
                Content::Gcs(gcs) => {
                    let data: Arc<dyn ContentStore> =
                        Arc::new(ObjectContentStore::gcs(gcs.to_cfg())?);
                    if gcs.packed {
                        let index: Arc<dyn ContentStore> =
                            Arc::new(LocalCasStore::open(index_dir(&gcs.index_dir)).await?);
                        Arc::new(PackStore::new(data, index))
                    } else {
                        data
                    }
                }
            };
            return Ok(Workspace::open_encrypted(meta, backend, key).await?);
        }

        let ws = match (&self.metadata, &self.content) {
            (Metadata::Sqlite { path }, Content::Local { path: cas }) => {
                let db = path.clone().unwrap_or_else(|| workspace.join("meta.db"));
                let cas = cas.clone().unwrap_or_else(|| workspace.join("cas"));
                match &enc {
                    Some(k) => Workspace::open_local_encrypted(&db, &cas, k).await?,
                    None => Workspace::open_local(&db, &cas).await?,
                }
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
