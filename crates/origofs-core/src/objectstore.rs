//! An [`ObjectStore`]-backed [`ContentStore`] — the object-storage / remote
//! backend (`docs/DESIGN.md` §4a).
//!
//! One adapter serves S3, R2, GCS, MinIO, and an in-memory store via the
//! `object_store` crate. S3/R2/MinIO and GCS-over-S3-interop go through
//! [`S3Config`]/[`ObjectContentStore::s3`]; **native** GCS (its JSON API + OAuth2,
//! so service-account / ADC / workload-identity credentials work) goes through
//! [`GcsConfig`]/[`ObjectContentStore::gcs`]. Because the in-memory store runs the
//! *same* adapter code as S3, the FS test suite that passes on `in_memory()`
//! exercises the S3 path (modulo network + credentials).

use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::types::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;

/// Connection settings for an S3-compatible backend.
///
/// `Debug` is hand-written so the secret access key cannot reach a log. A derived
/// one would print it, and this struct is exactly the sort of thing that ends up
/// inside a `tracing` field or an `anyhow` context during a connection failure —
/// the moment someone is most likely to paste the output somewhere.
#[derive(Clone, Default)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Custom endpoint (MinIO/R2/localstack, or GCS S3-interop at
    /// `https://storage.googleapis.com`). Omit for AWS S3. For *native* GCS auth
    /// (service account / ADC / workload identity) use [`GcsConfig`] instead — this
    /// S3 path authenticates only with GCS HMAC interop keys.
    pub endpoint: Option<String>,
    /// Allow plain HTTP (for local MinIO). Ignored without a custom endpoint.
    pub allow_http: bool,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Key prefix for stored objects (default `objects`).
    pub prefix: Option<String>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("allow_http", &self.allow_http)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "<set>"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .field("prefix", &self.prefix)
            .finish()
    }
}

/// Connection settings for a **native** Google Cloud Storage backend.
///
/// Unlike [`S3Config`] — which reaches GCS only through its S3-interop XML API and
/// authenticates with HMAC keys — this speaks GCS's own JSON API with OAuth2, so
/// standard Google credentials work. Credentials resolve in this order:
///
/// 1. an explicit service-account key ([`Self::service_account_key`]) or key file
///    ([`Self::service_account_path`]);
/// 2. Application Default Credentials — [`Self::application_credentials`], else the
///    `GOOGLE_APPLICATION_CREDENTIALS` env var or the well-known `gcloud` location;
/// 3. the GCE/GKE metadata server (workload identity) when nothing else is set.
#[derive(Clone, Default)]
pub struct GcsConfig {
    pub bucket: String,
    /// Path to a service-account JSON key file.
    pub service_account_path: Option<String>,
    /// Inline service-account JSON key (the file *contents*, not a path).
    pub service_account_key: Option<String>,
    /// Path to an Application Default Credentials JSON file. Leaving this unset
    /// still discovers ADC from `GOOGLE_APPLICATION_CREDENTIALS` / `gcloud`, then
    /// falls back to the metadata server.
    pub application_credentials: Option<String>,
    /// Key prefix for stored objects (default `objects`).
    pub prefix: Option<String>,
}

impl std::fmt::Debug for GcsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `service_account_key` is the *contents* of a private key file.
        f.debug_struct("GcsConfig")
            .field("bucket", &self.bucket)
            .field("service_account_path", &self.service_account_path)
            .field(
                "service_account_key",
                &self.service_account_key.as_ref().map(|_| "<redacted>"),
            )
            .field("application_credentials", &self.application_credentials)
            .field("prefix", &self.prefix)
            .finish()
    }
}

/// A content-addressed store over any `object_store` backend.
pub struct ObjectContentStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

/// Retry and timeout policy for every object-store backend.
///
/// `object_store` has defaults, but taking them meant origofs had no *stated*
/// behaviour against a flaky bucket — the operator could not know how long a stuck
/// request would hang, and a request with no timeout at all is the one that turns
/// a slow S3 into a wedged server. These are explicit, tunable, and documented:
///
/// * a request that has produced no response in `ORIGOFS_S3_TIMEOUT_SECS` is
///   abandoned, so a black-holed connection surfaces as a retryable error instead
///   of pinning a task forever;
/// * a connect that doesn't complete in `ORIGOFS_S3_CONNECT_TIMEOUT_SECS` fails
///   fast, which is the common shape of a mis-set endpoint;
/// * retries are bounded by both count and total elapsed time, so a persistent
///   outage produces an error the caller can classify rather than an unbounded
///   stall. `OrigoFSError` already marks these retryable, so callers can back off.
fn client_options() -> object_store::ClientOptions {
    fn secs(var: &str, default: u64) -> std::time::Duration {
        std::time::Duration::from_secs(
            std::env::var(var)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default),
        )
    }
    object_store::ClientOptions::new()
        .with_timeout(secs("ORIGOFS_S3_TIMEOUT_SECS", 60))
        .with_connect_timeout(secs("ORIGOFS_S3_CONNECT_TIMEOUT_SECS", 10))
}

/// See [`client_options`].
fn retry_config() -> object_store::RetryConfig {
    object_store::RetryConfig {
        max_retries: std::env::var("ORIGOFS_S3_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5),
        retry_timeout: std::time::Duration::from_secs(
            std::env::var("ORIGOFS_S3_RETRY_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(180),
        ),
        ..Default::default()
    }
}

impl ObjectContentStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// An in-memory object store — same adapter as S3, no network. For tests.
    pub fn in_memory() -> Self {
        Self::new(Arc::new(object_store::memory::InMemory::new()), "objects")
    }

    /// Build an S3-compatible content store (S3 / R2 / GCS-S3 / MinIO).
    pub fn s3(cfg: S3Config) -> Result<Self> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_client_options(client_options())
            .with_retry(retry_config());
        if let Some(endpoint) = &cfg.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(cfg.allow_http);
        }
        if let (Some(k), Some(s)) = (&cfg.access_key_id, &cfg.secret_access_key) {
            builder = builder.with_access_key_id(k).with_secret_access_key(s);
        }
        let store = builder.build().map_err(OrigoFSError::from)?;
        let prefix = cfg.prefix.clone().unwrap_or_else(|| "objects".to_string());
        Ok(Self::new(Arc::new(store), prefix))
    }

    /// Build a **native** GCS content store (GCS JSON API + OAuth2).
    ///
    /// See [`GcsConfig`] for the credential-resolution order (service account →
    /// ADC → workload identity). For a GCS emulator such as `fake-gcs-server`, set
    /// `service_account_path` to a JSON file whose `gcs_base_url` points at the
    /// emulator and that sets `disable_oauth: true`.
    pub fn gcs(cfg: GcsConfig) -> Result<Self> {
        use object_store::gcp::GoogleCloudStorageBuilder;
        // With no explicit service account, start from the environment so ADC env
        // vars and the workload-identity metadata server are honoured. With an
        // explicit key/path, start clean so an env-provided account can't collide
        // with it — the builder rejects a service-account path and key set together.
        let explicit_account =
            cfg.service_account_key.is_some() || cfg.service_account_path.is_some();
        let mut builder = if explicit_account {
            GoogleCloudStorageBuilder::new()
        } else {
            GoogleCloudStorageBuilder::from_env()
        }
        .with_bucket_name(&cfg.bucket)
        .with_client_options(client_options())
        .with_retry(retry_config());
        if let Some(key) = &cfg.service_account_key {
            builder = builder.with_service_account_key(key);
        } else if let Some(path) = &cfg.service_account_path {
            builder = builder.with_service_account_path(path);
        }
        if let Some(adc) = &cfg.application_credentials {
            builder = builder.with_application_credentials(adc);
        }
        let store = builder.build().map_err(OrigoFSError::from)?;
        let prefix = cfg.prefix.clone().unwrap_or_else(|| "objects".to_string());
        Ok(Self::new(Arc::new(store), prefix))
    }

    fn path_for(&self, hash: &Hash) -> OsPath {
        let hex = hash.to_hex();
        OsPath::from(format!("{}/{}/{}", self.prefix, &hex[0..2], &hex[2..]))
    }

    /// A sidecar's key. Deliberately a *sibling* of the content prefix rather than
    /// under it: `list()` enumerates `<prefix>/…`, so anything below it would be
    /// seen by garbage collection and swept. Losing the encryption salt that way
    /// would make every object in the bucket permanently undecryptable.
    fn sidecar_path(&self, name: &str) -> Result<OsPath> {
        if name.is_empty() || name.contains('/') || name.contains('\0') {
            return Err(OrigoFSError::InvalidPath(format!(
                "invalid sidecar name: {name:?}"
            )));
        }
        Ok(OsPath::from(format!("{}-meta/{name}", self.prefix)))
    }

    /// Named slots live under a **sibling** prefix, `<prefix>.meta/`. `list` pages
    /// `<prefix>/`, and object-store prefixes match whole path components, so
    /// `objects.meta/format` is never returned by a listing of `objects` — slots
    /// stay invisible to GC and to a recovery scan.
    fn slot_path(&self, name: &str) -> OsPath {
        OsPath::from(format!("{}.meta/{}", self.prefix, name))
    }
}

#[async_trait]
impl ContentStore for ObjectContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = Hash::of(bytes);
        let path = self.path_for(&hash);
        // Idempotent: content-addressed, so an existing object is identical.
        if self.store.head(&path).await.is_ok() {
            return Ok(hash);
        }
        self.store
            .put(&path, PutPayload::from(bytes.to_vec()))
            .await
            .map_err(OrigoFSError::from)?;
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        let path = self.path_for(key);
        if self.store.head(&path).await.is_ok() {
            return Ok(());
        }
        self.store
            .put(&path, PutPayload::from(bytes.to_vec()))
            .await
            .map_err(OrigoFSError::from)?;
        Ok(())
    }

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        // An object PUT replaces atomically: readers see the old object or the
        // new one, never neither.
        self.store
            .put(&self.path_for(key), PutPayload::from(bytes.to_vec()))
            .await
            .map_err(OrigoFSError::from)?;
        Ok(())
    }

    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        crate::content::validate_slot_name(name)?;
        // Overwrites: a slot is mutable, unlike a content-addressed object.
        self.store
            .put(&self.slot_path(name), PutPayload::from(bytes.to_vec()))
            .await?;
        Ok(())
    }

    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        crate::content::validate_slot_name(name)?;
        match self.store.get(&self.slot_path(name)).await {
            Ok(result) => result.bytes().await.map(Some).map_err(OrigoFSError::from),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(OrigoFSError::from(e)),
        }
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let path = self.path_for(hash);
        match self.store.get(&path).await {
            Ok(result) => result.bytes().await.map_err(OrigoFSError::from),
            Err(object_store::Error::NotFound { .. }) => {
                Err(OrigoFSError::ContentMissing(hash.to_hex()))
            }
            Err(e) => Err(OrigoFSError::from(e)),
        }
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        let path = self.path_for(hash);
        let meta = match self.store.head(&path).await {
            Ok(m) => m,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(OrigoFSError::ContentMissing(hash.to_hex()));
            }
            Err(e) => return Err(OrigoFSError::from(e)),
        };
        let size = meta.size;
        let start = off.min(size);
        let end = start.saturating_add(len).min(size);
        if start >= end {
            return Ok(Bytes::new());
        }
        self.store
            .get_range(&path, start..end)
            .await
            .map_err(OrigoFSError::from)
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        match self.store.head(&self.path_for(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(OrigoFSError::from(e)),
        }
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        use futures::StreamExt;
        let prefix = OsPath::from(self.prefix.clone());
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let location = meta.map_err(OrigoFSError::from)?.location;
            // `<prefix>/<aa>/<rest>` -> the 64-char hex address.
            let parts: Vec<&str> = location.as_ref().rsplit('/').collect();
            if parts.len() >= 2
                && let Some(h) = Hash::from_hex(&format!("{}{}", parts[1], parts[0]))
            {
                out.push(h);
            }
        }
        Ok(out)
    }

    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        match self.store.get(&self.sidecar_path(name)?).await {
            Ok(r) => Ok(Some(r.bytes().await.map_err(OrigoFSError::from)?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(OrigoFSError::from(e)),
        }
    }

    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        let path = self.sidecar_path(name)?;
        // A conditional create, so two processes bootstrapping the same fresh
        // store cannot each write a different random salt and have the second
        // silently invalidate the first's key.
        let opts = object_store::PutOptions::from(object_store::PutMode::Create);
        match self
            .store
            .put_opts(&path, PutPayload::from(bytes.to_vec()), opts)
            .await
        {
            Ok(_) => Ok(bytes.to_vec()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                match self.get_sidecar(name).await? {
                    Some(v) => Ok(v),
                    // Vanished between the failed create and the read.
                    None => Err(OrigoFSError::Content(format!(
                        "sidecar {name} exists but could not be read back"
                    ))),
                }
            }
            Err(e) => Err(OrigoFSError::from(e)),
        }
    }

    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        use futures::StreamExt;
        // Epoch seconds on both sides, so this needs no date-time crate of its own
        // (`last_modified` is a chrono type from `object_store`, but `.timestamp()`
        // is all we want from it).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let prefix = OsPath::from(self.prefix.clone());
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(OrigoFSError::from)?;
            let parts: Vec<&str> = meta.location.as_ref().rsplit('/').collect();
            if parts.len() >= 2
                && let Some(h) = Hash::from_hex(&format!("{}{}", parts[1], parts[0]))
            {
                // A future-dated object (clock skew between writer and bucket)
                // reports `None` — unknown, so the sweep leaves it alone.
                let age = now - meta.last_modified.timestamp();
                out.push((h, u64::try_from(age).ok()));
            }
        }
        Ok(out)
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        let path = self.path_for(hash);
        let size = match self.store.head(&path).await {
            Ok(m) => m.size,
            Err(object_store::Error::NotFound { .. }) => return Ok(0),
            Err(e) => return Err(OrigoFSError::from(e)),
        };
        match self.store.delete(&path).await {
            Ok(()) => Ok(size),
            Err(object_store::Error::NotFound { .. }) => Ok(0),
            Err(e) => Err(OrigoFSError::from(e)),
        }
    }

    async fn ping(&self) -> Result<()> {
        // A HEAD on a sentinel path: a NotFound means the bucket answered (it is
        // reachable and we are authenticated); any other error means we could not
        // reach or authenticate to it, which fails readiness.
        let probe = self.path_for(&Hash::of(b"origofs-health-probe"));
        match self.store.head(&probe).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(OrigoFSError::from(e)),
        }
    }
}
