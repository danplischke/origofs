//! One suite run across every content-store backend. The in-memory object store
//! exercises the *same* adapter as S3, so passing here validates the S3 path
//! (modulo network/credentials). A real S3 run is gated behind env vars below.

use origofs_core::{
    ContentStore, Hash, LocalCasStore, MemStore, ObjectContentStore, PackStore, S3Config,
    TieredStore, VerifyingStore,
};
use std::sync::Arc;

async fn suite<C: ContentStore>(store: C) {
    // put is content-addressed and idempotent
    let h = store.put(b"hello world").await.unwrap();
    assert_eq!(h, Hash::of(b"hello world"));
    assert_eq!(store.put(b"hello world").await.unwrap(), h);

    assert!(store.has(&h).await.unwrap());
    assert_eq!(&store.get(&h).await.unwrap()[..], b"hello world");

    // ranged reads, clamped to the blob end
    assert_eq!(&store.get_range(&h, 0, 5).await.unwrap()[..], b"hello");
    assert_eq!(&store.get_range(&h, 6, 100).await.unwrap()[..], b"world");
    assert_eq!(&store.get_range(&h, 100, 10).await.unwrap()[..], b"");

    // absent content
    let missing = Hash::of(b"nope");
    assert!(!store.has(&missing).await.unwrap());
    assert!(store.get(&missing).await.is_err());
}

#[tokio::test]
async fn mem_store() {
    suite(MemStore::new()).await;
}

#[tokio::test]
async fn local_cas_store() {
    let dir = tempfile::tempdir().unwrap();
    suite(LocalCasStore::open(dir.path()).await.unwrap()).await;
}

#[tokio::test]
async fn object_store_in_memory() {
    // Same adapter code path as S3.
    suite(ObjectContentStore::in_memory()).await;
}

#[tokio::test]
async fn tiered_store() {
    let dir = tempfile::tempdir().unwrap();
    let cache: Arc<dyn ContentStore> = Arc::new(LocalCasStore::open(dir.path()).await.unwrap());
    let backend: Arc<dyn ContentStore> = Arc::new(MemStore::new());
    suite(TieredStore::new(cache, backend)).await;
}

#[tokio::test]
async fn tiered_read_through_populates_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache: Arc<dyn ContentStore> = Arc::new(LocalCasStore::open(dir.path()).await.unwrap());
    let backend: Arc<dyn ContentStore> = Arc::new(MemStore::new());
    // Seed only the backend, then read through the tier.
    let h = backend.put(b"cached-through").await.unwrap();
    assert!(!cache.has(&h).await.unwrap());

    let tier = TieredStore::new(cache.clone(), backend);
    assert_eq!(&tier.get(&h).await.unwrap()[..], b"cached-through");
    assert!(
        cache.has(&h).await.unwrap(),
        "read should populate the cache"
    );
}

/// Build an [`S3Config`] from the `ORIGOFS_S3_TEST_*` env vars (e.g. MinIO). Panics
/// if the bucket var is unset — callers are `#[ignore]`d, so this only runs when a
/// test is invoked explicitly with `--ignored` and the env is configured.
fn s3_cfg_from_env(prefix: String) -> S3Config {
    S3Config {
        bucket: std::env::var("ORIGOFS_S3_TEST_BUCKET").expect("ORIGOFS_S3_TEST_BUCKET"),
        region: std::env::var("ORIGOFS_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
        endpoint: std::env::var("ORIGOFS_S3_TEST_ENDPOINT").ok(),
        allow_http: true,
        access_key_id: std::env::var("ORIGOFS_S3_TEST_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("ORIGOFS_S3_TEST_SECRET_ACCESS_KEY").ok(),
        session_token: std::env::var("ORIGOFS_S3_TEST_SESSION_TOKEN").ok(),
        prefix: Some(prefix),
    }
}

/// A per-run-unique object prefix so each gated test gets an isolated keyspace in
/// a shared (possibly persistent) bucket.
fn unique_prefix(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("origofs-test/{label}-{nanos}")
}

/// Real S3-compatible run. Set the env vars to enable (e.g. against MinIO):
///   ORIGOFS_S3_TEST_BUCKET, ORIGOFS_S3_TEST_REGION, ORIGOFS_S3_TEST_ENDPOINT,
///   ORIGOFS_S3_TEST_ACCESS_KEY_ID, ORIGOFS_S3_TEST_SECRET_ACCESS_KEY
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint; set ORIGOFS_S3_TEST_* to run"]
async fn s3_backend() {
    suite(ObjectContentStore::s3(s3_cfg_from_env("origofs-test".into())).unwrap()).await;
}

/// A8 (issue #70): real object-store semantics the in-memory adapter can't model
/// — a multi-megabyte blob (multipart upload), ranged GETs across part
/// boundaries, and durability that lives *in the bucket*: a fresh store over the
/// same bucket+prefix, sharing no in-process state, reads the object back.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint; set ORIGOFS_S3_TEST_* to run"]
async fn s3_large_object_multipart_and_bucket_persistence() {
    let prefix = unique_prefix("large");
    let store = ObjectContentStore::s3(s3_cfg_from_env(prefix.clone())).unwrap();

    // 6 MiB of non-uniform bytes (mod a prime, so no offset aligns with a power of
    // two) — large enough to exercise multipart upload and cross-boundary reads.
    let big: Vec<u8> = (0..6usize * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let h = store.put(&big).await.unwrap();
    assert_eq!(h, Hash::of(&big));
    assert_eq!(store.put(&big).await.unwrap(), h, "put is idempotent");
    assert!(store.has(&h).await.unwrap());
    assert_eq!(&store.get(&h).await.unwrap()[..], &big[..]);

    // Ranged reads spanning MiB boundaries, and clamped past the end.
    let mid = store.get_range(&h, 1_000_000, 2_000_000).await.unwrap();
    assert_eq!(&mid[..], &big[1_000_000..3_000_000]);
    let tail = store
        .get_range(&h, big.len() as u64 - 10, 999)
        .await
        .unwrap();
    assert_eq!(&tail[..], &big[big.len() - 10..]);
    assert_eq!(
        &store.get_range(&h, big.len() as u64, 16).await.unwrap()[..],
        b"",
        "a range at/after EOF is empty"
    );

    // Durability is in the bucket, not in-process: a fresh store reads it back.
    let reopened = ObjectContentStore::s3(s3_cfg_from_env(prefix)).unwrap();
    assert!(reopened.has(&h).await.unwrap());
    assert_eq!(&reopened.get(&h).await.unwrap()[..], &big[..]);
    assert!(
        reopened.get(&Hash::of(b"absent")).await.is_err(),
        "a missing key must error, not return empty"
    );
}

/// A8 (issue #70): the production content stack — VerifyingStore(PackStore(s3)) —
/// against a real bucket. Many small chunks batch into a few pack objects (few
/// big PUTs), the integrity layer re-hashes on read, and a fresh stack over the
/// same bucket+prefix and the same on-disk pack index recovers every chunk.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint; set ORIGOFS_S3_TEST_* to run"]
async fn s3_packed_verifying_stack_persists_in_bucket() {
    async fn stack(prefix: String, index_dir: &std::path::Path) -> VerifyingStore {
        let data: Arc<dyn ContentStore> =
            Arc::new(ObjectContentStore::s3(s3_cfg_from_env(prefix)).unwrap());
        let index: Arc<dyn ContentStore> = Arc::new(LocalCasStore::open(index_dir).await.unwrap());
        // Small pack target so 200 chunks seal into several pack objects.
        VerifyingStore::new(Arc::new(PackStore::with_target(data, index, 64 * 1024)))
    }

    let prefix = unique_prefix("packed");
    let index_dir = tempfile::tempdir().unwrap();
    let store = stack(prefix.clone(), index_dir.path()).await;

    let mut chunks = Vec::new();
    for i in 0..200u32 {
        let body = format!("chunk-{i:04}-{}", "payload".repeat(60));
        let h = store.put(body.as_bytes()).await.unwrap();
        chunks.push((h, body));
    }
    // Seal the open pack so the whole set is durable in the bucket.
    store.flush().await.unwrap();

    // Read every chunk back through the verifying+packed stack.
    for (h, body) in &chunks {
        assert!(store.has(h).await.unwrap());
        assert_eq!(&store.get(h).await.unwrap()[..], body.as_bytes());
    }

    // A fresh stack (same bucket+prefix, same on-disk pack index) recovers them —
    // no reliance on in-process pending state.
    let reopened = stack(prefix, index_dir.path()).await;
    for (h, body) in &chunks {
        assert!(
            reopened.has(h).await.unwrap(),
            "packed chunk must persist in the bucket"
        );
        assert_eq!(&reopened.get(h).await.unwrap()[..], body.as_bytes());
    }
    assert!(reopened.get(&Hash::of(b"absent")).await.is_err());
}

/// The native GCS adapter builds from an inline service-account key without any
/// network or key-parsing work (`disable_oauth` short-circuits token/PEM setup),
/// so this validates the `GcsConfig` plumbing offline. A real run is the env-gated
/// `gcs_backend` below.
#[tokio::test]
async fn gcs_store_constructs() {
    use origofs_core::GcsConfig;
    // Minimal service-account JSON: the fields serde requires, with oauth disabled
    // so `build()` never parses the (empty) key or fetches a token.
    let key = r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#;
    let store = ObjectContentStore::gcs(GcsConfig {
        bucket: "origofs-test-bucket".into(),
        service_account_key: Some(key.into()),
        prefix: Some("origofs-test".into()),
        ..Default::default()
    });
    assert!(
        store.is_ok(),
        "gcs() should build from an inline service-account key"
    );
}

/// The GCS **builder** is the only GCS-specific code there is.
///
/// Everything after construction — `path_for`, `touch`/`refresh_needed`,
/// `list_with_age`, ranged reads, `delete` — is `ObjectContentStore`, shared
/// verbatim with S3 and exercised end-to-end by the MinIO CI leg and the
/// in-memory adapter. What is *not* shared is this builder: credential
/// precedence, and whether a plaintext endpoint is permitted.
///
/// That second one had no coverage and was broken. `GcsConfig` had no
/// `allow_http`, so `object_store` rejected any `http://` endpoint with a
/// `BadScheme` builder error before a single request left the process — which
/// meant the native GCS backend could not be pointed at a local emulator at all.
/// S3 has had `allow_http` from the start (that is how the MinIO leg works); GCS
/// simply never got it, and with no GCS test leg nothing noticed.
///
/// (A `fake-gcs-server` CI leg is still not possible, but **not** for the
/// addressing reason previously claimed here. `object_store` addresses GCS
/// path-style — `format!("{base_url}/{bucket}/{path}")` — and given `-public-host`
/// fake-gcs-server serves path-style `GET`, `HEAD`, `DELETE` and `?list-type=2`
/// against it perfectly well; all four were verified returning 200. The real
/// blocker is uploads: it has no XML-API `PUT` at all, routing every path-style
/// PUT into its JSON `insertObject`, which demands
/// `uploadType=media|multipart|resumable` and answers `400 invalid uploadType` to
/// the bare XML PUT `object_store` sends — so the suite's very first `put` dies.
/// `oittaa/gcp-storage-emulator` implements an XML PUT but no XML DELETE and no
/// bucket-level list. So the honest coverage is this, plus a real bucket out of
/// band; see the caveat in README.md's backend list.)
#[tokio::test]
async fn gcs_builder_accepts_a_plaintext_emulator_endpoint() {
    use origofs_core::GcsConfig;
    // `gcs_base_url` pointing at a plaintext emulator, exactly as fake-gcs-server
    // or the Cloud Storage emulator is configured.
    let key = r#"{"gcs_base_url":"http://127.0.0.1:4443","disable_oauth":true,"private_key":"","private_key_id":"","client_email":""}"#;

    let refused = ObjectContentStore::gcs(GcsConfig {
        bucket: "b".into(),
        service_account_key: Some(key.into()),
        allow_http: false,
        ..Default::default()
    });
    // Construction itself succeeds either way; the scheme is enforced per request.
    assert!(refused.is_ok(), "builder should construct");

    let allowed = ObjectContentStore::gcs(GcsConfig {
        bucket: "b".into(),
        service_account_key: Some(key.into()),
        allow_http: true,
        ..Default::default()
    });
    assert!(
        allowed.is_ok(),
        "gcs() must accept a plaintext endpoint when allow_http is set"
    );
}

/// An explicit key and an explicit path are mutually exclusive to the builder, and
/// the absence of both must fall through to ADC / the metadata server rather than
/// erroring — the shape a workload-identity deployment relies on.
#[tokio::test]
async fn gcs_credential_precedence() {
    use origofs_core::GcsConfig;
    let key = r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#;

    // Inline key wins and does not collide with an env-provided account.
    assert!(
        ObjectContentStore::gcs(GcsConfig {
            bucket: "b".into(),
            service_account_key: Some(key.into()),
            ..Default::default()
        })
        .is_ok(),
        "an inline key alone must build"
    );

    // No credentials at all. On a machine with none configured the builder reports
    // it here rather than at first use, and the message must name the problem — a
    // workload-identity deployment that has lost its binding otherwise sees a bare
    // failure with nothing to act on.
    //
    // Asserted on the *shape* rather than on ok/err, because the answer legitimately
    // differs by environment: on GCE, or with GOOGLE_APPLICATION_CREDENTIALS set,
    // this succeeds. Both outcomes are correct; a silent nonsense store is not.
    match ObjectContentStore::gcs(GcsConfig {
        bucket: "b".into(),
        ..Default::default()
    }) {
        Ok(_) => { /* credentials were discoverable here — fine */ }
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("credential")
                    || msg.contains("account")
                    || msg.contains("auth")
                    || msg.contains("metadata"),
                "a credential-less GCS build should say so; got: {e}"
            );
        }
    }
}

/// Real native-GCS run. Set the env vars to enable (against a real bucket or
/// `fake-gcs-server`):
///   ORIGOFS_GCS_TEST_BUCKET (required),
///   ORIGOFS_GCS_TEST_SERVICE_ACCOUNT_PATH   (JSON key file; for fake-gcs-server, point
///     its `gcs_base_url` at the emulator and set `disable_oauth: true`),
///   ORIGOFS_GCS_TEST_SERVICE_ACCOUNT_KEY    (inline JSON; alternative to the path),
///   ORIGOFS_GCS_TEST_APPLICATION_CREDENTIALS (ADC file),
///   ORIGOFS_GCS_TEST_ALLOW_HTTP             (set for a plaintext emulator).
/// With none of the credential vars set, ADC / the metadata server are used.
#[tokio::test]
#[ignore = "requires a GCS bucket or emulator; set ORIGOFS_GCS_TEST_* to run"]
async fn gcs_backend() {
    use origofs_core::GcsConfig;
    let bucket = std::env::var("ORIGOFS_GCS_TEST_BUCKET").expect("ORIGOFS_GCS_TEST_BUCKET");
    let cfg = GcsConfig {
        bucket,
        service_account_path: std::env::var("ORIGOFS_GCS_TEST_SERVICE_ACCOUNT_PATH").ok(),
        service_account_key: std::env::var("ORIGOFS_GCS_TEST_SERVICE_ACCOUNT_KEY").ok(),
        application_credentials: std::env::var("ORIGOFS_GCS_TEST_APPLICATION_CREDENTIALS").ok(),
        prefix: Some("origofs-test".into()),
        // An emulator speaks plaintext; real GCS never does.
        allow_http: std::env::var("ORIGOFS_GCS_TEST_ALLOW_HTTP").is_ok(),
    };
    suite(ObjectContentStore::gcs(cfg).unwrap()).await;
}

/// Object ages against a **real** bucket are sane: a freshly-put object reports a
/// small age from both `list_with_age` and `age_of`, and the age-gated
/// `delete_if_older_than` respects it.
///
/// This is the leg the in-memory adapter can't model. Ages used to be computed as
/// local-now minus the bucket's `last_modified` — two different clocks — so a GC
/// host running fast inflated every age and could sweep a concurrent writer's
/// in-flight content. Now the store measures its offset against the bucket once
/// (a probe PUT + HEAD) and expresses every age on the bucket's own clock; this
/// asserts the probe and the arithmetic hold against a real S3 implementation,
/// with its second-granularity timestamps.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint; set ORIGOFS_S3_TEST_* to run"]
async fn s3_ages_are_measured_on_the_bucket_clock() {
    let prefix = unique_prefix("ages");
    let store = ObjectContentStore::s3(s3_cfg_from_env(prefix)).unwrap();

    let h = store.put(b"age me").await.unwrap();

    // A just-written object is inside any sane grace period, on either reading.
    let age = store
        .age_of(&h)
        .await
        .unwrap()
        .expect("a real bucket can date its objects");
    assert!(age <= 120, "fresh object reported {age}s old");
    let listed = store
        .list_with_age()
        .await
        .unwrap()
        .into_iter()
        .find(|(hash, _)| *hash == h)
        .and_then(|(_, a)| a)
        .expect("the object is listed with a known age");
    assert!(listed <= 120, "fresh object listed as {listed}s old");

    // The age-gated delete declines a fresh object and leaves it in place …
    assert_eq!(store.delete_if_older_than(&h, 600).await.unwrap(), None);
    assert!(store.has(&h).await.unwrap());
    // … and deletes it once the gate is waived.
    assert!(store.delete_if_older_than(&h, 0).await.unwrap().is_some());
    assert!(!store.has(&h).await.unwrap());
}
