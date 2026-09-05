//! Postgres TLS.
//!
//! `tokio-postgres` ships only `NoTls`, and that is what origofs passed — so it
//! could not reach any managed Postgres (RDS, Cloud SQL, Neon, and Supabase all
//! require TLS), and where it could connect, every path, actor name, and hash
//! crossed the network in cleartext.
//!
//! The connector-construction tests run anywhere. The handshake tests need a
//! Postgres with `ssl = on` and self-skip without one — set
//! `ORIGOFS_PG_TLS_TEST_URL` to a DSN for it and `ORIGOFS_PG_TLS_TEST_CA` to the
//! PEM that signed its certificate. See the `pg-tls` CI job.

use origofs_core::{PG_CA_FILE_ENV, PostgresMetadataStore, StoreLifecycle};

/// Serializes the tests, which all mutate the process-wide CA environment
/// variable. `std::env::set_var` is unsound if another thread reads concurrently.
/// A tokio mutex because the guard is held across `.await`s.
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn set_ca(path: Option<&str>) {
    // SAFETY: the mutex above makes this the only thread touching the environment.
    unsafe {
        match path {
            Some(p) => std::env::set_var(PG_CA_FILE_ENV, p),
            None => std::env::remove_var(PG_CA_FILE_ENV),
        }
    }
}

/// A malformed CA bundle must fail loudly at connect time, not silently leave the
/// connection unverified.
#[tokio::test]
async fn a_bad_ca_file_is_a_clean_error() {
    let _g = ENV.lock().await;
    let dir = tempfile::tempdir().unwrap();

    let missing = dir.path().join("nope.pem");
    set_ca(Some(missing.to_str().unwrap()));
    let Err(err) = PostgresMetadataStore::connect("host=127.0.0.1 dbname=x").await else {
        panic!("a missing CA file must be refused");
    };
    assert!(
        err.to_string().contains("cannot read"),
        "expected a clear read error, got: {err}"
    );

    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, b"not a certificate\n").unwrap();
    set_ca(Some(empty.to_str().unwrap()));
    let Err(err) = PostgresMetadataStore::connect("host=127.0.0.1 dbname=x").await else {
        panic!("a CA file with no certificates must be refused");
    };
    assert!(
        err.to_string().contains("no certificates"),
        "expected a clear parse error, got: {err}"
    );

    set_ca(None);
}

/// The real handshake, against a Postgres serving a certificate from a private CA.
#[tokio::test]
async fn tls_connects_when_the_certificate_verifies() {
    let _g = ENV.lock().await;
    let (Ok(dsn), Ok(ca)) = (
        std::env::var("ORIGOFS_PG_TLS_TEST_URL"),
        std::env::var("ORIGOFS_PG_TLS_TEST_CA"),
    ) else {
        eprintln!("skipping: set ORIGOFS_PG_TLS_TEST_URL and ORIGOFS_PG_TLS_TEST_CA");
        return;
    };

    set_ca(Some(&ca));
    let Ok(store) = PostgresMetadataStore::connect(&format!("{dsn} sslmode=require")).await else {
        panic!("connect over TLS");
    };
    store.init().await.expect("init over TLS");
    assert!(store.schema_version().await.expect("query over TLS") > 0);
    // Ask the server about origofs's own connection, rather than inferring
    // encryption from the absence of an error.
    assert!(
        store.server_ssl_self().await.expect("pg_stat_ssl"),
        "the server must report this connection as encrypted"
    );
    set_ca(None);
}

/// `sslmode=disable` still connects in the clear — a unix socket, a loopback test,
/// or a network someone else is already encrypting. Supplying a connector makes
/// TLS *available*; the DSN decides whether it is used.
#[tokio::test]
async fn sslmode_disable_still_connects_unencrypted() {
    let _g = ENV.lock().await;
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TLS_TEST_URL") else {
        eprintln!("skipping: set ORIGOFS_PG_TLS_TEST_URL");
        return;
    };
    set_ca(None);
    let Ok(store) = PostgresMetadataStore::connect(&format!("{dsn} sslmode=disable")).await else {
        panic!("connect with TLS disabled");
    };
    store.init().await.expect("init in the clear");
    assert!(
        !store.server_ssl_self().await.expect("pg_stat_ssl"),
        "sslmode=disable must not negotiate TLS"
    );
}

/// And refuses when it does not.
///
/// Deliberately stricter than libpq, where `sslmode=require` encrypts but verifies
/// nothing — an encrypted channel to an unauthenticated peer is not what an
/// operator asking for `require` believes they are getting.
#[tokio::test]
async fn tls_is_refused_when_the_certificate_cannot_be_verified() {
    let _g = ENV.lock().await;
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TLS_TEST_URL") else {
        eprintln!("skipping: set ORIGOFS_PG_TLS_TEST_URL");
        return;
    };

    // No CA configured, so the private-CA server certificate has no chain to a
    // trusted root.
    set_ca(None);
    let Ok(store) = PostgresMetadataStore::connect(&format!("{dsn} sslmode=require")).await else {
        panic!("the pool is built lazily, so connect() itself succeeds");
    };
    let err = store
        .schema_version()
        .await
        .expect_err("an unverifiable certificate must be refused");
    let msg = format!("{err}");
    assert!(
        msg.contains("TLS") || msg.contains("certificate"),
        "expected a TLS verification failure, got: {msg}"
    );
}
