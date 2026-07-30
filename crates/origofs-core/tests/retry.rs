//! Transient backend failures are retried; real answers are not.
//!
//! Postgres raises `40001`/`40P01` and SQLite raises `SQLITE_BUSY` as an ordinary
//! consequence of concurrency — all three mean "this transaction did not happen,
//! run it again". `OrigoFSError::retryable` has classified them since M4, but
//! nothing in the engine acted on the classification: the only callers were the
//! HTTP layer's status mapping and `tests/concurrency.rs`, which implemented its
//! own retry loop rather than relying on one.

use bytes::Bytes;
use origofs_core::{
    BackendOrigin, ContentStore, ErrorClass, Fs, Hash, MemStore, OrigoFSError, SqliteMetadataStore,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Fails the first `fail_first` `put`s with a backend error of the given class,
/// then behaves normally.
struct Flaky {
    inner: Arc<MemStore>,
    fail_first: usize,
    class: ErrorClass,
    puts: AtomicUsize,
}

impl Flaky {
    fn new(fail_first: usize, class: ErrorClass) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemStore::new()),
            fail_first,
            class,
            puts: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl ContentStore for Flaky {
    async fn put(&self, b: &[u8]) -> origofs_core::Result<Hash> {
        if self.puts.fetch_add(1, Ordering::SeqCst) < self.fail_first {
            return Err(OrigoFSError::Backend {
                origin: BackendOrigin::Content,
                class: self.class,
                source: "injected".into(),
            });
        }
        self.inner.put(b).await
    }
    async fn put_keyed(&self, k: &Hash, b: &[u8]) -> origofs_core::Result<()> {
        self.inner.put_keyed(k, b).await
    }
    async fn get(&self, h: &Hash) -> origofs_core::Result<Bytes> {
        self.inner.get(h).await
    }
    async fn get_range(&self, h: &Hash, o: u64, l: u64) -> origofs_core::Result<Bytes> {
        self.inner.get_range(h, o, l).await
    }
    async fn has(&self, h: &Hash) -> origofs_core::Result<bool> {
        self.inner.has(h).await
    }
    async fn list(&self) -> origofs_core::Result<Vec<Hash>> {
        self.inner.list().await
    }
    async fn list_with_age(&self) -> origofs_core::Result<Vec<(Hash, Option<u64>)>> {
        self.inner.list_with_age().await
    }
    async fn delete(&self, h: &Hash) -> origofs_core::Result<u64> {
        self.inner.delete(h).await
    }
}

async fn fs_over(content: Arc<Flaky>) -> Fs<Arc<SqliteMetadataStore>, Arc<Flaky>> {
    let fs = Fs::new(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        content,
    );
    fs.init().await.unwrap();
    fs
}

/// The whole operation is re-run, and the caller never sees the transient.
#[tokio::test]
async fn a_retryable_failure_is_re_run_transparently() {
    let content = Flaky::new(2, ErrorClass::Retryable);
    let fs = fs_over(content.clone()).await;

    fs.write("/notes.txt", b"body\n")
        .await
        .expect("two transient failures must not reach the caller");
    assert_eq!(&fs.read("/notes.txt").await.unwrap()[..], b"body\n");
    assert!(
        content.puts.load(Ordering::SeqCst) >= 3,
        "the operation must actually have been re-run, saw {} put(s)",
        content.puts.load(Ordering::SeqCst)
    );
}

/// Retries are bounded: a backend that keeps asking surfaces the real error
/// rather than looping, and it is still classified so the caller can decide.
#[tokio::test]
async fn retries_are_bounded_and_surface_the_backend_error() {
    let content = Flaky::new(usize::MAX, ErrorClass::Retryable);
    let fs = fs_over(content.clone()).await;

    let err = fs.write("/notes.txt", b"body\n").await.unwrap_err();
    assert!(err.retryable(), "the real backend error survives: {err:?}");
    let attempts = content.puts.load(Ordering::SeqCst);
    assert!(
        (2..=8).contains(&attempts),
        "expected a bounded handful of attempts, got {attempts}"
    );
}

/// A fatal backend error is a real answer about the request. Retrying it wastes
/// the caller's time and, for anything with a side effect, is worse than that.
#[tokio::test]
async fn a_fatal_failure_is_not_retried() {
    let content = Flaky::new(usize::MAX, ErrorClass::Fatal);
    let fs = fs_over(content.clone()).await;

    fs.write("/notes.txt", b"body\n").await.unwrap_err();
    assert_eq!(
        content.puts.load(Ordering::SeqCst),
        1,
        "a fatal error must be surfaced on the first attempt"
    );
}

/// Neither is an application-level conflict — a `cas_ref` that lost, or a
/// suggestion whose base moved. Retrying one would paper over the concurrent
/// change the caller needs to see.
#[tokio::test]
async fn an_application_conflict_is_not_retried() {
    let err = OrigoFSError::Conflict("branch moved concurrently".into());
    assert!(!err.retryable());
    assert!(!OrigoFSError::NotFound("/gone".into()).retryable());
    assert!(!OrigoFSError::AlreadyExists("/there".into()).retryable());
}
