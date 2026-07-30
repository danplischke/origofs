//! Error and result types for origofs-core.

use thiserror::Error;

/// Errors surfaced by the metadata store, content store, and engine.
#[derive(Error, Debug)]
pub enum OrigoFSError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("not a directory: {0}")]
    NotADirectory(String),

    #[error("is a directory: {0}")]
    IsADirectory(String),

    #[error("directory not empty: {0}")]
    DirectoryNotEmpty(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("conflict: {0}")]
    Conflict(String),

    /// The actor is not permitted to perform this operation — today, an actor
    /// whose [`WritePolicy`](crate::WritePolicy) is
    /// [`Propose`](crate::WritePolicy::Propose) attempting a direct mutation
    /// (§6). Distinct from [`InvalidArgument`](OrigoFSError::InvalidArgument):
    /// the request is well-formed and would have succeeded for a permitted
    /// actor, so a surface maps it to `403`, not `400`.
    #[error("denied: {0}")]
    Denied(String),

    #[error("too large: {0}")]
    TooLarge(String),

    #[error("corrupt object: {0}")]
    Corrupt(String),

    #[error("content missing for hash {0}")]
    ContentMissing(String),

    #[error("metadata store error: {0}")]
    Metadata(String),

    #[error("content store error: {0}")]
    Content(String),

    /// A driver error from the metadata or content backend, tagged with an
    /// [`ErrorClass`] so a caller can tell a retryable transient (a Postgres
    /// serialization failure, a dropped connection, an exhausted pool) from a
    /// fatal one **without string-matching** the message. The original driver
    /// error is preserved as the [`std::error::Error::source`], so the full cause
    /// chain survives for logging. Produced by the `From` impls below — the many
    /// hand-built [`OrigoFSError::Metadata`]/[`OrigoFSError::Content`] sites are
    /// genuine logic errors and stay as they are.
    #[error("{class} {origin} backend error: {source}")]
    Backend {
        origin: BackendOrigin,
        class: ErrorClass,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// How a backend driver error should be treated by a caller deciding whether to
/// retry. This is the classification the flattened `String` payloads used to
/// lose (`docs/DESIGN.md` M9 — production readiness).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// A transient failure very likely to succeed on retry — a Postgres
    /// serialization failure (`40001`) or deadlock (`40P01`), or a SQLite
    /// `BUSY`/`LOCKED`. Retry (typically after re-reading, for a transaction).
    Retryable,
    /// The backend is (transiently) unreachable — a dropped connection, an
    /// exhausted/timed-out connection pool. Retryable after a backoff, but it
    /// signals a health problem rather than mere contention.
    Unavailable,
    /// A non-transient error — a constraint violation, a malformed statement, a
    /// programming error. Retrying will not help.
    Fatal,
}

impl ErrorClass {
    /// Whether a caller should retry an operation that failed with this class
    /// (`Retryable` or `Unavailable`, possibly after a backoff).
    pub fn retryable(self) -> bool {
        matches!(self, ErrorClass::Retryable | ErrorClass::Unavailable)
    }

    fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Retryable => "retryable",
            ErrorClass::Unavailable => "unavailable",
            ErrorClass::Fatal => "fatal",
        }
    }
}

impl std::fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which store an [`OrigoFSError::Backend`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendOrigin {
    Metadata,
    Content,
}

impl std::fmt::Display for BackendOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            BackendOrigin::Metadata => "metadata",
            BackendOrigin::Content => "content",
        })
    }
}

impl OrigoFSError {
    /// Whether a caller should retry the operation that produced this error
    /// (possibly after a backoff). True only for transient backend failures — a
    /// serialization failure, a deadlock, a dropped connection, an exhausted pool.
    ///
    /// A [`Conflict`](OrigoFSError::Conflict) (an optimistic-concurrency mismatch)
    /// is deliberately **not** retryable here: the caller must re-read and rebuild
    /// against the new state, not blindly replay the same write.
    pub fn retryable(&self) -> bool {
        matches!(self, OrigoFSError::Backend { class, .. } if class.retryable())
    }

    /// The [`ErrorClass`] of a backend error, or `None` for the non-backend
    /// variants.
    pub fn class(&self) -> Option<ErrorClass> {
        match self {
            OrigoFSError::Backend { class, .. } => Some(*class),
            _ => None,
        }
    }

    /// A stable, machine-readable code for this error — for API error envelopes
    /// and structured logs. It stays constant across message wording changes, so
    /// downstream code can branch on it without string-matching the display text.
    pub fn code(&self) -> &'static str {
        use OrigoFSError::*;
        match self {
            NotFound(_) => "not_found",
            AlreadyExists(_) => "already_exists",
            NotADirectory(_) => "not_a_directory",
            IsADirectory(_) => "is_a_directory",
            DirectoryNotEmpty(_) => "directory_not_empty",
            InvalidPath(_) => "invalid_path",
            InvalidArgument(_) => "invalid_argument",
            Conflict(_) => "conflict",
            Denied(_) => "denied",
            TooLarge(_) => "too_large",
            Corrupt(_) => "corrupt",
            ContentMissing(_) => "content_missing",
            Metadata(_) => "metadata_error",
            Content(_) => "content_error",
            Io(_) => "io_error",
            Backend {
                class: ErrorClass::Retryable,
                ..
            } => "backend_retryable",
            Backend {
                class: ErrorClass::Unavailable,
                ..
            } => "backend_unavailable",
            Backend {
                class: ErrorClass::Fatal,
                ..
            } => "backend_error",
        }
    }
}

/// Classify a Postgres driver error. A server error response carries a SQLSTATE:
/// `40001` (serialization_failure) and `40P01` (deadlock_detected) are the
/// transaction-rollback codes worth retrying; any other SQLSTATE is a definite,
/// non-transient server error. No SQLSTATE means the failure never reached a
/// server response — a closed connection is `Unavailable`; anything else (a DSN
/// parse error, a protocol error) is `Fatal` and must not be retried.
fn classify_pg(e: &tokio_postgres::Error) -> ErrorClass {
    match e.code().map(|c| c.code()) {
        Some("40001") | Some("40P01") => ErrorClass::Retryable,
        Some(_) => ErrorClass::Fatal,
        None if e.is_closed() => ErrorClass::Unavailable,
        None => ErrorClass::Fatal,
    }
}

/// Classify a SQLite driver error. `SQLITE_BUSY`/`SQLITE_LOCKED` mean another
/// writer holds the single-writer lock — retry. An I/O or can't-open failure is
/// `Unavailable`; everything else is `Fatal`.
fn classify_sqlite(e: &rusqlite::Error) -> ErrorClass {
    use rusqlite::ffi::ErrorCode;
    match e {
        rusqlite::Error::SqliteFailure(err, _) => match err.code {
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => ErrorClass::Retryable,
            ErrorCode::CannotOpen | ErrorCode::SystemIoFailure | ErrorCode::DatabaseCorrupt => {
                ErrorClass::Unavailable
            }
            _ => ErrorClass::Fatal,
        },
        _ => ErrorClass::Fatal,
    }
}

impl From<rusqlite::Error> for OrigoFSError {
    fn from(e: rusqlite::Error) -> Self {
        OrigoFSError::Backend {
            origin: BackendOrigin::Metadata,
            class: classify_sqlite(&e),
            source: Box::new(e),
        }
    }
}

impl From<tokio_postgres::Error> for OrigoFSError {
    fn from(e: tokio_postgres::Error) -> Self {
        OrigoFSError::Backend {
            origin: BackendOrigin::Metadata,
            class: classify_pg(&e),
            source: Box::new(e),
        }
    }
}

impl From<deadpool_postgres::PoolError> for OrigoFSError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        // Pool exhaustion / acquisition timeout / a dead pooled connection — the
        // store is (transiently) unavailable. If the pool is surfacing a backend
        // error from creating a connection, classify that error directly.
        let class = match &e {
            deadpool_postgres::PoolError::Backend(be) => classify_pg(be),
            _ => ErrorClass::Unavailable,
        };
        OrigoFSError::Backend {
            origin: BackendOrigin::Metadata,
            class,
            source: Box::new(e),
        }
    }
}

impl From<object_store::Error> for OrigoFSError {
    fn from(e: object_store::Error) -> Self {
        // A missing object is a content-missing condition, not a backend failure
        // (the primary read path already maps this before conversion; this covers
        // the put/list/delete paths). object_store retries transient 5xx/429/
        // timeouts internally per its `RetryConfig`, so a *surfaced* error is
        // generally terminal — classify conservatively as `Fatal`.
        if matches!(e, object_store::Error::NotFound { .. }) {
            return OrigoFSError::ContentMissing(e.to_string());
        }
        OrigoFSError::Backend {
            origin: BackendOrigin::Content,
            class: ErrorClass::Fatal,
            source: Box::new(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, OrigoFSError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_busy_and_locked_are_retryable() {
        // SQLITE_BUSY (5) / SQLITE_LOCKED (6): another writer holds the lock.
        for raw in [5, 6] {
            let err = rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(raw),
                Some("locked".into()),
            );
            let e: OrigoFSError = err.into();
            assert!(e.retryable(), "raw {raw} should be retryable, got {e:?}");
            assert_eq!(e.code(), "backend_retryable");
            assert_eq!(e.class(), Some(ErrorClass::Retryable));
        }
    }

    #[test]
    fn sqlite_constraint_is_fatal() {
        // SQLITE_CONSTRAINT (19): a definite, non-transient error — do not retry.
        let err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(19),
            Some("UNIQUE constraint failed".into()),
        );
        let e: OrigoFSError = err.into();
        assert!(!e.retryable());
        assert_eq!(e.code(), "backend_error");
    }

    #[test]
    fn backend_error_preserves_the_source_chain() {
        use std::error::Error as _;
        let err = rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(5), Some("busy".into()));
        let e: OrigoFSError = err.into();
        assert!(
            e.source().is_some(),
            "driver error must survive as the source"
        );
    }
}
