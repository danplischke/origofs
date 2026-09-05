//! Error and result types for origofs-core.

use thiserror::Error;

/// Errors surfaced by the metadata store, content store, and engine.
///
/// `#[non_exhaustive]`: hardening this crate has meant adding variants (`Denied`
/// for the write policy, `UnsupportedVersion` for a newer on-disk format) and will
/// mean adding more. Without the attribute each one is a semver-major break for
/// every downstream `match`, which is a bad trade for a library whose whole
/// business is surfacing precise failures. Match the variants you handle and end
/// with a wildcard; [`code`](Self::code), [`retryable`](Self::retryable), and
/// [`class`](Self::class) give a stable, growth-safe way to branch without
/// enumerating variants at all.
#[derive(Error, Debug)]
#[non_exhaustive]
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

    /// An optimistic-concurrency failure with no more specific shape: a lost CAS,
    /// a resolution race, a create loop that never won.
    ///
    /// Two *named* conflicts split off from this one ([`StaleBase`](Self::StaleBase),
    /// [`ForeignWrite`](Self::ForeignWrite)) because they demand opposite
    /// recoveries and callers were reduced to matching the message string to tell
    /// them apart (#159). Use [`is_conflict`](Self::is_conflict) wherever you mean
    /// "any conflict" — a bare `Conflict(_)` pattern no longer covers the family.
    #[error("conflict: {0}")]
    Conflict(String),

    /// A suggestion's base changed since it was proposed, so accepting it would
    /// clobber the change that landed in between. The row is marked
    /// [`Superseded`](crate::suggest::SuggestionStatus::Superseded) as a side
    /// effect. **Recovery: re-diff against the new base and re-suggest.**
    ///
    /// A [`Conflict`](Self::Conflict) until #159; split out because the other
    /// thing that raised one — [`ForeignWrite`](Self::ForeignWrite) — needs the
    /// opposite response, and nothing but the message text distinguished them.
    #[error("conflict: {0}")]
    StaleBase(String),

    /// The row this request is about has already been resolved — a suggestion that
    /// is accepted, rejected or superseded is no longer awaiting anything.
    /// **Recovery: read its status; there is nothing to retry.**
    ///
    /// A [`InvalidArgument`](Self::InvalidArgument) until #164, which made it a
    /// `400` on the HTTP surface and a `ValueError` in Python — both saying the
    /// *request* was malformed when it was well-formed and merely out of date.
    /// It is the third thing a reviewing caller has to handle beside
    /// [`StaleBase`](Self::StaleBase) and the raced-CAS
    /// [`Conflict`](Self::Conflict), and unlike either of those it is terminal.
    #[error("conflict: {0}")]
    AlreadyResolved(String),

    /// The file was written outside the co-editing session since that session last
    /// agreed with the bytes on disk, so checkpointing the document would destroy
    /// the foreign write. **Recovery: re-read the file, reseed the document, and
    /// checkpoint again.**
    ///
    /// Raised where the two versions cannot be folded together: always on the tree
    /// shape (origofs cannot parse bytes back into nodes), and on the flat shape
    /// only when reconciliation itself is impossible — a missing sidecar, a removed
    /// file, bytes that are no longer UTF-8.
    #[error("conflict: {0}")]
    ForeignWrite(String),

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

    /// A content-store object carries a format version this build cannot decode:
    /// it was written by a **newer** origofs (`crate::format`).
    ///
    /// Deliberately distinct from [`Corrupt`](OrigoFSError::Corrupt) — the bytes
    /// are almost certainly intact and the fix is to upgrade the reader, not to
    /// restore from a backup. Before this existed, a bumped version byte failed
    /// the whole-magic comparison and surfaced as "malformed tree object",
    /// indistinguishable from bit rot.
    #[error(
        "unsupported {kind} format version {found} (this build reads up to v{max_supported}): \
         the object was written by a newer origofs — upgrade origofs to read it"
    )]
    UnsupportedVersion {
        /// The object kind, e.g. `tree`, `commit`, `manifest`, `ref snapshot`.
        kind: &'static str,
        /// The version found in the object header.
        found: u8,
        /// The highest version this build can decode.
        max_supported: u8,
    },

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
#[non_exhaustive]
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
#[non_exhaustive]
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
    /// A conflict (an optimistic-concurrency mismatch — see
    /// [`is_conflict`](Self::is_conflict) for the family) is deliberately **not**
    /// retryable here: the caller must re-read and rebuild against the new state,
    /// not blindly replay the same write.
    pub fn retryable(&self) -> bool {
        matches!(self, OrigoFSError::Backend { class, .. } if class.retryable())
    }

    /// Whether this is an optimistic-concurrency conflict of any shape —
    /// [`Conflict`](OrigoFSError::Conflict), [`StaleBase`](OrigoFSError::StaleBase),
    /// or [`ForeignWrite`](OrigoFSError::ForeignWrite).
    ///
    /// Splitting the two named conflicts out of `Conflict` (#159) means a bare
    /// `matches!(e, Conflict(_))` silently stops covering them, and every such site
    /// was a `409`/"the caller must re-read" decision that still applies to all
    /// three. Ask this instead; it grows with the family.
    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            OrigoFSError::Conflict(_)
                | OrigoFSError::StaleBase(_)
                | OrigoFSError::ForeignWrite(_)
                | OrigoFSError::AlreadyResolved(_)
        )
    }

    /// Whether this error means "an object was written by a newer origofs"
    /// ([`UnsupportedVersion`](OrigoFSError::UnsupportedVersion)). Surfaces can
    /// branch on it to tell the user to upgrade rather than to suspect data loss.
    pub fn is_unsupported_version(&self) -> bool {
        matches!(self, OrigoFSError::UnsupportedVersion { .. })
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
            StaleBase(_) => "stale_base",
            AlreadyResolved(_) => "already_resolved",
            ForeignWrite(_) => "foreign_write",
            Denied(_) => "denied",
            TooLarge(_) => "too_large",
            Corrupt(_) => "corrupt",
            UnsupportedVersion { .. } => "unsupported_version",
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
#[cfg(feature = "postgres")]
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

#[cfg(feature = "postgres")]
impl From<tokio_postgres::Error> for OrigoFSError {
    fn from(e: tokio_postgres::Error) -> Self {
        OrigoFSError::Backend {
            origin: BackendOrigin::Metadata,
            class: classify_pg(&e),
            source: Box::new(e),
        }
    }
}

#[cfg(feature = "postgres")]
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

#[cfg(feature = "object-store")]
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
    fn the_named_conflicts_are_conflicts_with_their_own_codes() {
        // #159: a caller must be able to tell "your proposal's base moved" from
        // "somebody wrote around your live document" *without* reading the message,
        // while everything that means "any conflict" keeps covering both.
        let stale = OrigoFSError::StaleBase("s".into());
        let foreign = OrigoFSError::ForeignWrite("f".into());
        let resolved = OrigoFSError::AlreadyResolved("r".into());
        let plain = OrigoFSError::Conflict("c".into());
        for e in [&stale, &foreign, &resolved, &plain] {
            assert!(e.is_conflict(), "{e:?} must read as a conflict");
            assert!(!e.retryable(), "{e:?} must not be blindly replayed");
            assert!(e.to_string().starts_with("conflict: "));
        }
        assert_eq!(stale.code(), "stale_base");
        assert_eq!(foreign.code(), "foreign_write");
        assert_eq!(resolved.code(), "already_resolved");
        assert_eq!(plain.code(), "conflict");
        assert!(!OrigoFSError::NotFound("x".into()).is_conflict());
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
