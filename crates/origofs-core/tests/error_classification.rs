//! Error classification (`docs/DESIGN.md` M9 — production readiness).
//!
//! Backend driver errors carry a retryable/fatal [`ErrorClass`] and a stable,
//! machine-readable code instead of being flattened to an opaque string, so a
//! downstream caller (or the HTTP API) can tell a transient failure worth
//! retrying from a fatal one *without* string-matching the message.

use origofs_core::{BackendOrigin, ErrorClass, OrigoFSError};
use std::error::Error as _;

fn backend(class: ErrorClass) -> OrigoFSError {
    OrigoFSError::Backend {
        origin: BackendOrigin::Metadata,
        class,
        source: Box::new(std::io::Error::other("driver failure")),
    }
}

#[test]
fn backend_class_drives_retryable() {
    assert!(backend(ErrorClass::Retryable).retryable());
    assert!(backend(ErrorClass::Unavailable).retryable());
    assert!(!backend(ErrorClass::Fatal).retryable());
}

#[test]
fn machine_codes_are_stable() {
    assert_eq!(backend(ErrorClass::Retryable).code(), "backend_retryable");
    assert_eq!(
        backend(ErrorClass::Unavailable).code(),
        "backend_unavailable"
    );
    assert_eq!(backend(ErrorClass::Fatal).code(), "backend_error");

    assert_eq!(OrigoFSError::NotFound("x".into()).code(), "not_found");
    assert_eq!(
        OrigoFSError::AlreadyExists("x".into()).code(),
        "already_exists"
    );
    assert_eq!(OrigoFSError::Conflict("x".into()).code(), "conflict");
    assert_eq!(
        OrigoFSError::InvalidArgument("x".into()).code(),
        "invalid_argument"
    );
    assert_eq!(
        OrigoFSError::ContentMissing("x".into()).code(),
        "content_missing"
    );
}

#[test]
fn conflict_is_not_retryable() {
    // An optimistic-concurrency mismatch: the caller must re-read and rebuild
    // against the new state, not blindly replay the same write.
    let e = OrigoFSError::Conflict("branch moved".into());
    assert!(!e.retryable());
    assert_eq!(e.class(), None);
}

#[test]
fn backend_error_exposes_class_and_source() {
    let e = backend(ErrorClass::Unavailable);
    assert_eq!(e.class(), Some(ErrorClass::Unavailable));
    assert!(
        e.source().is_some(),
        "the driver cause must survive for logging"
    );
}
