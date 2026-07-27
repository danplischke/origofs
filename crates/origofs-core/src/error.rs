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

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<rusqlite::Error> for OrigoFSError {
    fn from(e: rusqlite::Error) -> Self {
        OrigoFSError::Metadata(e.to_string())
    }
}

impl From<object_store::Error> for OrigoFSError {
    fn from(e: object_store::Error) -> Self {
        OrigoFSError::Content(e.to_string())
    }
}

impl From<tokio_postgres::Error> for OrigoFSError {
    fn from(e: tokio_postgres::Error) -> Self {
        OrigoFSError::Metadata(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, OrigoFSError>;
