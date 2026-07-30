//! origofs-core — the storage-agnostic core of the origofs filesystem.
//!
//! M0 wires together two pluggable abstractions and a working-tree engine:
//!
//! - [`MetadataStore`] — names, inodes, symlinks (SQLite in M0, Postgres in M2).
//! - [`ContentStore`] — content-addressed blobs ([`LocalCasStore`] in M0; S3 in M1).
//! - [`Fs`] — POSIX-flavored operations over the two.
//!
//! See `docs/DESIGN.md` for the full architecture and the milestone roadmap.

pub mod attribution;
pub mod chunk;
pub mod clock;
#[cfg(feature = "coedit")]
pub mod coedit;
pub mod collab;
pub mod content;
pub mod corpus;
pub mod encrypt;
pub mod engine;
pub mod error;
mod format;
pub mod gc;
pub mod interop;
pub mod merge;
pub mod metadata;
pub mod metrics;
pub mod migrations;
pub mod objectgraph;
pub mod objectstore;
pub mod pack;
pub mod postgres;
pub mod recover;
pub mod resync;
mod retry;
pub mod sqlite;
pub mod suggest;
pub mod types;
mod util;
pub mod version;
pub mod vfs;

pub use attribution::{
    Actor, ActorInit, ActorKind, BlameRange, EditOp, ToolCallInit, WriteCtx, WritePolicy,
};
pub use chunk::{AVG_CHUNK, ChunkRef, MAX_CHUNK, MIN_CHUNK, Manifest};
pub use clock::{Clock, SystemClock};
#[cfg(feature = "coedit")]
pub use coedit::{COEDIT_SIDECAR_DIR, CoeditDoc, coedit_sidecar_path};
pub use collab::{EVENT_CHANNEL, Event, EventInit, LiveDoc, PRESENCE_WINDOW_SECS, Presence};
pub use content::{ContentStore, LocalCasStore, MemStore, TieredStore, VerifyingStore};
pub use corpus::{Passage, PassageOptions, Segmentation};
pub use encrypt::EncryptedStore;
pub use engine::{Fs, validate_ref_name};
pub use error::{BackendOrigin, ErrorClass, OrigoFSError, Result};
pub use gc::{DEFAULT_GC_GRACE_SECS, GcStats};
pub use merge::{Conflict, MergeOutcome};
pub use metadata::{MetaTxn, MetadataStore};
pub use metrics::OpTimer;
pub use migrations::latest_schema_version;
pub use objectgraph::{
    Commit, CommitInfo, DiffEntry, DiffStatus, RefSnapshot, Tree, TreeEntry, TreeKind,
    VersioningMode,
};
pub use objectstore::{GcsConfig, ObjectContentStore, S3Config};
pub use pack::{DEFAULT_PACK_SIZE, PackStore};
#[cfg(feature = "coedit")]
pub use postgres::{CoeditRelayNote, CoeditRelaySub};
pub use postgres::{EventSubscription, PG_CA_FILE_ENV, PostgresMetadataStore};
pub use recover::RebuildReport;
pub use resync::{
    IdentityMap, ResyncOutcome, ResyncReport, TransferStats, carry_blame, resync, transfer,
};
pub use sqlite::SqliteMetadataStore;
pub use suggest::{
    Suggestion, SuggestionContent, SuggestionInit, SuggestionKind, SuggestionStatus, WriteOutcome,
};
pub use types::{DirEntry, DirEntryAttr, DirPage, FileKind, Hash, INO_ROOT, Ino, Inode, InodeInit};
