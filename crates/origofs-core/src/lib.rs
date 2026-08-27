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
#[cfg(feature = "coedit")]
pub mod coedit_tree;
pub mod collab;
pub mod content;
pub mod corpus;
#[cfg(feature = "encryption")]
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
#[cfg(feature = "object-store")]
pub mod objectstore;
pub mod pack;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod recover;
pub mod resync;
mod retry;
pub mod scope;
pub mod sqlite;
pub mod stats;
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
pub use coedit::{COEDIT_SIDECAR_DIR, CoeditDoc, SyncReply, coedit_sidecar_path};
#[cfg(feature = "coedit")]
pub use coedit_tree::{
    CoeditTreeDoc, DEFAULT_TREE_ROOT, NODE_KEY, TreeRun, TreeSpan, coedit_tree_sidecar_path,
};
pub use collab::{EVENT_CHANNEL, Event, EventInit, LiveDoc, PRESENCE_WINDOW_SECS, Presence};
pub use content::{
    CacheLimits, ContentStore, DEDUP_REFRESH_AFTER_SECS, LocalCasStore, MemStore, TieredStore,
    VerifyingStore,
};
pub use corpus::{Passage, PassageOptions, Segmentation};
#[cfg(feature = "encryption")]
pub use encrypt::EncryptedStore;
pub use engine::{Fs, validate_ref_name};
pub use error::{BackendOrigin, ErrorClass, OrigoFSError, Result};
pub use scope::{Scope, ScopeError};
pub use stats::{FsStat, Quota, STATFS_BLOCK_SIZE, Usage};

/// The largest value a single extended attribute may hold (issue #119).
///
/// 64 KiB, matching Linux's own per-value ceiling, so nothing that works on ext4
/// or XFS is refused here.
///
/// This is a hard rule rather than a tuning knob. An xattr is stored in the
/// **metadata** store, and the invariant the whole design rests on is that the
/// metadata DB references content by hash and never holds large bytes. Without a
/// cap, `setfattr` would be a supported way to write unbounded,
/// un-deduplicated, un-chunked data straight into the DB — precisely what the
/// metadata/content split exists to prevent. Raise it only alongside a decision
/// about where oversized attribute values would actually live.
pub const MAX_XATTR_LEN: usize = 64 * 1024;
pub use gc::{DEFAULT_GC_GRACE_SECS, GcStats};
pub use merge::{Conflict, MergeOutcome};
pub use metadata::{MetaTxn, MetadataStore};
pub use metrics::OpTimer;
pub use migrations::latest_schema_version;
pub use objectgraph::{
    Commit, CommitInfo, DiffEntry, DiffStatus, RefSnapshot, Tree, TreeEntry, TreeKind,
    VersioningMode,
};
#[cfg(feature = "object-store")]
pub use objectstore::{GcsConfig, ObjectContentStore, S3Config};
pub use pack::{DEFAULT_PACK_SIZE, PackStore};
#[cfg(feature = "coedit")]
#[cfg(feature = "postgres")]
pub use postgres::{CoeditRelayNote, CoeditRelaySub};
#[cfg(feature = "postgres")]
pub use postgres::{EventSubscription, PG_CA_FILE_ENV, PostgresMetadataStore};
pub use recover::RebuildReport;
pub use resync::{
    IdentityMap, ResyncOutcome, ResyncReport, TransferStats, carry_blame, resync, transfer,
};
pub use sqlite::SqliteMetadataStore;
pub use suggest::{
    Suggestion, SuggestionContent, SuggestionInit, SuggestionKind, SuggestionStatus, WriteOutcome,
};
pub use types::{
    DirEntry, DirEntryAttr, DirPage, FileKind, Hash, INO_ROOT, Ino, Inode, InodeInit, Owner,
};
