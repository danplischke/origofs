//! origofs-core — the storage-agnostic core of the origofs filesystem.
//!
//! M0 wires together two pluggable abstractions and a working-tree engine:
//!
//! - [`MetadataStore`] — names, inodes, symlinks (SQLite in M0, Postgres in M2).
//! - [`ContentStore`] — content-addressed blobs ([`LocalCasStore`] in M0; S3 in M1).
//! - [`Fs`] — POSIX-flavored operations over the two.
//!
//! See `docs/DESIGN.md` for the full architecture and the milestone roadmap.

// A library that panics takes the embedder's process down with it, so the
// library target may not `unwrap`, `expect`, `unreachable!` or panic out of a
// function that returns `Result`. The handful of genuinely infallible sites
// carry `#[expect(..., reason = "...")]`, which is itself checked: if the site
// stops being infallible the expectation goes stale and the build fails.
//
// Declared here rather than in the workspace `[lints]` table because a Cargo
// lints table applies to *every* target in the package, and an integration test
// that cannot `.unwrap()` is an integration test nobody writes. `not(test)`
// leaves the in-crate `#[cfg(test)]` modules alone; the `tests/` directory is a
// separate crate and never sees this attribute at all.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::panic_in_result_fn
    )
)]

pub mod acl;
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
pub mod perf;
pub mod portable;
pub mod posixlock;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod recover;
pub mod resync;
mod retry;
pub mod scope;
pub mod sqlite;
pub mod stats;
pub mod suggest;
pub mod trash;
pub mod types;
mod util;
pub mod version;
pub mod vfs;

pub use acl::{AclGrant, Perms};
pub use attribution::{
    Actor, ActorInit, ActorKind, BlameRange, EditOp, ToolCallInit, WriteCtx, WritePolicy,
};
pub use chunk::{AVG_CHUNK, ChunkRef, MAX_CHUNK, MIN_CHUNK, Manifest};
pub use clock::{Clock, SystemClock};
#[cfg(feature = "coedit")]
pub use coedit::{COEDIT_SIDECAR_DIR, CoeditDoc, SyncReply, coedit_sidecar_path};

/// Decoder entry points exposed for `cargo fuzz` only (`crates/origofs-core/fuzz`).
///
/// The framing parsers are private because nothing outside the co-editing code
/// should frame or unframe a sidecar. They are still `&[u8] -> Result<_>`
/// decoders of bytes read back from the content store, which the fuzz crate calls
/// "the ideal fuzz surface" — and a decoder nothing fuzzes is exactly how a
/// panic-on-malformed-input ships. This module is the narrowest way to give the
/// targets a handle without widening the real API.
///
/// Not a supported interface: hidden from the docs, and free to change or vanish.
#[cfg(feature = "coedit")]
#[doc(hidden)]
pub mod fuzz_support {
    use crate::error::Result;

    /// [`crate::coedit`]'s flat-sidecar framing, as owned bytes.
    pub fn parse_flat_sidecar(blob: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(crate::coedit::parse_sidecar(blob)?.map(|(h, s)| (h.to_vec(), s.to_vec())))
    }

    /// A parsed tree sidecar: the `XmlFragment` root, the 32-byte coherence hash
    /// of the body it crystallized, and the ydoc state.
    pub type TreeSidecarParts = (String, Vec<u8>, Vec<u8>);

    /// [`crate::coedit_tree`]'s tree-sidecar framing, as owned parts.
    pub fn parse_tree_sidecar(blob: &[u8]) -> Result<Option<TreeSidecarParts>> {
        Ok(crate::coedit_tree::parse_tree_sidecar(blob)?
            .map(|s| (s.root.to_string(), s.body_hash.to_vec(), s.state.to_vec())))
    }
}

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
pub use encrypt::{EncryptedStore, KdfParams};
pub use engine::{Fs, INTERNAL_DIR, is_internal_path, validate_ref_name};
pub use error::{BackendOrigin, ErrorClass, OrigoFSError, Result};
pub use portable::{Cell, DUMP_FORMAT, DUMP_TABLES, LoadReport, Row};
pub use scope::{Scope, ScopeError};
pub use stats::{FsStat, Quota, STATFS_BLOCK_SIZE, Usage};
pub use trash::{DEFAULT_TRASH_RETENTION_SECS, TrashEntry, TrashInit};

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
// The metadata backend, as its twelve concern-scoped parts plus the sum of them.
// Depend on `MetadataStore` where you need the whole store; name a part where you
// do not, which is most places.
pub use metadata::{
    AclStore, AttributionStore, CollabStore, ConfigStore, LockStore, MetaTxn, MetadataStore,
    NamespaceStore, PortableStore, RefStore, StoreLifecycle, SuggestionStore, TrashStore,
    WorkspaceRegistry,
};
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
pub use version::PathRevision;
