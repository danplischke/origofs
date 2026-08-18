//! The metadata store: names, inodes, and (in later milestones) refs, commits,
//! and attribution. Content bytes never live here — only content addresses do
//! (`docs/DESIGN.md` §4b).

use crate::attribution::{Actor, ActorInit, EditOp, EditOpInit, ToolCallInit, WritePolicy};
use crate::collab::{Event, EventInit, LiveDoc, Presence};
use crate::error::Result;
use crate::suggest::{Suggestion, SuggestionInit, SuggestionStatus};
use crate::types::{DirEntry, Hash, Ino, Inode, InodeInit};
use async_trait::async_trait;
use std::sync::Arc;

/// Abstracts the metadata backend so the same engine runs on SQLite (M0) or
/// Postgres (M2). The trait is intentionally intent-level; SQL dialects stay
/// behind the implementation.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Create the schema (idempotent) and ensure the root directory (`INO_ROOT`).
    async fn init(&self) -> Result<()>;

    /// The highest migration version currently applied to this store (0 if it has
    /// never been initialized). Compare with [`crate::latest_schema_version`] to
    /// tell whether a `migrate`/`init` would advance the schema.
    async fn schema_version(&self) -> Result<i64>;

    /// A cheap liveness probe of the metadata backend, for the readiness endpoint
    /// (`/readyz`). The default does a real backend round-trip via
    /// [`schema_version`](MetadataStore::schema_version) — enough to catch an
    /// exhausted pool or an unreachable database (surfaced as a classified
    /// `Backend`/`Unavailable` error). A backend may override it with something
    /// cheaper.
    async fn ping(&self) -> Result<()> {
        self.schema_version().await.map(|_| ())
    }

    /// Write a consistent snapshot of this store to `dest`, returning a
    /// human-readable description of what was produced.
    ///
    /// The metadata store is the one part of a workspace the content store cannot
    /// rebuild. `fsck --rebuild` recovers committed files, directories, symlinks,
    /// and branches from the bucket alone — but blame, the audit log, the actor
    /// registry, and every uncommitted edit exist *only* here. Three documents
    /// (`CLAUDE.md`, `DESIGN.md` §7, `README.md`) all say to back this up, and
    /// there was no way to.
    ///
    /// "Consistent" means concurrent writers are allowed: a snapshot must not
    /// require the workspace to be stopped, or it will not be taken. The default
    /// refuses rather than producing something that only looks like a backup —
    /// a backup you cannot restore is worse than an absent one, because it is
    /// discovered at the moment you need it.
    async fn backup_to(&self, dest: &std::path::Path) -> Result<String> {
        let _ = dest;
        Err(crate::error::OrigoFSError::InvalidArgument(
            "this metadata backend has no built-in backup; use the backend's own \
             tooling (for Postgres: pg_dump, or continuous archiving/PITR)"
                .into(),
        ))
    }

    /// Begin an atomic write transaction (`docs/DESIGN.md` §4b).
    ///
    /// A logical filesystem write is several statements — create an inode, link
    /// a dentry, set content, record blame, append an op-log entry — and if any
    /// step fails or the process crashes between them the store is left corrupt:
    /// a dangling dentry, an orphaned inode, or a content/blame mismatch. Route
    /// such writes through a transaction so they commit all-or-nothing. Dropping
    /// the returned [`MetaTxn`] without [`commit`](MetaTxn::commit) rolls back.
    ///
    /// SQLite uses `BEGIN IMMEDIATE` (one writer at a time); Postgres pins a
    /// pooled connection for the `BEGIN…COMMIT`.
    async fn begin(&self) -> Result<Box<dyn MetaTxn>>;

    /// Fetch an inode by number.
    async fn get_inode(&self, ino: Ino) -> Result<Option<Inode>>;

    /// Fetch many inodes in one round-trip (M16).
    ///
    /// The batched counterpart of [`get_inode`](MetadataStore::get_inode), so a
    /// `readdir` that needs attributes costs one query instead of one per entry.
    /// Each backend issues a single `IN (…)` / `= ANY(…)` query per batch (SQLite
    /// chunks the list to stay under its bound-parameter limit).
    ///
    /// Contract, deliberately forgiving so callers need not pre-clean their input:
    /// an `ino` with no row is simply absent from the result, duplicates in `inos`
    /// yield one row, and the order of the result is **unspecified** — index it by
    /// [`Inode::ino`]. Like `get_inode`, the lookup is by inode number alone
    /// (globally unique), so it is not workspace-filtered.
    async fn get_inodes(&self, inos: &[Ino]) -> Result<Vec<Inode>>;

    /// Allocate a new inode. `nlink` starts at 1; size at 0; no content.
    async fn create_inode(&self, init: InodeInit) -> Result<Ino>;

    /// Set an inode's content address and size (touches mtime/ctime).
    async fn set_content(&self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()>;

    /// Set an inode's link count.
    async fn set_nlink(&self, ino: Ino, nlink: i64) -> Result<()>;

    /// Replace an inode's **permission** bits (`chmod`), leaving the file-type
    /// bits intact — the stored `mode` carries both, and dropping the type half
    /// would corrupt the committed tree entry and the git exporter's exec-bit
    /// check. Touches `ctime`, as POSIX requires.
    async fn set_mode(&self, ino: Ino, mode: u32) -> Result<()>;

    /// Set an inode's owning user and/or group (`chown`). `None` leaves that half
    /// unchanged, because POSIX lets a caller supply only one. Touches `ctime`.
    async fn set_owner(&self, ino: Ino, uid: Option<u32>, gid: Option<u32>) -> Result<()>;

    /// Delete an inode and any symlink row. The caller ensures `nlink` hit 0.
    /// Reclaiming now-unreferenced content is deferred to GC (M9).
    async fn delete_inode(&self, ino: Ino) -> Result<()>;

    /// Resolve `name` within directory `parent`.
    async fn lookup(&self, parent: Ino, name: &str) -> Result<Option<Ino>>;

    /// Link `name` in `parent` to `ino`. Errors if the name already exists.
    async fn add_dentry(&self, parent: Ino, name: &str, ino: Ino) -> Result<()>;

    /// Unlink `name` from `parent` (no-op if absent).
    async fn remove_dentry(&self, parent: Ino, name: &str) -> Result<()>;

    /// List the entries of directory `parent`, ordered by name.
    ///
    /// Reads the *whole* directory into memory. Prefer
    /// [`list_dir_page`](MetadataStore::list_dir_page) on any path that serves a
    /// user-sized directory (the FUSE/NFS `readdir` surfaces); this stays for the
    /// many callers — commit, merge, gc, export — that genuinely walk everything.
    async fn list_dir(&self, parent: Ino) -> Result<Vec<DirEntry>>;

    /// One keyset ("seek") page of directory `parent`, ordered by name (M16).
    ///
    /// Returns at most `limit` entries whose name sorts strictly after
    /// `after_name` (from the start when `None`). Each backend implements this as
    /// a single indexed query — `WHERE parent_ino = ? AND name > ? ORDER BY name
    /// LIMIT ?` drives the `(parent_ino, name)` primary-key index as a range scan,
    /// so the cost is proportional to the page, not to the directory.
    ///
    /// Resume by passing the last returned name back as `after_name`. Because the
    /// cursor is a *name* and not an offset, entries created or removed before the
    /// cursor cannot shift the page and make the scan skip or repeat an entry —
    /// the property an `OFFSET`-based pager does not have.
    ///
    /// The ordering (and therefore the `>` comparison) is the backend's own text
    /// ordering — SQLite's `BINARY`, Postgres's column collation. Both are
    /// self-consistent within a store, which is all a keyset scan needs; the two
    /// backends may order non-ASCII names differently from each other, exactly as
    /// [`list_dir`](MetadataStore::list_dir) already does.
    async fn list_dir_page(
        &self,
        parent: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirEntry>>;

    /// The name `ino` is linked under in directory `parent`, or `None` if it is
    /// not a child of `parent` (M16).
    ///
    /// The inverse of [`lookup`](MetadataStore::lookup), and the bridge for a
    /// surface whose resume cookie is an inode number (NFSv3) onto the name-keyed
    /// pages of [`list_dir_page`](MetadataStore::list_dir_page). If a hard link
    /// gives `ino` several names in the same directory, the lexicographically
    /// first is returned so the mapping is deterministic.
    async fn dentry_name(&self, parent: Ino, ino: Ino) -> Result<Option<String>>;

    /// The directory `ino` is linked into, or `None` for the root (or an inode
    /// with no dentry).
    ///
    /// Lets an *inode-oriented* op walk **up** the tree — which the path API gets
    /// for free from the path itself, but FUSE/NFS handlers do not. `rename`'s
    /// "is the destination inside the thing I'm moving?" check needs it: without
    /// an upward walk the only alternative is scanning the source's whole subtree
    /// downward on every rename. Backed by `idx_dentry_ino`, so it is one indexed
    /// lookup per level.
    async fn parent_of(&self, ino: Ino) -> Result<Option<Ino>>;

    /// Number of entries directly under `parent`.
    async fn child_count(&self, parent: Ino) -> Result<usize>;

    /// Set (or replace) the target of a symlink inode.
    async fn set_symlink(&self, ino: Ino, target: &str) -> Result<()>;

    /// Fetch a symlink target, or `None` if `ino` is not a symlink.
    async fn get_symlink(&self, ino: Ino) -> Result<Option<String>>;

    // --- refs (branches / tags / HEAD) -----------------------------------

    /// Read a ref's value (a commit hex, or a symbolic `ref:<name>`).
    async fn get_ref(&self, name: &str) -> Result<Option<String>>;

    /// Set (upsert) a ref.
    async fn set_ref(&self, name: &str, value: &str) -> Result<()>;

    /// Compare-and-swap a ref: succeed only if its current value equals `expect`
    /// (`None` meaning "must not exist"). Returns whether the swap happened.
    async fn cas_ref(&self, name: &str, expect: Option<&str>, new: &str) -> Result<bool>;

    /// Delete a ref (no-op if absent).
    async fn delete_ref(&self, name: &str) -> Result<()>;

    /// List all refs as `(name, value)` pairs.
    async fn list_refs(&self) -> Result<Vec<(String, String)>>;

    // --- workspace config ------------------------------------------------

    async fn get_config(&self, key: &str) -> Result<Option<String>>;
    async fn set_config(&self, key: &str, value: &str) -> Result<()>;
    /// Atomically increment the integer config counter at `key` (creating it at
    /// `1`) and return the new value. A single statement, so concurrent callers
    /// each get a distinct, strictly increasing value — unlike a read-then-write.
    async fn bump_counter(&self, key: &str) -> Result<i64>;

    // --- workspaces (multi-workspace in one store) -----------------------

    /// Return a handle to this same store bound to `workspace_id`, sharing the
    /// underlying connection/pool. The workspace-scoped ops — inodes, the working
    /// tree, refs, config, conflicts, and locks — then apply to that workspace;
    /// the registry ops below and the shared tables (actors, sessions, blame,
    /// audit, events) are store-wide regardless. A freshly opened store is bound
    /// to the `default` workspace (id 1). See `docs/MULTI_TENANCY.md`.
    fn with_workspace(&self, workspace_id: i64) -> Arc<dyn MetadataStore>;

    /// Create a workspace named `name` with its own fresh root directory inode,
    /// returning `(id, root_ino)`. Errors with `AlreadyExists` if the name exists.
    async fn create_workspace(&self, name: &str) -> Result<(i64, Ino)>;

    /// Resolve a workspace by name to `(id, root_ino)`, or `None` if absent.
    async fn lookup_workspace(&self, name: &str) -> Result<Option<(i64, Ino)>>;

    /// Every workspace as `(id, name, root_ino)`, oldest first.
    async fn list_workspaces(&self) -> Result<Vec<(i64, String, Ino)>>;

    // --- working tree ----------------------------------------------------

    /// Clear the entire working tree (all dentries, symlinks, and inodes except
    /// the root) — used by `checkout` before materializing a commit.
    async fn truncate_tree(&self) -> Result<()>;

    // --- merge: conflicts + locks ----------------------------------------

    /// Record (upsert) an unresolved merge conflict at `path`.
    async fn set_conflict(&self, path: &str, kind: &str) -> Result<()>;

    /// List unresolved conflicts as `(path, kind)`.
    async fn list_conflicts(&self) -> Result<Vec<(String, String)>>;

    /// Clear all recorded conflicts (e.g. once a merge is committed).
    async fn clear_conflicts(&self) -> Result<()>;

    /// Acquire an exclusive lock on `path` for `owner`; `false` if already held.
    async fn acquire_lock(&self, path: &str, owner: &str, at: i64) -> Result<bool>;

    /// Release `owner`'s lock on `path`; `false` if not held by `owner`.
    async fn release_lock(&self, path: &str, owner: &str) -> Result<bool>;

    /// List held locks as `(path, owner, acquired_at)`.
    async fn list_locks(&self) -> Result<Vec<(String, String, i64)>>;

    // --- attribution -----------------------------------------------------

    async fn create_actor(&self, init: ActorInit) -> Result<i64>;
    async fn get_actor(&self, id: i64) -> Result<Option<Actor>>;
    /// Set an actor's write policy (direct vs. propose-only). Actor-agnostic — the
    /// gate is a property of the actor, not their kind.
    async fn set_write_policy(&self, actor_id: i64, policy: WritePolicy) -> Result<()>;
    /// Look up an actor by external identity (`auth_subject`). At most one exists
    /// (a partial UNIQUE index enforces it); returns `None` if unregistered.
    async fn actor_by_subject(&self, subject: &str) -> Result<Option<Actor>>;
    /// Every registered actor, oldest first. Lets a caller resolve the bare
    /// `actor_id` carried by events/suggestions/presence to a name+kind without
    /// having created the actor itself.
    async fn list_actors(&self) -> Result<Vec<Actor>>;
    async fn create_session(
        &self,
        actor_id: i64,
        client: Option<&str>,
        started_at: i64,
    ) -> Result<i64>;
    async fn record_tool_call(&self, tc: ToolCallInit) -> Result<i64>;
    async fn append_edit_op(&self, op: EditOpInit) -> Result<i64>;
    async fn list_edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>>;
    /// Set (upsert) the line-authorship map for a *content version* (a blob's
    /// manifest hash), so blame survives checkout/merge and never desyncs from
    /// the content it describes. Empty content has no blame.
    async fn set_blob_blame(&self, content: &Hash, runs: &str) -> Result<()>;
    /// Fetch the line-authorship map for a content version, if recorded.
    async fn get_blob_blame(&self, content: &Hash) -> Result<Option<String>>;

    // --- collaboration: change feed + presence ---------------------------

    /// Append an event to the change feed, returning its monotonic `seq`.
    async fn append_event(&self, ev: EventInit, ts: i64) -> Result<i64>;
    /// Events strictly after `after_seq`, oldest first, capped at `limit`.
    async fn events_since(&self, after_seq: i64, limit: i64) -> Result<Vec<Event>>;
    /// Upsert a session's presence heartbeat (and current path).
    async fn touch_presence(
        &self,
        session_id: i64,
        actor_id: i64,
        path: Option<&str>,
        at: i64,
    ) -> Result<()>;
    /// Sessions with `last_seen >= since_ts`, most recently seen first.
    async fn active_presence(&self, since_ts: i64) -> Result<Vec<Presence>>;
    /// Delete presence rows with `last_seen < older_than` (keeps the table from
    /// growing without bound — one row accretes per session otherwise). Returns
    /// the number reaped.
    async fn reap_presence(&self, older_than: i64) -> Result<u64>;

    // --- live CRDT documents ---------------------------------------------

    /// Upsert the live-document marker for `path` (see [`LiveDoc`]). `since` is
    /// only set when the row is created, so re-marking an already-live path keeps
    /// the time it first went live.
    ///
    /// `checkpointed_at` is `Some` only when this call *follows a checkpoint* —
    /// then it records when the durable bytes were crystallized. `None` leaves any
    /// previous stamp alone, so merely re-marking a path (a second joiner) never
    /// claims a checkpoint that did not happen.
    async fn set_live_doc(
        &self,
        path: &str,
        session_id: Option<i64>,
        actor_id: i64,
        content_hash: Option<&str>,
        at: i64,
        checkpointed_at: Option<i64>,
    ) -> Result<()>;
    /// The live marker for `path`, if it has one.
    async fn get_live_doc(&self, path: &str) -> Result<Option<LiveDoc>>;
    /// Every live marker in this workspace, ordered by path.
    async fn list_live_docs(&self) -> Result<Vec<LiveDoc>>;
    /// Drop `path`'s live marker (no-op if absent).
    async fn clear_live_doc(&self, path: &str) -> Result<()>;

    // --- agent-suggestion review queue -----------------------------------

    /// Record a new (pending) suggestion, returning its id.
    async fn create_suggestion(&self, init: SuggestionInit, ts: i64) -> Result<i64>;
    /// Fetch a suggestion by id.
    async fn get_suggestion(&self, id: i64) -> Result<Option<Suggestion>>;
    /// Suggestions filtered by `status` and/or `path`, newest first.
    async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        path: Option<&str>,
    ) -> Result<Vec<Suggestion>>;
    /// Transition a *pending* suggestion to `status`, stamping who/when.
    /// Returns `false` if it wasn't pending (already resolved / not found).
    async fn resolve_suggestion(
        &self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool>;
}

/// An in-progress atomic write, returned by [`MetadataStore::begin`].
///
/// It exposes only the write subset a logical filesystem operation needs. Reads
/// (existence checks, `get_inode`) are done on the store *before* `begin`; the
/// store's own constraints — chiefly the unique `(parent, name)` dentry index —
/// together with all-or-nothing rollback ensure a losing race (two creators of
/// the same path) errors and unwinds cleanly instead of orphaning an inode.
///
/// Mutations are staged and become visible only on [`commit`](Self::commit).
/// Dropping without committing rolls the whole transaction back.
#[async_trait]
pub trait MetaTxn: Send {
    /// Allocate a new inode (`nlink` = 1, no content). Returns its number.
    async fn create_inode(&mut self, init: InodeInit) -> Result<Ino>;
    /// Set an inode's content address and size.
    async fn set_content(&mut self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()>;
    /// Compare-and-set an inode's content: apply `content`/`size` only if the
    /// inode's current content still equals `expected` (null-safe), returning
    /// whether it applied. Lets an attributed write be conditional on the file not
    /// having changed since it was read — the atomic apply behind a suggestion
    /// accept (optimistic concurrency; no lost updates).
    async fn set_content_if(
        &mut self,
        ino: Ino,
        expected: Option<&Hash>,
        content: Option<Hash>,
        size: u64,
    ) -> Result<bool>;
    /// Set an inode's link count.
    async fn set_nlink(&mut self, ino: Ino, nlink: i64) -> Result<()>;

    /// Add `delta` to an inode's link count and return the new value.
    ///
    /// Prefer this over reading `nlink` and calling [`set_nlink`](Self::set_nlink)
    /// with an absolute value. The read has to happen before the transaction (the
    /// trait exposes no reads — see the note above), so two concurrent unlinks of
    /// two hard links to one inode both read 2 and both write 1: both names go and
    /// the inode is leaked at `nlink = 1` with nothing pointing at it, forever.
    /// A delta computed by the database cannot lose an update that way.
    ///
    /// Latent today — nothing creates a second link — but it is the kind of thing
    /// that is a silent leak the day a `link` op is added, and the delta form is
    /// simpler than the read it replaces.
    async fn adjust_nlink(&mut self, ino: Ino, delta: i64) -> Result<i64>;

    /// Delete an inode **only if** no dentry names it as a parent, returning
    /// whether it applied. `false` means the directory is not empty.
    ///
    /// `rmdir` cannot ask "is it empty?" and then delete: the answer is read
    /// before the transaction opens, so a `mkdir` landing in between leaves its
    /// dentry parented to an inode that no longer exists — a file that is in the
    /// table and reachable from nothing.
    ///
    /// Implementations must claim the inode's row before testing emptiness, with
    /// the same statement [`add_dentry`](Self::add_dentry) uses, so that the two
    /// operations contend on one row. The conditional delete on its own is not
    /// enough on Postgres; the backend implementations explain why, and
    /// `postgres_rmdir_racing_mkdir_never_orphans_a_dentry` is the check.
    ///
    /// This is application-level integrity. A foreign key from
    /// `dentry.parent_ino` to `inode.ino` would give it for free and in share
    /// mode (so concurrent creates in one directory would stop serializing); the
    /// schema declares no foreign keys at all today, which is why
    /// `PRAGMA foreign_keys=ON` has nothing to enforce.
    async fn delete_inode_if_childless(&mut self, ino: Ino) -> Result<bool>;
    /// Delete an inode (and any symlink row).
    async fn delete_inode(&mut self, ino: Ino) -> Result<()>;
    /// Link `name` in `parent` to `ino`. Errors if the name already exists.
    /// Link `ino` into `parent` under `name`.
    ///
    /// Fails with [`OrigoFSError::NotFound`] if `parent` no longer exists, and —
    /// this is the part that matters — takes the parent's row for update so that
    /// an `rmdir` of it and this insert cannot both succeed. Callers resolve the
    /// parent *before* the transaction, so without that contention a directory
    /// removed in between leaves this dentry pointing at nothing: a row invisible
    /// to `ls`, to `build_tree`, and to the GC mark, exactly like a `rename` into
    /// its own descendant.
    ///
    /// The cost is that concurrent creates in one directory serialize on the
    /// parent row. A foreign key from `dentry.parent_ino` to `inode.ino` would
    /// buy the same guarantee in share mode, and is the better long-term answer;
    /// the schema declares no foreign keys at all today, which is why
    /// `PRAGMA foreign_keys=ON` has nothing to enforce.
    async fn add_dentry(&mut self, parent: Ino, name: &str, ino: Ino) -> Result<()>;
    /// Unlink `name` from `parent` (no-op if absent).
    async fn remove_dentry(&mut self, parent: Ino, name: &str) -> Result<()>;
    /// Set (or replace) a symlink target.
    async fn set_symlink(&mut self, ino: Ino, target: &str) -> Result<()>;
    /// Set (or replace) the line-authorship map for a content version.
    async fn set_blob_blame(&mut self, content: &Hash, runs: &str) -> Result<()>;
    /// Append an op-log entry, returning its id.
    async fn append_edit_op(&mut self, op: EditOpInit) -> Result<i64>;
    /// Delete the whole working tree (every inode except the root, and all
    /// dentries/symlinks) as part of this transaction — so a `truncate` +
    /// rematerialize (checkout/merge/rebuild) commits atomically and a failure or
    /// concurrent reader never sees a half-emptied tree. Blame (keyed by content
    /// hash) is deliberately not cleared.
    async fn truncate_tree(&mut self) -> Result<()>;
    // --- refs, conflicts, config, feed, suggestions -------------------------
    //
    // These exist so a *logical* operation can be one transaction. Without them
    // `commit`, `checkout`, `merge`, and `accept_suggestion` each had to touch the
    // working tree in a transaction and then make several further writes outside
    // it, so an interruption between the parts left the workspace in a state no
    // caller could produce deliberately: a branch advanced onto a tree that was
    // never materialized, conflict markers with no `MERGE_HEAD`, a suggestion
    // still `Pending` after its edit had landed. See `docs/DESIGN.md` §7's
    // "torn multi-step mutations".

    /// Set (or replace) a ref, as part of this transaction.
    async fn set_ref(&mut self, name: &str, value: &str) -> Result<()>;
    /// Compare-and-set a ref (`expect` `None` = "must not exist"), returning
    /// whether it applied. The branch advance every commit/merge is built on.
    async fn cas_ref(&mut self, name: &str, expect: Option<&str>, new: &str) -> Result<bool>;
    /// Delete a ref (no-op if absent).
    async fn delete_ref(&mut self, name: &str) -> Result<()>;
    /// Record a merge conflict at `path`.
    async fn set_conflict(&mut self, path: &str, kind: &str) -> Result<()>;
    /// Drop every recorded conflict.
    async fn clear_conflicts(&mut self) -> Result<()>;
    /// Set a config value (the ref-mirror generation pointer, versioning mode…).
    async fn set_config(&mut self, key: &str, value: &str) -> Result<()>;
    /// Append a change-feed event, returning its sequence number.
    ///
    /// In the transaction so a subscriber cannot observe an event for a mutation
    /// that rolled back, nor miss one that committed — the feed is a log of
    /// *applied* state or it is not trustworthy.
    async fn append_event(&mut self, ev: EventInit, ts: i64) -> Result<i64>;
    /// Transition a suggestion out of `pending`, returning whether it applied
    /// (`false` means someone else resolved it first).
    async fn resolve_suggestion(
        &mut self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool>;

    /// Commit every staged mutation atomically. Consumes the transaction.
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Discard every staged mutation, **awaiting** the rollback. Consumes the
    /// transaction.
    ///
    /// Dropping a transaction also rolls it back, and for most error paths that
    /// is enough. It is *not* enough when the very next thing the caller does
    /// depends on the rollback having finished — the retry loops that drop a
    /// transaction and immediately re-read (`engine.rs`'s create races,
    /// `attribution.rs`'s conditional write) are exactly that shape. `Drop`
    /// cannot await, so the Postgres implementation has to hand the rollback to a
    /// spawned task and let the pinned connection return to the pool only once it
    /// finishes; a caller racing that task can be given the same connection while
    /// its transaction is still open. Rolling back explicitly removes the race
    /// instead of narrowing it.
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Delegating impl so `Arc<dyn MetadataStore>` (and `Arc<ConcreteStore>`) is
/// itself a [`MetadataStore`]. This lets a workspace pick its backend at runtime.
#[async_trait]
impl<T: MetadataStore + ?Sized> MetadataStore for Arc<T> {
    async fn init(&self) -> Result<()> {
        (**self).init().await
    }
    async fn schema_version(&self) -> Result<i64> {
        (**self).schema_version().await
    }
    async fn ping(&self) -> Result<()> {
        (**self).ping().await
    }
    async fn backup_to(&self, dest: &std::path::Path) -> Result<String> {
        (**self).backup_to(dest).await
    }
    async fn begin(&self) -> Result<Box<dyn MetaTxn>> {
        (**self).begin().await
    }
    async fn get_inode(&self, ino: Ino) -> Result<Option<Inode>> {
        (**self).get_inode(ino).await
    }
    async fn get_inodes(&self, inos: &[Ino]) -> Result<Vec<Inode>> {
        (**self).get_inodes(inos).await
    }
    async fn create_inode(&self, init: InodeInit) -> Result<Ino> {
        (**self).create_inode(init).await
    }
    async fn set_content(&self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        (**self).set_content(ino, content, size).await
    }
    async fn set_nlink(&self, ino: Ino, nlink: i64) -> Result<()> {
        (**self).set_nlink(ino, nlink).await
    }
    async fn set_mode(&self, ino: Ino, mode: u32) -> Result<()> {
        (**self).set_mode(ino, mode).await
    }
    async fn set_owner(&self, ino: Ino, uid: Option<u32>, gid: Option<u32>) -> Result<()> {
        (**self).set_owner(ino, uid, gid).await
    }
    async fn delete_inode(&self, ino: Ino) -> Result<()> {
        (**self).delete_inode(ino).await
    }
    async fn lookup(&self, parent: Ino, name: &str) -> Result<Option<Ino>> {
        (**self).lookup(parent, name).await
    }
    async fn add_dentry(&self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
        (**self).add_dentry(parent, name, ino).await
    }
    async fn remove_dentry(&self, parent: Ino, name: &str) -> Result<()> {
        (**self).remove_dentry(parent, name).await
    }
    async fn list_dir(&self, parent: Ino) -> Result<Vec<DirEntry>> {
        (**self).list_dir(parent).await
    }
    async fn list_dir_page(
        &self,
        parent: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirEntry>> {
        (**self).list_dir_page(parent, after_name, limit).await
    }
    async fn dentry_name(&self, parent: Ino, ino: Ino) -> Result<Option<String>> {
        (**self).dentry_name(parent, ino).await
    }
    async fn parent_of(&self, ino: Ino) -> Result<Option<Ino>> {
        (**self).parent_of(ino).await
    }
    async fn child_count(&self, parent: Ino) -> Result<usize> {
        (**self).child_count(parent).await
    }
    async fn set_symlink(&self, ino: Ino, target: &str) -> Result<()> {
        (**self).set_symlink(ino, target).await
    }
    async fn get_symlink(&self, ino: Ino) -> Result<Option<String>> {
        (**self).get_symlink(ino).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<String>> {
        (**self).get_ref(name).await
    }
    async fn set_ref(&self, name: &str, value: &str) -> Result<()> {
        (**self).set_ref(name, value).await
    }
    async fn cas_ref(&self, name: &str, expect: Option<&str>, new: &str) -> Result<bool> {
        (**self).cas_ref(name, expect, new).await
    }
    async fn delete_ref(&self, name: &str) -> Result<()> {
        (**self).delete_ref(name).await
    }
    async fn list_refs(&self) -> Result<Vec<(String, String)>> {
        (**self).list_refs().await
    }
    async fn get_config(&self, key: &str) -> Result<Option<String>> {
        (**self).get_config(key).await
    }
    async fn set_config(&self, key: &str, value: &str) -> Result<()> {
        (**self).set_config(key, value).await
    }
    async fn bump_counter(&self, key: &str) -> Result<i64> {
        (**self).bump_counter(key).await
    }
    fn with_workspace(&self, workspace_id: i64) -> Arc<dyn MetadataStore> {
        (**self).with_workspace(workspace_id)
    }
    async fn create_workspace(&self, name: &str) -> Result<(i64, Ino)> {
        (**self).create_workspace(name).await
    }
    async fn lookup_workspace(&self, name: &str) -> Result<Option<(i64, Ino)>> {
        (**self).lookup_workspace(name).await
    }
    async fn list_workspaces(&self) -> Result<Vec<(i64, String, Ino)>> {
        (**self).list_workspaces().await
    }
    async fn truncate_tree(&self) -> Result<()> {
        (**self).truncate_tree().await
    }
    async fn set_conflict(&self, path: &str, kind: &str) -> Result<()> {
        (**self).set_conflict(path, kind).await
    }
    async fn list_conflicts(&self) -> Result<Vec<(String, String)>> {
        (**self).list_conflicts().await
    }
    async fn clear_conflicts(&self) -> Result<()> {
        (**self).clear_conflicts().await
    }
    async fn acquire_lock(&self, path: &str, owner: &str, at: i64) -> Result<bool> {
        (**self).acquire_lock(path, owner, at).await
    }
    async fn release_lock(&self, path: &str, owner: &str) -> Result<bool> {
        (**self).release_lock(path, owner).await
    }
    async fn list_locks(&self) -> Result<Vec<(String, String, i64)>> {
        (**self).list_locks().await
    }
    async fn create_actor(&self, init: ActorInit) -> Result<i64> {
        (**self).create_actor(init).await
    }
    async fn get_actor(&self, id: i64) -> Result<Option<Actor>> {
        (**self).get_actor(id).await
    }
    async fn set_write_policy(&self, actor_id: i64, policy: WritePolicy) -> Result<()> {
        (**self).set_write_policy(actor_id, policy).await
    }
    async fn actor_by_subject(&self, subject: &str) -> Result<Option<Actor>> {
        (**self).actor_by_subject(subject).await
    }
    async fn list_actors(&self) -> Result<Vec<Actor>> {
        (**self).list_actors().await
    }
    async fn create_session(
        &self,
        actor_id: i64,
        client: Option<&str>,
        started_at: i64,
    ) -> Result<i64> {
        (**self).create_session(actor_id, client, started_at).await
    }
    async fn record_tool_call(&self, tc: ToolCallInit) -> Result<i64> {
        (**self).record_tool_call(tc).await
    }
    async fn append_edit_op(&self, op: EditOpInit) -> Result<i64> {
        (**self).append_edit_op(op).await
    }
    async fn list_edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        (**self).list_edit_ops(actor_id, session_id).await
    }
    async fn set_blob_blame(&self, content: &Hash, runs: &str) -> Result<()> {
        (**self).set_blob_blame(content, runs).await
    }
    async fn get_blob_blame(&self, content: &Hash) -> Result<Option<String>> {
        (**self).get_blob_blame(content).await
    }
    async fn append_event(&self, ev: EventInit, ts: i64) -> Result<i64> {
        (**self).append_event(ev, ts).await
    }
    async fn events_since(&self, after_seq: i64, limit: i64) -> Result<Vec<Event>> {
        (**self).events_since(after_seq, limit).await
    }
    async fn touch_presence(
        &self,
        session_id: i64,
        actor_id: i64,
        path: Option<&str>,
        at: i64,
    ) -> Result<()> {
        (**self)
            .touch_presence(session_id, actor_id, path, at)
            .await
    }
    async fn active_presence(&self, since_ts: i64) -> Result<Vec<Presence>> {
        (**self).active_presence(since_ts).await
    }
    async fn reap_presence(&self, older_than: i64) -> Result<u64> {
        (**self).reap_presence(older_than).await
    }
    async fn set_live_doc(
        &self,
        path: &str,
        session_id: Option<i64>,
        actor_id: i64,
        content_hash: Option<&str>,
        at: i64,
        checkpointed_at: Option<i64>,
    ) -> Result<()> {
        (**self)
            .set_live_doc(
                path,
                session_id,
                actor_id,
                content_hash,
                at,
                checkpointed_at,
            )
            .await
    }
    async fn get_live_doc(&self, path: &str) -> Result<Option<LiveDoc>> {
        (**self).get_live_doc(path).await
    }
    async fn list_live_docs(&self) -> Result<Vec<LiveDoc>> {
        (**self).list_live_docs().await
    }
    async fn clear_live_doc(&self, path: &str) -> Result<()> {
        (**self).clear_live_doc(path).await
    }
    async fn create_suggestion(&self, init: SuggestionInit, ts: i64) -> Result<i64> {
        (**self).create_suggestion(init, ts).await
    }
    async fn get_suggestion(&self, id: i64) -> Result<Option<Suggestion>> {
        (**self).get_suggestion(id).await
    }
    async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        path: Option<&str>,
    ) -> Result<Vec<Suggestion>> {
        (**self).list_suggestions(status, path).await
    }
    async fn resolve_suggestion(
        &self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool> {
        (**self)
            .resolve_suggestion(id, status, resolved_by, ts)
            .await
    }
}
