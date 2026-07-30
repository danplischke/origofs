//! Garbage collection: reclaim content-store objects no commit or live file
//! references (`docs/DESIGN.md` §7 hardening; roadmap M9).
//!
//! origofs writes are content-addressed and never overwritten, so overwriting a
//! file, deleting it, or abandoning a branch leaves its old chunks/manifests
//! behind. GC is a mark-and-sweep: mark everything reachable from the **refs**
//! (every branch + `MERGE_HEAD`, walked through commits → trees → manifests →
//! chunks and symlink blobs) and from the **live working tree** (uncommitted
//! file bodies), then delete every content object that wasn't marked.
//!
//! Audit-only fields such as an edit-op's `pre_hash` are *not* roots: reverts
//! reconstruct from current content + the blame map, so a superseded body's
//! blobs are exactly what GC should reclaim.
//!
//! **Reachability alone is not a safe sweep criterion.** Content is written
//! *before* the metadata that references it — that ordering is the durability
//! barrier — so every write has a window in which its chunks are stored and
//! nothing points at them yet. A sweep that trusts reachability deletes exactly
//! that: the write in flight. Measured on the unfixed code, a writer racing GC
//! failed with `ContentMissing` on content it had itself just stored, and files
//! that had already been committed became permanently unreadable.
//!
//! So the sweep is also **age-gated**: an object is reclaimed only once it has
//! been unreferenced *and* untouched for longer than [`DEFAULT_GC_GRACE_SECS`],
//! which must exceed the longest write-to-commit window. Ages come from the
//! backend itself ([`ContentStore::list_with_age`]); an object the backend cannot
//! date is never swept. A GC **lease** additionally keeps two collections from
//! overlapping, since a sweep is destructive and running it twice at once doubles
//! the exposure for no benefit.
//!
//! **The age gate has a second half, and it is not optional.** `put` is
//! deduplicating: it returns early when the content is already stored, and an
//! object that already exists does not get a fresh timestamp. So the gate as
//! described above protects only *newly written* bytes. A writer that dedups onto
//! an old, currently-unreferenced object — reverting a file, shared boilerplate,
//! checking out an older commit — gets `Ok(hash)` for content this sweep is about
//! to reclaim, and the commit that follows references a hash that no longer
//! exists. Every backend therefore refreshes an object's recency on the dedup path
//! ([`ContentStore::touch`]), gated at
//! [`DEDUP_REFRESH_AFTER_SECS`](crate::content::DEDUP_REFRESH_AFTER_SECS) so the common case
//! stays free. [`Fs::gc_with_grace`] rejects a grace below
//! that floor, because the band between the two is precisely where the race lives.
//!
//! This is not a substitute for running GC when the workspace is calm — it is what
//! makes doing so on a *live, shared* workspace safe, which is the only option in
//! a workspace agents are always writing to.

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::objectgraph::TreeKind;
use crate::suggest::SuggestionStatus;
use crate::types::{FileKind, Hash, Ino};
use async_recursion::async_recursion;
use std::collections::HashSet;

/// How long an object must have been unreferenced before GC will sweep it.
///
/// Must exceed the longest window between a write storing its content and the
/// transaction referencing it committing. Ten minutes is far beyond any single
/// write and still lets a periodic collection reclaim churn promptly; the cost of
/// being generous is only that garbage lingers one extra cycle.
pub const DEFAULT_GC_GRACE_SECS: u64 = 600;

/// Key of the advisory lease that serializes collections. Not a valid path — no
/// user `lock` can collide with it, because `validate_component` rejects the NUL.
const GC_LEASE_KEY: &str = "\0gc-lease";

/// How long a GC lease is honored before another collection may take it. Bounds
/// how long a crashed collector blocks the next one.
const GC_LEASE_SECS: i64 = 3600;

/// What a GC pass reclaimed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Distinct objects kept because they were reachable.
    pub reachable: usize,
    /// Objects deleted because nothing referenced them.
    pub deleted: usize,
    /// Bytes freed by the deletions.
    pub bytes_freed: u64,
    /// Unreferenced objects left alone because they were younger than the grace
    /// period — a write in flight, or recent churn that the next pass will take.
    pub skipped_young: usize,
    /// Unreferenced objects left alone because the backend could not date them.
    /// Non-zero means the store cannot be collected safely.
    pub skipped_undated: usize,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Run a mark-and-sweep collection over the content store, using the default
    /// grace period ([`DEFAULT_GC_GRACE_SECS`]).
    pub async fn gc(&self) -> Result<GcStats> {
        self.gc_with_grace(DEFAULT_GC_GRACE_SECS).await
    }

    /// [`gc`](Self::gc) with an explicit grace period, in seconds.
    ///
    /// Only content that has been unreferenced *and* untouched for at least
    /// `grace_secs` is swept. The grace must exceed the longest window between a
    /// write storing its content and the transaction that references it
    /// committing — see the sweep below for why. `0` restores the old
    /// reachability-only behaviour and is unsafe with any concurrent writer; it
    /// exists for tests and for a genuinely quiesced store.
    ///
    /// Any other value below [`DEDUP_REFRESH_AFTER_SECS`](crate::content::DEDUP_REFRESH_AFTER_SECS)
    /// is **rejected** rather
    /// than honoured. The age gate has two halves: the sweep skips young objects,
    /// and a deduplicating `put` refreshes an object that has gone stale
    /// ([`ContentStore::touch`]). That refresh only
    /// fires past `DEDUP_REFRESH_AFTER_SECS`, so a grace shorter than it leaves a
    /// band where an object is old enough to sweep but not old enough to have been
    /// refreshed — the exact race the gate exists to close. A caller asking for
    /// that band has asked for something that cannot be delivered safely, and
    /// silently widening it would hide the problem; `0` remains available as an
    /// explicit, documented opt-out.
    pub async fn gc_with_grace(&self, grace_secs: u64) -> Result<GcStats> {
        if grace_secs > 0 && grace_secs < crate::content::DEDUP_REFRESH_AFTER_SECS {
            return Err(OrigoFSError::InvalidArgument(format!(
                "gc grace of {grace_secs}s is below the {}s dedup-refresh floor: a sweep with \
                 this grace can reclaim content a concurrent write has just deduplicated onto. \
                 Use at least {}s, or 0 to sweep a quiesced store with no age gate at all.",
                crate::content::DEDUP_REFRESH_AFTER_SECS,
                crate::content::DEDUP_REFRESH_AFTER_SECS,
            )));
        }
        self.gc_with_grace_unchecked(grace_secs).await
    }

    async fn gc_with_grace_unchecked(&self, grace_secs: u64) -> Result<GcStats> {
        // One collection at a time. Nothing guarded this before: two `gc()` calls
        // would interleave a mark against the other's sweep. The lease is the
        // existing advisory-lock table under a key no path can be, and it expires
        // so a collector that died mid-run doesn't block the next one forever.
        let now = self.now_secs();
        let owner = format!("gc-{}", std::process::id());
        if !self.meta.acquire_lock(GC_LEASE_KEY, &owner, now).await? {
            let held = self
                .meta
                .list_locks()
                .await?
                .into_iter()
                .find(|(p, _, _)| p == GC_LEASE_KEY);
            match held {
                // Stale: the previous collector is gone. Take it over.
                Some((_, holder, at)) if now - at > GC_LEASE_SECS => {
                    self.meta.release_lock(GC_LEASE_KEY, &holder).await?;
                    if !self.meta.acquire_lock(GC_LEASE_KEY, &owner, now).await? {
                        return Err(OrigoFSError::Conflict(
                            "another garbage collection is already running".into(),
                        ));
                    }
                }
                _ => {
                    return Err(OrigoFSError::Conflict(
                        "another garbage collection is already running".into(),
                    ));
                }
            }
        }
        let out = self.gc_locked(grace_secs).await;
        // Release even on failure — the lease outliving a failed run would block
        // every retry until it expired.
        let _ = self.meta.release_lock(GC_LEASE_KEY, &owner).await;
        out
    }

    async fn gc_locked(&self, grace_secs: u64) -> Result<GcStats> {
        let mut marked: HashSet<Hash> = HashSet::new();

        // Content is shared across every workspace in the store, so GC marks from
        // ALL of them — a per-workspace sweep would delete another workspace's live
        // content (`docs/MULTI_TENANCY.md`). Refs are workspace-scoped, so mark
        // them through each workspace's handle; the working tree is walked from
        // each workspace's own root inode (dentry/inode reads are keyed by ino, so
        // the default handle traverses any root correctly).
        // Every root below is workspace-scoped (refs, working tree, pending
        // suggestions, and the ref-mirror pointer all live in per-workspace
        // rows/config), so each must be marked through *its own* workspace handle.
        // Marking any of them for only the calling workspace would sweep another
        // workspace's live/recoverable content out of the shared store.
        for (id, _name, root_ino) in self.meta.list_workspaces().await? {
            let ws = self.meta.with_workspace(id);
            // Root 1: every ref. Branch refs and MERGE_HEAD hold commit hashes;
            // the symbolic HEAD ("ref:<branch>") isn't a hash and is skipped.
            for (_name, value) in ws.list_refs().await? {
                if let Some(commit) = Hash::from_hex(&value) {
                    self.mark_commit(commit, &mut marked).await?;
                }
            }
            // Root 2: the live working tree (uncommitted bodies aren't committed).
            self.mark_working(root_ino, &mut marked).await?;
            // Root 3: pending suggestions. A proposed body lives only in the CAS
            // until the suggestion is accepted — referenced by no ref and no working
            // file — so without this root a GC pass would reclaim it and a later
            // `accept_suggestion`/`suggestion_diff` would fail with `ContentMissing`.
            for s in ws
                .list_suggestions(Some(SuggestionStatus::Pending), None)
                .await?
            {
                for hex in [s.base_hash.as_deref(), s.proposed_hash.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if let Some(h) = Hash::from_hex(hex) {
                        self.mark_manifest(h, &mut marked).await?;
                    }
                }
            }
            // Root 4: this workspace's live ref-mirror snapshot (recovery aid; see
            // `mirror_refs`). Only the current one is kept — superseded snapshots are
            // unreferenced and get reclaimed, so mirrors never accumulate.
            if let Some(h) = Self::mirror_hash_of(ws.as_ref()).await? {
                marked.insert(h);
            }
        }

        // Sweep: delete what is unmarked *and* old enough to be safe.
        //
        // Reachability alone is not a safe criterion. Content is written before
        // the metadata that references it — that ordering is the durability
        // barrier — so every write has a window in which its chunks are stored
        // and nothing points at them yet. Sweeping on reachability deletes
        // exactly that: the write in flight. Measured on the unfixed code, a
        // writer racing GC failed with `ContentMissing` on content it had just
        // stored, and files that had been committed became permanently
        // unreadable.
        //
        // So an object is only swept once it has been unreferenced for longer
        // than any single write could plausibly take. An object whose age the
        // backend cannot report is never swept.
        let mut stats = GcStats {
            reachable: marked.len(),
            ..Default::default()
        };
        for (hash, age) in self.content.list_with_age().await? {
            if marked.contains(&hash) {
                continue;
            }
            match age {
                Some(secs) if secs >= grace_secs => {
                    stats.bytes_freed += self.content.delete(&hash).await?;
                    stats.deleted += 1;
                }
                Some(_) => stats.skipped_young += 1,
                None => stats.skipped_undated += 1,
            }
        }
        Ok(stats)
    }

    #[async_recursion]
    async fn mark_commit(&self, hash: Hash, marked: &mut HashSet<Hash>) -> Result<()> {
        if !marked.insert(hash) {
            return Ok(());
        }
        let commit = self.commit_object(&hash).await?;
        self.mark_tree(commit.tree, marked).await?;
        for parent in commit.parents {
            self.mark_commit(parent, marked).await?;
        }
        Ok(())
    }

    #[async_recursion]
    async fn mark_tree(&self, hash: Hash, marked: &mut HashSet<Hash>) -> Result<()> {
        if !marked.insert(hash) {
            return Ok(());
        }
        let tree = self.tree_object(&hash).await?;
        for e in tree.entries {
            match e.kind {
                TreeKind::Dir => self.mark_tree(e.hash, marked).await?,
                TreeKind::File => self.mark_manifest(e.hash, marked).await?,
                TreeKind::Symlink => {
                    marked.insert(e.hash); // symlink-target blob
                }
            }
        }
        Ok(())
    }

    /// Mark a blob manifest and every chunk it references.
    async fn mark_manifest(&self, manifest_hash: Hash, marked: &mut HashSet<Hash>) -> Result<()> {
        if !marked.insert(manifest_hash) {
            return Ok(());
        }
        let manifest = self.load_manifest(&manifest_hash).await?;
        for c in manifest.chunks {
            marked.insert(c.hash);
        }
        Ok(())
    }

    #[async_recursion]
    async fn mark_working(&self, dir_ino: Ino, marked: &mut HashSet<Hash>) -> Result<()> {
        for de in self.meta.list_dir(dir_ino).await? {
            match de.kind {
                FileKind::Dir => self.mark_working(de.ino, marked).await?,
                FileKind::File => {
                    if let Some(inode) = self.meta.get_inode(de.ino).await?
                        && let Some(mhash) = inode.content
                    {
                        self.mark_manifest(mhash, marked).await?;
                    }
                }
                // Working-tree symlink targets live in the metadata store, not
                // the content store, so they hold no content roots.
                FileKind::Symlink => {}
            }
        }
        Ok(())
    }
}
