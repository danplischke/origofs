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
//! GC assumes a quiescent store — it is not safe to run concurrently with
//! writers, since a freshly `put` chunk is briefly unreferenced. Run it when the
//! workspace is idle (a generational grace period is future work).

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::objectgraph::TreeKind;
use crate::suggest::SuggestionStatus;
use crate::types::{FileKind, Hash, Ino};
use async_recursion::async_recursion;
use std::collections::HashSet;

/// What a GC pass reclaimed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Distinct objects kept because they were reachable.
    pub reachable: usize,
    /// Objects deleted because nothing referenced them.
    pub deleted: usize,
    /// Bytes freed by the deletions.
    pub bytes_freed: u64,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Run a mark-and-sweep collection over the content store.
    pub async fn gc(&self) -> Result<GcStats> {
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

        // Sweep: delete everything not marked.
        let mut stats = GcStats {
            reachable: marked.len(),
            ..Default::default()
        };
        for hash in self.content.list().await? {
            if !marked.contains(&hash) {
                stats.bytes_freed += self.content.delete(&hash).await?;
                stats.deleted += 1;
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
