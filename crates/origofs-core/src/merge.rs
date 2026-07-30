//! Three-way merge (`docs/DESIGN.md` §4c): merge-base, fast-forward, per-path
//! tree reconciliation, diff3 text merge, chunk-granular binary merge, and
//! conflict handling.
//!
//! Clean merges produce a two-parent merge commit directly. Conflicting merges
//! leave the working tree with the conflicting content (text: `<<<<<<<` markers;
//! binary: `ours` kept plus a `<name>.theirs` sibling), record the conflicts, and
//! set `MERGE_HEAD` — the next `commit` picks up the second parent and clears the
//! merge state.
//!
//! # Live co-editing documents
//!
//! A path with an open live CRDT document ([`LiveDoc`], roadmap M8) has a durable
//! blob that is a *checkpoint*: real, attributed, and possibly behind what people
//! are typing into the `Y.Doc` right now. A three-way merge that touches such a
//! path is therefore merging bytes that may lag. [`Fs::merge_live`] reports those
//! paths alongside the outcome, and [`Fs::merge`] logs them.

use crate::chunk::Manifest;
use crate::collab::LiveDoc;
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::objectgraph::{Commit, Tree, TreeEntry, TreeKind};
use crate::types::Hash;
use async_recursion::async_recursion;
use std::collections::{BTreeSet, HashMap, HashSet};

const MERGE_HEAD: &str = "MERGE_HEAD";

/// A single unresolved conflict from a merge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    pub path: String,
    pub kind: String,
}

/// The result of a merge.
#[derive(Clone, Debug)]
pub enum MergeOutcome {
    /// `theirs` is already reachable from HEAD; nothing to do.
    AlreadyUpToDate,
    /// HEAD was an ancestor of `theirs`; the branch advanced with no merge commit.
    FastForward(Hash),
    /// A clean merge commit (two parents).
    Merged(Hash),
    /// Conflicts remain in the working tree; resolve them, then `commit`.
    Conflicts(Vec<Conflict>),
}

struct FileMerge {
    hash: Hash,
    conflict: bool,
    /// For a binary conflict: theirs manifest, materialized as `<name>.theirs`.
    theirs_sibling: Option<Hash>,
}

fn entry_map(tree: &Tree) -> HashMap<&str, &TreeEntry> {
    tree.entries.iter().map(|e| (e.name.as_str(), e)).collect()
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    async fn commit_at(&self, h: &Hash) -> Result<Commit> {
        Commit::decode(&self.content.get(h).await?)
    }

    async fn load_tree_opt(&self, hash: Option<Hash>) -> Result<Tree> {
        match hash {
            Some(h) => Tree::decode(&self.content.get(&h).await?),
            None => Ok(Tree::default()),
        }
    }

    /// All commits reachable from `start` (inclusive).
    async fn ancestors(&self, start: Hash) -> Result<HashSet<Hash>> {
        let mut seen = HashSet::new();
        let mut stack = vec![start];
        while let Some(h) = stack.pop() {
            if !seen.insert(h) {
                continue;
            }
            for p in self.commit_at(&h).await?.parents {
                stack.push(p);
            }
        }
        Ok(seen)
    }

    /// Whether `ancestor` is reachable from `descendant` (inclusive).
    pub async fn is_ancestor(&self, ancestor: Hash, descendant: Hash) -> Result<bool> {
        Ok(self.ancestors(descendant).await?.contains(&ancestor))
    }

    /// The best common ancestor (merge base) of `a` and `b`, if any.
    ///
    /// A merge base is a common ancestor that is **not** itself an ancestor of any
    /// *other* common ancestor — i.e. maximal in the ancestry order, which is the
    /// fork point. Ranking common ancestors by hop distance alone is not enough:
    /// the root commit is a common ancestor of every pair, so on any history with
    /// more than one commit of shared trunk a distance-only rule can base the merge
    /// on the root and diff3 against pre-fork content — producing spurious conflicts
    /// and "clean" merges that resurrect lines deleted before the fork.
    ///
    /// A criss-cross history can leave several maximal candidates. We do not build a
    /// virtual recursive base (git's `recursive`/`ort` strategy); we take the
    /// candidate nearest `a`, breaking ties by hash so that the same pair of commits
    /// always merges the same way.
    pub async fn merge_base(&self, a: Hash, b: Hash) -> Result<Option<Hash>> {
        // Min hop distance from `a`, plus the parent edges — recorded here so the
        // candidate walk below needs no further object loads.
        let mut depth: HashMap<Hash, u32> = HashMap::new();
        let mut parents_of: HashMap<Hash, Vec<Hash>> = HashMap::new();
        let mut frontier = vec![(a, 0u32)];
        while let Some((h, d)) = frontier.pop() {
            if depth.get(&h).is_some_and(|&e| e <= d) {
                continue;
            }
            depth.insert(h, d);
            let parents = match parents_of.get(&h) {
                Some(p) => p.clone(),
                None => {
                    let p = self.commit_at(&h).await?.parents;
                    parents_of.insert(h, p.clone());
                    p
                }
            };
            for p in parents {
                frontier.push((p, d + 1));
            }
        }

        // Common ancestors: reachable from `b` and from `a` (hence in `depth`).
        let common: HashSet<Hash> = self
            .ancestors(b)
            .await?
            .into_iter()
            .filter(|h| depth.contains_key(h))
            .collect();

        // Every *proper* ancestor of a common ancestor is itself common, and is
        // superseded by it — so the merge bases are the common ancestors that no
        // other common ancestor reaches. Each edge here is already in `parents_of`,
        // because every common ancestor is an ancestor of `a`.
        let mut superseded: HashSet<Hash> = HashSet::new();
        let mut stack: Vec<Hash> = common
            .iter()
            .flat_map(|h| parents_of.get(h).cloned().unwrap_or_default())
            .collect();
        while let Some(h) = stack.pop() {
            if !superseded.insert(h) {
                continue;
            }
            stack.extend(parents_of.get(&h).cloned().unwrap_or_default());
        }

        Ok(common
            .into_iter()
            .filter(|h| !superseded.contains(h))
            .min_by_key(|h| (depth[h], *h.as_bytes())))
    }

    // --- file bodies ------------------------------------------------------

    pub(crate) async fn read_body(&self, mhash: &Hash) -> Result<Vec<u8>> {
        let manifest = self.load_manifest(mhash).await?;
        let mut buf = Vec::with_capacity(manifest.size as usize);
        for c in &manifest.chunks {
            buf.extend_from_slice(&self.content.get(&c.hash).await?);
        }
        Ok(buf)
    }

    async fn write_body(&self, data: &[u8]) -> Result<Hash> {
        match self.store_body(data).await? {
            (Some(h), _) => Ok(h),
            (None, _) => self.content.put(&Manifest::default().encode()).await,
        }
    }

    // --- merge ------------------------------------------------------------

    /// Merge commit `theirs` into the current branch.
    ///
    /// Identical to [`merge_live`](Self::merge_live), discarding its live-document
    /// warnings after logging them — see there for what those are and why they are
    /// a warning rather than a conflict.
    pub async fn merge(&self, theirs: Hash, author: &str, message: &str) -> Result<MergeOutcome> {
        let (outcome, stale) = self.merge_live(theirs, author, message).await?;
        for doc in &stale {
            tracing::warn!(
                path = %doc.path,
                "merged a path with an open live co-editing document; its durable bytes \
                 may lag the Y.Doc — checkpoint the co-editing room and re-check"
            );
        }
        Ok(outcome)
    }

    /// [`merge`](Self::merge), also reporting every merged path that had an **open
    /// live CRDT document** at merge time (`docs/DESIGN.md` §7 / roadmap M8).
    ///
    /// While a path is live its durable blob is a checkpoint that may be behind the
    /// `Y.Doc` collaborators are typing into ([`LiveDoc`]), so a three-way merge
    /// over it merges bytes that may lag. This surfaces those paths so a caller —
    /// [`crate::resync`], a UI, a release build — can say so, or checkpoint the
    /// co-editing room and merge again.
    ///
    /// **It is a warning, not a conflict, and never blocks the merge.** That is the
    /// same rule [`read_live`](Self::read_live) documents, for the same reasons: the
    /// engine cannot force a checkpoint (the `Y.Doc` is in-process state owned by a
    /// co-editing room, possibly in another worker), the durable bytes are a real
    /// attributed state rather than garbage, and recording a conflict would set
    /// `MERGE_HEAD` and demand manual resolution of a file on which nothing actually
    /// conflicts — surprising, and it would make merging impossible for as long as
    /// anyone keeps an editor open. Reporting the fact leaves the choice with the
    /// caller.
    ///
    /// Works with the `coedit` feature off: nothing sets the marker then, so the
    /// reported list is simply always empty.
    pub async fn merge_live(
        &self,
        theirs: Hash,
        author: &str,
        message: &str,
    ) -> Result<(MergeOutcome, Vec<LiveDoc>)> {
        self.ensure_commits_enabled().await?;
        let branch = self.current_branch().await?.ok_or_else(|| {
            OrigoFSError::InvalidArgument("cannot merge with a detached HEAD".into())
        })?;
        let ours = self.head_commit().await?.ok_or_else(|| {
            OrigoFSError::InvalidArgument("cannot merge before the first commit".into())
        })?;

        if ours == theirs || self.is_ancestor(theirs, ours).await? {
            return Ok((MergeOutcome::AlreadyUpToDate, Vec::new()));
        }

        // Live co-editing markers, keyed by path. One query, and usually empty —
        // the whole live-document check below costs nothing when nobody is
        // co-editing (and always, with the `coedit` feature off).
        let live: HashMap<String, LiveDoc> = self
            .live_paths()
            .await?
            .into_iter()
            .map(|d| (d.path.clone(), d))
            .collect();

        if self.is_ancestor(ours, theirs).await? {
            let theirs_commit = self.commit_at(&theirs).await?;
            // A fast-forward doesn't merge bytes, it materializes theirs wholesale
            // — which still overwrites a live path. Report the ones it changes.
            let stale = self.live_changed_between(ours, theirs, &live).await?;
            // The checked ref advance and the tree it describes commit together.
            // The CAS still comes first *within* the transaction, so a branch that
            // moved concurrently aborts before any of the tree work is staged;
            // committing them separately meant a crash in between advanced the
            // branch while the working tree still held the old content, and the
            // next commit would snapshot that stale tree on top — silently
            // reverting the merge.
            let mut txn = self.meta.begin().await?;
            if !txn
                .cas_ref(&branch, Some(&ours.to_hex()), &theirs.to_hex())
                .await?
            {
                return Err(OrigoFSError::Conflict(format!(
                    "branch {branch} moved concurrently; retry the merge"
                )));
            }
            self.replace_working_tree_in(&mut *txn, theirs_commit.tree)
                .await?;
            txn.commit().await?;
            self.mirror_refs().await?;
            return Ok((MergeOutcome::FastForward(theirs), stale));
        }

        let base = self.merge_base(ours, theirs).await?;
        let base_tree = match base {
            Some(b) => Some(self.commit_at(&b).await?.tree),
            None => None,
        };
        let ours_tree = self.commit_at(&ours).await?.tree;
        let theirs_tree = self.commit_at(&theirs).await?.tree;

        let mut conflicts = Vec::new();
        let mut stale = Vec::new();
        let merged_tree = self
            .merge_trees(
                base_tree,
                Some(ours_tree),
                Some(theirs_tree),
                "",
                &mut conflicts,
                &live,
                &mut stale,
            )
            .await?;

        if conflicts.is_empty() {
            let commit = Commit {
                tree: merged_tree,
                parents: vec![ours, theirs],
                author: author.to_string(),
                message: message.to_string(),
                timestamp: self.now_secs(),
            };
            let commit_hash = self.content.put(&commit.encode()).await?;
            // Ref advance + working tree in one transaction. The CAS is still
            // first, so a concurrent branch move aborts before anything is
            // staged; and because they now commit together, a crash can no longer
            // leave the branch advanced onto a tree that was never materialized.
            let mut txn = self.meta.begin().await?;
            if !txn
                .cas_ref(&branch, Some(&ours.to_hex()), &commit_hash.to_hex())
                .await?
            {
                return Err(OrigoFSError::Conflict(format!(
                    "branch {branch} moved concurrently; retry the merge"
                )));
            }
            self.replace_working_tree_in(&mut *txn, merged_tree).await?;
            txn.commit().await?;
            self.mirror_refs().await?;
            Ok((MergeOutcome::Merged(commit_hash), stale))
        } else {
            // Conflicts: reflect the merge (with markers) and record MERGE_HEAD;
            // the ref intentionally does NOT advance until the user commits.
            //
            // All of it in one transaction. This was the worst of the torn states:
            // five separate writes, so a crash before `MERGE_HEAD` landed left a
            // working tree full of conflict markers with *no record that a merge
            // was in progress* — and the next commit then produced a single-parent
            // commit containing the markers, dropping `theirs` from history
            // entirely. A concurrent reader could also catch the window between
            // `clear_conflicts` and the re-inserts and see zero conflicts over a
            // marker-laden tree.
            let mut txn = self.meta.begin().await?;
            self.replace_working_tree_in(&mut *txn, merged_tree).await?;
            txn.clear_conflicts().await?;
            for c in &conflicts {
                txn.set_conflict(&c.path, &c.kind).await?;
            }
            txn.set_ref(MERGE_HEAD, &theirs.to_hex()).await?;
            txn.commit().await?;
            self.mirror_refs().await?;
            Ok((MergeOutcome::Conflicts(conflicts), stale))
        }
    }

    /// The live documents among `live` whose content differs between commits `a`
    /// and `b` — the fast-forward path's version of "this merge touched a path
    /// whose durable bytes may lag". Short-circuits when nothing is live.
    async fn live_changed_between(
        &self,
        a: Hash,
        b: Hash,
        live: &HashMap<String, LiveDoc>,
    ) -> Result<Vec<LiveDoc>> {
        if live.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .diff(&a.to_hex(), &b.to_hex())
            .await?
            .into_iter()
            .filter_map(|d| live.get(&d.path).cloned())
            .collect())
    }

    /// Three-way merge of directory trees; returns the merged tree hash and
    /// accumulates conflicts (and any live-document warnings).
    #[async_recursion]
    #[allow(clippy::too_many_arguments)]
    async fn merge_trees(
        &self,
        base: Option<Hash>,
        ours: Option<Hash>,
        theirs: Option<Hash>,
        prefix: &str,
        conflicts: &mut Vec<Conflict>,
        live: &HashMap<String, LiveDoc>,
        stale: &mut Vec<LiveDoc>,
    ) -> Result<Hash> {
        let bt = self.load_tree_opt(base).await?;
        let ot = self.load_tree_opt(ours).await?;
        let tt = self.load_tree_opt(theirs).await?;
        let bmap = entry_map(&bt);
        let omap = entry_map(&ot);
        let tmap = entry_map(&tt);

        let mut names: BTreeSet<String> = BTreeSet::new();
        for e in ot.entries.iter().chain(tt.entries.iter()) {
            names.insert(e.name.clone());
        }

        let mut merged: Vec<TreeEntry> = Vec::new();
        for name in &names {
            let n = name.as_str();
            let b = bmap.get(n).copied();
            let o = omap.get(n).copied();
            let t = tmap.get(n).copied();
            let path = format!("{prefix}/{name}");
            match (o, t) {
                (None, None) => {}
                (Some(oe), None) => {
                    if b.is_none() {
                        // Absent from the base and from theirs: *we added it*.
                        // Nothing to reconcile — keep it. (Without this case, two
                        // sides adding different files — the ordinary offline
                        // divergence — would conflict on every added path.)
                        merged.push(oe.clone());
                    } else if b == Some(oe) {
                        // ours unchanged, theirs deleted -> delete
                    } else {
                        merged.push(oe.clone());
                        conflicts.push(Conflict {
                            path,
                            kind: "modify/delete".into(),
                        });
                    }
                }
                (None, Some(te)) => {
                    if b.is_none() {
                        // They added it and we never had it: take it.
                        merged.push(te.clone());
                    } else if b == Some(te) {
                        // theirs unchanged, ours deleted -> delete
                    } else {
                        merged.push(te.clone());
                        conflicts.push(Conflict {
                            path,
                            kind: "delete/modify".into(),
                        });
                    }
                }
                (Some(oe), Some(te)) => {
                    // The two sides disagree about this path, so whatever we pick
                    // replaces its bytes. If a live CRDT document is open on it,
                    // the bytes we are reasoning about may already be stale — say
                    // so (advisory; it never blocks the merge).
                    if oe != te
                        && let Some(doc) = live.get(&path)
                    {
                        stale.push(doc.clone());
                    }
                    if oe == te {
                        merged.push(oe.clone());
                    } else if b == Some(oe) {
                        merged.push(te.clone());
                    } else if b == Some(te) {
                        merged.push(oe.clone());
                    } else if oe.kind == TreeKind::Dir && te.kind == TreeKind::Dir {
                        let base_sub = b.filter(|e| e.kind == TreeKind::Dir).map(|e| e.hash);
                        let sub = self
                            .merge_trees(
                                base_sub,
                                Some(oe.hash),
                                Some(te.hash),
                                &path,
                                conflicts,
                                live,
                                stale,
                            )
                            .await?;
                        merged.push(TreeEntry {
                            name: name.clone(),
                            mode: oe.mode,
                            kind: TreeKind::Dir,
                            hash: sub,
                        });
                    } else if oe.kind == TreeKind::File && te.kind == TreeKind::File {
                        let base_h = b.filter(|e| e.kind == TreeKind::File).map(|e| e.hash);
                        let fm = self.merge_file(oe.hash, te.hash, base_h).await?;
                        merged.push(TreeEntry {
                            name: name.clone(),
                            mode: oe.mode,
                            kind: TreeKind::File,
                            hash: fm.hash,
                        });
                        if fm.conflict {
                            conflicts.push(Conflict {
                                path: path.clone(),
                                kind: "content".into(),
                            });
                        }
                        if let Some(sib) = fm.theirs_sibling {
                            merged.push(TreeEntry {
                                name: format!("{name}.theirs"),
                                mode: te.mode,
                                kind: TreeKind::File,
                                hash: sib,
                            });
                        }
                    } else if oe.kind == TreeKind::Symlink && te.kind == TreeKind::Symlink {
                        merged.push(oe.clone());
                        conflicts.push(Conflict {
                            path,
                            kind: "symlink".into(),
                        });
                    } else {
                        merged.push(oe.clone());
                        conflicts.push(Conflict {
                            path,
                            kind: "type".into(),
                        });
                    }
                }
            }
        }
        merged.sort_by(|a, b| a.name.cmp(&b.name));
        self.content.put(&Tree { entries: merged }.encode()).await
    }

    /// Three-way merge of a single file. Text uses line-level diff3; binary uses
    /// a chunk-granular merge on the manifest's chunk sequence.
    async fn merge_file(&self, ours: Hash, theirs: Hash, base: Option<Hash>) -> Result<FileMerge> {
        let ours_b = self.read_body(&ours).await?;
        let theirs_b = self.read_body(&theirs).await?;
        let base_b = match base {
            Some(h) => self.read_body(&h).await?,
            None => Vec::new(),
        };

        let text = std::str::from_utf8(&ours_b).is_ok()
            && std::str::from_utf8(&theirs_b).is_ok()
            && std::str::from_utf8(&base_b).is_ok();

        if text {
            let base_s = std::str::from_utf8(&base_b).unwrap();
            let ours_s = std::str::from_utf8(&ours_b).unwrap();
            let theirs_s = std::str::from_utf8(&theirs_b).unwrap();
            let (body, conflict) = match diffy::merge(base_s, ours_s, theirs_s) {
                Ok(merged) => (merged, false),
                Err(conflicted) => (conflicted, true),
            };
            return Ok(FileMerge {
                hash: self.write_body(body.as_bytes()).await?,
                conflict,
                theirs_sibling: None,
            });
        }

        // Binary: content is addressed by hash, so equality is a 32-byte compare.
        // We do NOT diff3 the chunk-hash sequence — that line-merges hash-lines
        // and silently corrupts binaries with repeated chunks (padding/sparse),
        // producing a self-consistent but wrong manifest with `conflict=false`.
        // Only the trivially-clean cases auto-resolve; any real divergence is a
        // conflict (keep ours, surface theirs as a `.theirs` sibling).
        if ours == theirs {
            return Ok(FileMerge {
                hash: ours,
                conflict: false,
                theirs_sibling: None,
            });
        }
        if base == Some(ours) {
            // ours is unchanged since base → take theirs.
            return Ok(FileMerge {
                hash: theirs,
                conflict: false,
                theirs_sibling: None,
            });
        }
        if base == Some(theirs) {
            // theirs is unchanged since base → keep ours.
            return Ok(FileMerge {
                hash: ours,
                conflict: false,
                theirs_sibling: None,
            });
        }
        // Both sides diverged from base (or no common base): a real conflict.
        Ok(FileMerge {
            hash: ours,
            conflict: true,
            theirs_sibling: Some(theirs),
        })
    }

    // --- locks (git-LFS-style) -------------------------------------------

    /// Acquire an exclusive lock on `path` for `owner`; `false` if already held.
    pub async fn lock(&self, path: &str, owner: &str) -> Result<bool> {
        self.meta.acquire_lock(path, owner, self.now_secs()).await
    }

    /// Release `owner`'s lock on `path`.
    pub async fn unlock(&self, path: &str, owner: &str) -> Result<bool> {
        self.meta.release_lock(path, owner).await
    }

    /// List held locks as `(path, owner, acquired_at)`.
    pub async fn locks(&self) -> Result<Vec<(String, String, i64)>> {
        self.meta.list_locks().await
    }

    /// List unresolved merge conflicts as `(path, kind)`.
    pub async fn conflicts(&self) -> Result<Vec<(String, String)>> {
        self.meta.list_conflicts().await
    }
}
