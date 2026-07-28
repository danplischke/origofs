//! Three-way merge (`docs/DESIGN.md` §4c): merge-base, fast-forward, per-path
//! tree reconciliation, diff3 text merge, chunk-granular binary merge, and
//! conflict handling.
//!
//! Clean merges produce a two-parent merge commit directly. Conflicting merges
//! leave the working tree with the conflicting content (text: `<<<<<<<` markers;
//! binary: `ours` kept plus a `<name>.theirs` sibling), record the conflicts, and
//! set `MERGE_HEAD` — the next `commit` picks up the second parent and clears the
//! merge state.

use crate::chunk::Manifest;
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
    pub async fn merge_base(&self, a: Hash, b: Hash) -> Result<Option<Hash>> {
        // Min distance from `a` to each of its ancestors.
        let mut depth: HashMap<Hash, u32> = HashMap::new();
        let mut frontier = vec![(a, 0u32)];
        while let Some((h, d)) = frontier.pop() {
            if depth.get(&h).is_some_and(|&e| e <= d) {
                continue;
            }
            depth.insert(h, d);
            for p in self.commit_at(&h).await?.parents {
                frontier.push((p, d + 1));
            }
        }
        // Among common ancestors, the one closest to `a` (greatest depth) is the base.
        Ok(self
            .ancestors(b)
            .await?
            .into_iter()
            .filter_map(|h| depth.get(&h).map(|&d| (d, h)))
            .max_by_key(|(d, _)| *d)
            .map(|(_, h)| h))
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
    pub async fn merge(&self, theirs: Hash, author: &str, message: &str) -> Result<MergeOutcome> {
        self.ensure_commits_enabled().await?;
        let branch = self.current_branch().await?.ok_or_else(|| {
            OrigoFSError::InvalidArgument("cannot merge with a detached HEAD".into())
        })?;
        let ours = self.head_commit().await?.ok_or_else(|| {
            OrigoFSError::InvalidArgument("cannot merge before the first commit".into())
        })?;

        if ours == theirs || self.is_ancestor(theirs, ours).await? {
            return Ok(MergeOutcome::AlreadyUpToDate);
        }
        if self.is_ancestor(ours, theirs).await? {
            let theirs_commit = self.commit_at(&theirs).await?;
            // Advance the ref FIRST (checked): if the branch moved concurrently
            // the CAS fails and we abort *before* clobbering the working tree,
            // rather than silently overwriting it and reporting success.
            let swapped = self
                .meta
                .cas_ref(&branch, Some(&ours.to_hex()), &theirs.to_hex())
                .await?;
            if !swapped {
                return Err(OrigoFSError::Conflict(format!(
                    "branch {branch} moved concurrently; retry the merge"
                )));
            }
            self.replace_working_tree(theirs_commit.tree).await?;
            self.mirror_refs().await?;
            return Ok(MergeOutcome::FastForward(theirs));
        }

        let base = self.merge_base(ours, theirs).await?;
        let base_tree = match base {
            Some(b) => Some(self.commit_at(&b).await?.tree),
            None => None,
        };
        let ours_tree = self.commit_at(&ours).await?.tree;
        let theirs_tree = self.commit_at(&theirs).await?.tree;

        let mut conflicts = Vec::new();
        let merged_tree = self
            .merge_trees(
                base_tree,
                Some(ours_tree),
                Some(theirs_tree),
                "",
                &mut conflicts,
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
            // Advance the ref FIRST (checked), then reflect it in the working
            // tree — so a concurrent branch move aborts the merge before we
            // overwrite the working tree or drop `theirs` from history.
            let swapped = self
                .meta
                .cas_ref(&branch, Some(&ours.to_hex()), &commit_hash.to_hex())
                .await?;
            if !swapped {
                return Err(OrigoFSError::Conflict(format!(
                    "branch {branch} moved concurrently; retry the merge"
                )));
            }
            self.replace_working_tree(merged_tree).await?;
            self.mirror_refs().await?;
            Ok(MergeOutcome::Merged(commit_hash))
        } else {
            // Conflicts: reflect the merge (with markers) and record MERGE_HEAD;
            // the ref intentionally does NOT advance until the user commits.
            self.replace_working_tree(merged_tree).await?;
            self.meta.clear_conflicts().await?;
            for c in &conflicts {
                self.meta.set_conflict(&c.path, &c.kind).await?;
            }
            self.meta.set_ref(MERGE_HEAD, &theirs.to_hex()).await?;
            self.mirror_refs().await?;
            Ok(MergeOutcome::Conflicts(conflicts))
        }
    }

    /// Three-way merge of directory trees; returns the merged tree hash and
    /// accumulates conflicts.
    #[async_recursion]
    async fn merge_trees(
        &self,
        base: Option<Hash>,
        ours: Option<Hash>,
        theirs: Option<Hash>,
        prefix: &str,
        conflicts: &mut Vec<Conflict>,
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
                    if b == Some(oe) {
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
                    if b == Some(te) {
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
                    if oe == te {
                        merged.push(oe.clone());
                    } else if b == Some(oe) {
                        merged.push(te.clone());
                    } else if b == Some(te) {
                        merged.push(oe.clone());
                    } else if oe.kind == TreeKind::Dir && te.kind == TreeKind::Dir {
                        let base_sub = b.filter(|e| e.kind == TreeKind::Dir).map(|e| e.hash);
                        let sub = self
                            .merge_trees(base_sub, Some(oe.hash), Some(te.hash), &path, conflicts)
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
