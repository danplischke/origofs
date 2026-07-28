//! The versioning engine (`docs/DESIGN.md` §4c): commits, branches, checkout,
//! log, and status/diff, layered on the working-tree engine.
//!
//! The git-style object graph ([`crate::objectgraph`]) is the source of truth for
//! committed state; the inode/dentry working tree is a mutable view. `commit`
//! snapshots the working tree into trees + a commit; `checkout` materializes a
//! commit back into the working tree. Versioning is opt-in via the workspace's
//! `versioning` config (`off` disables commits entirely).

use crate::chunk::Manifest;
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::{MetaTxn, MetadataStore};
use crate::objectgraph::{
    Commit, CommitInfo, DiffEntry, DiffStatus, RefSnapshot, Tree, TreeEntry, TreeKind,
    VersioningMode,
};
use crate::types::{FileKind, Hash, INO_ROOT, Ino, InodeInit};
use async_recursion::async_recursion;
use std::collections::BTreeMap;

const HEAD: &str = "HEAD";
const DEFAULT_BRANCH: &str = "main";
/// Config key: hex address of the live ref-mirror snapshot (a GC root).
const REFS_MIRROR_HASH: &str = "refs_mirror";
/// Config key: monotonic generation of the last ref-mirror snapshot written.
const REFS_MIRROR_GEN: &str = "refs_mirror_gen";

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Ensure HEAD and the default versioning mode exist (called by `init`).
    pub async fn init_versioning(&self) -> Result<()> {
        if self.meta.get_ref(HEAD).await?.is_none() {
            self.meta
                .set_ref(HEAD, &format!("ref:{DEFAULT_BRANCH}"))
                .await?;
        }
        if self.meta.get_config("versioning").await?.is_none() {
            self.meta
                .set_config("versioning", VersioningMode::Native.as_str())
                .await?;
        }
        Ok(())
    }

    /// The workspace's versioning mode (defaults to `native`).
    pub async fn versioning_mode(&self) -> Result<VersioningMode> {
        Ok(self
            .meta
            .get_config("versioning")
            .await?
            .and_then(|s| VersioningMode::parse(&s))
            .unwrap_or(VersioningMode::Native))
    }

    pub async fn set_versioning_mode(&self, mode: VersioningMode) -> Result<()> {
        self.meta.set_config("versioning", mode.as_str()).await
    }

    pub(crate) async fn ensure_commits_enabled(&self) -> Result<()> {
        if !self.versioning_mode().await?.commits_enabled() {
            return Err(OrigoFSError::InvalidArgument(
                "versioning is disabled (off mode)".to_string(),
            ));
        }
        Ok(())
    }

    /// Mirror the whole ref table into the content store as a [`RefSnapshot`], so
    /// branch names + tips can be recovered from the bucket alone if the metadata
    /// DB is lost (see [`Self::rebuild_from_content`]). Called after every
    /// ref-advancing operation. Cheap — one small object — and errors propagate,
    /// so a mirror is never silently skipped.
    ///
    /// The generation is bumped and persisted *before* the object is written, so
    /// it is strictly monotonic even if a crash interleaves; the live snapshot's
    /// hash is then recorded in `config` so GC keeps exactly it and reaps
    /// superseded snapshots.
    pub(crate) async fn mirror_refs(&self) -> Result<()> {
        // Atomic increment: concurrent ref-advancing operations each get a
        // distinct, strictly increasing generation, so a recovery scan can pick
        // the newest snapshot unambiguously — no read-then-write race (audit #21).
        let generation = self.meta.bump_counter(REFS_MIRROR_GEN).await? as u64;
        let refs = self.meta.list_refs().await?;
        let hash = self
            .content
            .put(&RefSnapshot { generation, refs }.encode())
            .await?;
        // Persist the object before any metadata references it (same barrier a
        // commit uses), then point `config` at the new live snapshot.
        self.content.flush().await?;
        self.meta
            .set_config(REFS_MIRROR_HASH, &hash.to_hex())
            .await?;
        Ok(())
    }

    /// The hash of the live ref-mirror snapshot, if any (a GC root).
    pub(crate) async fn refs_mirror_hash(&self) -> Result<Option<Hash>> {
        Ok(self
            .meta
            .get_config(REFS_MIRROR_HASH)
            .await?
            .and_then(|s| Hash::from_hex(&s)))
    }

    /// The current branch name (from HEAD), or `None` if HEAD is detached.
    pub async fn current_branch(&self) -> Result<Option<String>> {
        match self.meta.get_ref(HEAD).await? {
            Some(v) => Ok(v.strip_prefix("ref:").map(|s| s.to_string())),
            None => Ok(None),
        }
    }

    /// The commit HEAD points at, or `None` on an unborn branch.
    pub async fn head_commit(&self) -> Result<Option<Hash>> {
        let head = match self.meta.get_ref(HEAD).await? {
            Some(v) => v,
            None => return Ok(None),
        };
        let value = match head.strip_prefix("ref:") {
            Some(branch) => match self.meta.get_ref(branch).await? {
                Some(v) => v,
                None => return Ok(None), // unborn branch
            },
            None => head, // detached HEAD holds a commit hex directly
        };
        Ok(Hash::from_hex(&value))
    }

    /// Snapshot the working tree into a new commit, advancing the current branch.
    pub async fn commit(&self, author: &str, message: &str) -> Result<Hash> {
        self.ensure_commits_enabled().await?;
        let branch = self.current_branch().await?.ok_or_else(|| {
            OrigoFSError::InvalidArgument("cannot commit with a detached HEAD".into())
        })?;
        let parent = self.head_commit().await?;

        // A merge in progress contributes the incoming commit as a second parent.
        let merge_head = self
            .meta
            .get_ref("MERGE_HEAD")
            .await?
            .and_then(|s| Hash::from_hex(&s));
        let mut parents: Vec<Hash> = parent.iter().copied().collect();
        if let Some(mh) = merge_head
            && !parents.contains(&mh)
        {
            parents.push(mh);
        }

        let tree = self.build_tree(INO_ROOT).await?;
        let commit = Commit {
            tree,
            parents,
            author: author.to_string(),
            message: message.to_string(),
            timestamp: self.now_secs(),
        };
        let commit_hash = self.content.put(&commit.encode()).await?;
        // Durability barrier: seal any open pack so the whole snapshot is
        // persisted before the branch ref advances to it (no-op unless the
        // content store batches writes).
        self.content.flush().await?;

        let expect = parent.map(|h| h.to_hex());
        let swapped = self
            .meta
            .cas_ref(&branch, expect.as_deref(), &commit_hash.to_hex())
            .await?;
        if !swapped {
            return Err(OrigoFSError::Metadata(
                "branch moved concurrently; retry the commit".to_string(),
            ));
        }
        // Merge resolved: clear the in-progress state.
        if merge_head.is_some() {
            self.meta.delete_ref("MERGE_HEAD").await?;
            self.meta.clear_conflicts().await?;
        }
        self.mirror_refs().await?;
        Ok(commit_hash)
    }

    /// Recursively snapshot directory `dir_ino` into a tree object; returns its hash.
    #[async_recursion]
    async fn build_tree(&self, dir_ino: Ino) -> Result<Hash> {
        let mut entries = Vec::new();
        for de in self.meta.list_dir(dir_ino).await? {
            let inode = self
                .meta
                .get_inode(de.ino)
                .await?
                .ok_or_else(|| OrigoFSError::NotFound(format!("ino {}", de.ino)))?;
            let (kind, hash) = match de.kind {
                FileKind::Dir => (TreeKind::Dir, self.build_tree(de.ino).await?),
                FileKind::File => {
                    let h = match inode.content {
                        Some(h) => h,
                        None => self.content.put(&Manifest::default().encode()).await?,
                    };
                    (TreeKind::File, h)
                }
                FileKind::Symlink => {
                    let target = self.meta.get_symlink(de.ino).await?.unwrap_or_default();
                    (
                        TreeKind::Symlink,
                        self.content.put(target.as_bytes()).await?,
                    )
                }
            };
            entries.push(TreeEntry {
                name: de.name,
                mode: inode.mode,
                kind,
                hash,
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        self.content.put(&Tree { entries }.encode()).await
    }

    /// Create a branch at the current HEAD commit.
    pub async fn create_branch(&self, name: &str) -> Result<()> {
        self.ensure_commits_enabled().await?;
        let head = self.head_commit().await?.ok_or_else(|| {
            OrigoFSError::InvalidArgument("cannot branch before the first commit".into())
        })?;
        if !self.meta.cas_ref(name, None, &head.to_hex()).await? {
            return Err(OrigoFSError::AlreadyExists(format!("branch {name}")));
        }
        self.mirror_refs().await?;
        Ok(())
    }

    /// Branch names (all refs except HEAD) with their commit hashes.
    pub async fn list_branches(&self) -> Result<Vec<(String, Hash)>> {
        let mut out = Vec::new();
        for (name, value) in self.meta.list_refs().await? {
            if name == HEAD {
                continue;
            }
            if let Some(h) = Hash::from_hex(&value) {
                out.push((name, h));
            }
        }
        Ok(out)
    }

    /// Switch the working tree to `branch`, materializing its commit.
    pub async fn checkout(&self, branch: &str) -> Result<()> {
        self.ensure_commits_enabled().await?;
        let value = self
            .meta
            .get_ref(branch)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("branch {branch}")))?;
        let commit_hash =
            Hash::from_hex(&value).ok_or_else(|| OrigoFSError::Metadata("bad ref value".into()))?;
        let commit = Commit::decode(&self.content.get(&commit_hash).await?)?;

        self.replace_working_tree(commit.tree).await?;
        self.meta.set_ref(HEAD, &format!("ref:{branch}")).await?;
        self.mirror_refs().await?;
        Ok(())
    }

    /// Atomically replace the working tree with the contents of `tree_hash`:
    /// truncate the current tree and rematerialize the new one inside a single
    /// metadata transaction, so a failure mid-materialize — or a concurrent
    /// reader — never observes a half-emptied tree (audit #9). Used by checkout,
    /// merge, and rebuild.
    pub(crate) async fn replace_working_tree(&self, tree_hash: Hash) -> Result<()> {
        let mut txn = self.meta.begin().await?;
        txn.truncate_tree().await?;
        self.materialize_into_txn(&mut *txn, tree_hash, INO_ROOT)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    /// Materialize a tree's entries as children of `parent_ino`, staging every
    /// inode/dentry in `txn` so the whole subtree commits atomically.
    #[async_recursion]
    async fn materialize_into_txn(
        &self,
        txn: &mut dyn MetaTxn,
        tree_hash: Hash,
        parent_ino: Ino,
    ) -> Result<()> {
        let tree = Tree::decode(&self.content.get(&tree_hash).await?)?;
        for e in &tree.entries {
            match e.kind {
                TreeKind::Dir => {
                    let ino = txn
                        .create_inode(InodeInit {
                            kind: FileKind::Dir,
                            mode: e.mode,
                        })
                        .await?;
                    txn.add_dentry(parent_ino, &e.name, ino).await?;
                    self.materialize_into_txn(&mut *txn, e.hash, ino).await?;
                }
                TreeKind::File => {
                    let ino = txn
                        .create_inode(InodeInit {
                            kind: FileKind::File,
                            mode: e.mode,
                        })
                        .await?;
                    let size = Manifest::decode(&self.content.get(&e.hash).await?)?.size;
                    txn.set_content(ino, Some(e.hash), size).await?;
                    txn.add_dentry(parent_ino, &e.name, ino).await?;
                }
                TreeKind::Symlink => {
                    let ino = txn
                        .create_inode(InodeInit {
                            kind: FileKind::Symlink,
                            mode: e.mode,
                        })
                        .await?;
                    let target =
                        String::from_utf8_lossy(&self.content.get(&e.hash).await?).into_owned();
                    txn.set_symlink(ino, &target).await?;
                    txn.add_dentry(parent_ino, &e.name, ino).await?;
                }
            }
        }
        Ok(())
    }

    /// Commit history from HEAD, following first parents.
    pub async fn log(&self) -> Result<Vec<CommitInfo>> {
        let mut out = Vec::new();
        let mut cursor = self.head_commit().await?;
        while let Some(hash) = cursor {
            let commit = Commit::decode(&self.content.get(&hash).await?)?;
            cursor = commit.parents.first().copied();
            out.push(CommitInfo { hash, commit });
        }
        Ok(out)
    }

    /// Changes between the working tree and HEAD (like `git status`).
    pub async fn status(&self) -> Result<Vec<DiffEntry>> {
        let base = match self.head_commit().await? {
            Some(h) => {
                let commit = Commit::decode(&self.content.get(&h).await?)?;
                let mut map = BTreeMap::new();
                self.flatten_tree(commit.tree, String::new(), &mut map)
                    .await?;
                map
            }
            None => BTreeMap::new(),
        };
        let mut work = BTreeMap::new();
        self.flatten_working(INO_ROOT, String::new(), &mut work)
            .await?;
        Ok(diff_maps(&base, &work))
    }

    /// Resolve a ref name (branch, `HEAD`, tag) or a raw commit hex to a commit
    /// hash. This is how the diff API accepts either `"main"` or a commit id.
    pub async fn resolve_commit(&self, name: &str) -> Result<Hash> {
        if let Some(v) = self.meta.get_ref(name).await? {
            // A branch/tag holds a commit hex; HEAD holds `ref:<branch>`.
            let target = match v.strip_prefix("ref:") {
                Some(branch) => self
                    .meta
                    .get_ref(branch)
                    .await?
                    .ok_or_else(|| OrigoFSError::NotFound(format!("branch {branch}")))?,
                None => v,
            };
            return Hash::from_hex(&target)
                .ok_or_else(|| OrigoFSError::Metadata("bad ref value".into()));
        }
        Hash::from_hex(name).ok_or_else(|| OrigoFSError::NotFound(format!("ref or commit {name}")))
    }

    /// Flatten a commit's tree to a `path → content-hash` map (the whole file
    /// set, addressed).
    async fn flatten_commit(&self, commit_hash: Hash) -> Result<BTreeMap<String, Hash>> {
        let commit = Commit::decode(&self.content.get(&commit_hash).await?)?;
        let mut map = BTreeMap::new();
        self.flatten_tree(commit.tree, String::new(), &mut map)
            .await?;
        Ok(map)
    }

    /// The set of paths that differ between two refs/commits (`from` → `to`),
    /// each Added / Modified / Deleted.
    ///
    /// This is the cheap half of a UI branch comparison: it compares the two
    /// trees by **content address**, so unchanged files (equal hash) cost a
    /// 32-byte compare and never touch the chunk store. Only the paths this
    /// returns need a real line diff — see [`Self::diff_file`].
    pub async fn diff(&self, from: &str, to: &str) -> Result<Vec<DiffEntry>> {
        let base = self
            .flatten_commit(self.resolve_commit(from).await?)
            .await?;
        let target = self.flatten_commit(self.resolve_commit(to).await?).await?;
        Ok(diff_maps(&base, &target))
    }

    /// A unified line diff of one `path` between two refs/commits. Returns an
    /// empty string when the file is byte-identical (or absent) on both sides.
    /// Binary content is compared lossily as UTF-8.
    pub async fn diff_file(&self, from: &str, to: &str, path: &str) -> Result<String> {
        let base = self
            .flatten_commit(self.resolve_commit(from).await?)
            .await?;
        let target = self.flatten_commit(self.resolve_commit(to).await?).await?;
        // Fast path: identical (or both-absent) content addresses — no reads.
        if base.get(path) == target.get(path) {
            return Ok(String::new());
        }
        let old = self.side_text(base.get(path)).await?;
        let new = self.side_text(target.get(path)).await?;
        Ok(diffy::create_patch(&old, &new).to_string())
    }

    /// Reconstruct one side of a file diff as text (empty if the path is absent).
    async fn side_text(&self, hash: Option<&Hash>) -> Result<String> {
        match hash {
            Some(h) => {
                let bytes = self.content_bytes(h).await?;
                Ok(String::from_utf8_lossy(&bytes).into_owned())
            }
            None => Ok(String::new()),
        }
    }

    #[async_recursion]
    async fn flatten_working(
        &self,
        dir_ino: Ino,
        prefix: String,
        map: &mut BTreeMap<String, Hash>,
    ) -> Result<()> {
        for de in self.meta.list_dir(dir_ino).await? {
            let path = format!("{prefix}/{}", de.name);
            match de.kind {
                FileKind::Dir => self.flatten_working(de.ino, path, map).await?,
                FileKind::File => {
                    let inode = self
                        .meta
                        .get_inode(de.ino)
                        .await?
                        .ok_or_else(|| OrigoFSError::NotFound(path.clone()))?;
                    let h = match inode.content {
                        Some(h) => h,
                        None => Hash::of(&Manifest::default().encode()),
                    };
                    map.insert(path, h);
                }
                FileKind::Symlink => {
                    let target = self.meta.get_symlink(de.ino).await?.unwrap_or_default();
                    map.insert(path, Hash::of(target.as_bytes()));
                }
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn flatten_tree(
        &self,
        tree_hash: Hash,
        prefix: String,
        map: &mut BTreeMap<String, Hash>,
    ) -> Result<()> {
        let tree = Tree::decode(&self.content.get(&tree_hash).await?)?;
        for e in &tree.entries {
            let path = format!("{prefix}/{}", e.name);
            match e.kind {
                TreeKind::Dir => self.flatten_tree(e.hash, path, map).await?,
                TreeKind::File | TreeKind::Symlink => {
                    map.insert(path, e.hash);
                }
            }
        }
        Ok(())
    }
}

fn diff_maps(base: &BTreeMap<String, Hash>, work: &BTreeMap<String, Hash>) -> Vec<DiffEntry> {
    let mut out = Vec::new();
    for (path, wh) in work {
        match base.get(path) {
            None => out.push(DiffEntry {
                path: path.clone(),
                status: DiffStatus::Added,
            }),
            Some(bh) if bh != wh => out.push(DiffEntry {
                path: path.clone(),
                status: DiffStatus::Modified,
            }),
            _ => {}
        }
    }
    for path in base.keys() {
        if !work.contains_key(path) {
            out.push(DiffEntry {
                path: path.clone(),
                status: DiffStatus::Deleted,
            });
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}
