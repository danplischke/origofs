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
use crate::types::{FileKind, Hash, Ino, InodeInit};
use async_recursion::async_recursion;
use std::collections::BTreeMap;

/// A tree swap resolved down to the rows it will write: every content object it
/// needs, already fetched, decoded and name-validated. Built by
/// [`Fs::plan_materialize`] *before* a transaction opens, then replayed inside
/// one — see that method for why the two halves must not be interleaved.
pub(crate) struct MaterializePlan {
    entries: Vec<PlanEntry>,
}

struct PlanEntry {
    name: String,
    mode: u32,
    node: PlanNode,
}

enum PlanNode {
    Dir(MaterializePlan),
    File { hash: Hash, size: u64 },
    Symlink(String),
}

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
        let mut refs = self.meta.list_refs().await?;
        // Tag the snapshot with this workspace's name (resolved from its root inode
        // via the registry), so a rebuild after a metadata-DB loss can recover each
        // workspace of a multi-workspace store into the right place
        // (`docs/MULTI_TENANCY.md`). Reuses the refs vec — no object-format change;
        // the recovery side skips this reserved key like it skips HEAD/MERGE_HEAD.
        if let Some(name) = self.workspace_name().await? {
            refs.push((crate::recover::WORKSPACE_MIRROR_KEY.to_string(), name));
        }
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

    /// This engine's workspace name, resolved from its root inode via the registry
    /// (`"default"` for the root workspace). `None` only if no registry row matches
    /// the root — e.g. a pre-`workspace`-table store — in which case the mirror is
    /// left untagged and recovery treats it as the default workspace.
    pub(crate) async fn workspace_name(&self) -> Result<Option<String>> {
        Ok(self
            .meta
            .list_workspaces()
            .await?
            .into_iter()
            .find(|(_, _, root)| *root == self.root_ino)
            .map(|(_, name, _)| name))
    }

    /// The ref-mirror snapshot hash recorded in a *specific* workspace's config.
    /// GC needs this per workspace: the mirror pointer lives in the workspace-scoped
    /// `config`, so marking only the calling workspace's mirror would sweep every
    /// other workspace's recovery snapshot out of the shared content store.
    pub(crate) async fn mirror_hash_of(meta: &dyn MetadataStore) -> Result<Option<Hash>> {
        Ok(meta
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

    /// Snapshot the working tree into a new commit, attributed to `ctx` and
    /// subject to its write policy (§6).
    ///
    /// A commit crystallizes whatever is in the working tree into history and
    /// advances the branch — including, when a merge is in progress, resolving it.
    /// That is a trusted act even though it authors no bytes itself, so a
    /// propose-only actor is refused. There is no "propose a commit" equivalent:
    /// the reviewable unit is the edit, and a propose-only actor's edits are held
    /// in the suggestion queue precisely so they never reach a commit unreviewed.
    ///
    /// [`commit`](Self::commit) remains the unattributed form for the CLI, tests,
    /// and internal machinery.
    pub async fn commit_as(
        &self,
        ctx: crate::WriteCtx,
        author: &str,
        message: &str,
    ) -> Result<Hash> {
        self.ensure_may_write(ctx, "commit").await?;
        self.commit(author, message).await
    }

    /// Snapshot the working tree into a new commit, advancing the current branch.
    pub async fn commit(&self, author: &str, message: &str) -> Result<Hash> {
        crate::retry::retrying("commit", || self.commit_attempt(author, message)).await
    }

    /// One attempt at [`commit`](Self::commit); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn commit_attempt(&self, author: &str, message: &str) -> Result<Hash> {
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

        let tree = self.build_tree(self.root_ino).await?;
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

        // The branch advance and the merge-state clear commit together. Separately,
        // a crash after the CAS left `MERGE_HEAD` set and the conflicts recorded
        // forever: the workspace reads as permanently mid-merge, and the *next*
        // commit re-adds the same second parent.
        let expect = parent.map(|h| h.to_hex());
        let mut txn = self.meta.begin().await?;
        let swapped = txn
            .cas_ref(&branch, expect.as_deref(), &commit_hash.to_hex())
            .await?;
        if !swapped {
            // Dropping rolls back; nothing was applied.
            return Err(OrigoFSError::Metadata(
                "branch moved concurrently; retry the commit".to_string(),
            ));
        }
        if merge_head.is_some() {
            txn.delete_ref("MERGE_HEAD").await?;
            txn.clear_conflicts().await?;
        }
        txn.commit().await?;
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
        crate::engine::validate_ref_name(name)?;
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
        crate::retry::retrying("checkout", || self.checkout_attempt(branch)).await
    }

    /// One attempt at [`checkout`](Self::checkout); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn checkout_attempt(&self, branch: &str) -> Result<()> {
        // Validated even though the ref must already exist: `HEAD` is written as
        // `ref:{branch}`, and the git layer turns that back into a host path.
        crate::engine::validate_ref_name(branch)?;
        self.ensure_commits_enabled().await?;
        let value = self
            .meta
            .get_ref(branch)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("branch {branch}")))?;
        let commit_hash =
            Hash::from_hex(&value).ok_or_else(|| OrigoFSError::Metadata("bad ref value".into()))?;
        let commit = Commit::decode(&self.content.get(&commit_hash).await?)?;
        let plan = self.plan_materialize(commit.tree).await?;

        // The tree swap and the HEAD move commit together. Separately, a crash
        // between them left branch B's tree in the working tree while HEAD still
        // named branch A — and the *next* commit would then snapshot B's tree onto
        // A's tip, whose CAS succeeds because A's parent hasn't moved. A checkout
        // silently force-overwriting the branch you came from.
        let mut txn = self.meta.begin().await?;
        self.replace_working_tree_in(&mut *txn, &plan).await?;
        txn.set_ref(HEAD, &format!("ref:{branch}")).await?;
        txn.commit().await?;
        self.mirror_refs().await?;
        Ok(())
    }

    /// Atomically replace the working tree with the contents of `tree_hash`:
    /// truncate the current tree and rematerialize the new one inside a single
    /// metadata transaction, so a failure mid-materialize — or a concurrent
    /// reader — never observes a half-emptied tree (audit #9). Used by rebuild.
    ///
    /// Prefer [`replace_working_tree_in`](Self::replace_working_tree_in) when the
    /// caller has ref or conflict state to write alongside the tree: a tree swap
    /// that commits *separately* from the ref describing it is exactly the torn
    /// state `docs/DESIGN.md` §7 claims not to exist.
    pub(crate) async fn replace_working_tree(&self, tree_hash: Hash) -> Result<()> {
        let plan = self.plan_materialize(tree_hash).await?;
        let mut txn = self.meta.begin().await?;
        self.replace_working_tree_in(&mut *txn, &plan).await?;
        txn.commit().await?;
        Ok(())
    }

    /// Stage a whole-tree replacement into an existing transaction, so the caller
    /// can commit it together with the refs and conflict rows that describe it.
    ///
    /// Takes an already-resolved [`MaterializePlan`] rather than a tree hash, so
    /// that every content read happens *before* the transaction opens — see
    /// [`plan_materialize`](Self::plan_materialize) for why that matters.
    pub(crate) async fn replace_working_tree_in(
        &self,
        txn: &mut dyn MetaTxn,
        plan: &MaterializePlan,
    ) -> Result<()> {
        txn.truncate_tree().await?;
        self.apply_plan_into_txn(txn, &plan.entries, self.root_ino)
            .await
    }

    /// Read and decode everything a tree swap will need from the content store,
    /// producing a plan the transaction can replay without touching content.
    ///
    /// **This split is a liveness requirement, not a tidiness one.** The SQLite
    /// backend's `MetaTxn` holds an owned, *blocking* `parking_lot` guard on the
    /// single connection from `begin` to `commit`. Materializing a tree directly
    /// into the transaction awaited `content.get()` once per node while holding
    /// that guard — an S3 round trip per node — so any other task that touched
    /// metadata in the meantime blocked a runtime worker on the guard. On a
    /// `current_thread` runtime that is a hard deadlock: the blocked thread is
    /// the only one that could have polled the transaction to completion, and it
    /// takes the timer driver down with it, so even a `tokio::time::timeout`
    /// wrapped around the operation never fires. On a multi-thread runtime it
    /// parks one worker per waiting caller. See `tests/materialize.rs`.
    ///
    /// This is also the door where a *stored* name comes from the content store
    /// rather than from a caller, so every entry name is validated here. Objects
    /// are integrity-checked against their address, but an address only proves
    /// the bytes are the ones that were written — not that whoever wrote them was
    /// honest. A tree reached from a shared bucket, a `git import`, or a `resync`
    /// peer can name an entry `..`, and without this check it would land in the
    /// dentry table, breaking the invariant that a poisoned name can never be
    /// stored (and so can never escape a later host materialization).
    ///
    /// Rejecting here fails the whole plan *before* any transaction opens, so a
    /// hostile tree yields a clean error and an untouched working tree rather
    /// than a half-applied one. Deliberately *not* enforced in `Tree::decode` —
    /// GC and diff must still be able to walk such a tree in order to reclaim or
    /// describe it.
    #[async_recursion]
    pub(crate) async fn plan_materialize(&self, tree_hash: Hash) -> Result<MaterializePlan> {
        let tree = Tree::decode(&self.content.get(&tree_hash).await?)?;
        let mut entries = Vec::with_capacity(tree.entries.len());
        for e in &tree.entries {
            crate::engine::validate_component(&e.name)?;
            let node = match e.kind {
                TreeKind::Dir => PlanNode::Dir(self.plan_materialize(e.hash).await?),
                TreeKind::File => PlanNode::File {
                    hash: e.hash,
                    size: Manifest::decode(&self.content.get(&e.hash).await?)?.size,
                },
                TreeKind::Symlink => PlanNode::Symlink(
                    String::from_utf8_lossy(&self.content.get(&e.hash).await?).into_owned(),
                ),
            };
            entries.push(PlanEntry {
                name: e.name.clone(),
                mode: e.mode,
                node,
            });
        }
        Ok(MaterializePlan { entries })
    }

    /// Replay a plan as inode/dentry rows under `parent_ino`. Pure metadata: no
    /// content store call happens here, which is what keeps the transaction from
    /// parking on I/O while it holds the connection.
    #[async_recursion]
    async fn apply_plan_into_txn(
        &self,
        txn: &mut dyn MetaTxn,
        entries: &[PlanEntry],
        parent_ino: Ino,
    ) -> Result<()> {
        for e in entries {
            match &e.node {
                PlanNode::Dir(children) => {
                    let ino = txn
                        .create_inode(InodeInit {
                            kind: FileKind::Dir,
                            mode: e.mode,
                        })
                        .await?;
                    txn.add_dentry(parent_ino, &e.name, ino).await?;
                    self.apply_plan_into_txn(&mut *txn, &children.entries, ino)
                        .await?;
                }
                PlanNode::File { hash, size } => {
                    let ino = txn
                        .create_inode(InodeInit {
                            kind: FileKind::File,
                            mode: e.mode,
                        })
                        .await?;
                    txn.set_content(ino, Some(*hash), *size).await?;
                    txn.add_dentry(parent_ino, &e.name, ino).await?;
                }
                PlanNode::Symlink(target) => {
                    let ino = txn
                        .create_inode(InodeInit {
                            kind: FileKind::Symlink,
                            mode: e.mode,
                        })
                        .await?;
                    txn.set_symlink(ino, target).await?;
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
        self.flatten_working(self.root_ino, String::new(), &mut work)
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
