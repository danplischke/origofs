//! Rebuild a workspace's metadata from the content store alone (`docs/DESIGN.md`
//! §7 recovery).
//!
//! origofs keeps a git-style content-addressed Merkle DAG in the [`ContentStore`] —
//! commits reference trees, trees reference blob manifests and sub-trees, and a
//! manifest lists the ordered chunks of a file. That graph is *self-describing*:
//! given a commit, every directory, filename, and file body (reassembled from its
//! chunks) can be reconstructed without the metadata DB. The one thing the graph
//! doesn't carry is the mutable ref table (branch → tip), which normally lives
//! only in the DB — so [`Fs::mirror_refs`] additionally writes a [`RefSnapshot`]
//! into the store on every ref change. Together they let a bare content store
//! bootstrap a fresh DB after a loss.
//!
//! What this recovers: the working tree (dirs, files, symlinks), branch names +
//! tips, and which branch was checked out. What it does **not**: per-line blame,
//! the edit-op audit, actors/sessions, the change feed, or uncommitted edits —
//! those live only in the DB, and any work never captured in a commit is not in
//! the object graph to recover.
//!
//! [`ContentStore`]: crate::ContentStore

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::format;
use crate::metadata::MetadataStore;
use crate::objectgraph::{Commit, RefSnapshot, Tree, TreeKind};
use crate::types::Hash;
use async_recursion::async_recursion;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const HEAD: &str = "HEAD";
const DEFAULT_BRANCH: &str = "main";
const MERGE_HEAD: &str = "MERGE_HEAD";
/// The reserved ref key a ref-mirror snapshot carries its workspace name under, so
/// a rebuild can recover each workspace of a multi-workspace store into the right
/// place (`docs/MULTI_TENANCY.md`; written by [`Fs::mirror_refs`]). Recovery skips
/// it as a "ref" the same way it skips `HEAD`/`MERGE_HEAD`.
pub(crate) const WORKSPACE_MIRROR_KEY: &str = "\0origofs.workspace";
/// The name of the store's root workspace (recovered into the rebuild target).
const DEFAULT_WS_NAME: &str = "default";

/// What a recovery scan found and (for [`Fs::rebuild_from_content`]) restored.
/// `#[non_exhaustive]`: callers read this, they never construct it, so adding a
/// counter should not be a breaking change. (Config structs like `S3Config` are
/// deliberately left constructible — a caller has to be able to build those, and
/// `..Default::default()` already absorbs new fields there.)
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RebuildReport {
    /// Objects read from the content store.
    pub objects_scanned: usize,
    /// Objects that failed their integrity check while scanning (skipped).
    pub corrupt: usize,
    /// Commit objects found in the store.
    pub commits_found: usize,
    /// `true` if branch names/tips came from a ref-mirror snapshot; `false` if
    /// they were inferred from head commits (branch names are then synthetic).
    pub used_mirror: bool,
    /// `(name, commit_hex)` for every branch recovered.
    pub branches: Vec<(String, String)>,
    /// The branch materialized into the working tree (a rebuild), or the one that
    /// would be (a dry-run scan).
    pub checked_out: Option<String>,
    /// Directories, files, and symlinks materialized into the working tree.
    /// Populated by a rebuild; left zero by a read-only scan. Aggregated across
    /// every workspace recovered.
    pub dirs: usize,
    pub files: usize,
    pub symlinks: usize,
    /// Additional (non-`default`) workspaces recovered from tagged ref mirrors — a
    /// multi-workspace store restores each of its workspaces (`docs/MULTI_TENANCY.md`).
    pub extra_workspaces: usize,
    /// Commit / ref-snapshot objects whose header names a known origofs object
    /// kind but a format version this build cannot decode — they were written by
    /// a **newer** origofs ([`crate::format`]).
    ///
    /// A dry-run [`scan_content`](Fs::scan_content) only reports these;
    /// [`rebuild_from_content`](Fs::rebuild_from_content) *fails* when one of them
    /// would change what it restores, rather than quietly recovering less than the
    /// store holds.
    pub unsupported: usize,
    /// `(kind, version)` for the objects counted by `unsupported`, deduped and
    /// sorted — e.g. `[("commit", 2)]`.
    pub unsupported_kinds: Vec<(String, u8)>,
}

/// The commit DAG + the newest ref-mirror snapshot recovered *per workspace* from a
/// content-store scan (keyed by workspace name; `default` for the root workspace).
struct Scan {
    commits: HashMap<Hash, Commit>,
    mirrors: HashMap<String, RefSnapshot>,
    /// Commit objects this build is too old to decode, by address → version.
    unsupported_commits: HashMap<Hash, u8>,
    /// Versions of the ref-mirror snapshots this build is too old to decode.
    unsupported_refs: Vec<u8>,
}

impl Scan {
    /// Why this scan must not be turned into a rebuild, if it must not.
    ///
    /// A recovery tool's worst failure mode is restoring *less* than the store
    /// holds while reporting success, so an object written by a newer origofs is
    /// an error whenever it could change the outcome. The test is deliberately
    /// narrow, because a raw data chunk can begin with an object tag by
    /// coincidence and must not be able to block a legitimate rebuild:
    ///
    /// - **any** unreadable ref mirror — it may be the newest one, and its
    ///   `generation` (the field that would prove otherwise) is inside the bytes
    ///   we can't parse;
    /// - an unreadable commit that a readable mirror names as a branch tip;
    /// - with no readable mirror at all, any unreadable commit — head inference
    ///   over a partial DAG would invent wrong branches.
    ///
    /// A stray unreadable object that is *not* load-bearing only lands in the
    /// report's `unsupported` count.
    fn blocking_unsupported(&self) -> Option<OrigoFSError> {
        if let Some(&v) = self.unsupported_refs.iter().max() {
            return Some(format::REFS.unsupported(v));
        }
        if self.unsupported_commits.is_empty() {
            return None;
        }
        if self.mirrors.is_empty() {
            let v = *self.unsupported_commits.values().max().expect("non-empty");
            return Some(format::COMMIT.unsupported(v));
        }
        self.mirrors
            .values()
            .flat_map(|snap| &snap.refs)
            .filter_map(|(_, value)| Hash::from_hex(value))
            .filter_map(|h| self.unsupported_commits.get(&h))
            .max()
            .map(|&v| format::COMMIT.unsupported(v))
    }
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Read-only: scan the content store and report what a rebuild *would*
    /// recover (commits, branches, the branch that would be checked out), without
    /// touching the metadata DB.
    ///
    /// Never fails on an object written by a newer origofs — it counts them in
    /// [`RebuildReport::unsupported`] so an operator can diagnose the store with
    /// an old binary. [`rebuild_from_content`](Self::rebuild_from_content) is the
    /// one that refuses.
    pub async fn scan_content(&self) -> Result<RebuildReport> {
        let mut report = RebuildReport::default();
        let scan = self.scan(&mut report).await?;
        if scan.mirrors.is_empty() {
            // No mirror: infer a single (default) workspace from head commits.
            let branches = infer_heads(&scan.commits);
            report.branches = branches
                .iter()
                .map(|(n, h)| (n.clone(), h.to_hex()))
                .collect();
            report.checked_out = pick_checkout(&branches, None);
            return Ok(report);
        }
        report.used_mirror = true;
        // Aggregate what every workspace's mirror would restore (dry run).
        let mut names: Vec<&String> = scan.mirrors.keys().collect();
        names.sort();
        for name in names {
            let (branches, head) = resolve_mirror(&scan.mirrors[name], &scan.commits);
            for (n, h) in &branches {
                report.branches.push((n.clone(), h.to_hex()));
            }
            if name == DEFAULT_WS_NAME {
                report.checked_out = pick_checkout(&branches, head);
            } else {
                report.extra_workspaces += 1;
            }
        }
        Ok(report)
    }

    /// Rebuild refs and the working tree(s) from the object graph in the content
    /// store. Call on a freshly [`init`](Fs::init)ed workspace whose DB is empty
    /// but whose content store is the surviving one. Returns a [`RebuildReport`].
    ///
    /// A multi-workspace store restores **each** of its workspaces: the `default`
    /// workspace into `self`, and every other tagged workspace into a freshly
    /// created registry entry of its own (`docs/MULTI_TENANCY.md`).
    ///
    /// This **resets the working tree** to the recovered commit, so run it for
    /// recovery, not against a live DB with uncommitted work. Attribution is not
    /// recovered (it lives only in the DB). Reading every object also
    /// integrity-checks it: a corrupt object is skipped and counted.
    ///
    /// Fails with [`OrigoFSError::UnsupportedVersion`] if the store holds a
    /// load-bearing object written by a newer origofs (see
    /// `Scan::blocking_unsupported`) — restoring a silently truncated history is
    /// worse than refusing. Upgrade origofs and re-run; `scan_content` still works
    /// on the old binary and reports what it can't read.
    pub async fn rebuild_from_content(&self) -> Result<RebuildReport>
    where
        C: Clone,
    {
        let mut report = RebuildReport::default();
        let scan = self.scan(&mut report).await?;
        // Refuse rather than restore a partial view: see `Scan::blocking_unsupported`.
        // The dry run reports the same finding without failing, so the operator can
        // see the full picture with `scan_content` before upgrading.
        if let Some(e) = scan.blocking_unsupported() {
            return Err(e);
        }

        if scan.mirrors.is_empty() {
            // No usable mirror: infer a single default workspace (synthetic branch
            // names). Multi-workspace recovery relies on the tagged mirrors.
            let synthetic = RefSnapshot {
                generation: 0,
                refs: infer_heads(&scan.commits)
                    .into_iter()
                    .map(|(n, h)| (n, h.to_hex()))
                    .collect(),
            };
            recover_into(self, &synthetic, &scan.commits, &mut report).await?;
            return Ok(report);
        }

        report.used_mirror = true;
        // The `default` workspace recovers into `self`; the rest into new registry
        // entries. Deterministic order: default first, then names sorted.
        if let Some(def) = scan.mirrors.get(DEFAULT_WS_NAME) {
            recover_into(self, def, &scan.commits, &mut report).await?;
        }
        let mut names: Vec<&String> = scan.mirrors.keys().collect();
        names.sort();
        for name in names {
            if name == DEFAULT_WS_NAME {
                continue;
            }
            // Adopt an existing registry row so rebuild is idempotent: a second pass,
            // or a rebuild onto a store that already knows this workspace, must
            // re-materialize its tree rather than fail on a duplicate-name insert.
            let (id, root) = match self.meta.lookup_workspace(name).await? {
                Some(existing) => existing,
                None => self.meta.create_workspace(name).await?,
            };
            let scoped: Arc<dyn MetadataStore> = self.meta.with_workspace(id);
            // A sibling engine bound to the recovered workspace, sharing this one's
            // content store + clock (built directly — the scoped handle is a trait
            // object, not necessarily `M`).
            let sub: Fs<Arc<dyn MetadataStore>, C> = Fs {
                meta: scoped,
                content: self.content.clone(),
                clock: self.clock.clone(),
                root_ino: root,
                // A *different* workspace, with its own scoped metadata store and
                // so its own grants: a fresh cache, never `self`'s.
                acl_cache: Default::default(),
            };
            recover_into(&sub, &scan.mirrors[name], &scan.commits, &mut report).await?;
            report.extra_workspaces += 1;
        }
        Ok(report)
    }

    /// Scan every object, classifying commits and keeping the newest ref-mirror
    /// snapshot **per workspace** (from its `WORKSPACE_MIRROR_KEY` tag; `default`
    /// when untagged). Trees, manifests, chunks, and symlink targets are followed
    /// on demand during materialization, so they're ignored here. Fills the scan
    /// counters on `report`. Reading each object integrity-checks it; corrupt
    /// objects are skipped and counted.
    ///
    /// Objects are classified by their 4-byte **type tag** before decoding, so an
    /// object of a known kind that this build is too old to decode is counted as
    /// `unsupported` instead of being silently ignored. (Trying `Commit::decode`
    /// and falling through on *any* error, as this used to, made "written by a
    /// newer origofs" indistinguishable from "not a commit" — a rebuild would then
    /// report success having quietly dropped the history it couldn't read.)
    async fn scan(&self, report: &mut RebuildReport) -> Result<Scan> {
        let mut commits: HashMap<Hash, Commit> = HashMap::new();
        let mut mirrors: HashMap<String, RefSnapshot> = HashMap::new();
        let mut unsupported_commits: HashMap<Hash, u8> = HashMap::new();
        let mut unsupported_refs: Vec<u8> = Vec::new();
        let all = self.content.list().await?;
        report.objects_scanned = all.len();
        for hash in all {
            let bytes = match self.content.get(&hash).await {
                Ok(b) => b,
                Err(_) => {
                    report.corrupt += 1;
                    continue;
                }
            };
            if format::COMMIT.tagged(&bytes) {
                match Commit::decode(&bytes) {
                    // Guard a chunk that merely starts with the commit tag: a real
                    // commit's tree object is present in the store.
                    Ok(commit) => {
                        if self.content.has(&commit.tree).await.unwrap_or(false) {
                            commits.insert(hash, commit);
                        }
                    }
                    Err(OrigoFSError::UnsupportedVersion { found, .. }) => {
                        unsupported_commits.insert(hash, found);
                    }
                    // Malformed under a version we *do* support: a data chunk that
                    // happens to start with the tag, not a commit.
                    Err(_) => {}
                }
            } else if format::REFS.tagged(&bytes) {
                match RefSnapshot::decode(&bytes) {
                    Ok(snap) => {
                        let ws = snap
                            .refs
                            .iter()
                            .find(|(k, _)| k == WORKSPACE_MIRROR_KEY)
                            .map(|(_, v)| v.clone())
                            .unwrap_or_else(|| DEFAULT_WS_NAME.to_string());
                        if mirrors
                            .get(&ws)
                            .is_none_or(|s| snap.generation > s.generation)
                        {
                            mirrors.insert(ws, snap);
                        }
                    }
                    Err(OrigoFSError::UnsupportedVersion { found, .. }) => {
                        unsupported_refs.push(found);
                    }
                    Err(_) => {}
                }
            }
        }
        report.commits_found = commits.len();
        report.unsupported = unsupported_commits.len() + unsupported_refs.len();
        let mut kinds: Vec<(String, u8)> = unsupported_commits
            .values()
            .map(|&v| (format::COMMIT.name().to_string(), v))
            .chain(
                unsupported_refs
                    .iter()
                    .map(|&v| (format::REFS.name().to_string(), v)),
            )
            .collect();
        kinds.sort();
        kinds.dedup();
        report.unsupported_kinds = kinds;
        Ok(Scan {
            commits,
            mirrors,
            unsupported_commits,
            unsupported_refs,
        })
    }

    /// Count the dirs/files/symlinks reachable from a tree (for the report).
    #[async_recursion]
    async fn tally_tree(&self, tree_hash: Hash, report: &mut RebuildReport) -> Result<()> {
        let tree = Tree::decode(&self.content.get(&tree_hash).await?)?;
        for e in &tree.entries {
            match e.kind {
                TreeKind::Dir => {
                    report.dirs += 1;
                    self.tally_tree(e.hash, report).await?;
                }
                TreeKind::File => report.files += 1,
                TreeKind::Symlink => report.symlinks += 1,
            }
        }
        Ok(())
    }
}

/// Recover one workspace `target` from its ref-mirror `snap`: write its branch
/// refs, materialize the checked-out branch's tree, restore `HEAD`, and re-mirror.
/// Generic over the target's metadata handle so it serves both `self` (the default
/// workspace) and the freshly-created sibling workspaces.
async fn recover_into<M: MetadataStore, C: ContentStore>(
    target: &Fs<M, C>,
    snap: &RefSnapshot,
    commits: &HashMap<Hash, Commit>,
    report: &mut RebuildReport,
) -> Result<()> {
    let (branches, head_target) = resolve_mirror(snap, commits);
    for (name, h) in &branches {
        target.meta.set_ref(name, &h.to_hex()).await?;
        report.branches.push((name.clone(), h.to_hex()));
    }
    if let Some(branch) = pick_checkout(&branches, head_target) {
        let tip = branches
            .iter()
            .find(|(n, _)| *n == branch)
            .map(|(_, h)| *h)
            .expect("checkout branch is one we just recovered");
        let tree = commits
            .get(&tip)
            .expect("branch tip is a scanned commit")
            .tree;
        target.replace_working_tree(tree).await?;
        target.meta.set_ref(HEAD, &format!("ref:{branch}")).await?;
        target.tally_tree(tree, report).await?;
        // The first workspace recovered (the default) sets the report's checkout.
        report.checked_out.get_or_insert(branch);
    }
    // Re-establish a fresh ref mirror so the recovered workspace is protected again
    // (and superseded snapshots become collectable).
    if !branches.is_empty() {
        target.mirror_refs().await?;
    }
    Ok(())
}

/// Resolve one workspace's mirror into its branch list + mirrored HEAD target.
/// Skips `HEAD`, an in-progress `MERGE_HEAD`, and the reserved workspace-name tag;
/// keeps only branches whose tip commit was actually scanned. Pure (no I/O).
fn resolve_mirror(
    snap: &RefSnapshot,
    commits: &HashMap<Hash, Commit>,
) -> (Vec<(String, Hash)>, Option<String>) {
    let mut head_target = None;
    let mut branches: Vec<(String, Hash)> = Vec::new();
    for (name, value) in &snap.refs {
        if name == HEAD {
            // The mirror is bytes from the content store, which a rebuild may be
            // pointed at without trusting it. A `HEAD` naming `../..` would escape
            // when the git layer writes `refs/heads/<name>`, so drop it and let
            // `pick_checkout` fall back to a real branch.
            head_target = value
                .strip_prefix("ref:")
                .filter(|b| crate::engine::validate_ref_name(b).is_ok())
                .map(str::to_string);
        } else if name == MERGE_HEAD || name == WORKSPACE_MIRROR_KEY {
            continue; // an in-progress merge / the workspace-name tag, not a branch
        } else if crate::engine::validate_ref_name(name).is_err() {
            // Skip rather than abort: a rebuild is a recovery tool, and one
            // poisoned ref must not cost the operator every other branch.
            continue;
        } else if let Some(h) = Hash::from_hex(value)
            && commits.contains_key(&h)
        {
            branches.push((name.clone(), h));
        }
    }
    (branches, head_target)
}

/// No usable mirror: infer heads (commits nothing else has as a parent) as a
/// single default workspace's branches, with synthetic names.
fn infer_heads(commits: &HashMap<Hash, Commit>) -> Vec<(String, Hash)> {
    let mut parents: HashSet<Hash> = HashSet::new();
    for c in commits.values() {
        parents.extend(c.parents.iter().copied());
    }
    let mut heads: Vec<Hash> = commits
        .keys()
        .copied()
        .filter(|h| !parents.contains(h))
        .collect();
    heads.sort_by_key(|h| h.to_hex()); // deterministic naming
    let mut branches = Vec::new();
    if heads.len() == 1 {
        branches.push((DEFAULT_BRANCH.to_string(), heads[0]));
    } else {
        for (i, h) in heads.into_iter().enumerate() {
            branches.push((format!("recovered-{}", i + 1), h));
        }
    }
    branches
}

/// Pick the branch to check out: the mirrored HEAD if it names a recovered
/// branch, else `main`, else the first branch.
fn pick_checkout(branches: &[(String, Hash)], head_target: Option<String>) -> Option<String> {
    head_target
        .filter(|b| branches.iter().any(|(n, _)| n == b))
        .or_else(|| {
            branches
                .iter()
                .map(|(n, _)| n)
                .find(|n| n.as_str() == DEFAULT_BRANCH)
                .or_else(|| branches.first().map(|(n, _)| n))
                .cloned()
        })
}
