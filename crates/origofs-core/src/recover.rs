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
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::objectgraph::{Commit, RefSnapshot, Tree, TreeKind};
use crate::types::Hash;
use async_recursion::async_recursion;
use std::collections::{HashMap, HashSet};

const HEAD: &str = "HEAD";
const DEFAULT_BRANCH: &str = "main";
const MERGE_HEAD: &str = "MERGE_HEAD";

/// What a recovery scan found and (for [`Fs::rebuild_from_content`]) restored.
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
    /// Populated by a rebuild; left zero by a read-only scan.
    pub dirs: usize,
    pub files: usize,
    pub symlinks: usize,
}

/// The commit DAG + newest ref mirror recovered from a content-store scan.
struct Scan {
    commits: HashMap<Hash, Commit>,
    newest_mirror: Option<RefSnapshot>,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Read-only: scan the content store and report what a rebuild *would*
    /// recover (commits, branches, the branch that would be checked out), without
    /// touching the metadata DB.
    pub async fn scan_content(&self) -> Result<RebuildReport> {
        let mut report = RebuildReport::default();
        let scan = self.scan(&mut report).await?;
        let (branches, head_target, used_mirror) = resolve_refs(&scan);
        report.used_mirror = used_mirror;
        report.branches = branches
            .iter()
            .map(|(n, h)| (n.clone(), h.to_hex()))
            .collect();
        report.checked_out = pick_checkout(&branches, head_target);
        Ok(report)
    }

    /// Rebuild refs and the working tree from the object graph in the content
    /// store. Call on a freshly [`init`](Fs::init)ed workspace whose DB is empty
    /// but whose content store is the surviving one. Returns a [`RebuildReport`].
    ///
    /// This **resets the working tree** to the recovered commit, so run it for
    /// recovery, not against a live DB with uncommitted work. Attribution is not
    /// recovered (it lives only in the DB). Reading every object also
    /// integrity-checks it: a corrupt object is skipped and counted.
    pub async fn rebuild_from_content(&self) -> Result<RebuildReport> {
        let mut report = RebuildReport::default();
        let scan = self.scan(&mut report).await?;
        let (branches, head_target, used_mirror) = resolve_refs(&scan);
        report.used_mirror = used_mirror;

        // Write the branch refs.
        for (name, h) in &branches {
            self.meta.set_ref(name, &h.to_hex()).await?;
            report.branches.push((name.clone(), h.to_hex()));
        }

        // Materialize the checked-out branch's tree into the working tree.
        if let Some(branch) = pick_checkout(&branches, head_target) {
            let tip = branches
                .iter()
                .find(|(n, _)| *n == branch)
                .map(|(_, h)| *h)
                .expect("checkout branch is one we just recovered");
            let tree = scan
                .commits
                .get(&tip)
                .expect("branch tip is a scanned commit")
                .tree;
            self.replace_working_tree(tree).await?;
            self.meta.set_ref(HEAD, &format!("ref:{branch}")).await?;
            self.tally_tree(tree, &mut report).await?;
            report.checked_out = Some(branch);
        }

        // Re-establish a fresh ref mirror so the recovered workspace is protected
        // again (and superseded snapshots become collectable).
        if !branches.is_empty() {
            self.mirror_refs().await?;
        }
        Ok(report)
    }

    /// Scan every object, classifying commits and the newest ref-mirror snapshot.
    /// Trees, manifests, chunks, and symlink targets are followed on demand during
    /// materialization, so they're ignored here. Fills the scan counters on
    /// `report`. Reading each object integrity-checks it; corrupt objects are
    /// skipped and counted.
    async fn scan(&self, report: &mut RebuildReport) -> Result<Scan> {
        let mut commits: HashMap<Hash, Commit> = HashMap::new();
        let mut newest_mirror: Option<RefSnapshot> = None;
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
            if let Ok(commit) = Commit::decode(&bytes) {
                // Guard a chunk that merely starts with the commit magic: a real
                // commit's tree object is present in the store.
                if self.content.has(&commit.tree).await.unwrap_or(false) {
                    commits.insert(hash, commit);
                }
            } else if let Ok(snap) = RefSnapshot::decode(&bytes)
                && newest_mirror
                    .as_ref()
                    .is_none_or(|s| snap.generation > s.generation)
            {
                newest_mirror = Some(snap);
            }
        }
        report.commits_found = commits.len();
        Ok(Scan {
            commits,
            newest_mirror,
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

/// Resolve the recovered ref set: prefer the newest mirror snapshot; otherwise
/// infer heads (commits that are no other commit's parent). Returns the branch
/// list, the mirrored HEAD target branch (if any), and whether the mirror was
/// used. Pure — no I/O — so both the dry-run scan and the rebuild share it.
fn resolve_refs(scan: &Scan) -> (Vec<(String, Hash)>, Option<String>, bool) {
    let mut head_target = None;
    let mut branches: Vec<(String, Hash)> = Vec::new();
    if let Some(snap) = &scan.newest_mirror {
        for (name, value) in &snap.refs {
            if name == HEAD {
                head_target = value.strip_prefix("ref:").map(str::to_string);
            } else if name == MERGE_HEAD {
                continue; // don't resurrect an in-progress merge
            } else if let Some(h) = Hash::from_hex(value)
                && scan.commits.contains_key(&h)
            {
                branches.push((name.clone(), h));
            }
        }
    }
    if !branches.is_empty() {
        return (branches, head_target, true);
    }

    // No usable mirror: infer heads = commits nothing else has as a parent.
    let mut parents: HashSet<Hash> = HashSet::new();
    for c in scan.commits.values() {
        parents.extend(c.parents.iter().copied());
    }
    let mut heads: Vec<Hash> = scan
        .commits
        .keys()
        .copied()
        .filter(|h| !parents.contains(h))
        .collect();
    heads.sort_by_key(|h| h.to_hex()); // deterministic naming
    if heads.len() == 1 {
        branches.push((DEFAULT_BRANCH.to_string(), heads[0]));
    } else {
        for (i, h) in heads.into_iter().enumerate() {
            branches.push((format!("recovered-{}", i + 1), h));
        }
    }
    (branches, None, false)
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
