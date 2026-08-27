//! Offline → reconnect reconciliation (`docs/DESIGN.md` §4b/§4c).
//!
//! §4b promises that SQLite is origofs's *solo/offline* mode and that "solo edits
//! reconcile on reconnect via the same merge machinery (§4c)". This module is that
//! path. It moves commits between **two independent workspaces that may not share
//! either backend** — a laptop's SQLite + local CAS and a team's Postgres + S3 —
//! and then hands the divergence to the existing three-way merge engine
//! ([`crate::merge`]) rather than growing a second one.
//!
//! Two operations:
//!
//! - [`transfer`] — copy the commit closure reachable from a head into another
//!   content store, stopping at any object the destination already has. This is a
//!   "fetch" or a "push" depending on which way you point it.
//! - [`resync`] — the reconnect itself: fetch, decide (up-to-date / fast-forward
//!   either way / merge), and push the result back under a `cas_ref` so a
//!   concurrent remote writer is never clobbered.
//!
//! # What travels, and what does not
//!
//! Commits, trees, blob manifests, chunks and symlink-target blobs travel: they
//! are content-addressed, so copying them is idempotent and a retry is free.
//!
//! **Attribution does not travel for free.** As `CLAUDE.md` puts it, "the content
//! store can rebuild the DB, but not attribution" — blame, the op-log, and the
//! actor registry live only in the metadata DB, and the two sides' DBs number
//! their actors and sessions independently. Because blame is keyed by *content
//! hash* (`docs/DESIGN.md` §4d / M9), it is nonetheless tractable, and [`resync`]
//! does carry it, in **both** directions:
//!
//! - every file blob in the transferred head's snapshot has its byte-range blame
//!   map copied to the other side, with each `actor_id`/`session_id` **remapped**
//!   into the destination's registry (see [`IdentityMap`]);
//! - actors are matched on `auth_subject`, so the same person or agent resolves to
//!   one actor on repeated resyncs rather than accumulating duplicates. An actor
//!   with no `auth_subject` gets a stable synthetic one
//!   (`origofs-resync:<kind>:<display name>`) so it is still idempotent;
//! - sessions have no external identity, so each source session is re-minted as a
//!   fresh destination session (client `origofs-resync`) — grouping *within* one
//!   resync is preserved, which is what `revert_session` needs, but the same
//!   offline session resynced twice is not deduplicated.
//!
//! Deliberately **not** carried: the `edit_op` log (keyed by inode and local
//! session, neither of which survives the crossing), the audit log, the change
//! feed, presence, pending suggestions, and locks. Blame of *historical*
//! (non-head) content versions is not carried either — `blame` answers for the
//! file as it is now, and that is the snapshot we copy.
//!
//! # Preconditions, and why
//!
//! - **Both workspaces must have versioning enabled.** With `versioning = off`
//!   there is no commit DAG to reconcile; rather than silently doing something
//!   else (a whole-tree overwrite, say), [`resync`] returns
//!   [`OrigoFSError::InvalidArgument`].
//! - **The local workspace must have `branch` checked out**, because the merge
//!   engine merges *into the current branch*.
//! - **Both working trees must be clean** (no changes relative to their branch
//!   head). Locally this is forced: a merge rewrites the working tree, so
//!   uncommitted work would be destroyed. Remotely it is forced only when the
//!   remote actually has `branch` checked out — advancing a branch ref underneath
//!   a dirty working tree would leave the shared filesystem's next commit quietly
//!   reverting the merge. A branch the remote does *not* have checked out is just
//!   a ref, and moves freely. Commit (or discard) first, then resync.
//!
//! # Concurrency
//!
//! The remote ref only ever moves by [`MetadataStore::cas_ref`], expecting the
//! head we merged against. If another writer advanced it in the meantime the swap
//! fails and the whole attempt is retried against the new remote head — up to
//! [`MAX_ATTEMPTS`] times — rather than force-writing over them. A conflicted
//! merge never advances the remote ref at all.

use crate::attribution::{Actor, ActorInit};
use crate::chunk::Manifest;
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::merge::{Conflict, MergeOutcome};
use crate::metadata::MetadataStore;
use crate::objectgraph::{Commit, Tree, TreeKind};
use crate::types::Hash;
use std::collections::{HashMap, HashSet};

/// How many times [`resync`] re-reads the remote head and retries after losing a
/// `cas_ref` race. Each retry re-merges against the new remote head, so a handful
/// is plenty; the bound only stops a pathologically busy branch from spinning.
pub const MAX_ATTEMPTS: usize = 5;

/// Client string recorded on sessions minted in the destination workspace to
/// stand in for a source session (see the module docs on identity mapping).
const RESYNC_CLIENT: &str = "origofs-resync";

/// How much moved in one direction of a [`transfer`].
/// `#[non_exhaustive]`: callers read this, they never construct it, so adding a
/// counter should not be a breaking change. (Config structs like `S3Config` are
/// deliberately left constructible — a caller has to be able to build those, and
/// `..Default::default()` already absorbs new fields there.)
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransferStats {
    /// Objects actually written to the destination.
    pub objects: usize,
    /// Bytes those objects occupy (uncompressed, as addressed).
    pub bytes: u64,
    /// Objects the destination already had, where the walk stopped.
    pub skipped: usize,
}

impl TransferStats {
    fn add(&mut self, other: TransferStats) {
        self.objects += other.objects;
        self.bytes += other.bytes;
        self.skipped += other.skipped;
    }
}

/// What a [`resync`] did.
// Deliberately NOT `#[non_exhaustive]`. This is an *outcome* enum: its whole
// purpose is to make the caller handle each case, so a new variant should be a
// compile error at every call site. `non_exhaustive` would force a wildcard arm
// that silently swallows it instead — the opposite of the intent. Adding a
// variant here is a breaking change on purpose.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResyncOutcome {
    /// Both sides were already at the same commit (or neither had one).
    UpToDate,
    /// The remote branch fast-forwarded to the local head.
    Pushed(Hash),
    /// The local branch fast-forwarded to the remote head.
    FastForwarded(Hash),
    /// The two had diverged; a merge commit was made locally and pushed.
    Merged(Hash),
    /// The two had diverged and the merge conflicted. The conflicts are in the
    /// **local** working tree with `MERGE_HEAD` set, exactly as an ordinary
    /// `merge` leaves them; the remote ref was not advanced.
    Conflicted,
}

impl ResyncOutcome {
    /// A short, stable word for logs and the CLI.
    pub fn as_str(&self) -> &'static str {
        match self {
            ResyncOutcome::UpToDate => "up-to-date",
            ResyncOutcome::Pushed(_) => "pushed",
            ResyncOutcome::FastForwarded(_) => "fast-forward",
            ResyncOutcome::Merged(_) => "merged",
            ResyncOutcome::Conflicted => "conflicted",
        }
    }

    /// The commit the branch ended up at, when the resync moved one.
    pub fn head(&self) -> Option<Hash> {
        match self {
            ResyncOutcome::Pushed(h)
            | ResyncOutcome::FastForwarded(h)
            | ResyncOutcome::Merged(h) => Some(*h),
            _ => None,
        }
    }
}

/// The result of a [`resync`]: what happened, what moved, and what needs a human.
/// `#[non_exhaustive]`: callers read this, they never construct it, so adding a
/// counter should not be a breaking change. (Config structs like `S3Config` are
/// deliberately left constructible — a caller has to be able to build those, and
/// `..Default::default()` already absorbs new fields there.)
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ResyncReport {
    /// The branch that was reconciled.
    pub branch: String,
    pub outcome: ResyncOutcome,
    /// Objects copied remote → local.
    pub fetched: TransferStats,
    /// Objects copied local → remote.
    pub pushed: TransferStats,
    /// Blob blame maps carried remote → local.
    pub blame_fetched: usize,
    /// Blob blame maps carried local → remote.
    pub blame_pushed: usize,
    /// Unresolved conflicts, when the outcome is [`ResyncOutcome::Conflicted`].
    pub conflicts: Vec<Conflict>,
    /// Paths merged while an open live CRDT document may have been ahead of their
    /// durable bytes (see [`Fs::merge_live`]). Advisory — the merge still ran.
    pub stale_live_paths: Vec<String>,
    /// How many times a lost `cas_ref` race forced a retry.
    pub cas_retries: usize,
    /// Whether the remote's working tree was rematerialized at the new head (only
    /// when the remote had `branch` checked out).
    pub remote_tree_updated: bool,
}

impl ResyncReport {
    fn new(branch: &str) -> Self {
        Self {
            branch: branch.to_string(),
            outcome: ResyncOutcome::UpToDate,
            fetched: TransferStats::default(),
            pushed: TransferStats::default(),
            blame_fetched: 0,
            blame_pushed: 0,
            conflicts: Vec::new(),
            stale_live_paths: Vec::new(),
            cas_retries: 0,
            remote_tree_updated: false,
        }
    }
}

/// The kind of object a hash addresses, so the walk knows how to read its edges.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ObjKind {
    Commit,
    Tree,
    /// A blob *manifest* (a chunk list) — not the file bytes.
    Manifest,
    /// A chunk or a symlink-target blob: no outgoing edges.
    Leaf,
}

/// Copy every object reachable from commit `head` in `from`'s content store into
/// `to`'s, stopping at any object `to` already has.
///
/// The walk is the same one GC marks with (commits → trees → manifests → chunks
/// and symlink blobs), but writing instead of marking, and cut short at objects
/// the destination already holds.
///
/// **The cut is only sound because objects are written children-first.** The
/// presence of an object implies the presence of its whole closure, so a walk may
/// stop at it: an interrupted transfer leaves a *prefix* of the closure behind,
/// never a hole under a present parent, and re-running it resumes correctly.
///
/// That ordering needs a real reverse topological sort, which is why discovery
/// and ordering are separate passes. Recording nodes in DFS *pop* order and
/// writing that list reversed is not equivalent, and fails on ordinary input: a
/// commit that leaves a file untouched reuses the previous commit's manifest, so
/// the manifest is discovered while walking the *older* commit — before the newer
/// tree that points at it — and reversing then puts the tree first. A crash in
/// between leaves that tree present with its manifest missing, and every later
/// transfer cuts at the tree and reports success over a hole.
///
/// Content writes are content-addressed and idempotent, so a retry — or two
/// concurrent transfers of overlapping history — costs bytes, never correctness.
pub async fn transfer<M1, C1, M2, C2>(
    from: &Fs<M1, C1>,
    to: &Fs<M2, C2>,
    head: Hash,
) -> Result<TransferStats>
where
    M1: MetadataStore,
    C1: ContentStore,
    M2: MetadataStore,
    C2: ContentStore,
{
    // Phase 1 — discover the closure, cutting at anything the destination
    // already has, and record each object's outgoing edges for phase 2.
    let mut seen: HashSet<Hash> = HashSet::new();
    let mut edges: HashMap<Hash, Vec<Hash>> = HashMap::new();
    let mut stats = TransferStats::default();
    let mut stack = vec![(head, ObjKind::Commit)];

    while let Some((hash, kind)) = stack.pop() {
        if !seen.insert(hash) {
            continue;
        }
        if to.content.has(&hash).await? {
            stats.skipped += 1;
            continue;
        }
        let children: Vec<(Hash, ObjKind)> = match kind {
            ObjKind::Commit => {
                let commit = Commit::decode(&from.get_object(&hash).await?)?;
                std::iter::once((commit.tree, ObjKind::Tree))
                    .chain(commit.parents.into_iter().map(|p| (p, ObjKind::Commit)))
                    .collect()
            }
            ObjKind::Tree => {
                let tree = Tree::decode(&from.get_object(&hash).await?)?;
                tree.entries
                    .into_iter()
                    .map(|e| {
                        let k = match e.kind {
                            TreeKind::Dir => ObjKind::Tree,
                            TreeKind::File => ObjKind::Manifest,
                            TreeKind::Symlink => ObjKind::Leaf,
                        };
                        (e.hash, k)
                    })
                    .collect()
            }
            ObjKind::Manifest => Manifest::decode(&from.get_object(&hash).await?)?
                .chunks
                .into_iter()
                .map(|c| (c.hash, ObjKind::Leaf))
                .collect(),
            ObjKind::Leaf => Vec::new(),
        };
        edges.insert(hash, children.iter().map(|(h, _)| *h).collect());
        stack.extend(children);
    }

    // Phase 2 — order the discovered objects children-first: an iterative
    // post-order over the edges recorded above. Iterative because history depth
    // and tree depth are both unbounded, and the objects are a DAG rather than a
    // tree, so a node reachable by several paths must be emitted exactly once —
    // after all of its children, whichever path got there first.
    //
    // Edges pointing outside `edges` are objects the destination already has (the
    // cut) and are simply not traversed. There are no cycles to break: an object's
    // address is a hash of its bytes, so it cannot name itself or an ancestor.
    let mut order: Vec<Hash> = Vec::with_capacity(edges.len());
    let mut emitted: HashSet<Hash> = HashSet::new();
    let mut walk: Vec<(Hash, bool)> = Vec::new();
    if edges.contains_key(&head) {
        walk.push((head, false));
    }
    while let Some((hash, expanded)) = walk.pop() {
        if emitted.contains(&hash) {
            continue;
        }
        if expanded {
            emitted.insert(hash);
            order.push(hash);
            continue;
        }
        // Re-push as expanded *under* the children, so it is popped again — and
        // emitted — only once every child has been.
        walk.push((hash, true));
        for child in edges.get(&hash).into_iter().flatten() {
            if edges.contains_key(child) && !emitted.contains(child) {
                walk.push((*child, false));
            }
        }
    }
    debug_assert_eq!(
        order.len(),
        edges.len(),
        "every discovered object is reachable from head, so all must be ordered"
    );

    // Phase 3 — write, children before parents.
    for hash in &order {
        let bytes = from.get_object(hash).await?;
        let written = to.put_object(&bytes).await?;
        // Both stores address by BLAKE3 of the same plaintext, so a mismatch means
        // one of them is not addressing what it claims — refuse rather than build
        // a tree that points at bytes nobody can resolve.
        if written != *hash {
            return Err(OrigoFSError::Corrupt(format!(
                "transfer wrote {} but the source addressed it as {}",
                written.to_hex(),
                hash.to_hex()
            )));
        }
        stats.objects += 1;
        stats.bytes += bytes.len() as u64;
    }
    // Durability barrier before any ref can point at this (mirrors `commit`).
    to.content.flush().await?;
    Ok(stats)
}

/// Every file blob-manifest hash in the snapshot commit `head` points at.
async fn snapshot_manifests<M, C>(fs: &Fs<M, C>, head: Hash) -> Result<HashSet<Hash>>
where
    M: MetadataStore,
    C: ContentStore,
{
    let commit = Commit::decode(&fs.get_object(&head).await?)?;
    let mut manifests = HashSet::new();
    let mut seen_trees = HashSet::new();
    let mut stack = vec![commit.tree];
    while let Some(t) = stack.pop() {
        if !seen_trees.insert(t) {
            continue;
        }
        for e in Tree::decode(&fs.get_object(&t).await?)?.entries {
            match e.kind {
                TreeKind::Dir => stack.push(e.hash),
                TreeKind::File => {
                    manifests.insert(e.hash);
                }
                TreeKind::Symlink => {}
            }
        }
    }
    Ok(manifests)
}

/// A memo of source-identity → destination-identity for one resync run.
///
/// Actor ids and session ids are database-local integers, so a blame map copied
/// verbatim would credit whoever happens to hold that id on the other side. This
/// translates them: actors through `auth_subject` (stable across runs), sessions
/// by minting a fresh destination session per source session (they have no
/// external identity to match on).
#[derive(Default)]
pub struct IdentityMap {
    actors: HashMap<i64, Option<i64>>,
    sessions: HashMap<(i64, i64), i64>,
}

/// How deep a controller ("this agent was launched by …") chain is followed when
/// recreating an actor on the far side. Deep chains are pathological; the bound
/// also breaks a cycle in a corrupt registry.
const MAX_CONTROLLER_DEPTH: usize = 8;

impl IdentityMap {
    /// The destination actor id for `src`, creating it if needed. `None` when the
    /// source registry has no such actor — the blame that names it cannot be
    /// honestly re-attributed, so its blob is skipped rather than mis-credited.
    async fn actor<M1, C1, M2, C2>(
        &mut self,
        from: &Fs<M1, C1>,
        to: &Fs<M2, C2>,
        src: i64,
    ) -> Result<Option<i64>>
    where
        M1: MetadataStore,
        C1: ContentStore,
        M2: MetadataStore,
        C2: ContentStore,
    {
        if let Some(cached) = self.actors.get(&src) {
            return Ok(*cached);
        }
        // Walk the controller chain up first, then recreate it top-down, so an
        // agent's `controller_actor_id` still points at its human on the far side.
        let mut chain: Vec<Actor> = Vec::new();
        let mut cursor = Some(src);
        let mut guard: HashSet<i64> = HashSet::new();
        while let Some(id) = cursor {
            if chain.len() >= MAX_CONTROLLER_DEPTH || !guard.insert(id) {
                break;
            }
            match from.get_actor(id).await? {
                Some(a) => {
                    cursor = a.controller_actor_id;
                    chain.push(a);
                }
                None => {
                    // A dangling controller link is tolerable (the chain simply
                    // ends); a dangling *root* actor is not.
                    if chain.is_empty() {
                        self.actors.insert(src, None);
                        return Ok(None);
                    }
                    break;
                }
            }
        }

        let mut controller: Option<i64> = None;
        for actor in chain.iter().rev() {
            let mapped = match self.actors.get(&actor.id).and_then(|m| *m) {
                Some(existing) => existing,
                None => {
                    // Match on the caller's own identity when it has one, so the
                    // same person/agent converges on one destination actor across
                    // repeated resyncs; otherwise mint a stable synthetic subject
                    // for the same reason.
                    let subject = actor.auth_subject.clone().unwrap_or_else(|| {
                        format!(
                            "{RESYNC_CLIENT}:{}:{}",
                            actor.kind.as_str(),
                            actor.display_name
                        )
                    });
                    let id = to
                        .find_or_create_actor(ActorInit {
                            kind: Some(actor.kind),
                            display_name: actor.display_name.clone(),
                            auth_subject: Some(subject),
                            agent_model: actor.agent_model.clone(),
                            agent_vendor: actor.agent_vendor.clone(),
                            controller_actor_id: controller,
                        })
                        .await?;
                    self.actors.insert(actor.id, Some(id));
                    id
                }
            };
            controller = Some(mapped);
        }
        Ok(self.actors.get(&src).copied().flatten())
    }

    /// The destination session id standing in for source session `src_session`
    /// (owned by destination actor `dst_actor`). `0` — the blame map's "no
    /// session" sentinel — maps to itself.
    async fn session<M2, C2>(
        &mut self,
        to: &Fs<M2, C2>,
        src_actor: i64,
        src_session: i64,
        dst_actor: i64,
    ) -> Result<i64>
    where
        M2: MetadataStore,
        C2: ContentStore,
    {
        if src_session == 0 {
            return Ok(0);
        }
        if let Some(existing) = self.sessions.get(&(src_actor, src_session)) {
            return Ok(*existing);
        }
        let id = to.create_session(dst_actor, Some(RESYNC_CLIENT)).await?;
        self.sessions.insert((src_actor, src_session), id);
        Ok(id)
    }
}

/// One `actor,session,len` run of a blame map. The encoding is
/// `crate::attribution`'s (`actor,session,len` joined by `;`); it is parsed here
/// rather than shared because the map type is private to that module and this is
/// the only other place that needs to *rewrite* rather than read one.
struct Run {
    actor: i64,
    session: i64,
    len: u64,
}

fn parse_blame(s: &str) -> Option<Vec<Run>> {
    s.split(';')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut it = p.split(',');
            Some(Run {
                actor: it.next()?.parse().ok()?,
                session: it.next()?.parse().ok()?,
                len: it.next()?.parse().ok()?,
            })
        })
        .collect()
}

/// Re-encode runs, coalescing neighbours that became the same author under the
/// remapping (keeping the canonical form `BlameMap::from_spans` produces).
fn encode_blame(runs: &[Run]) -> String {
    let mut out: Vec<(i64, i64, u64)> = Vec::with_capacity(runs.len());
    for r in runs {
        if r.len == 0 {
            continue;
        }
        match out.last_mut() {
            Some(last) if last.0 == r.actor && last.1 == r.session => last.2 += r.len,
            _ => out.push((r.actor, r.session, r.len)),
        }
    }
    out.iter()
        .map(|(a, s, l)| format!("{a},{s},{l}"))
        .collect::<Vec<_>>()
        .join(";")
}

/// Copy the byte-range blame maps of every file in `head`'s snapshot from `from`
/// to `to`, remapping actor and session ids through `ids`. Returns how many maps
/// were written.
///
/// Blame is keyed by content hash, so this is a pure metadata copy alongside the
/// content [`transfer`] already did — no bytes are re-read. A blob the destination
/// already has blame for is left alone (blame travels *with* a content version,
/// so re-deriving it would only churn); a blob naming an actor the source registry
/// cannot resolve is skipped whole, because a partial remap would silently
/// mis-credit lines.
pub async fn carry_blame<M1, C1, M2, C2>(
    from: &Fs<M1, C1>,
    to: &Fs<M2, C2>,
    head: Hash,
    ids: &mut IdentityMap,
) -> Result<usize>
where
    M1: MetadataStore,
    C1: ContentStore,
    M2: MetadataStore,
    C2: ContentStore,
{
    let mut carried = 0usize;
    for manifest in snapshot_manifests(from, head).await? {
        let Some(encoded) = from.meta.get_blob_blame(&manifest).await? else {
            continue;
        };
        if to.meta.get_blob_blame(&manifest).await?.is_some() {
            continue;
        }
        let Some(runs) = parse_blame(&encoded) else {
            continue;
        };
        let mut remapped = Vec::with_capacity(runs.len());
        let mut resolvable = true;
        for r in runs {
            let Some(actor) = ids.actor(from, to, r.actor).await? else {
                resolvable = false;
                break;
            };
            let session = ids.session(to, r.actor, r.session, actor).await?;
            remapped.push(Run {
                actor,
                session,
                len: r.len,
            });
        }
        if !resolvable {
            tracing::warn!(
                blob = %manifest.to_hex(),
                "resync: skipping blame for a blob whose author is not in the source registry"
            );
            continue;
        }
        to.meta
            .set_blob_blame(&manifest, &encode_blame(&remapped))
            .await?;
        carried += 1;
    }
    Ok(carried)
}

/// Reconcile an offline/solo workspace with a shared one over `branch`, using the
/// ordinary three-way merge engine for any divergence.
///
/// The algorithm, per attempt:
///
/// 1. Read both heads. Fetch the remote head into `local` **first**, so every
///    ancestry question can be answered from the local object store.
/// 2. Equal heads → [`ResyncOutcome::UpToDate`], nothing written.
/// 3. Remote head is an ancestor of ours → push and `cas_ref` the remote branch
///    forward ([`ResyncOutcome::Pushed`]).
/// 4. Ours is an ancestor of the remote head → the merge engine fast-forwards the
///    local branch and working tree ([`ResyncOutcome::FastForwarded`]).
/// 5. Otherwise diverged → `merge()` produces a merge commit locally, which is
///    then pushed and `cas_ref`-ed onto the remote branch
///    ([`ResyncOutcome::Merged`]). If the merge conflicts, the conflicts are left
///    in the *local* working tree exactly as a normal merge leaves them and the
///    remote ref is untouched ([`ResyncOutcome::Conflicted`]) — resolve, commit,
///    and resync again.
///
/// A lost `cas_ref` race re-runs the whole attempt against the new remote head,
/// up to [`MAX_ATTEMPTS`] times, instead of force-writing. See the module docs for
/// the preconditions (clean trees, versioning on, `branch` checked out locally),
/// what attribution is carried, and what is not.
pub async fn resync<M1, C1, M2, C2>(
    local: &Fs<M1, C1>,
    remote: &Fs<M2, C2>,
    branch: &str,
    author: &str,
    message: &str,
) -> Result<ResyncReport>
where
    M1: MetadataStore,
    C1: ContentStore,
    M2: MetadataStore,
    C2: ContentStore,
{
    if !local.versioning_mode().await?.commits_enabled()
        || !remote.versioning_mode().await?.commits_enabled()
    {
        return Err(OrigoFSError::InvalidArgument(
            "resync needs a commit DAG on both sides; this workspace has versioning = off \
             (there is nothing to reconcile — set `native` or `git` first)"
                .into(),
        ));
    }
    if local.current_branch().await?.as_deref() != Some(branch) {
        return Err(OrigoFSError::InvalidArgument(format!(
            "resync merges into the current branch; check out {branch} locally first"
        )));
    }

    let mut ids_push = IdentityMap::default();
    let mut ids_fetch = IdentityMap::default();
    let mut report = ResyncReport::new(branch);
    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 {
            report.cas_retries += 1;
        }
        let done = attempt_resync(
            local,
            remote,
            branch,
            author,
            message,
            &mut report,
            &mut ids_push,
            &mut ids_fetch,
        )
        .await?;
        if done {
            return Ok(report);
        }
    }
    Err(OrigoFSError::Conflict(format!(
        "remote branch {branch} kept moving; gave up after {MAX_ATTEMPTS} resync attempts"
    )))
}

/// One resync attempt. `Ok(true)` when it settled; `Ok(false)` when it lost a
/// `cas_ref` race on the remote branch and should be retried against the new head.
#[allow(clippy::too_many_arguments)]
async fn attempt_resync<M1, C1, M2, C2>(
    local: &Fs<M1, C1>,
    remote: &Fs<M2, C2>,
    branch: &str,
    author: &str,
    message: &str,
    report: &mut ResyncReport,
    ids_push: &mut IdentityMap,
    ids_fetch: &mut IdentityMap,
) -> Result<bool>
where
    M1: MetadataStore,
    C1: ContentStore,
    M2: MetadataStore,
    C2: ContentStore,
{
    if !local.status().await?.is_empty() {
        return Err(OrigoFSError::InvalidArgument(
            "resync rewrites the local working tree; commit or discard your changes first".into(),
        ));
    }
    // The remote's tree is only at stake when it actually has `branch` checked
    // out; otherwise the branch is just a ref and moves freely.
    let remote_checked_out = remote.current_branch().await?.as_deref() == Some(branch);
    if remote_checked_out && !remote.status().await?.is_empty() {
        return Err(OrigoFSError::InvalidArgument(format!(
            "the remote workspace has uncommitted changes on {branch}; \
             commit them there before resyncing"
        )));
    }

    let local_head = local.branch_head(branch).await?;
    let remote_head = remote.branch_head(branch).await?;

    match (local_head, remote_head) {
        // Neither side has ever committed: nothing to reconcile.
        (None, None) => {
            report.outcome = ResyncOutcome::UpToDate;
            Ok(true)
        }
        // First sync of a workspace that has no history yet: fetch and adopt.
        (None, Some(rh)) => {
            report.fetched.add(transfer(remote, local, rh).await?);
            report.blame_fetched += carry_blame(remote, local, rh, ids_fetch).await?;
            if !local.cas_branch(branch, None, rh).await? {
                return Ok(false);
            }
            local.checkout(branch).await?;
            report.outcome = ResyncOutcome::FastForwarded(rh);
            Ok(true)
        }
        // The remote has never seen this branch: a first push.
        (Some(lh), None) => {
            push(
                local,
                remote,
                branch,
                lh,
                None,
                remote_checked_out,
                report,
                ids_push,
            )
            .await
        }
        (Some(lh), Some(rh)) => {
            if lh == rh {
                report.outcome = ResyncOutcome::UpToDate;
                return Ok(true);
            }
            // Fetch first: every ancestry question below is answered from the
            // local object store, so it must hold the remote head's closure.
            report.fetched.add(transfer(remote, local, rh).await?);
            report.blame_fetched += carry_blame(remote, local, rh, ids_fetch).await?;

            if local.is_ancestor(rh, lh).await? {
                // We are strictly ahead: the remote fast-forwards.
                return push(
                    local,
                    remote,
                    branch,
                    lh,
                    Some(rh),
                    remote_checked_out,
                    report,
                    ids_push,
                )
                .await;
            }

            // Behind or diverged — hand it to the merge engine, which fast-forwards
            // or three-way merges as appropriate (and records conflicts the usual way).
            let (outcome, stale) = local.merge_live(rh, author, message).await?;
            for doc in stale {
                if !report.stale_live_paths.contains(&doc.path) {
                    report.stale_live_paths.push(doc.path);
                }
            }
            match outcome {
                MergeOutcome::AlreadyUpToDate => {
                    report.outcome = ResyncOutcome::UpToDate;
                    Ok(true)
                }
                MergeOutcome::FastForward(h) => {
                    report.outcome = ResyncOutcome::FastForwarded(h);
                    Ok(true)
                }
                MergeOutcome::Conflicts(conflicts) => {
                    report.conflicts = conflicts;
                    report.outcome = ResyncOutcome::Conflicted;
                    Ok(true)
                }
                MergeOutcome::Merged(h) => {
                    let settled = push(
                        local,
                        remote,
                        branch,
                        h,
                        Some(rh),
                        remote_checked_out,
                        report,
                        ids_push,
                    )
                    .await?;
                    if settled {
                        report.outcome = ResyncOutcome::Merged(h);
                    }
                    Ok(settled)
                }
            }
        }
    }
}

/// Transfer `head` to the remote, carry its blame, and CAS the remote branch onto
/// it. `Ok(false)` means the CAS was lost and the caller should retry.
#[allow(clippy::too_many_arguments)]
async fn push<M1, C1, M2, C2>(
    local: &Fs<M1, C1>,
    remote: &Fs<M2, C2>,
    branch: &str,
    head: Hash,
    expect: Option<Hash>,
    remote_checked_out: bool,
    report: &mut ResyncReport,
    ids: &mut IdentityMap,
) -> Result<bool>
where
    M1: MetadataStore,
    C1: ContentStore,
    M2: MetadataStore,
    C2: ContentStore,
{
    report.pushed.add(transfer(local, remote, head).await?);
    // Carry attribution before the ref moves: blame is keyed by content hash, so
    // writing it early is harmless if the CAS is then lost, and it is never
    // missing for a head the remote has already adopted.
    report.blame_pushed += carry_blame(local, remote, head, ids).await?;
    if !remote.cas_branch(branch, expect, head).await? {
        return Ok(false);
    }
    if remote_checked_out {
        // The remote has this branch checked out and (checked above) a clean tree,
        // so materialize the new head — otherwise the shared filesystem would keep
        // serving the pre-merge files.
        remote.checkout(branch).await?;
        report.remote_tree_updated = true;
    }
    report.outcome = ResyncOutcome::Pushed(head);
    Ok(true)
}
