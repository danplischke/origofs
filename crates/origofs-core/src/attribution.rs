//! Attribution & provenance (`docs/DESIGN.md` §4d): who edited which lines.
//!
//! Every attributed write ([`Fs::write_as`]) records an append-only [`EditOp`]
//! (the durable ground truth, linked to an actor/session/tool-call) and stores a
//! line-level authorship map. [`Fs::blame`] then reports, per line range, whether
//! a **human** or **agent** wrote it — so a shared human+agent workspace can
//! always tell who did what.
//!
//! Blame is keyed by **content version** — a blob's manifest hash — not by inode
//! (M9). Because the map travels with the bytes it describes, blame survives
//! checkout (the tree is rebuilt, but each inode points back at the same content)
//! and can never desync from the file it annotates: a version with no recorded
//! authorship — e.g. one produced by a plain, non-attributed [`Fs::write`] —
//! simply blames to nothing rather than showing a previous version's runs (H7).
//! Attribution is also move- and whitespace-aware, so a re-indent or a reorder
//! keeps a line's original author instead of crediting the reformatter (M10).

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::types::{Hash, Ino};
use similar::{ChangeTag, TextDiff};
use std::collections::{HashMap, VecDeque};

/// Whether an actor is a person, an autonomous agent, or the system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorKind {
    Human,
    Agent,
    System,
}

impl ActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActorKind::Human => "human",
            ActorKind::Agent => "agent",
            ActorKind::System => "system",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "human" => Some(ActorKind::Human),
            "agent" => Some(ActorKind::Agent),
            "system" => Some(ActorKind::System),
            _ => None,
        }
    }
}

/// Fields to register a new actor.
#[derive(Clone, Debug, Default)]
pub struct ActorInit {
    pub kind: Option<ActorKind>,
    pub display_name: String,
    pub auth_subject: Option<String>,
    pub agent_model: Option<String>,
    pub agent_vendor: Option<String>,
    /// The human/actor that launched this agent (provenance chain).
    pub controller_actor_id: Option<i64>,
}

impl ActorInit {
    pub fn human(display_name: impl Into<String>, auth_subject: Option<String>) -> Self {
        Self {
            kind: Some(ActorKind::Human),
            display_name: display_name.into(),
            auth_subject,
            ..Default::default()
        }
    }
    pub fn agent(
        display_name: impl Into<String>,
        model: impl Into<String>,
        controller: Option<i64>,
    ) -> Self {
        Self {
            kind: Some(ActorKind::Agent),
            display_name: display_name.into(),
            agent_model: Some(model.into()),
            controller_actor_id: controller,
            ..Default::default()
        }
    }
}

/// A registered actor.
#[derive(Clone, Debug)]
pub struct Actor {
    pub id: i64,
    pub kind: ActorKind,
    pub display_name: String,
    pub auth_subject: Option<String>,
    pub agent_model: Option<String>,
    pub agent_vendor: Option<String>,
    pub controller_actor_id: Option<i64>,
    pub created_at: i64,
}

/// A recorded tool-call audit entry (agentfs-style), optionally linked from edits.
#[derive(Clone, Debug, Default)]
pub struct ToolCallInit {
    pub session_id: Option<i64>,
    pub actor_id: Option<i64>,
    pub name: String,
    pub parameters: Option<String>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub started_at: i64,
    pub completed_at: i64,
    pub duration_ms: i64,
}

/// Fields for an append-only edit-op log entry.
#[derive(Clone, Debug)]
pub struct EditOpInit {
    pub session_id: Option<i64>,
    pub actor_id: i64,
    pub tool_call_id: Option<i64>,
    pub ino: Ino,
    pub path: String,
    pub op: String,
    pub byte_start: i64,
    pub byte_len: i64,
    pub pre_hash: Option<String>,
    pub post_hash: Option<String>,
    pub ts: i64,
}

/// A stored edit-op log entry.
#[derive(Clone, Debug)]
pub struct EditOp {
    pub id: i64,
    pub session_id: Option<i64>,
    pub actor_id: i64,
    pub tool_call_id: Option<i64>,
    pub ino: Ino,
    pub path: String,
    pub op: String,
    pub byte_start: i64,
    pub byte_len: i64,
    pub pre_hash: Option<String>,
    pub post_hash: Option<String>,
    pub ts: i64,
}

/// The actor context for an attributed write.
#[derive(Clone, Copy, Debug)]
pub struct WriteCtx {
    pub actor: i64,
    pub session: Option<i64>,
    pub tool_call: Option<i64>,
}

impl WriteCtx {
    pub fn actor(actor: i64) -> Self {
        Self {
            actor,
            session: None,
            tool_call: None,
        }
    }
    pub fn session(actor: i64, session: i64) -> Self {
        Self {
            actor,
            session: Some(session),
            tool_call: None,
        }
    }
    fn sid(&self) -> i64 {
        self.session.unwrap_or(0)
    }
}

/// One coalesced authorship run over consecutive lines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlameRun {
    actor: i64,
    session: i64,
    lines: u32,
}

/// A file's line-authorship map, stored as `actor,session,lines;...`.
#[derive(Clone, Debug, Default)]
struct BlameMap {
    runs: Vec<BlameRun>,
}

impl BlameMap {
    fn from_per_line(authors: &[(i64, i64)]) -> Self {
        let mut runs: Vec<BlameRun> = Vec::new();
        for &(actor, session) in authors {
            match runs.last_mut() {
                Some(r) if r.actor == actor && r.session == session => r.lines += 1,
                _ => runs.push(BlameRun {
                    actor,
                    session,
                    lines: 1,
                }),
            }
        }
        BlameMap { runs }
    }

    fn per_line(&self) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        for r in &self.runs {
            for _ in 0..r.lines {
                out.push((r.actor, r.session));
            }
        }
        out
    }

    fn encode(&self) -> String {
        self.runs
            .iter()
            .map(|r| format!("{},{},{}", r.actor, r.session, r.lines))
            .collect::<Vec<_>>()
            .join(";")
    }

    fn decode(s: &str) -> BlameMap {
        let runs = s
            .split(';')
            .filter(|p| !p.is_empty())
            .filter_map(|p| {
                let mut it = p.split(',');
                Some(BlameRun {
                    actor: it.next()?.parse().ok()?,
                    session: it.next()?.parse().ok()?,
                    lines: it.next()?.parse().ok()?,
                })
            })
            .collect();
        BlameMap { runs }
    }
}

/// One blame result: an inclusive 1-based line range and its author.
#[derive(Clone, Debug)]
pub struct BlameRange {
    pub line_start: u32,
    pub line_end: u32,
    pub actor: Actor,
    pub session: Option<i64>,
}

fn is_text(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok()
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    // --- registry ---------------------------------------------------------

    pub async fn create_actor(&self, init: ActorInit) -> Result<i64> {
        self.meta.create_actor(init).await
    }

    pub async fn get_actor(&self, id: i64) -> Result<Option<Actor>> {
        self.meta.get_actor(id).await
    }

    /// Look up an actor by external identity (`auth_subject`), if registered.
    pub async fn actor_by_subject(&self, subject: &str) -> Result<Option<Actor>> {
        self.meta.actor_by_subject(subject).await
    }

    /// Every registered actor, oldest first — for resolving the bare `actor_id`
    /// carried by events, suggestions, and presence to a name + kind.
    pub async fn list_actors(&self) -> Result<Vec<Actor>> {
        self.meta.list_actors().await
    }

    /// Idempotently map an external identity to an actor: return the actor already
    /// registered for `init.auth_subject`, or create one and return its id. This
    /// is how an application binds its own user id to an origofs actor without keeping
    /// a side table. Race-safe: a concurrent create that loses the unique-index
    /// race resolves to the winner. Requires `init.auth_subject` to be set.
    pub async fn find_or_create_actor(&self, init: ActorInit) -> Result<i64> {
        let subject = init.auth_subject.clone().ok_or_else(|| {
            OrigoFSError::InvalidArgument("find_or_create_actor requires auth_subject".into())
        })?;
        if let Some(a) = self.meta.actor_by_subject(&subject).await? {
            return Ok(a.id);
        }
        match self.meta.create_actor(init).await {
            Ok(id) => Ok(id),
            // A concurrent writer may have created it between our lookup and
            // insert; if the subject now resolves, that's the winner, not an error.
            Err(e) => match self.meta.actor_by_subject(&subject).await? {
                Some(a) => Ok(a.id),
                None => Err(e),
            },
        }
    }

    /// [`find_or_create_actor`](Self::find_or_create_actor) for a human, keyed by
    /// `auth_subject` (e.g. your app's user id / JWT subject).
    pub async fn find_or_create_human(
        &self,
        auth_subject: &str,
        display_name: &str,
    ) -> Result<i64> {
        self.find_or_create_actor(ActorInit::human(
            display_name,
            Some(auth_subject.to_string()),
        ))
        .await
    }

    /// [`find_or_create_actor`](Self::find_or_create_actor) for an agent, keyed by
    /// `auth_subject`.
    pub async fn find_or_create_agent(
        &self,
        auth_subject: &str,
        display_name: &str,
        model: &str,
        controller: Option<i64>,
    ) -> Result<i64> {
        let mut init = ActorInit::agent(display_name, model, controller);
        init.auth_subject = Some(auth_subject.to_string());
        self.find_or_create_actor(init).await
    }

    /// Register a new agent actor whose controller is `controller`.
    pub async fn create_agent(
        &self,
        name: &str,
        model: &str,
        controller: Option<i64>,
    ) -> Result<i64> {
        self.create_actor(ActorInit::agent(name, model, controller))
            .await
    }

    /// Register a new human actor.
    pub async fn create_human(&self, name: &str, auth_subject: Option<&str>) -> Result<i64> {
        self.create_actor(ActorInit::human(name, auth_subject.map(|s| s.to_string())))
            .await
    }

    pub async fn create_session(&self, actor_id: i64, client: Option<&str>) -> Result<i64> {
        self.meta
            .create_session(actor_id, client, self.now_secs())
            .await
    }

    pub async fn record_tool_call(&self, tc: ToolCallInit) -> Result<i64> {
        self.meta.record_tool_call(tc).await
    }

    // --- attributed write -------------------------------------------------

    /// Write `data` to `path`, attributing the change to `ctx`'s actor and
    /// updating per-line authorship. Creates the file if needed.
    pub async fn write_as(&self, ctx: WriteCtx, path: &str, data: &[u8]) -> Result<()> {
        self.write_as_inner(ctx, path, data, None).await
    }

    /// Like [`write_as`](Self::write_as), but applies the write only if `path`'s
    /// current content still equals `expected` (null-safe: `None` = "expected to
    /// be absent/empty"), returning [`OrigoFSError::Conflict`] otherwise. The check
    /// and the write commit atomically, so an accepted suggestion can't clobber a
    /// concurrent update that slipped in after its staleness check (audit #13/#18).
    pub async fn write_as_expecting(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        expected: Option<Hash>,
    ) -> Result<()> {
        self.write_as_inner(ctx, path, data, Some(expected)).await
    }

    async fn write_as_inner(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        expect: Option<Option<Hash>>,
    ) -> Result<()> {
        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;

        // Content durable first (store_body is idempotent, so it's computed once
        // and reused across create-race retries), then commit blame + content +
        // op-log together with the file's creation, so a crash can never leave a
        // visible file with mismatched content/blame or a "successful" write
        // half-recorded (C1). The op-log — the durable attribution ground truth —
        // lands in the same transaction as the content it describes.
        let (mhash, size) = self.store_body(data).await?;

        // The lookup is *before* the transaction, so a concurrent writer can
        // create the same new path in between. On that unique-index failure we
        // roll back and retry: a plain write adopts their inode and applies as an
        // update; a conditional write that required the file to be absent fails
        // with `Conflict` (mirrors `mkdir_p`'s create-race handling).
        for _ in 0..crate::engine::CREATE_RETRIES {
            let existing = self.lookup_file(parent, name, path).await?;

            // Prior content + authorship (reads, before the txn). A new file
            // starts from empty bytes and no authorship.
            let (pre_hash, old_bytes, old_authors) = match existing {
                Some(ino) => {
                    let inode = self
                        .meta
                        .get_inode(ino)
                        .await?
                        .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
                    let pre = inode.content;
                    let bytes = match pre {
                        Some(h) => self.read_body(&h).await?,
                        None => Vec::new(),
                    };
                    // Prior authorship comes from the *content* the inode points
                    // at, so it survives checkout/merge and never desyncs (M9).
                    let authors = match &pre {
                        Some(h) => match self.meta.get_blob_blame(h).await? {
                            Some(s) => BlameMap::decode(&s).per_line(),
                            None => Vec::new(),
                        },
                        None => Vec::new(),
                    };
                    (pre, bytes, authors)
                }
                None => (None, Vec::new(), Vec::new()),
            };

            // Compute the new line authorship against whatever is there now.
            let blame = if is_text(&old_bytes) && is_text(data) {
                let new_authors =
                    diff_authors(&old_bytes, data, &old_authors, (ctx.actor, ctx.sid()));
                BlameMap::from_per_line(&new_authors)
            } else {
                // Binary: file-level attribution (single unit).
                BlameMap::from_per_line(&[(ctx.actor, ctx.sid())])
            };

            let mut tx = self.meta.begin().await?;
            let ino = match existing {
                Some(ino) => ino,
                None => match Self::create_file_in(tx.as_mut(), parent, name).await {
                    Ok(ino) => ino,
                    Err(OrigoFSError::AlreadyExists(_)) => {
                        drop(tx);
                        // A conditional write that required absence can't proceed
                        // once the file exists.
                        if matches!(expect, Some(None)) {
                            return Err(OrigoFSError::Conflict(format!(
                                "{path} was created concurrently"
                            )));
                        }
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            // Blame is keyed by the new content version (its manifest hash); an
            // empty file has no content and no blame.
            if let Some(h) = mhash {
                tx.set_blob_blame(&h, &blame.encode()).await?;
            }
            match &expect {
                None => tx.set_content(ino, mhash, size).await?,
                // Conditional apply: only write if the content is still what the
                // caller based this write on. On mismatch the whole transaction
                // rolls back (undoing the blame staged just above).
                Some(expected) => {
                    if !tx
                        .set_content_if(ino, expected.as_ref(), mhash, size)
                        .await?
                    {
                        return Err(OrigoFSError::Conflict(format!(
                            "{path} changed since the write was based on it"
                        )));
                    }
                }
            }
            tx.append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino,
                path: path.to_string(),
                op: "write".to_string(),
                byte_start: 0,
                byte_len: data.len() as i64,
                pre_hash: pre_hash.map(|h| h.to_hex()),
                post_hash: mhash.map(|h| h.to_hex()),
                ts: self.now_secs(),
            })
            .await?;
            tx.commit().await?;
            return Ok(());
        }
        Err(OrigoFSError::Conflict(format!(
            "{path}: too many concurrent creators"
        )))
    }

    // --- queries ----------------------------------------------------------

    /// Per-line-range authorship for `path`, distinguishing human vs agent.
    pub async fn blame(&self, path: &str) -> Result<Vec<BlameRange>> {
        let ino = self.resolve(path).await?;
        // Blame lives with the content version the inode points at (M9); an empty
        // file, or content with no recorded authorship, blames to nothing.
        let inode = self
            .meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(path.to_string()))?;
        let Some(content) = inode.content else {
            return Ok(Vec::new());
        };
        let map = match self.meta.get_blob_blame(&content).await? {
            Some(s) => BlameMap::decode(&s),
            None => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        let mut line: u32 = 1;
        for r in &map.runs {
            let actor = self
                .meta
                .get_actor(r.actor)
                .await?
                .ok_or_else(|| OrigoFSError::NotFound(format!("actor {}", r.actor)))?;
            out.push(BlameRange {
                line_start: line,
                line_end: line + r.lines - 1,
                actor,
                session: (r.session != 0).then_some(r.session),
            });
            line += r.lines;
        }
        Ok(out)
    }

    /// The edit-op log for `actor` (optionally narrowed to one `session`).
    pub async fn edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        self.meta.list_edit_ops(actor_id, session_id).await
    }

    /// Revert every line an actor wrote in a session, across all files they
    /// touched. Returns the number of files changed. The removed lines are
    /// dropped; remaining lines keep their authorship.
    pub async fn revert_session(&self, actor_id: i64, session_id: i64) -> Result<usize> {
        // Distinct files this actor touched in this session (from the op-log).
        let ops = self.meta.list_edit_ops(actor_id, Some(session_id)).await?;
        let mut paths: Vec<(Ino, String)> = Vec::new();
        for op in ops {
            if !paths.iter().any(|(i, _)| *i == op.ino) {
                paths.push((op.ino, op.path));
            }
        }

        let mut changed = 0;
        for (ino, path) in paths {
            // Blame and the current bytes both come from the content the inode
            // points at (M9); an empty file, or content with no recorded blame,
            // is skipped.
            let Some(inode) = self.meta.get_inode(ino).await? else {
                continue;
            };
            let Some(content_hash) = inode.content else {
                continue;
            };
            let Some(map_s) = self.meta.get_blob_blame(&content_hash).await? else {
                continue;
            };
            let authors = BlameMap::decode(&map_s).per_line();
            let Ok(current) =
                std::str::from_utf8(&self.read_body(&content_hash).await?).map(str::to_owned)
            else {
                continue; // binary: skip line-revert
            };
            let lines: Vec<&str> = split_lines(&current);
            if lines.len() != authors.len() {
                continue; // out of sync; skip conservatively
            }

            let mut kept_body = String::new();
            let mut kept_authors: Vec<(i64, i64)> = Vec::new();
            let mut removed = false;
            for (line, &(a, s)) in lines.iter().zip(authors.iter()) {
                if a == actor_id && s == session_id {
                    removed = true; // drop this line
                } else {
                    kept_body.push_str(line);
                    kept_authors.push((a, s));
                }
            }
            if !removed {
                continue;
            }

            let (mhash, size) = self.store_body(kept_body.as_bytes()).await?;
            // Content, blame, and the revert op-log entry for this file commit
            // atomically, keeping content and authorship in lockstep (C1).
            let mut tx = self.meta.begin().await?;
            tx.set_content(ino, mhash, size).await?;
            if let Some(h) = mhash {
                tx.set_blob_blame(&h, &BlameMap::from_per_line(&kept_authors).encode())
                    .await?;
            }
            tx.append_edit_op(EditOpInit {
                session_id: None,
                actor_id,
                tool_call_id: None,
                ino,
                path,
                op: "revert".to_string(),
                byte_start: 0,
                byte_len: size as i64,
                pre_hash: None,
                post_hash: mhash.map(|h| h.to_hex()),
                ts: self.now_secs(),
            })
            .await?;
            tx.commit().await?;
            changed += 1;
        }
        Ok(changed)
    }
}

/// Split text into lines the way `TextDiff::from_lines` tokenizes (keeping
/// trailing newlines), so line counts line up with the diff indices.
fn split_lines(s: &str) -> Vec<&str> {
    s.split_inclusive('\n').collect()
}

/// Compute per-new-line authorship. Unchanged lines keep their prior author. A
/// line that only *moved* or was re-indented keeps its author too: its
/// whitespace-normalized content is matched against the lines deleted in the
/// same diff, so a reorder or a pure re-indent isn't credited to the current
/// writer (M10). Genuinely new lines are attributed to `new_author`.
fn diff_authors(
    old: &[u8],
    new: &[u8],
    old_authors: &[(i64, i64)],
    new_author: (i64, i64),
) -> Vec<(i64, i64)> {
    let old_s = std::str::from_utf8(old).unwrap_or("");
    let new_s = std::str::from_utf8(new).unwrap_or("");
    let diff = TextDiff::from_lines(old_s, new_s);

    // Index the deleted lines by normalized content -> queue of their authors, so
    // a matching inserted line (a move, or a whitespace-only change) reclaims the
    // original author instead of being credited to the current writer.
    let mut moved: HashMap<String, VecDeque<(i64, i64)>> = HashMap::new();
    for change in diff.iter_all_changes() {
        if change.tag() == ChangeTag::Delete
            && let Some(i) = change.old_index()
        {
            let author = old_authors.get(i).copied().unwrap_or(new_author);
            moved
                .entry(normalize_line(change.value()))
                .or_default()
                .push_back(author);
        }
    }

    let mut out: Vec<(i64, i64)> = Vec::new();
    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Equal => {
                let author = change
                    .old_index()
                    .and_then(|i| old_authors.get(i).copied())
                    .unwrap_or(new_author);
                out.push(author);
            }
            ChangeTag::Insert => {
                // A moved / re-indented line reclaims its author; a genuinely new
                // line belongs to the current writer.
                let author = moved
                    .get_mut(&normalize_line(change.value()))
                    .and_then(|q| q.pop_front())
                    .unwrap_or(new_author);
                out.push(author);
            }
            ChangeTag::Delete => {}
        }
    }
    out
}

/// A line's content with surrounding whitespace (and its newline) stripped, so a
/// re-indented line matches its original for move/whitespace-aware attribution.
fn normalize_line(line: &str) -> String {
    line.trim().to_string()
}
