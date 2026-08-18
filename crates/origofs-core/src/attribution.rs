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
use crate::types::{Hash, Ino, Inode};
use similar::{ChangeTag, TextDiff};
use std::collections::{HashMap, VecDeque};

/// Whether an actor is a person, an autonomous agent, or the system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
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

/// How an actor's direct writes are governed — a bounded, actor-agnostic trust
/// gate (§6). It is a property of the *actor*, not their [`ActorKind`], so a
/// trusted agent can be [`Direct`](WritePolicy::Direct) while an untrusted human
/// contributor is [`Propose`](WritePolicy::Propose).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum WritePolicy {
    /// May write straight to the working tree (the default).
    #[default]
    Direct,
    /// Direct writes are refused; edits must go through the suggestion queue for
    /// review by a *different* actor before they land.
    Propose,
}

impl WritePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            WritePolicy::Direct => "direct",
            WritePolicy::Propose => "propose",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "direct" => Some(WritePolicy::Direct),
            "propose" => Some(WritePolicy::Propose),
            _ => None,
        }
    }
    /// The stored integer form (the `actor.write_policy` column).
    pub fn as_i64(self) -> i64 {
        match self {
            WritePolicy::Direct => 0,
            WritePolicy::Propose => 1,
        }
    }
    /// Decode the stored integer. An unrecognized value decodes to the
    /// *restrictive* policy, not the permissive one.
    ///
    /// This is an authorization control, so it fails closed: a value this binary
    /// doesn't know is one a newer binary wrote (say a future `ReadOnly`), and
    /// resolving that to `Direct` would silently *grant* direct writes to an actor
    /// an operator had deliberately restricted. Routing an edit through review is
    /// recoverable; letting an unreviewed one land is not.
    pub fn from_i64(v: i64) -> Self {
        match v {
            0 => WritePolicy::Direct,
            _ => WritePolicy::Propose,
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
    /// How this actor's direct writes are governed (§6).
    pub write_policy: WritePolicy,
}

/// A recorded tool-call audit entry, optionally linked from edits.
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

/// Which prior content version an attributed write is allowed to land on.
///
/// The engine derives authorship by diffing against a version it read *before*
/// the transaction, so the version it lands on has to be pinned — otherwise the
/// blame describes a predecessor that was already replaced, and the `edit_op`
/// log records a `pre_hash` that was never the thing overwritten.
#[derive(Clone, Copy, Debug)]
enum WriteBase {
    /// The content read at the start of this attempt. A concurrent write is a
    /// [`OrigoFSError::Conflict`] — the default for [`Fs::write_as`].
    AsRead,
    /// A caller-supplied expectation (`None` = "expected absent/empty"), for
    /// [`Fs::write_as_expecting`] and the suggestion-accept path.
    Expected(Option<Hash>),
    /// Unconditional. Only the CRDT checkpoint path, which owns the document and
    /// reconciles out-of-band writes itself — see [`Fs::write_as_blamed`].
    Clobber,
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

/// One coalesced authorship run over a consecutive byte span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlameRun {
    actor: i64,
    session: i64,
    /// Byte length of the span this run covers.
    len: u64,
}

/// A file's byte-range authorship map (`docs/DESIGN.md` §5 — blame is
/// per-byte-range, not per-line), stored as `actor,session,len;...`. Ordinary
/// line-based writes produce runs whose spans align to line boundaries; a
/// co-edited CRDT checkpoint (roadmap M8) attributes sub-line, character-level
/// spans through the same map, losslessly.
#[derive(Clone, Debug, Default)]
struct BlameMap {
    runs: Vec<BlameRun>,
}

impl BlameMap {
    /// Build from `(actor, session, byte_len)` spans, coalescing adjacent runs of
    /// the same author. Zero-length spans are dropped.
    fn from_spans(spans: &[(i64, i64, u64)]) -> Self {
        let mut runs: Vec<BlameRun> = Vec::new();
        for &(actor, session, len) in spans {
            if len == 0 {
                continue;
            }
            match runs.last_mut() {
                Some(r) if r.actor == actor && r.session == session => r.len += len,
                _ => runs.push(BlameRun {
                    actor,
                    session,
                    len,
                }),
            }
        }
        BlameMap { runs }
    }

    /// Total number of bytes this map covers.
    fn total(&self) -> u64 {
        self.runs.iter().map(|r| r.len).sum()
    }

    /// The `(actor, session, len)` spans covering byte range `[start, start+len)`,
    /// clipped to that window (returned lengths sum to the overlap).
    fn slice(&self, start: u64, len: u64) -> Vec<(i64, i64, u64)> {
        let end = start + len;
        let mut out = Vec::new();
        let mut pos = 0u64;
        for r in &self.runs {
            let rstart = pos;
            let rend = pos + r.len;
            pos = rend;
            let s = rstart.max(start);
            let e = rend.min(end);
            if s < e {
                out.push((r.actor, r.session, e - s));
            }
            if rend >= end {
                break;
            }
        }
        out
    }

    fn encode(&self) -> String {
        self.runs
            .iter()
            .map(|r| format!("{},{},{}", r.actor, r.session, r.len))
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
                    len: it.next()?.parse().ok()?,
                })
            })
            .collect();
        BlameMap { runs }
    }
}

/// One blame result: a contiguous same-author span. `byte_start`/`byte_end` are
/// the exact `[start, end)` byte range; `line_start`/`line_end` are the inclusive
/// 1-based lines it touches (for a line-oriented UI). Sub-line, character-level
/// authorship from co-editing is representable — two authors on one line are two
/// ranges that share that line number.
#[derive(Clone, Debug)]
pub struct BlameRange {
    pub line_start: u32,
    pub line_end: u32,
    pub byte_start: u64,
    pub byte_end: u64,
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

    /// Set an actor's write policy — `direct` (may write straight to the tree) or
    /// `propose` (writes are routed through the suggestion queue for review by a
    /// different actor). A bounded, actor-agnostic trust gate (§6).
    pub async fn set_write_policy(&self, actor_id: i64, policy: WritePolicy) -> Result<()> {
        self.meta.set_write_policy(actor_id, policy).await
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
    ///
    /// # Concurrency
    ///
    /// **Optimistic, not last-writer-wins.** Authorship is derived by diffing
    /// `data` against the file's current content, so the write is applied only if
    /// that content is *still* current when the transaction commits; if another
    /// writer landed in between, this returns [`OrigoFSError::Conflict`] and
    /// nothing is written.
    ///
    /// It was previously unconditional, and the loser of a race lost their bytes
    /// with no error and no signal to anyone — while the winner's blame was
    /// derived from a version that no longer existed, so the `edit_op` log
    /// recorded a `pre_hash` that was never the thing overwritten. A human and an
    /// agent editing one file at the same moment is the scenario this system
    /// exists for; silently dropping one of them is the wrong answer, and the
    /// op-log is supposed to be attribution's ground truth.
    ///
    /// A `Conflict` is deliberately *not* retried by [`crate::retry`] — it is a
    /// real answer about the caller's request, and re-running the write would
    /// re-derive it against the new content and clobber the change the caller
    /// needs to see. Read, merge, and write again.
    ///
    /// [`write_as_expecting`](Self::write_as_expecting) is the same guarantee
    /// against a caller-chosen base; [`write_as_blamed`](Self::write_as_blamed)
    /// deliberately keeps the unconditional behaviour — see there.
    pub async fn write_as(&self, ctx: WriteCtx, path: &str, data: &[u8]) -> Result<()> {
        crate::retry::retrying("write_as", || self.write_as_attempt(ctx, path, data)).await
    }

    /// One attempt at [`write_as`](Self::write_as); see [`crate::retry`] for why the
    /// retry wrapper sits outside the whole operation rather than inside it.
    async fn write_as_attempt(&self, ctx: WriteCtx, path: &str, data: &[u8]) -> Result<()> {
        self.write_as_inner(ctx, path, data, WriteBase::AsRead, None)
            .await
    }

    /// Write `data`, attributing byte spans to explicit `(actor_id, session_id,
    /// byte_len)` authors rather than deriving authorship with the [`diff_spans`]
    /// heuristic. This is the CRDT/editor-authoritative path (roadmap M8): a
    /// co-edited document knows each character run's true author, so a checkpoint
    /// lands losslessly — sub-line and interleaved authorship is preserved exactly,
    /// which the line diff cannot recover across moves, duplication, or in-line
    /// edits (audit #34 M10).
    ///
    /// `ctx` is the actor performing the write (recorded in the op-log as the
    /// checkpoint author); `spans` drives the blame index and its `byte_len`s must
    /// sum to `data.len()` (contiguous, in order). The explicit map replaces prior
    /// authorship wholesale. Requires UTF-8 text.
    ///
    /// # Concurrency
    ///
    /// Unlike [`write_as`](Self::write_as), this write is unconditional. The
    /// caller here is a CRDT coordinator checkpointing a document it owns: the
    /// `Y.Doc` is the live truth and the durable blob is a snapshot of it, so a
    /// checkpoint that refused because the blob moved would strand the document
    /// rather than protect it. Out-of-band writes are reconciled by the coedit
    /// layer, which re-seeds the doc from the file — see [`crate::coedit`].
    pub async fn write_as_blamed(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        spans: &[(i64, i64, u64)],
    ) -> Result<()> {
        if !is_text(data) {
            return Err(OrigoFSError::InvalidArgument(
                "write_as_blamed requires UTF-8 text".into(),
            ));
        }
        let covered: u64 = spans.iter().map(|&(_, _, len)| len).sum();
        if covered != data.len() as u64 {
            return Err(OrigoFSError::InvalidArgument(format!(
                "write_as_blamed: spans cover {covered} bytes for {}-byte content",
                data.len()
            )));
        }
        self.write_as_inner(
            ctx,
            path,
            data,
            WriteBase::Clobber,
            Some(BlameMap::from_spans(spans)),
        )
        .await
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
        self.write_as_inner(ctx, path, data, WriteBase::Expected(expected), None)
            .await
    }

    async fn write_as_inner(
        &self,
        ctx: WriteCtx,
        path: &str,
        data: &[u8],
        base: WriteBase,
        blame_override: Option<BlameMap>,
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
            // starts from empty bytes and an empty authorship map.
            let (pre_hash, old_bytes, old_map) = match existing {
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
                    let map = match &pre {
                        Some(h) => match self.meta.get_blob_blame(h).await? {
                            Some(s) => BlameMap::decode(&s),
                            None => BlameMap::default(),
                        },
                        None => BlameMap::default(),
                    };
                    (pre, bytes, map)
                }
                None => (None, Vec::new(), BlameMap::default()),
            };

            // Compute the new byte-range authorship. An explicit map (the
            // CRDT/editor path via `write_as_blamed`) is authoritative and replaces
            // prior authorship wholesale; otherwise derive it against whatever is
            // there now — move/whitespace-aware for text (carrying unchanged lines'
            // exact sub-line spans), file-level for binary.
            let blame = if let Some(explicit) = &blame_override {
                explicit.clone()
            } else if is_text(&old_bytes) && is_text(data) {
                diff_spans(&old_bytes, &old_map, data, (ctx.actor, ctx.sid()))
            } else {
                // Binary: file-level attribution (single span).
                BlameMap::from_spans(&[(ctx.actor, ctx.sid(), data.len() as u64)])
            };

            let mut tx = self.meta.begin().await?;
            let ino = match existing {
                Some(ino) => ino,
                None => match Self::create_file_in(tx.as_mut(), parent, name).await {
                    Ok(ino) => ino,
                    Err(OrigoFSError::AlreadyExists(_)) => {
                        // Awaited: the retry re-reads immediately. See
                        // `MetaTxn::rollback`.
                        tx.rollback().await?;
                        // A conditional write that required absence can't proceed
                        // once the file exists.
                        if matches!(base, WriteBase::Expected(None)) {
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
            // Conditional apply: the write lands only on the content its blame
            // was derived from. On mismatch the whole transaction rolls back,
            // undoing the blame staged just above, and the caller is told rather
            // than having their bytes silently replaced.
            match &base {
                WriteBase::AsRead => {
                    if !tx
                        .set_content_if(ino, pre_hash.as_ref(), mhash, size)
                        .await?
                    {
                        return Err(OrigoFSError::Conflict(format!(
                            "{path} was written concurrently; re-read it and write again"
                        )));
                    }
                }
                WriteBase::Expected(expected) => {
                    if !tx
                        .set_content_if(ino, expected.as_ref(), mhash, size)
                        .await?
                    {
                        return Err(OrigoFSError::Conflict(format!(
                            "{path} changed since the write was based on it"
                        )));
                    }
                }
                // The CRDT checkpoint path — see `write_as_blamed`.
                WriteBase::Clobber => tx.set_content(ino, mhash, size).await?,
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

    /// Write a file by **streaming** from a blocking reader, attributed to `ctx`.
    ///
    /// The attributed counterpart of [`Fs::write_reader`](crate::Fs::write_reader),
    /// and the only way to write a file larger than memory while still recording
    /// who wrote it. Until this existed, streaming and attribution were mutually
    /// exclusive: `write_reader` was the sole streaming path and it is
    /// unattributed, so the moment a caller supplied an actor — the entire premise
    /// of origofs — the whole body had to be resident. That is why
    /// `origofs write --from big.bin` streamed but `--actor 7` did not.
    ///
    /// Subject to the §6 write policy: a propose-only actor is refused with
    /// [`OrigoFSError::Denied`]. There is deliberately no propose-shaped variant —
    /// a suggestion records proposed bytes as a *content hash*, so `suggest`
    /// already streams by construction and queuing a multi-gigabyte proposal for
    /// human review is not a meaningful operation.
    ///
    /// # Blame differs from `write_as`, on purpose
    ///
    /// [`write_as`](Self::write_as) computes byte-range authorship by diffing
    /// against the previous body, which it holds in memory. A streaming write
    /// cannot: not having the body resident is the whole point. So this attributes
    /// the **entire file** to `ctx` as a single span.
    ///
    /// That is the honest answer rather than a compromise — a streamed write *is* a
    /// wholesale replacement, so "this actor wrote all of it" is exactly true. But
    /// it means streaming a file that previously had mixed authorship discards the
    /// per-line history the buffered path would have preserved. Use `write_as` when
    /// the file fits in memory and its line-level provenance matters; use this when
    /// it does not fit, or when the write genuinely replaces everything.
    ///
    /// Unlike `write_as`, this is **unconditional**: there is no compare-and-set
    /// against the content the blame was derived from, because it was not derived
    /// from any prior content. A concurrent writer's bytes are replaced, and the
    /// `edit_op` records the `pre_hash` that was actually overwritten.
    pub async fn write_reader_as<R>(&self, ctx: WriteCtx, path: &str, reader: R) -> Result<()>
    where
        R: std::io::Read + Send + 'static,
    {
        self.ensure_may_write(ctx, "write a file").await?;

        let (parent, name) = self.resolve_parent(path).await?;
        self.ensure_dir(parent).await?;

        // Stream the body into content first. Content is durable and idempotent
        // (content-addressed) when this returns, so the metadata commit below can
        // retry a create race without touching the reader again — which matters
        // here more than anywhere, since the reader may be a network socket or a
        // multi-gigabyte file that cannot be replayed.
        let (mhash, size) = self.stream_body(path, reader).await?;

        // Whole-file authorship — see the note above.
        let blame = BlameMap::from_spans(&[(ctx.actor, ctx.sid(), size)]);
        let encoded = blame.encode();

        for _ in 0..crate::engine::CREATE_RETRIES {
            let existing = self.lookup_file(parent, name, path).await?;
            // Read the prior content hash for the op-log's `pre_hash`. Only the
            // hash — not the bytes — so this stays O(1) in file size.
            let pre_hash = match existing {
                Some(ino) => self.meta.get_inode(ino).await?.and_then(|i| i.content),
                None => None,
            };

            let mut tx = self.meta.begin().await?;
            let ino = match existing {
                Some(ino) => ino,
                None => match Self::create_file_in(tx.as_mut(), parent, name).await {
                    Ok(ino) => ino,
                    Err(OrigoFSError::AlreadyExists(_)) => {
                        tx.rollback().await?;
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            // Blame is keyed by the new content version; an empty file has neither.
            if let Some(h) = mhash {
                tx.set_blob_blame(&h, &encoded).await?;
            }
            tx.set_content(ino, mhash, size).await?;
            tx.append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino,
                path: path.to_string(),
                op: "write".to_string(),
                byte_start: 0,
                // `size` is a `u64` and the column is a `BIGINT`; a file past
                // `i64::MAX` (8 EiB) is unreachable, but saturate rather than wrap
                // so the op-log can never record a negative length.
                byte_len: i64::try_from(size).unwrap_or(i64::MAX),
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

    // --- attributed namespace mutations (issue #78) -----------------------
    //
    // The raw `remove`/`rename`/`mkdir_p`/`symlink` on `Fs` take no `WriteCtx`:
    // they are the *namespace* primitives, used by checkout, merge materialization
    // and suggestion application, where there is no requesting actor to record.
    // That left two holes at once — deleting a file had no author, and a mutation
    // with no actor could not be policy-checked, so a propose-only agent could
    // `rm` what it was forbidden to overwrite.
    //
    // These wrappers close both: they carry the actor, enforce the §6 write policy,
    // and append to the op-log. A surface should reach for these; internal
    // machinery keeps calling the raw forms and stays exempt by construction.
    //
    // The op-log entry for a namespace change carries no byte range (`byte_len` 0)
    // — nothing was authored — and `edit_op.ino` has no foreign key, so a removal's
    // op outlives the inode it names, which is what an append-only audit trail
    // requires.

    /// Remove a file or empty directory, attributed to `ctx` and subject to its
    /// write policy.
    ///
    /// Prefer [`remove_or_propose`](Self::remove_or_propose) on a surface that
    /// accepts requests from possibly-untrusted actors: it queues a propose-only
    /// actor's removal for review instead of refusing it outright.
    pub async fn remove_as(&self, ctx: WriteCtx, path: &str) -> Result<()> {
        self.ensure_may_write(ctx, "remove files").await?;
        // Capture identity and content *before* the removal: afterwards the inode
        // is gone and the op-log could not name what was destroyed.
        let inode = self.stat(path).await?;
        self.remove(path).await?;
        self.meta
            .append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino: inode.ino,
                path: path.to_string(),
                op: "remove".to_string(),
                byte_start: 0,
                byte_len: 0,
                pre_hash: inode.content.map(|h| h.to_hex()),
                post_hash: None,
                ts: self.now_secs(),
            })
            .await?;
        Ok(())
    }

    /// Rename/move `from` to `to`, attributed to `ctx` and subject to its write
    /// policy.
    ///
    /// The op-log entry records the *destination* path — where the inode now lives.
    /// The source is recoverable from the inode's earlier ops, and the change-feed
    /// event emitted at the workspace boundary carries `from → to` in its `detail`.
    pub async fn rename_as(&self, ctx: WriteCtx, from: &str, to: &str) -> Result<()> {
        self.ensure_may_write(ctx, "rename files").await?;
        let inode = self.stat(from).await?;
        self.rename(from, to).await?;
        self.meta
            .append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino: inode.ino,
                path: to.to_string(),
                op: "rename".to_string(),
                byte_start: 0,
                byte_len: 0,
                pre_hash: inode.content.map(|h| h.to_hex()),
                post_hash: inode.content.map(|h| h.to_hex()),
                ts: self.now_secs(),
            })
            .await?;
        Ok(())
    }

    /// Create a directory (and any missing parents), attributed to `ctx` and
    /// subject to its write policy.
    pub async fn mkdir_as(&self, ctx: WriteCtx, path: &str) -> Result<Ino> {
        self.ensure_may_write(ctx, "create directories").await?;
        let ino = self.mkdir_p(path).await?;
        self.meta
            .append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino,
                path: path.to_string(),
                op: "mkdir".to_string(),
                byte_start: 0,
                byte_len: 0,
                pre_hash: None,
                post_hash: None,
                ts: self.now_secs(),
            })
            .await?;
        Ok(ino)
    }

    /// Create a symlink at `linkpath` pointing at `target`, attributed to `ctx`
    /// and subject to its write policy.
    pub async fn symlink_as(&self, ctx: WriteCtx, target: &str, linkpath: &str) -> Result<Ino> {
        self.ensure_may_write(ctx, "create symlinks").await?;
        let ino = self.symlink(target, linkpath).await?;
        self.meta
            .append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino,
                path: linkpath.to_string(),
                op: "symlink".to_string(),
                byte_start: 0,
                byte_len: 0,
                pre_hash: None,
                post_hash: None,
                ts: self.now_secs(),
            })
            .await?;
        Ok(ino)
    }

    /// Change a path's permission bits, attributed to `ctx` and subject to its
    /// write policy.
    ///
    /// A propose-only actor is **refused** rather than queued: there is no
    /// propose-shaped equivalent of a `chmod`, the same way there is none for
    /// `symlink_as`. Metadata is still a mutation an untrusted agent should not be
    /// able to make unreviewed — `chmod 000` on a file it may not write is a denial
    /// of service the write policy exists to prevent.
    ///
    /// The recorded op has no byte range (like `mkdir`/`symlink`): mode is metadata
    /// about a file, not authored content, so it belongs in the op-log without
    /// touching the blame index.
    pub async fn chmod_as(&self, ctx: WriteCtx, path: &str, mode: u32) -> Result<Inode> {
        self.ensure_may_write(ctx, "change permissions").await?;
        let inode = self.chmod(path, mode).await?;
        self.meta
            .append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino: inode.ino,
                path: path.to_string(),
                op: "chmod".to_string(),
                byte_start: 0,
                byte_len: 0,
                pre_hash: None,
                post_hash: None,
                ts: self.now_secs(),
            })
            .await?;
        Ok(inode)
    }

    /// Change a path's owning user and/or group, attributed to `ctx` and subject to
    /// its write policy. See [`Fs::chmod_as`] for why this refuses rather than
    /// queues, and why the op carries no byte range.
    pub async fn chown_as(
        &self,
        ctx: WriteCtx,
        path: &str,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<Inode> {
        self.ensure_may_write(ctx, "change ownership").await?;
        let inode = self.chown(path, uid, gid).await?;
        self.meta
            .append_edit_op(EditOpInit {
                session_id: ctx.session,
                actor_id: ctx.actor,
                tool_call_id: ctx.tool_call,
                ino: inode.ino,
                path: path.to_string(),
                op: "chown".to_string(),
                byte_start: 0,
                byte_len: 0,
                pre_hash: None,
                post_hash: None,
                ts: self.now_secs(),
            })
            .await?;
        Ok(inode)
    }

    // --- queries ----------------------------------------------------------

    /// Per-range authorship for `path`, distinguishing human vs agent. Each result
    /// is a contiguous same-author byte span with its `[byte_start, byte_end)` and
    /// the 1-based line range it touches — so a line co-authored at character
    /// granularity (M8) yields one range per author instead of one collapsed line.
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
        // Map byte offsets to 1-based line numbers by walking the content once
        // alongside the (in-order) runs: a byte is on line `1 + newlines before it`.
        let bytes = self.read_body(&content).await?;
        // The runs must describe exactly this body before any of them is used to
        // index into it. `BlameMap::decode` is fully lenient — any `u64` length
        // parses — and the runs are not necessarily this process's own work:
        // `resync::carry_blame` copies a *peer's* blame string verbatim, only
        // remapping actor ids, and a hand-edited or corrupt `blob_blame` row gets
        // here too. Without this the slice below panics on an out-of-range index
        // and `pos + r.len` overflows. `revert_session` has always made exactly
        // this check; `blame` skipping it was an oversight, not a contract.
        if map.total() != bytes.len() as u64 {
            return Err(OrigoFSError::Corrupt(format!(
                "{path}: blame map covers {} bytes but the content is {} — refusing \
                 to attribute against a mismatched body",
                map.total(),
                bytes.len(),
            )));
        }
        let mut out = Vec::new();
        let mut pos: u64 = 0; // byte offset at the start of the current run
        let mut line: u32 = 1; // line number at `pos`
        for r in &map.runs {
            let start = pos;
            let end = pos + r.len;
            let slice = &bytes[start as usize..end as usize];
            // `line_end` is the line of the run's last byte (end-exclusive, so we
            // count newlines strictly before that final byte).
            let newlines_before_last = slice
                .split_last()
                .map(|(_, head)| head.iter().filter(|&&b| b == b'\n').count())
                .unwrap_or(0) as u32;
            let line_start = line;
            let line_end = line_start + newlines_before_last;
            line += slice.iter().filter(|&&b| b == b'\n').count() as u32;
            pos = end;

            let actor = self
                .meta
                .get_actor(r.actor)
                .await?
                .ok_or_else(|| OrigoFSError::NotFound(format!("actor {}", r.actor)))?;
            out.push(BlameRange {
                line_start,
                line_end,
                byte_start: start,
                byte_end: end,
                actor,
                session: (r.session != 0).then_some(r.session),
            });
        }
        Ok(out)
    }

    /// The edit-op log for `actor` (optionally narrowed to one `session`).
    pub async fn edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        self.meta.list_edit_ops(actor_id, session_id).await
    }

    /// Revert every line an actor wrote in a session, across all files they
    /// touched. Returns the paths actually changed. The removed lines are
    /// dropped; remaining lines keep their authorship.
    ///
    /// # Scoping the blast radius
    ///
    /// `path_prefix` bounds the revert to one subtree. A multi-tenant host — one
    /// workspace, tenant-scoped paths — needs this: an "undo this agent's work"
    /// button lives in *some* tenant's UI, and if the session happened to touch a
    /// path outside that tenant, an unscoped revert silently reverts it too
    /// (#94).
    ///
    /// The prefix matches on **directory boundaries**, so `/tenant-a` covers
    /// `/tenant-a` and `/tenant-a/notes.txt` but never `/tenant-abc/notes.txt`. A
    /// trailing slash is accepted and ignored, and the prefix must be absolute.
    ///
    /// Doing it here rather than in the caller is the point. The obvious
    /// workaround — call [`edit_ops`](Self::edit_ops), check the paths, then
    /// revert — reads the session's reach and acts on it in two separate calls,
    /// so a write landing in between is reverted without ever having been
    /// checked. Here the reach is read and filtered before anything is written.
    ///
    /// Per-file atomicity is unchanged: each file's content, blame, and op-log
    /// entry still commit together, but the files are separate transactions, so a
    /// failure partway leaves earlier files reverted. That was already true and
    /// the scope doesn't change it — what it guarantees is that nothing *outside*
    /// the prefix is touched, not that the whole revert is one atom.
    pub async fn revert_session(
        &self,
        actor_id: i64,
        session_id: i64,
        path_prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let scope = path_prefix.map(PathScope::new).transpose()?;

        // Distinct files this actor touched in this session (from the op-log),
        // filtered to the scope *before* any of them is written.
        let ops = self.meta.list_edit_ops(actor_id, Some(session_id)).await?;
        let mut paths: Vec<(Ino, String)> = Vec::new();
        for op in ops {
            if let Some(s) = &scope
                && !s.covers(&op.path)
            {
                continue;
            }
            if !paths.iter().any(|(i, _)| *i == op.ino) {
                paths.push((op.ino, op.path));
            }
        }

        let mut changed = Vec::new();
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
            let map = BlameMap::decode(&map_s);
            let bytes = self.read_body(&content_hash).await?;
            let Ok(current) = std::str::from_utf8(&bytes).map(str::to_owned) else {
                continue; // binary: skip line-revert
            };
            if map.total() != bytes.len() as u64 {
                continue; // out of sync; skip conservatively
            }

            // Drop each line the actor's session *solely* authored; keep every
            // other line with its exact byte spans (a line co-authored at sub-line
            // granularity is kept intact — byte-precise revert of interleaved
            // co-edits is future work, #33).
            let mut kept_body = String::new();
            let mut kept_spans: Vec<(i64, i64, u64)> = Vec::new();
            let mut removed = false;
            let mut off: u64 = 0;
            for line in split_lines(&current) {
                let len = line.len() as u64;
                let spans = map.slice(off, len);
                let solely_target = !spans.is_empty()
                    && spans
                        .iter()
                        .all(|&(a, s, _)| a == actor_id && s == session_id);
                if solely_target {
                    removed = true;
                } else {
                    kept_body.push_str(line);
                    kept_spans.extend(spans);
                }
                off += len;
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
                tx.set_blob_blame(&h, &BlameMap::from_spans(&kept_spans).encode())
                    .await?;
            }
            tx.append_edit_op(EditOpInit {
                session_id: None,
                actor_id,
                tool_call_id: None,
                ino,
                path: path.clone(),
                op: "revert".to_string(),
                byte_start: 0,
                byte_len: size as i64,
                pre_hash: None,
                post_hash: mhash.map(|h| h.to_hex()),
                ts: self.now_secs(),
            })
            .await?;
            tx.commit().await?;
            changed.push(path);
        }
        Ok(changed)
    }
}

/// A `/`-boundary path prefix, for scoping an operation to a subtree.
///
/// The naive `path.starts_with(prefix)` is wrong in a way that matters for the
/// multi-tenant case it exists to serve: `/tenant-a` would also match
/// `/tenant-abc`, so the scope would leak into exactly the neighbouring tenant it
/// was meant to exclude.
struct PathScope {
    /// Normalized: absolute, no trailing slash (so the root is `""`).
    prefix: String,
}

impl PathScope {
    fn new(prefix: &str) -> Result<Self> {
        if !prefix.starts_with('/') {
            return Err(OrigoFSError::InvalidArgument(format!(
                "path prefix must be absolute, got {prefix:?}"
            )));
        }
        Ok(Self {
            prefix: prefix.trim_end_matches('/').to_string(),
        })
    }

    /// Whether `path` is the prefix itself or sits under it.
    fn covers(&self, path: &str) -> bool {
        // The root prefix (`/`, normalized to `""`) covers everything.
        if self.prefix.is_empty() {
            return true;
        }
        path == self.prefix
            || (path.starts_with(&self.prefix)
                && path.as_bytes().get(self.prefix.len()) == Some(&b'/'))
    }
}

#[cfg(test)]
mod path_scope_tests {
    use super::PathScope;

    #[test]
    fn a_prefix_matches_only_on_directory_boundaries() {
        let s = PathScope::new("/tenant-a").unwrap();
        assert!(s.covers("/tenant-a"));
        assert!(s.covers("/tenant-a/notes.txt"));
        assert!(s.covers("/tenant-a/deep/nested/f"));
        // The whole point: a sibling sharing a textual prefix is not covered.
        assert!(!s.covers("/tenant-abc/notes.txt"));
        assert!(!s.covers("/tenant-a2"));
        assert!(!s.covers("/other/tenant-a"));
    }

    #[test]
    fn a_trailing_slash_is_accepted_and_means_the_same_thing() {
        let with = PathScope::new("/tenant-a/").unwrap();
        assert!(with.covers("/tenant-a/notes.txt"));
        assert!(!with.covers("/tenant-abc/notes.txt"));
    }

    #[test]
    fn the_root_covers_everything() {
        let s = PathScope::new("/").unwrap();
        assert!(s.covers("/"));
        assert!(s.covers("/anything/at/all"));
    }

    #[test]
    fn a_relative_prefix_is_refused() {
        assert!(PathScope::new("tenant-a").is_err());
    }
}

/// Split text into lines the way `TextDiff::from_lines` tokenizes (keeping
/// trailing newlines), so line counts line up with the diff indices.
fn split_lines(s: &str) -> Vec<&str> {
    s.split_inclusive('\n').collect()
}

/// Derive byte-range authorship for `new` against `old` (whose authorship is
/// `old_map`). Unchanged lines carry their prior spans verbatim — including
/// sub-line, character-level authorship from a co-edited checkpoint. A line that
/// only *moved* or was re-indented reclaims its origin author too: its
/// whitespace-normalized content is matched against a line deleted in the same
/// diff, so a reorder or a pure re-indent isn't credited to the current writer
/// (M10). Genuinely new lines are attributed to `new_author`.
fn diff_spans(old: &[u8], old_map: &BlameMap, new: &[u8], new_author: (i64, i64)) -> BlameMap {
    let old_s = std::str::from_utf8(old).unwrap_or("");
    let new_s = std::str::from_utf8(new).unwrap_or("");
    let diff = TextDiff::from_lines(old_s, new_s);

    // Old lines with their byte offsets, so a matched old-line index maps to the
    // exact byte span whose authorship we carry forward.
    let old_lines: Vec<&str> = split_lines(old_s);
    let mut old_offsets: Vec<u64> = Vec::with_capacity(old_lines.len());
    let mut acc = 0u64;
    for l in &old_lines {
        old_offsets.push(acc);
        acc += l.len() as u64;
    }
    let old_span = |i: usize| -> (u64, u64) { (old_offsets[i], old_lines[i].len() as u64) };
    // Carry an old byte range's authorship into `len` new bytes, padding any part
    // the old map didn't cover (e.g. the prior content was written unattributed)
    // with the current writer — so a carried line always covers exactly `len`
    // bytes and the map stays in lockstep with the content.
    let carry = |start: u64, len: u64| -> Vec<(i64, i64, u64)> {
        let mut v = old_map.slice(start, len);
        let covered: u64 = v.iter().map(|&(_, _, l)| l).sum();
        if covered < len {
            v.push((new_author.0, new_author.1, len - covered));
        }
        v
    };

    // Deleted lines by normalized content -> queue of their old indices, so a
    // matching inserted line reclaims that line's spans (a move / re-indent).
    let mut moved: HashMap<String, VecDeque<usize>> = HashMap::new();
    for change in diff.iter_all_changes() {
        if change.tag() == ChangeTag::Delete
            && let Some(i) = change.old_index()
        {
            moved
                .entry(normalize_line(change.value()))
                .or_default()
                .push_back(i);
        }
    }

    let mut spans: Vec<(i64, i64, u64)> = Vec::new();
    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Equal => match change.old_index() {
                // Unchanged line: carry its exact prior spans.
                Some(i) => {
                    let (start, len) = old_span(i);
                    spans.extend(carry(start, len));
                }
                None => spans.push((new_author.0, new_author.1, change.value().len() as u64)),
            },
            ChangeTag::Insert => {
                let blen = change.value().len() as u64;
                match moved
                    .get_mut(&normalize_line(change.value()))
                    .and_then(|q| q.pop_front())
                {
                    // Pure move (same bytes): carry the prior spans. Re-indent
                    // (different bytes): keep the origin author over the new line.
                    Some(i) => {
                        let (start, len) = old_span(i);
                        if len == blen {
                            spans.extend(carry(start, len));
                        } else {
                            let (a, s) = old_map
                                .slice(start, len)
                                .first()
                                .map(|&(a, s, _)| (a, s))
                                .unwrap_or(new_author);
                            spans.push((a, s, blen));
                        }
                    }
                    // Genuinely new line: the current writer.
                    None => spans.push((new_author.0, new_author.1, blen)),
                }
            }
            ChangeTag::Delete => {}
        }
    }
    BlameMap::from_spans(&spans)
}

/// A line's content with surrounding whitespace (and its newline) stripped, so a
/// re-indented line matches its original for move/whitespace-aware attribution.
fn normalize_line(line: &str) -> String {
    line.trim().to_string()
}

#[cfg(test)]
mod blame_props {
    //! B2 (issue #70): property tests for the pure blame interval math on
    //! [`BlameMap`] — coalescing preserves the total byte count, the wire
    //! encoding roundtrips, slicing the full range reconstructs the runs, and a
    //! windowed slice returns exactly the overlap. These are the byte-range
    //! invariants `write_as`/`revert_session` rely on.
    use super::BlameMap;
    use proptest::prelude::*;

    /// Arbitrary `(actor, session, byte_len)` spans over a small actor/session
    /// space (so adjacent same-author runs actually occur and get coalesced).
    fn arb_spans() -> impl Strategy<Value = Vec<(i64, i64, u64)>> {
        prop::collection::vec((0i64..4, 0i64..3, 0u64..1000), 0..32)
    }

    proptest! {
        /// Coalescing adjacent same-author runs preserves the total byte count
        /// (zero-length spans are dropped, contributing nothing).
        #[test]
        fn from_spans_preserves_total(spans in arb_spans()) {
            let expect: u64 = spans.iter().filter(|(_, _, l)| *l > 0).map(|(_, _, l)| l).sum();
            prop_assert_eq!(BlameMap::from_spans(&spans).total(), expect);
        }

        /// `from_spans` never leaves two adjacent runs with the same author.
        #[test]
        fn from_spans_coalesces_adjacent(spans in arb_spans()) {
            let m = BlameMap::from_spans(&spans);
            for w in m.runs.windows(2) {
                prop_assert!(w[0].actor != w[1].actor || w[0].session != w[1].session);
            }
        }

        /// The `actor,session,len;...` wire form roundtrips.
        #[test]
        fn encode_decode_roundtrips(spans in arb_spans()) {
            let m = BlameMap::from_spans(&spans);
            prop_assert_eq!(BlameMap::decode(&m.encode()).runs, m.runs);
        }

        /// Slicing the full `[0, total)` range and re-coalescing reconstructs the
        /// original runs exactly.
        #[test]
        fn slice_full_range_reconstructs(spans in arb_spans()) {
            let m = BlameMap::from_spans(&spans);
            let total = m.total();
            let sliced: Vec<(i64, i64, u64)> = m.slice(0, total);
            prop_assert_eq!(BlameMap::from_spans(&sliced).runs, m.runs);
        }

        /// A windowed slice returns lengths summing to exactly the overlap of
        /// `[start, start+len)` with the covered `[0, total)`.
        #[test]
        fn slice_window_sums_to_overlap(
            spans in arb_spans(),
            start in 0u64..2000,
            len in 0u64..2000,
        ) {
            let m = BlameMap::from_spans(&spans);
            let total = m.total();
            let end = start.saturating_add(len);
            let got: u64 = m.slice(start, len).iter().map(|(_, _, l)| l).sum();
            let expect = total.min(end).saturating_sub(total.min(start));
            prop_assert_eq!(got, expect);
        }
    }
}
