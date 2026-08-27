//! Live collaboration: a change feed + presence for a shared human+agent
//! workspace (`docs/DESIGN.md` §7 / roadmap M8).
//!
//! When several actors — humans and agents — share one workspace, each needs to
//! see what the others are doing *as it happens*: who touched which file, who
//! committed, who is currently active and where. This module records an
//! append-only **event feed** (a monotonic `seq` cursor other writers tail) and
//! **presence** (per-session heartbeat with a current path). On Postgres, every
//! appended event also fires `LISTEN/NOTIFY` so consumers can be pushed to
//! instead of polling; SQLite consumers poll the feed by cursor.
//!
//! Events are emitted at the workspace API boundary (see `origofs-sdk`), so internal
//! engine operations — materializing a checkout, importing history — don't flood
//! the feed; only user/agent-initiated actions do.

use crate::attribution::ActorKind;
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::Result;
use crate::metadata::MetadataStore;

/// The channel Postgres backends `NOTIFY` on when an event is appended.
pub const EVENT_CHANNEL: &str = "origofs_events";

/// A change to record in the feed.
#[derive(Clone, Debug)]
pub struct EventInit {
    pub actor_id: Option<i64>,
    pub session_id: Option<i64>,
    /// A short verb: `write`, `mkdir`, `remove`, `rename`, `symlink`, `commit`,
    /// `lock`, `unlock`, `suggest`.
    pub kind: String,
    pub path: String,
    /// Optional extra context (rename target, commit message, lock owner, …).
    pub detail: Option<String>,
    /// The branch the change happened on (`None` for detached HEAD / unknown),
    /// so a per-branch UI can attribute and filter the feed.
    pub branch: Option<String>,
}

/// A recorded feed event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub seq: i64,
    pub actor_id: Option<i64>,
    pub session_id: Option<i64>,
    pub kind: String,
    pub path: String,
    pub detail: Option<String>,
    pub ts: i64,
    pub branch: Option<String>,
}

/// A currently-active session: who it is and where they are.
#[derive(Clone, Debug)]
pub struct Presence {
    pub session_id: i64,
    pub actor_id: i64,
    pub display_name: String,
    pub kind: ActorKind,
    pub path: Option<String>,
    pub last_seen: i64,
}

/// Default presence window: sessions seen within this many seconds are "active".
pub const PRESENCE_WINDOW_SECS: i64 = 60;

/// A path with an **open live CRDT document** (roadmap M8; issue #75 §3.4).
///
/// While a path is live, its durable CAS blob is a *checkpoint* — a real,
/// attributed state of the document, but possibly behind the `Y.Doc` people are
/// currently typing into. Byte readers ([`Fs::read`], the three-way merge, the git
/// export path) consult this so they can tell "these bytes are the whole truth"
/// from "these bytes may lag an open editor".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveDoc {
    /// The co-edited path.
    pub path: String,
    /// The session that most recently opened or checkpointed the document.
    pub session_id: Option<i64>,
    /// The actor behind that session.
    pub actor_id: i64,
    /// The file's content address (hex manifest hash) as of the last checkpoint,
    /// or `None` if the file was empty/absent then. This is the marker that makes
    /// an **out-of-band** write to a live path detectable: if the file's current
    /// content address differs, somebody wrote around the live document, and the
    /// next checkpoint reconciles instead of clobbering.
    pub content_hash: Option<String>,
    /// When the path first became live. Does not move while it stays live — for
    /// "how stale might these bytes be", read [`checkpointed_at`](Self::checkpointed_at).
    pub since: i64,
    /// When the durable bytes were last crystallized, or `None` if this document
    /// has been open but never checkpointed.
    ///
    /// [`since`](Self::since) says how long the path has been live, which is not
    /// the same question. This is the one a reader actually has: the durable blob
    /// is exactly as old as this, so a UI can say "last saved 3 minutes ago"
    /// instead of only "this may be stale" (#97).
    pub checkpointed_at: Option<i64>,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Append an event to the change feed, returning its `seq` cursor.
    pub async fn record_event(&self, ev: EventInit) -> Result<i64> {
        self.meta.append_event(ev, self.now_secs()).await
    }

    /// Events strictly after `after_seq`, oldest first (cursor-based tailing).
    pub async fn events_since(&self, after_seq: i64, limit: i64) -> Result<Vec<Event>> {
        self.meta.events_since(after_seq, limit).await
    }

    /// Heartbeat a session's presence, optionally noting the path it is on.
    pub async fn touch_presence(
        &self,
        session_id: i64,
        actor_id: i64,
        path: Option<&str>,
    ) -> Result<()> {
        self.meta
            .touch_presence(session_id, actor_id, path, self.now_secs())
            .await
    }

    /// Sessions active within `window_secs`, most recently seen first.
    pub async fn presence(&self, window_secs: i64) -> Result<Vec<Presence>> {
        self.meta
            .active_presence(self.now_secs() - window_secs)
            .await
    }

    /// Reap presence rows older than `grace_secs` so the table doesn't grow
    /// without bound (one row accretes per session). Call periodically; use a
    /// grace comfortably larger than the presence window. Returns rows reaped.
    pub async fn reap_presence(&self, grace_secs: i64) -> Result<u64> {
        self.meta.reap_presence(self.now_secs() - grace_secs).await
    }

    // --- live CRDT documents (issue #75 §3.4) -----------------------------

    /// Mark `path` as having an open live CRDT document, recording the file's
    /// current content address as the coherence marker. Called by
    /// [`open_coedit`](Self::open_coedit); cleared by
    /// [`end_coedit`](Self::end_coedit).
    pub async fn mark_live(&self, ctx: crate::attribution::WriteCtx, path: &str) -> Result<()> {
        self.set_live_marker(ctx, path, false).await
    }

    /// [`mark_live`](Self::mark_live), but also stamping *now* as the moment the
    /// durable bytes were crystallized. Called by
    /// [`checkpoint_coedit`](Self::checkpoint_coedit), and only from there — a
    /// checkpoint stamp that any re-mark could set would be a lie the moment a
    /// second editor joined.
    pub async fn mark_checkpointed(
        &self,
        ctx: crate::attribution::WriteCtx,
        path: &str,
    ) -> Result<()> {
        self.set_live_marker(ctx, path, true).await
    }

    async fn set_live_marker(
        &self,
        ctx: crate::attribution::WriteCtx,
        path: &str,
        checkpointed: bool,
    ) -> Result<()> {
        let content = self.current_content_hex(path).await?;
        let now = self.now_secs();
        self.meta
            .set_live_doc(
                path,
                ctx.session,
                ctx.actor,
                content.as_deref(),
                now,
                checkpointed.then_some(now),
            )
            .await
    }

    /// Clear `path`'s live marker — the co-editing session is over and the durable
    /// blob is once again the whole truth. Idempotent.
    ///
    /// A marker left behind (a crashed worker, a `open_coedit` whose caller simply
    /// dropped the doc) is deliberately the *safe* failure direction: a reader is
    /// told the bytes may lag when they in fact do not, which costs it a needless
    /// re-check. The opposite — a live document with no marker — is the one that
    /// would mislead, and that cannot happen because the marker is written before
    /// the document is handed out.
    pub async fn end_coedit(&self, path: &str) -> Result<()> {
        self.meta.clear_live_doc(path).await
    }

    /// The live marker for `path`, or `None` when nothing has it open.
    pub async fn live_doc(&self, path: &str) -> Result<Option<LiveDoc>> {
        self.meta.get_live_doc(path).await
    }

    /// Every path currently marked live, ordered by path.
    pub async fn live_paths(&self) -> Result<Vec<LiveDoc>> {
        self.meta.list_live_docs().await
    }

    /// Read `path` **and** report whether it is live — the byte reader's version of
    /// [`read`](Self::read) for callers that care whether the bytes may lag an open
    /// `Y.Doc` (the git export path, a three-way merge, a UI that wants to warn).
    ///
    /// # What a byte reader should do when a path is live
    ///
    /// **Surface the staleness; do not block, fail, or force a checkpoint.** That is
    /// the least surprising behaviour, for three reasons:
    ///
    /// 1. *A read must not write.* Forcing a checkpoint would make reading a file
    ///    mutate the workspace, append to the op-log, and need an actor to attribute
    ///    the checkpoint to — a reader has none.
    /// 2. *The engine cannot force one anyway.* The live `Y.Doc` is in-process state
    ///    owned by a co-editing room, quite possibly in a different worker. The
    ///    metadata store knows a document is open; it does not have the document.
    ///    The only component that can checkpoint is the room itself
    ///    (`origofs_sdk::api::Coordinator::checkpoint_all`).
    /// 3. *The durable bytes are never garbage.* They are a previously checkpointed,
    ///    fully attributed state — just possibly an older one. Returning them with a
    ///    "may lag" flag is honest; erroring out because a colleague has an editor
    ///    open is not.
    ///
    /// So: `read` keeps its contract unchanged, and a caller that needs the freshest
    /// bytes asks the co-editing coordinator to checkpoint first, then reads.
    pub async fn read_live(&self, path: &str) -> Result<(bytes::Bytes, Option<LiveDoc>)> {
        let bytes = self.read(path).await?;
        let live = self.meta.get_live_doc(path).await?;
        Ok((bytes, live))
    }
}
