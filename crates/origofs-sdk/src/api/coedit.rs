//! Live co-editing transport (roadmap M8): a WebSocket endpoint that speaks the
//! Yjs **y-sync** binary protocol, so an unmodified Yjs editor (PlateJS,
//! `y-websocket`, …) connects and collaborates directly.
//!
//! The rest of the HTTP API is stateless and per-request: each call opens the
//! workspace, does its work, and returns. Live co-editing can't be — every socket
//! editing `/notes.md` must share **one** CRDT so their edits merge. That shared,
//! long-lived state is the [`Coordinator`]: a registry of [`Room`]s, each owning
//! one attributed [`CoeditDoc`] and a fan-out channel. A room is created on the
//! first join (opening the document) and, when the last socket leaves,
//! checkpointed into the byte-range blame index and evicted.
//!
//! Identity is resolved exactly as everywhere else — server-side, never trusted
//! from the client, and through the same [`Authenticator`](super::Authenticator)
//! as every other route. Because browsers can't set headers on a WebSocket
//! upgrade, the credential may instead ride in `Sec-WebSocket-Protocol`
//! (`new WebSocket(url, ["origofs", token])` — the one header a browser *can*
//! set) or, less well, in a `?token=` query param. Content a socket contributes
//! is attributed to *its* principal by the engine, no matter what the bytes
//! claim, and a connection whose credential names no session gets one opened for
//! it so its edits are revertible as a unit.
//!
//! **Multiple workers.** The registry is per-process, so two sockets editing one
//! document on *different* workers would drift. When the workspace is
//! Postgres-backed, the coordinator bridges them over the cross-worker relay
//! (`LISTEN/NOTIFY` + a small op table): every attributed delta is published, and
//! a background task applies peers' deltas to this worker's rooms and fans them
//! out to its sockets, so all replicas converge. A joining room replays recent ops
//! to catch up. On SQLite (single-writer) the relay is simply off.

use super::{AppState, abspath};
use crate::{CoeditDoc, CoeditTreeDoc, OrigoFSError, TreeSpan, Workspace, WriteCtx};
use axum::{
    body::Bytes,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};

/// How many y-sync frames a slow socket can fall behind before it's dropped from
/// the fan-out. A dropped frame only costs a resync on the next edit (the CRDT
/// converges regardless), so this can stay modest.
const FANOUT_CAPACITY: usize = 256;

/// The fan-out origin tag for frames that arrived over the cross-worker relay
/// rather than from a local socket, so every local socket receives them (no local
/// connection id equals `u64::MAX`).
#[cfg(feature = "postgres")]
const RELAY_ORIGIN: u64 = u64::MAX;

/// Which document shape a room holds: the flat `Y.Text` every source file uses, or
/// the `Y.XmlFragment` tree a rich-text editor binds to (#92).
///
/// The two share one wire protocol — that is the whole point of factoring the
/// y-sync driver in `origofs-core` — and differ only in how they reach durable
/// storage. A flat room checkpoints itself, because the server can materialize its
/// bytes. A tree room cannot: only the host owns the schema, so the server persists
/// the CRDT sidecar and the host lands the body through
/// [`checkpoint_tree`](Coordinator::checkpoint_tree).
enum RoomDoc {
    Flat(CoeditDoc),
    Tree(CoeditTreeDoc),
}

impl RoomDoc {
    fn sync_start(&self) -> Vec<u8> {
        match self {
            Self::Flat(d) => d.sync_start(),
            Self::Tree(d) => d.sync_start(),
        }
    }

    fn handle_sync(&self, ctx: WriteCtx, data: &[u8]) -> Result<crate::SyncReply, OrigoFSError> {
        match self {
            Self::Flat(d) => d.handle_sync(ctx, data),
            Self::Tree(d) => d.handle_sync(ctx, data),
        }
    }

    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    fn apply_relayed(&self, frame: &[u8]) -> Result<(), OrigoFSError> {
        match self {
            Self::Flat(d) => d.apply_relayed(frame),
            Self::Tree(d) => d.apply_relayed(frame),
        }
    }
}

/// A live co-editing room: one shared, attributed CRDT plus a fan-out channel.
/// Every socket editing the same path attaches to the same `Room`.
struct Room {
    /// The shared document. `handle_sync` holds this only for the (synchronous)
    /// duration of applying one payload — never across an `.await`.
    doc: Mutex<RoomDoc>,
    /// Origin-tagged y-sync frames fanned out to the room. Each socket skips
    /// frames it originated — it already applied its own edit locally.
    tx: broadcast::Sender<(u64, Bytes)>,
    /// Milliseconds (since the coordinator's epoch) of the most recent edit, and
    /// of the most recent checkpoint. The checkpoint sweeper reads both to decide
    /// whether this room is due; `dirty` says whether there is anything to write.
    ///
    /// Atomics rather than a lock: the edit stamp is touched on every applied
    /// frame — the hottest path in the room — and taking the registry lock there
    /// would serialize typing across every document on the worker.
    last_edit_ms: AtomicU64,
    last_checkpoint_ms: AtomicU64,
    dirty: AtomicBool,
}

impl Room {
    /// Record that an edit just landed, so the sweeper knows there is something to
    /// checkpoint and when the room last went quiet.
    fn touch_edit(&self, epoch: Instant) {
        self.last_edit_ms
            .store(elapsed_ms(epoch), Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Whether this room is due for a periodic checkpoint under `policy`.
    ///
    /// Two independent triggers, because they answer different questions:
    /// **idle** bounds how long a *finished* burst of typing sits un-durable, and
    /// **interval** bounds how far behind a *continuous* editing session can get —
    /// which idle alone never would, since it keeps being reset.
    fn is_due(&self, policy: &CheckpointPolicy, now_ms: u64) -> bool {
        if !self.dirty.load(Ordering::Relaxed) {
            return false;
        }
        let since_edit = now_ms.saturating_sub(self.last_edit_ms.load(Ordering::Relaxed));
        let since_checkpoint =
            now_ms.saturating_sub(self.last_checkpoint_ms.load(Ordering::Relaxed));
        let idle_due = policy
            .idle_after
            .is_some_and(|d| since_edit >= d.as_millis() as u64);
        let interval_due = policy
            .max_interval
            .is_some_and(|d| since_checkpoint >= d.as_millis() as u64);
        idle_due || interval_due
    }
}

/// Milliseconds since the coordinator's epoch. One monotonic clock for the whole
/// registry, so room timings are comparable and immune to wall-clock jumps.
fn elapsed_ms(epoch: Instant) -> u64 {
    epoch.elapsed().as_millis() as u64
}

/// When a live co-editing room's document is written back to durable storage.
///
/// # Why this exists
///
/// A room's CRDT lives in process memory. Without a policy it reaches durable
/// storage only when the **last socket leaves** — and a browser tab left open on a
/// document is an open room, so "last leave" can be hours away. Until then
/// `read`/`read_range` serve the last checkpoint and blame carries only the runs
/// folded in at that point. The live marker tells a reader the bytes may lag,
/// which is the right primitive, but over a long session "may lag" stops being a
/// useful statement. And if the worker dies in between — a deploy, an OOM — the
/// un-checkpointed part of the session is gone from the durable side. On Postgres
/// the relay table bounds that exposure to its replay window; on SQLite the relay
/// is off entirely, so the exposure is the whole session (#97).
///
/// [`Default`] checkpoints a room 5 seconds after it goes quiet, and at least
/// every 60 seconds while it stays busy.
#[derive(Clone, Copy, Debug)]
pub struct CheckpointPolicy {
    /// Checkpoint this long after the last edit — bounds how long a finished burst
    /// of typing sits un-durable. `None` disables the idle trigger.
    pub idle_after: Option<Duration>,
    /// Checkpoint at least this often while edits keep arriving — bounds a
    /// *continuous* session, which the idle timer never would because each
    /// keystroke resets it. `None` disables the interval trigger.
    pub max_interval: Option<Duration>,
    /// How often to look for due rooms. Also the granularity of the two triggers
    /// above, so there is no point setting it finer than the smaller of them.
    pub tick: Duration,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            idle_after: Some(Duration::from_secs(5)),
            max_interval: Some(Duration::from_secs(60)),
            tick: Duration::from_secs(1),
        }
    }
}

impl CheckpointPolicy {
    /// Checkpoint only on last leave — the behaviour before periodic checkpointing
    /// existed. Every edit between the last checkpoint and a crash is lost from the
    /// durable side (bounded by the relay's replay window on Postgres, unbounded on
    /// SQLite), so this is for an embedder that drives `checkpoint_all` itself.
    pub fn on_last_leave_only() -> Self {
        Self {
            idle_after: None,
            max_interval: None,
            tick: Duration::from_secs(1),
        }
    }

    /// Whether any trigger is armed (otherwise the sweeper is pointless).
    fn is_armed(&self) -> bool {
        self.idle_after.is_some() || self.max_interval.is_some()
    }
}

/// How a room is addressed in the registry — and, for a tree room, on the
/// cross-worker relay, whose `path` column is just an opaque routing key.
///
/// A path may legitimately be open in both shapes at once (a flat room for a
/// terminal editor, a tree room for a browser one), and the two must never share a
/// document, so the key carries the shape. `\0` cannot occur in a path —
/// `validate_component` refuses it — so it is a safe separator.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum RoomKey {
    Flat(String),
    Tree { root: String, path: String },
}

impl RoomKey {
    /// The workspace path this room edits.
    fn path(&self) -> &str {
        match self {
            Self::Flat(p) => p,
            Self::Tree { path, .. } => path,
        }
    }

    /// The routing key peers publish and subscribe under.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    fn relay_key(&self) -> String {
        match self {
            Self::Flat(p) => p.clone(),
            Self::Tree { root, path } => format!("tree\0{root}\0{path}"),
        }
    }
}

/// One registry entry: the room, how many sockets are attached, and the identity
/// a background checkpoint runs as.
struct RoomSlot {
    room: Arc<Room>,
    conns: usize,
    /// The most recent joiner's context. A periodic checkpoint has no connection
    /// of its own to borrow an identity from, and this is the same one the final
    /// checkpoint on last leave would have used. It only ever names the op-log
    /// entry and backstops a span the CRDT left unattributed — every real run
    /// keeps the author the engine stamped on it when it was typed.
    ctx: WriteCtx,
}

/// The room registry backing the WebSocket endpoint. Cheap to clone (a handful of
/// `Arc`s); hand a clone to the router state.
#[derive(Clone)]
pub struct Coordinator {
    ws: Arc<Workspace>,
    rooms: Arc<Mutex<HashMap<RoomKey, RoomSlot>>>,
    next_conn: Arc<AtomicU64>,
    /// This worker's unique id, tagged on every published op so the relay drain
    /// can skip our own echo. Only meaningful with the relay, i.e. on Postgres.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    origin: Arc<str>,
    /// Whether the cross-worker relay is available (Postgres-backed workspace).
    relay: bool,
    /// Guards one-time spawn of the relay drain task.
    relay_started: Arc<AtomicBool>,
    /// When rooms are written back to durable storage (#97).
    policy: CheckpointPolicy,
    /// Guards one-time spawn of the checkpoint sweeper.
    sweeper_started: Arc<AtomicBool>,
    /// The monotonic origin every room's timestamps are measured against.
    epoch: Instant,
}

impl Coordinator {
    pub fn new(ws: Arc<Workspace>) -> Self {
        // Without the Postgres backend compiled in there is no relay to run: a
        // single-worker co-editing deployment is a supported configuration, so
        // this is `false` rather than an unsupported build.
        #[cfg(feature = "postgres")]
        let relay = ws.is_postgres();
        #[cfg(not(feature = "postgres"))]
        let relay = false;
        Self {
            ws,
            rooms: Arc::new(Mutex::new(HashMap::new())),
            next_conn: Arc::new(AtomicU64::new(0)),
            origin: Arc::from(new_origin()),
            relay,
            relay_started: Arc::new(AtomicBool::new(false)),
            policy: CheckpointPolicy::default(),
            sweeper_started: Arc::new(AtomicBool::new(false)),
            epoch: Instant::now(),
        }
    }

    /// Use `policy` instead of the default for periodic checkpointing.
    pub fn with_checkpoint_policy(mut self, policy: CheckpointPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Spawn the checkpoint sweeper once, on the first socket. A no-op when no
    /// trigger is armed, or after the first call.
    ///
    /// Driven here rather than left to the host on purpose: a host has no signal
    /// about room activity — it cannot see when a document went quiet — so
    /// "call `checkpoint_all` on a timer" is both more work and strictly worse,
    /// since it writes idle rooms and misses busy ones.
    fn ensure_sweeper(&self) {
        if !self.policy.is_armed() || self.sweeper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let coord = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(coord.policy.tick);
            // A checkpoint can outlast a tick on a slow store; skipping the missed
            // ticks is right — bursting to catch up would pile writes onto a store
            // already struggling.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                coord.checkpoint_due().await;
            }
        });
    }

    /// Checkpoint every room the policy says is due, leaving them live.
    ///
    /// The registry lock is taken only to pick the due rooms and released before
    /// any I/O, so a checkpoint on a slow store never blocks a join or a leave.
    /// A room that gets evicted in between is simply checkpointed once more than
    /// strictly needed — the write is idempotent for unchanged content.
    async fn checkpoint_due(&self) {
        let now = elapsed_ms(self.epoch);
        let due: Vec<(RoomKey, Arc<Room>, WriteCtx)> = {
            let guard = self.rooms.lock().await;
            guard
                .iter()
                .filter(|(_, slot)| slot.room.is_due(&self.policy, now))
                .map(|(k, slot)| (k.clone(), slot.room.clone(), slot.ctx))
                .collect()
        };
        for (key, room, ctx) in due {
            // Clear `dirty` *before* the write, so an edit landing during it marks
            // the room dirty again and gets its own checkpoint. The other order
            // would swallow that edit until the next one arrived.
            room.dirty.store(false, Ordering::Relaxed);
            room.last_checkpoint_ms.store(now, Ordering::Relaxed);
            if self.write_back(&key, &room, ctx).await.is_err() {
                // Put it back in the queue rather than waiting for the next edit:
                // a failed checkpoint means these bytes are still not durable.
                room.dirty.store(true, Ordering::Relaxed);
                tracing::warn!(path = key.path(), "coedit: periodic checkpoint failed");
            }
        }
    }

    /// Write a room back to durable storage, as far as the server is able to.
    ///
    /// A flat room checkpoints in full — text and blame — because the server can
    /// materialize its bytes. A tree room only gets its **sidecar** persisted: the
    /// body is the host's serialization and the server has no serializer, so
    /// producing one here would mean inventing a document model. The file and its
    /// blame move when the host calls [`checkpoint_tree`](Self::checkpoint_tree);
    /// this call is what keeps a crash from costing the editing history in between.
    async fn write_back(
        &self,
        key: &RoomKey,
        room: &Room,
        ctx: WriteCtx,
    ) -> Result<(), OrigoFSError> {
        let doc = room.doc.lock().await;
        match &*doc {
            RoomDoc::Flat(d) => self.ws.checkpoint_coedit(ctx, key.path(), d).await,
            RoomDoc::Tree(d) => self.ws.persist_coedit_tree(key.path(), d).await,
        }
    }

    /// Spawn the relay drain task once, on the first socket (we're in an async
    /// context by then). A no-op without the relay, or after the first call.
    fn ensure_relay(&self) {
        if !self.relay || self.relay_started.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(feature = "postgres")]
        tokio::spawn(relay_drain(
            self.ws.clone(),
            self.rooms.clone(),
            self.origin.clone(),
        ));
    }

    /// Attach to the room for `path`, creating it (opening the document, attributed
    /// to `ctx` for any promotion of existing file text) on the first join. A freshly
    /// created room replays recent relayed ops so it catches up to peers on other
    /// workers before its first socket syncs.
    async fn join(&self, key: &RoomKey, ctx: WriteCtx) -> Result<Arc<Room>, OrigoFSError> {
        let mut rooms = self.rooms.lock().await;
        if let Some(slot) = rooms.get_mut(key) {
            slot.conns += 1;
            // The newest joiner is who a background checkpoint runs as, matching
            // what the final checkpoint on last leave would have used.
            slot.ctx = ctx;
            return Ok(slot.room.clone());
        }
        let doc = match key {
            RoomKey::Flat(path) => RoomDoc::Flat(self.ws.open_coedit(ctx, path).await?),
            RoomKey::Tree { root, path } => {
                RoomDoc::Tree(self.ws.open_coedit_tree(ctx, path, root).await?)
            }
        };
        // The relay is Postgres-backed (`LISTEN`/`NOTIFY`), so without that backend
        // compiled in there is nothing to catch up from — a single-worker build is
        // a valid configuration, not a degraded one.
        #[cfg(feature = "postgres")]
        if self.relay {
            // Ensure the relay table exists before this room takes edits, so the
            // first publish can't race it, then replay recent ops to catch up.
            let _ = self.ws.coedit_relay_init().await;
            if let Ok(notes) = self.ws.coedit_replay(&key.relay_key()).await {
                for note in notes {
                    let _ = doc.apply_relayed(&note.delta);
                }
            }
        }
        let (tx, _) = broadcast::channel(FANOUT_CAPACITY);
        let now = elapsed_ms(self.epoch);
        let room = Arc::new(Room {
            doc: Mutex::new(doc),
            tx,
            last_edit_ms: AtomicU64::new(now),
            // A fresh room counts as just-checkpointed: its content came *from* the
            // durable blob, so the interval trigger should measure from now rather
            // than firing immediately on a room nobody has typed into.
            last_checkpoint_ms: AtomicU64::new(now),
            dirty: AtomicBool::new(false),
        });
        rooms.insert(
            key.clone(),
            RoomSlot {
                room: room.clone(),
                conns: 1,
                ctx,
            },
        );
        Ok(room)
    }

    /// Land a tree room's bytes: the host's serialized `body` plus the span map
    /// saying which bytes came from which co-edit node (#92).
    ///
    /// This is the tree shape's checkpoint. It runs against the **live** room when
    /// one exists — so the node ids the host cites resolve against the same stamps
    /// its socket is seeing — and falls back to the document on disk when the host
    /// checkpoints without a socket attached.
    pub(crate) async fn checkpoint_tree(
        &self,
        path: &str,
        root: &str,
        ctx: WriteCtx,
        body: &[u8],
        spans: &[TreeSpan],
    ) -> Result<(), OrigoFSError> {
        let key = RoomKey::Tree {
            root: root.to_string(),
            path: path.to_string(),
        };
        let room = self.rooms.lock().await.get(&key).map(|s| s.room.clone());
        let Some(room) = room else {
            let doc = self.ws.open_coedit_tree(ctx, path, root).await?;
            return self
                .ws
                .checkpoint_coedit_tree(ctx, path, &doc, body, spans)
                .await;
        };
        let doc = room.doc.lock().await;
        let RoomDoc::Tree(doc) = &*doc else {
            return Err(OrigoFSError::InvalidArgument(format!(
                "{path} is open as a flat co-editing document; it checkpoints itself"
            )));
        };
        self.ws
            .checkpoint_coedit_tree(ctx, path, doc, body, spans)
            .await?;
        // The host has crystallized these bytes, so the room is no longer behind —
        // clearing `dirty` keeps the sweeper from immediately re-persisting a
        // sidecar the checkpoint just wrote.
        room.dirty.store(false, Ordering::Relaxed);
        room.last_checkpoint_ms
            .store(elapsed_ms(self.epoch), Ordering::Relaxed);
        Ok(())
    }

    /// Publish an attributed delta (a y-sync frame) to peer workers, if the relay
    /// is on. Fire-and-forget: CRDT merges commute, so relay order doesn't matter,
    /// and the socket loop never blocks on the database.
    fn relay_publish(&self, key: &RoomKey, frame: Bytes) {
        if !self.relay {
            return;
        }
        #[cfg(feature = "postgres")]
        {
            let ws = self.ws.clone();
            let origin = self.origin.clone();
            let route = key.relay_key();
            tokio::spawn(async move {
                let _ = ws.coedit_publish(&route, &origin, &frame).await;
            });
        }
        // Nothing to publish to without the relay; the local fan-out above has
        // already delivered this frame to every socket on this worker.
        #[cfg(not(feature = "postgres"))]
        let _ = (key, frame);
    }

    /// Detach from the room for `path`. When the last socket leaves, checkpoint the
    /// document (landing every collaborator's byte spans in blame and persisting
    /// the CRDT sidecar), clear the path's **live** marker so byte readers stop
    /// being told the durable blob may lag, and evict the room. The checkpoint runs
    /// under the registry lock so a concurrent join can't fork a fresh room off a
    /// half-written sidecar — or clear the marker out from under a room that is
    /// still taking edits.
    async fn leave(&self, key: &RoomKey, ctx: WriteCtx) {
        let mut rooms = self.rooms.lock().await;
        let evict = match rooms.get_mut(key) {
            Some(slot) => {
                slot.conns = slot.conns.saturating_sub(1);
                slot.conns == 0
            }
            None => return,
        };
        if evict {
            if let Some(slot) = rooms.get(key) {
                // Best-effort: a failed write-back must not wedge the registry, but
                // it does mean this room's since-last edits aren't yet durable.
                let _ = self.write_back(key, &slot.room, ctx).await;
            }
            // Only after the final write-back: until it lands, the durable blob
            // really does lag the document, and the marker is what says so.
            //
            // A tree room clears the marker too, even though the *file* may still
            // lag — the host's last checkpoint is as fresh as its bytes will get,
            // and leaving the flag set would tell every future reader that a
            // document nobody has open may be stale.
            let _ = self.ws.end_coedit(key.path()).await;
            rooms.remove(key);
        }
    }

    /// Checkpoint every live room without evicting it — the durability knob for
    /// long-lived rooms whose sockets never all disconnect. An embedder can call
    /// this on a timer; per-room state loss on a crash is bounded by the interval.
    pub async fn checkpoint_all(&self, ctx: WriteCtx) {
        let rooms: Vec<(RoomKey, Arc<Room>)> = {
            let guard = self.rooms.lock().await;
            guard
                .iter()
                .map(|(k, slot)| (k.clone(), slot.room.clone()))
                .collect()
        };
        for (key, room) in rooms {
            let _ = self.write_back(&key, &room, ctx).await;
        }
    }
}

/// A fresh, practically-unique worker id (128 random bits, hex).
fn new_origin() -> String {
    let mut b = [0u8; 16];
    getrandom::getrandom(&mut b).expect("getrandom for worker id");
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The cross-worker relay drain: subscribe to peers' published deltas and, for
/// each one this worker is hosting, merge it (without re-attribution — it was
/// already attributed by its origin) and fan it out to this worker's sockets.
/// Runs until the subscription connection closes; a non-Postgres workspace makes
/// `coedit_subscribe` error and the task exits immediately (single-worker mode).
#[cfg(feature = "postgres")]
async fn relay_drain(
    ws: Arc<Workspace>,
    rooms: Arc<Mutex<HashMap<RoomKey, RoomSlot>>>,
    origin: Arc<str>,
) {
    let mut sub = match ws.coedit_subscribe().await {
        Ok(s) => s,
        Err(_) => return,
    };
    loop {
        let batch = match sub.recv().await {
            Ok(b) if !b.is_empty() => b,
            _ => break, // empty batch => connection closed; error => give up
        };
        for note in batch {
            if &*origin == note.origin.as_str() {
                continue; // our own op — already applied and fanned out locally
            }
            let room = rooms
                .lock()
                .await
                .iter()
                .find(|(key, _)| key.relay_key() == note.path)
                .map(|(_, slot)| slot.room.clone());
            let Some(room) = room else {
                continue; // not hosting this document on this worker
            };
            {
                let doc = room.doc.lock().await;
                if doc.apply_relayed(&note.delta).is_err() {
                    continue;
                }
            }
            // Deliberately *not* `touch_edit`: the worker that originated this
            // edit marked its own room dirty and will checkpoint it. Marking it
            // here too would have every worker hosting the document write the same
            // converged content, which is wasted I/O rather than extra safety.
            let _ = room.tx.send((RELAY_ORIGIN, Bytes::from(note.delta)));
        }
    }
}

/// The `?token=` query for browser WebSocket clients that can't set headers.
///
/// Kept working, but no longer the only option for a browser — see
/// [`authenticate_ws`] for why `Sec-WebSocket-Protocol` is the better one.
#[derive(Deserialize)]
pub(crate) struct TokenQuery {
    token: Option<String>,
}

/// The tree socket's query: a credential (as above) plus the `XmlFragment` root the
/// editor binds to, since editors differ (`y-prosemirror` uses `"prosemirror"`,
/// `@platejs/yjs` is configurable). Defaults to
/// [`DEFAULT_TREE_ROOT`](crate::DEFAULT_TREE_ROOT).
#[derive(Deserialize)]
pub(crate) struct TreeQuery {
    token: Option<String>,
    root: Option<String>,
}

/// The subprotocol a browser client offers to carry its credential:
/// `Sec-WebSocket-Protocol: origofs, <token>`. The server echoes back `origofs`
/// (never the token) as the selected protocol.
const AUTH_SUBPROTOCOL: &str = "origofs";

/// `GET /coedit/{*path}` — upgrade to a y-sync WebSocket for live co-editing.
/// Cap on a single inbound y-sync frame.
///
/// The co-editing socket is outside `DefaultBodyLimit`'s reach (that layer only
/// sees buffering extractors), so without this the route had no size bound of any
/// kind while every other write path had one. 16 MiB is generous for a CRDT update
/// over a text document — an initial `SyncStep2` carrying a large document's whole
/// state is the biggest legitimate frame — and bounds what one socket can make the
/// server allocate.
const MAX_COEDIT_FRAME: usize = 16 * 1024 * 1024;

pub(crate) async fn coedit_ws(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(q): Query<TokenQuery>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let path = abspath(&path);
    upgrade_room(
        state,
        RoomKey::Flat(path),
        q.token.as_deref(),
        headers,
        upgrade,
    )
    .await
}

/// `GET /coedit-tree/{*path}` — the same y-sync socket over the **tree** document
/// shape (#92), so `@platejs/yjs`, `y-prosemirror`, `y-slate` or TipTap can bind
/// natively instead of mirroring a flat `Y.Text`.
///
/// Identical in every other respect: same authentication, same frame cap, same
/// per-connection session. It differs only in what reaches durable storage — see
/// [`Coordinator::checkpoint_tree`], which the host calls with its own serialized
/// bytes, because origofs does not own the document schema.
pub(crate) async fn coedit_tree_ws(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(q): Query<TreeQuery>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let key = RoomKey::Tree {
        root: q
            .root
            .unwrap_or_else(|| crate::DEFAULT_TREE_ROOT.to_string()),
        path: abspath(&path),
    };
    upgrade_room(state, key, q.token.as_deref(), headers, upgrade).await
}

/// Authenticate an upgrade and hand the socket to its room. Shared by both shapes
/// so neither can drift from the other on identity, framing, or session binding.
async fn upgrade_room(
    state: AppState,
    key: RoomKey,
    token: Option<&str>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let principal = match authenticate_ws(&state, &headers, token).await {
        Some(p) => p,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                "unauthenticated: a valid credential is required",
            )
                .into_response();
        }
    };
    // A connection with no session produces edits `revert_session` can never undo
    // — on the surface that generates *more* edits than any other, since every
    // keystroke is one. So open a session for the connection when the credential
    // didn't bind one. One session per connection is the only sensible unit here:
    // it is exactly the span of "what this person typed in this sitting", which is
    // what a reviewer wants to undo (#98).
    let ctx = match session_bound_ctx(&state, &principal).await {
        Ok(ctx) => ctx,
        Err(e) => return super::ApiError::OrigoFS(e).into_response(),
    };
    let coord = state.coedit.clone();
    // `DefaultBodyLimit` does not apply to WebSocket frames, so `ApiOptions::
    // max_body_bytes` silently did not govern this route at all — the one hole in
    // the request budget. A y-sync frame is a CRDT update for a text document;
    // `MAX_COEDIT_FRAME` is far above any legitimate one and far below what an
    // unbounded frame could allocate.
    upgrade
        .max_message_size(MAX_COEDIT_FRAME)
        .max_frame_size(MAX_COEDIT_FRAME)
        // Echo `origofs` back as the selected subprotocol when the client offered
        // it. A browser fails the handshake if it proposes protocols and the
        // server names none of them, so the credential-carrying form only works
        // with this. Only the marker is ever echoed — never the token beside it.
        .protocols([AUTH_SUBPROTOCOL])
        .on_upgrade(move |socket| serve_socket(coord, ctx, key, socket))
}

/// One `(byte_start, byte_end, node)` entry of a checkpoint's span map.
#[derive(Deserialize)]
pub(crate) struct TreeSpanDto {
    start: u64,
    end: u64,
    node: String,
}

/// `POST /coedit-tree/checkpoint/{*path}` — the host lands a tree document's bytes.
#[derive(Deserialize)]
pub(crate) struct TreeCheckpointReq {
    /// The host's serialization of the document. UTF-8 text; origofs stores it
    /// verbatim and never re-derives it.
    body: String,
    /// Which byte ranges came from which co-edit node. Ordered, non-overlapping,
    /// on character boundaries. Ranges left uncovered — the serializer's own
    /// punctuation — are attributed to the authenticated actor.
    #[serde(default)]
    spans: Vec<TreeSpanDto>,
    /// The `XmlFragment` root, matching the socket's `?root=`.
    #[serde(default)]
    root: Option<String>,
}

/// Checkpoint a tree-shaped co-edited document (#92).
///
/// The flat shape has no equivalent route because it does not need one: the server
/// can materialize a `Y.Text`, so it checkpoints itself on a timer and on last
/// leave. A tree's bytes exist only once the host's serializer has run, so the host
/// is the only party that can supply them — along with the span map that says which
/// bytes came from which node. Authorship is still resolved server-side from
/// origofs's own stamps; the request names byte ranges and node ids, never an actor.
pub(crate) async fn coedit_tree_checkpoint(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
    axum::Json(req): axum::Json<TreeCheckpointReq>,
) -> Response {
    let Some(principal) = state.auth.authenticate(&headers).await else {
        return (
            StatusCode::UNAUTHORIZED,
            "unauthenticated: a valid credential is required",
        )
            .into_response();
    };
    let ctx = match session_bound_ctx(&state, &principal).await {
        Ok(ctx) => ctx,
        Err(e) => return super::ApiError::OrigoFS(e).into_response(),
    };
    let root = req
        .root
        .unwrap_or_else(|| crate::DEFAULT_TREE_ROOT.to_string());
    let spans: Vec<TreeSpan> = req
        .spans
        .into_iter()
        .map(|s| TreeSpan::new(s.start, s.end, s.node))
        .collect();
    let path = abspath(&path);
    match state
        .coedit
        .checkpoint_tree(&path, &root, ctx, req.body.as_bytes(), &spans)
        .await
    {
        Ok(()) => axum::Json(serde_json::json!({
            "path": path,
            "bytes": req.body.len(),
            "spans": spans.len(),
        }))
        .into_response(),
        Err(e) => super::ApiError::OrigoFS(e).into_response(),
    }
}

/// Authenticate a WebSocket upgrade, in the order a client should prefer.
///
/// 1. **The real upgrade headers** — programmatic clients and same-origin cookies.
/// 2. **`Sec-WebSocket-Protocol: origofs, <token>`** — the one header a *browser*
///    can set on an upgrade. This is the recommended browser path.
/// 3. **`?token=`** — kept working, because it was the documented answer, but it
///    is the worst place for a credential: URLs land in access logs, proxy logs,
///    and `Referer`-adjacent tooling by default, while a subprotocol value does
///    not (#98).
///
/// All three synthesize a `Bearer` credential and run through the same
/// [`Authenticator`](super::Authenticator), so a host writes its auth once.
async fn authenticate_ws(
    state: &AppState,
    headers: &HeaderMap,
    token: Option<&str>,
) -> Option<super::Principal> {
    if let Some(p) = state.auth.authenticate(headers).await {
        return Some(p);
    }
    if let Some(t) = subprotocol_token(headers)
        && let Some(p) = authenticate_token(state, &t).await
    {
        return Some(p);
    }
    authenticate_token(state, token?).await
}

/// Run a bare token through the host's [`Authenticator`] by synthesizing the
/// `Authorization: Bearer …` header it already knows how to read.
async fn authenticate_token(state: &AppState, token: &str) -> Option<super::Principal> {
    let mut synth = HeaderMap::new();
    synth.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).ok()?,
    );
    state.auth.authenticate(&synth).await
}

/// The credential offered as a WebSocket subprotocol, i.e. the first entry after
/// `origofs` in `Sec-WebSocket-Protocol: origofs, <token>`.
///
/// Returns `None` unless the list actually leads with our marker, so an unrelated
/// subprotocol negotiation is never mistaken for a credential.
fn subprotocol_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get("sec-websocket-protocol")?.to_str().ok()?;
    let mut parts = raw.split(',').map(str::trim).filter(|s| !s.is_empty());
    if parts.next()? != AUTH_SUBPROTOCOL {
        return None;
    }
    parts.next().map(str::to_string)
}

/// The [`WriteCtx`] a socket's edits are attributed to, with a session guaranteed.
///
/// A credential that binds a session is used as-is — the host has said what unit
/// of work this is. A bare actor credential gets a fresh session opened for the
/// connection, because the alternative is edits stamped `(actor, None)`, which
/// `revert_session` can never undo. That is the feature the op-log exists for,
/// missing on the surface that produces the most edits (#98).
async fn session_bound_ctx(
    state: &AppState,
    principal: &super::Principal,
) -> Result<WriteCtx, OrigoFSError> {
    if let Some(s) = principal.session {
        return Ok(WriteCtx::session(principal.actor, s));
    }
    let session = state
        .ws
        .create_session(principal.actor, Some("coedit"))
        .await?;
    Ok(WriteCtx::session(principal.actor, session))
}

/// Drive one connected socket: greet it, then pump y-sync frames both ways —
/// inbound frames applied to (and attributed in) the shared doc and fanned out to
/// peers, peers' frames delivered to this socket. Leaves the room on disconnect.
async fn serve_socket(coord: Coordinator, ctx: WriteCtx, key: RoomKey, socket: WebSocket) {
    coord.ensure_relay(); // idempotent; starts the cross-worker drain on first use
    coord.ensure_sweeper(); // idempotent; starts periodic checkpointing (#97)
    let room = match coord.join(&key, ctx).await {
        Ok(r) => r,
        Err(_) => return, // couldn't open the doc; drop the connection
    };
    let conn_id = coord.next_conn.fetch_add(1, Ordering::Relaxed);
    let mut rx = room.tx.subscribe();
    let (mut sink, mut stream) = socket.split();

    // Greet with SyncStep1 so the client sends us what it has (and learns what it
    // lacks in our SyncStep2 answer).
    let greeting = {
        let doc = room.doc.lock().await;
        doc.sync_start()
    };
    if sink.send(Message::Binary(greeting.into())).await.is_err() {
        coord.leave(&key, ctx).await;
        return;
    }

    loop {
        tokio::select! {
            incoming = stream.next() => {
                let Some(Ok(msg)) = incoming else { break };
                match msg {
                    Message::Binary(data) => {
                        let out = {
                            let doc = room.doc.lock().await;
                            doc.handle_sync(ctx, &data)
                        };
                        let Ok(out) = out else { continue }; // skip a malformed frame
                        if !out.reply.is_empty()
                            && sink.send(Message::Binary(out.reply.into())).await.is_err()
                        {
                            break;
                        }
                        if !out.broadcast.is_empty() {
                            // A frame worth broadcasting is a frame that changed
                            // the document, which is exactly what makes the room
                            // due for a checkpoint.
                            room.touch_edit(coord.epoch);
                            let frame = Bytes::from(out.broadcast);
                            // Local sockets first (instant), then peer workers.
                            let _ = room.tx.send((conn_id, frame.clone()));
                            coord.relay_publish(&key, frame);
                        }
                    }
                    Message::Close(_) => break,
                    _ => {} // text/ping/pong — axum answers pings itself
                }
            }
            bcast = rx.recv() => {
                match bcast {
                    // Deliver peers' frames; skip our own echo.
                    Ok((origin, frame)) if origin != conn_id => {
                        if sink.send(Message::Binary(frame)).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    // Fell too far behind: the CRDT reconverges on the next edit.
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    coord.leave(&key, ctx).await;
}
