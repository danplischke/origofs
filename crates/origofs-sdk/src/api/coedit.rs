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
//! from the client. Because browsers can't set headers on a WebSocket, the token
//! may ride in a `?token=` query param; it is authenticated through the same
//! [`Authenticator`](super::Authenticator) as every other route. Content a socket
//! contributes is attributed to *its* principal by the engine, no matter what the
//! bytes claim.
//!
//! **Multiple workers.** The registry is per-process, so two sockets editing one
//! document on *different* workers would drift. When the workspace is
//! Postgres-backed, the coordinator bridges them over the cross-worker relay
//! (`LISTEN/NOTIFY` + a small op table): every attributed delta is published, and
//! a background task applies peers' deltas to this worker's rooms and fans them
//! out to its sockets, so all replicas converge. A joining room replays recent ops
//! to catch up. On SQLite (single-writer) the relay is simply off.

use super::{AppState, abspath};
use crate::{CoeditDoc, OrigoFSError, Workspace, WriteCtx};
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

/// A live co-editing room: one shared, attributed CRDT plus a fan-out channel.
/// Every socket editing the same path attaches to the same `Room`.
struct Room {
    /// The shared document. `handle_sync` holds this only for the (synchronous)
    /// duration of applying one payload — never across an `.await`.
    doc: Mutex<CoeditDoc>,
    /// Origin-tagged y-sync frames fanned out to the room. Each socket skips
    /// frames it originated — it already applied its own edit locally.
    tx: broadcast::Sender<(u64, Bytes)>,
}

/// One registry entry: the room and how many sockets are currently attached.
struct RoomSlot {
    room: Arc<Room>,
    conns: usize,
}

/// The room registry backing the WebSocket endpoint. Cheap to clone (a handful of
/// `Arc`s); hand a clone to the router state.
#[derive(Clone)]
pub struct Coordinator {
    ws: Arc<Workspace>,
    rooms: Arc<Mutex<HashMap<String, RoomSlot>>>,
    next_conn: Arc<AtomicU64>,
    /// This worker's unique id, tagged on every published op so the relay drain
    /// can skip our own echo. Only meaningful with the relay, i.e. on Postgres.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    origin: Arc<str>,
    /// Whether the cross-worker relay is available (Postgres-backed workspace).
    relay: bool,
    /// Guards one-time spawn of the relay drain task.
    relay_started: Arc<AtomicBool>,
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
    async fn join(&self, path: &str, ctx: WriteCtx) -> Result<Arc<Room>, OrigoFSError> {
        let mut rooms = self.rooms.lock().await;
        if let Some(slot) = rooms.get_mut(path) {
            slot.conns += 1;
            return Ok(slot.room.clone());
        }
        let doc = self.ws.open_coedit(ctx, path).await?;
        // The relay is Postgres-backed (`LISTEN`/`NOTIFY`), so without that backend
        // compiled in there is nothing to catch up from — a single-worker build is
        // a valid configuration, not a degraded one.
        #[cfg(feature = "postgres")]
        if self.relay {
            // Ensure the relay table exists before this room takes edits, so the
            // first publish can't race it, then replay recent ops to catch up.
            let _ = self.ws.coedit_relay_init().await;
            if let Ok(notes) = self.ws.coedit_replay(path).await {
                for note in notes {
                    let _ = doc.apply_relayed(&note.delta);
                }
            }
        }
        let (tx, _) = broadcast::channel(FANOUT_CAPACITY);
        let room = Arc::new(Room {
            doc: Mutex::new(doc),
            tx,
        });
        rooms.insert(
            path.to_string(),
            RoomSlot {
                room: room.clone(),
                conns: 1,
            },
        );
        Ok(room)
    }

    /// Publish an attributed delta (a y-sync frame) to peer workers, if the relay
    /// is on. Fire-and-forget: CRDT merges commute, so relay order doesn't matter,
    /// and the socket loop never blocks on the database.
    fn relay_publish(&self, path: &str, frame: Bytes) {
        if !self.relay {
            return;
        }
        #[cfg(feature = "postgres")]
        {
            let ws = self.ws.clone();
            let origin = self.origin.clone();
            let path = path.to_string();
            tokio::spawn(async move {
                let _ = ws.coedit_publish(&path, &origin, &frame).await;
            });
        }
        // Nothing to publish to without the relay; the local fan-out above has
        // already delivered this frame to every socket on this worker.
        #[cfg(not(feature = "postgres"))]
        let _ = (path, frame);
    }

    /// Detach from the room for `path`. When the last socket leaves, checkpoint the
    /// document (landing every collaborator's byte spans in blame and persisting
    /// the CRDT sidecar), clear the path's **live** marker so byte readers stop
    /// being told the durable blob may lag, and evict the room. The checkpoint runs
    /// under the registry lock so a concurrent join can't fork a fresh room off a
    /// half-written sidecar — or clear the marker out from under a room that is
    /// still taking edits.
    async fn leave(&self, path: &str, ctx: WriteCtx) {
        let mut rooms = self.rooms.lock().await;
        let evict = match rooms.get_mut(path) {
            Some(slot) => {
                slot.conns = slot.conns.saturating_sub(1);
                slot.conns == 0
            }
            None => return,
        };
        if evict {
            if let Some(slot) = rooms.get(path) {
                let doc = slot.room.doc.lock().await;
                // Best-effort: a failed checkpoint must not wedge the registry, but
                // it does mean this room's since-last edits aren't yet durable.
                let _ = self.ws.checkpoint_coedit(ctx, path, &doc).await;
            }
            // Only after the final checkpoint: until it lands, the durable blob
            // really does lag the document, and the marker is what says so.
            let _ = self.ws.end_coedit(path).await;
            rooms.remove(path);
        }
    }

    /// Checkpoint every live room without evicting it — the durability knob for
    /// long-lived rooms whose sockets never all disconnect. An embedder can call
    /// this on a timer; per-room state loss on a crash is bounded by the interval.
    pub async fn checkpoint_all(&self, ctx: WriteCtx) {
        let rooms: Vec<(String, Arc<Room>)> = {
            let guard = self.rooms.lock().await;
            guard
                .iter()
                .map(|(p, slot)| (p.clone(), slot.room.clone()))
                .collect()
        };
        for (path, room) in rooms {
            let doc = room.doc.lock().await;
            let _ = self.ws.checkpoint_coedit(ctx, &path, &doc).await;
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
    rooms: Arc<Mutex<HashMap<String, RoomSlot>>>,
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
            let Some(room) = rooms.lock().await.get(&note.path).map(|s| s.room.clone()) else {
                continue; // not hosting this document on this worker
            };
            {
                let doc = room.doc.lock().await;
                if doc.apply_relayed(&note.delta).is_err() {
                    continue;
                }
            }
            let _ = room.tx.send((RELAY_ORIGIN, Bytes::from(note.delta)));
        }
    }
}

/// The `?token=` query for browser WebSocket clients that can't set headers.
#[derive(Deserialize)]
pub(crate) struct TokenQuery {
    token: Option<String>,
}

/// `GET /coedit/{*path}` — upgrade to a y-sync WebSocket for live co-editing.
pub(crate) async fn coedit_ws(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(q): Query<TokenQuery>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let principal = match authenticate_ws(&state, &headers, q.token.as_deref()).await {
        Some(p) => p,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                "unauthenticated: a valid credential is required",
            )
                .into_response();
        }
    };
    let path = abspath(&path);
    let coord = state.coedit.clone();
    upgrade.on_upgrade(move |socket| serve_socket(coord, principal.write_ctx(), path, socket))
}

/// Authenticate a WebSocket upgrade: the real upgrade headers first (programmatic
/// clients, cookies), then — for browsers that can't set headers — a `?token=`
/// query param synthesized as a `Bearer` credential and run through the same
/// [`Authenticator`](super::Authenticator).
async fn authenticate_ws(
    state: &AppState,
    headers: &HeaderMap,
    token: Option<&str>,
) -> Option<super::Principal> {
    if let Some(p) = state.auth.authenticate(headers).await {
        return Some(p);
    }
    let token = token?;
    let mut synth = HeaderMap::new();
    synth.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).ok()?,
    );
    state.auth.authenticate(&synth).await
}

/// Drive one connected socket: greet it, then pump y-sync frames both ways —
/// inbound frames applied to (and attributed in) the shared doc and fanned out to
/// peers, peers' frames delivered to this socket. Leaves the room on disconnect.
async fn serve_socket(coord: Coordinator, ctx: WriteCtx, path: String, socket: WebSocket) {
    coord.ensure_relay(); // idempotent; starts the cross-worker drain on first use
    let room = match coord.join(&path, ctx).await {
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
        coord.leave(&path, ctx).await;
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
                            let frame = Bytes::from(out.broadcast);
                            // Local sockets first (instant), then peer workers.
                            let _ = room.tx.send((conn_id, frame.clone()));
                            coord.relay_publish(&path, frame);
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

    coord.leave(&path, ctx).await;
}
