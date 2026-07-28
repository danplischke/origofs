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
//! [`Authenticator`](crate::Authenticator) as every other route. Content a socket
//! contributes is attributed to *its* principal by the engine, no matter what the
//! bytes claim.

use crate::{abspath, AppState};
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{header::AUTHORIZATION, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use futures::{SinkExt, StreamExt};
use origofs_sdk::{CoeditDoc, OrigoFSError, Workspace, WriteCtx};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

/// How many y-sync frames a slow socket can fall behind before it's dropped from
/// the fan-out. A dropped frame only costs a resync on the next edit (the CRDT
/// converges regardless), so this can stay modest.
const FANOUT_CAPACITY: usize = 256;

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

/// The room registry backing the WebSocket endpoint. Cheap to clone (a couple of
/// `Arc`s); hand a clone to the router state.
#[derive(Clone)]
pub struct Coordinator {
    ws: Arc<Workspace>,
    rooms: Arc<Mutex<HashMap<String, RoomSlot>>>,
    next_conn: Arc<AtomicU64>,
}

impl Coordinator {
    pub fn new(ws: Arc<Workspace>) -> Self {
        Self {
            ws,
            rooms: Arc::new(Mutex::new(HashMap::new())),
            next_conn: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Attach to the room for `path`, creating it (opening the document, attributed
    /// to `ctx` for any promotion of existing file text) on the first join.
    async fn join(&self, path: &str, ctx: WriteCtx) -> Result<Arc<Room>, OrigoFSError> {
        let mut rooms = self.rooms.lock().await;
        if let Some(slot) = rooms.get_mut(path) {
            slot.conns += 1;
            return Ok(slot.room.clone());
        }
        let doc = self.ws.open_coedit(ctx, path).await?;
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

    /// Detach from the room for `path`. When the last socket leaves, checkpoint the
    /// document (landing every collaborator's byte spans in blame and persisting
    /// the CRDT sidecar) and evict the room. The checkpoint runs under the registry
    /// lock so a concurrent join can't fork a fresh room off a half-written sidecar.
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
/// [`Authenticator`](crate::Authenticator).
async fn authenticate_ws(
    state: &AppState,
    headers: &HeaderMap,
    token: Option<&str>,
) -> Option<crate::Principal> {
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
                            let _ = room.tx.send((conn_id, Bytes::from(out.broadcast)));
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
