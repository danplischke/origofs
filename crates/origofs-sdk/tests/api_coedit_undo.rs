//! Undo/redo over the HTTP surface, against a live co-editing room (#146).
//!
//! Two things are being asserted that the engine-level tests cannot reach. The
//! **fan-out**: a client that presses Ctrl+Z has not applied the undo locally —
//! the pop happened on the server — so unlike its own edits it must *receive*
//! the resulting frame, and so must every other socket in the room. And the
//! **authorization**: an undo is a write, so it takes `WRITE` at the path exactly
//! as opening the document does. A socket authenticates but does not authorize,
//! which is the same reasoning that made the checkpoints re-check.
#![cfg(all(feature = "api", feature = "coedit"))]

use axum::Router;
use axum::body::Body;
use futures::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use origofs_sdk::api::{BearerAuth, router};
use origofs_sdk::{Perms, Workspace};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tower::ServiceExt;
use yrs::sync::{Awareness, DefaultProtocol, Message as Y, Protocol, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, Text, Transact};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn frame(msgs: &[Y]) -> Vec<u8> {
    let mut e = EncoderV1::new();
    for m in msgs {
        m.encode(&mut e);
    }
    e.to_vec()
}

async fn next_frame(sock: &mut Socket) -> Option<Vec<u8>> {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), sock.next()).await {
            Ok(Some(Ok(Ws::Binary(b)))) => return Some(b.to_vec()),
            Ok(Some(Ok(Ws::Close(_)))) | Ok(None) => return None,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Err(_) => return None,
        }
    }
}

async fn pump(sock: &mut Socket, awareness: &Awareness) {
    let Some(data) = next_frame(sock).await else {
        return;
    };
    let responses = DefaultProtocol.handle(awareness, &data).unwrap();
    if !responses.is_empty() {
        sock.send(Ws::Binary(frame(&responses))).await.unwrap();
    }
}

fn text_of(awareness: &Awareness) -> String {
    let text = awareness.doc().get_or_insert_text("content");
    text.get_string(&awareness.doc().transact())
}

/// Drain frames until `want` arrives, or give up — a converged client is the
/// assertion, and polling is how a fan-out test observes one.
async fn converge(sock: &mut Socket, awareness: &Awareness, want: &str) {
    for _ in 0..8 {
        if text_of(awareness) == want {
            return;
        }
        pump(sock, awareness).await;
    }
}

struct Server {
    addr: std::net::SocketAddr,
    /// A clone of the served router, for the plain-HTTP half of these tests.
    ///
    /// It shares the served copy's `AppState` — and therefore the very same
    /// `Coordinator` and room registry — because both are `Arc`s behind a
    /// `Clone`. So a `oneshot` here reaches the rooms the live WebSockets are
    /// attached to, and the suite needs no HTTP client dependency to drive a
    /// route that happens to sit beside a socket.
    app: Router,
    ws: Arc<Workspace>,
    _dir: tempfile::TempDir,
}

/// A workspace with alice and bob, both able to write everywhere, behind a real
/// server on an ephemeral port.
async fn serve() -> (Server, i64, i64) {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alice = ws.create_human("alice", None).await.unwrap();
    let alice_s = ws.create_session(alice, Some("web")).await.unwrap();
    let bob = ws.create_human("bob", None).await.unwrap();
    let bob_s = ws.create_session(bob, Some("web")).await.unwrap();
    let auth = BearerAuth::new()
        .with_token("tok-alice", alice, Some(alice_s))
        .with_token("tok-bob", bob, Some(bob_s));
    let ws = Arc::new(ws);
    let app = router(ws.clone(), Arc::new(auth));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = app.clone();
    tokio::spawn(async move { axum::serve(listener, served).await.unwrap() });
    (
        Server {
            addr,
            app,
            ws,
            _dir: dir,
        },
        alice,
        bob,
    )
}

/// `POST /v1/coedit-undo/{path}` as `tok`, returning the status and body.
async fn post_undo(srv: &Server, tok: &str, path: &str, redo: bool) -> (u16, String) {
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(format!("/v1/coedit-undo/{path}"))
        .header("authorization", format!("Bearer {tok}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "redo": redo })).unwrap(),
        ))
        .unwrap();
    let res = srv.app.clone().oneshot(req).await.unwrap();
    let status = res.status().as_u16();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// The same request with no credential at all.
async fn post_undo_anonymous(srv: &Server, path: &str) -> u16 {
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(format!("/v1/coedit-undo/{path}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "redo": false })).unwrap(),
        ))
        .unwrap();
    srv.app
        .clone()
        .oneshot(req)
        .await
        .unwrap()
        .status()
        .as_u16()
}

/// The whole round trip: alice types, presses Ctrl+Z, and both her own socket and
/// bob's converge on the undone document.
#[tokio::test]
async fn an_undo_request_reaches_every_socket_in_the_room() {
    let (srv, _alice, _bob) = serve().await;
    let url = |tok: &str| format!("ws://{}/v1/coedit/doc.md?token={tok}", srv.addr);
    let (mut a, _) = tokio_tungstenite::connect_async(url("tok-alice"))
        .await
        .unwrap();
    let (mut b, _) = tokio_tungstenite::connect_async(url("tok-bob"))
        .await
        .unwrap();
    let a_aware = Awareness::new(Doc::new());
    let b_aware = Awareness::new(Doc::new());
    pump(&mut a, &a_aware).await;
    pump(&mut b, &b_aware).await;

    let update = {
        let text = a_aware.doc().get_or_insert_text("content");
        let mut txn = a_aware.doc().transact_mut();
        text.insert(&mut txn, 0, "hello from alice");
        txn.encode_update_v1()
    };
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();
    converge(&mut b, &b_aware, "hello from alice").await;
    assert_eq!(text_of(&b_aware), "hello from alice");

    let (status, body) = post_undo(&srv, "tok-alice", "doc.md", false).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"changed\":true"), "{body}");

    // Alice's own socket must receive it: the pop happened on the server, so
    // unlike her own typing she has not applied it locally.
    converge(&mut a, &a_aware, "").await;
    assert_eq!(
        text_of(&a_aware),
        "",
        "the socket that asked for the undo never received it"
    );
    // And so must every other socket in the room.
    converge(&mut b, &b_aware, "").await;
    assert_eq!(text_of(&b_aware), "");

    // Redo comes back through the same channel.
    let (status, body) = post_undo(&srv, "tok-alice", "doc.md", true).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"changed\":true"), "{body}");
    converge(&mut a, &a_aware, "hello from alice").await;
    assert_eq!(text_of(&a_aware), "hello from alice");
}

/// Bob cannot undo alice's typing, even sharing her room on an authenticated
/// socket. This is the origin scoping, reaching all the way through the surface.
#[tokio::test]
async fn one_actor_cannot_undo_anothers_work_through_the_route() {
    let (srv, _alice, _bob) = serve().await;
    let url = |tok: &str| format!("ws://{}/v1/coedit/doc.md?token={tok}", srv.addr);
    let (mut a, _) = tokio_tungstenite::connect_async(url("tok-alice"))
        .await
        .unwrap();
    let (mut b, _) = tokio_tungstenite::connect_async(url("tok-bob"))
        .await
        .unwrap();
    let a_aware = Awareness::new(Doc::new());
    let b_aware = Awareness::new(Doc::new());
    pump(&mut a, &a_aware).await;
    pump(&mut b, &b_aware).await;

    let update = {
        let text = a_aware.doc().get_or_insert_text("content");
        let mut txn = a_aware.doc().transact_mut();
        text.insert(&mut txn, 0, "alice wrote this");
        txn.encode_update_v1()
    };
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();
    converge(&mut b, &b_aware, "alice wrote this").await;

    // Bob has WRITE, so he is authorized — and still has nothing to undo,
    // because the stack is scoped to his own origins.
    let (status, body) = post_undo(&srv, "tok-bob", "doc.md", false).await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("\"changed\":false"),
        "bob undid alice's typing: {body}"
    );
    assert_eq!(text_of(&b_aware), "alice wrote this");
}

/// An undo is a write, so it takes `WRITE` at the path. Bob can read the document
/// but not write it, and must be refused rather than quietly no-op'd — an editor
/// showing the key working while nothing happens is worse than one that says no.
#[tokio::test]
async fn undo_is_refused_without_write_at_the_path() {
    let (srv, alice, bob) = serve().await;

    srv.ws
        .write_as(origofs_sdk::WriteCtx::actor(alice), "/doc.md", b"seed\n")
        .await
        .unwrap();
    // Default-deny, then read-only for bob: he may look, not write.
    srv.ws.set_acl_default_deny(true).await.unwrap();
    srv.ws
        .grant(alice, "/", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    srv.ws
        .grant(bob, "/", Perms::READ, Some(alice))
        .await
        .unwrap();

    // Alice opens the room and types, so there is a stack to be refused at.
    let url = format!("ws://{}/v1/coedit/doc.md?token=tok-alice", srv.addr);
    let (mut a, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let a_aware = Awareness::new(Doc::new());
    pump(&mut a, &a_aware).await;

    let (status, body) = post_undo(&srv, "tok-bob", "doc.md", false).await;
    assert_eq!(
        status, 403,
        "an actor without WRITE must be refused an undo, not silently no-op'd: {body}"
    );
}

/// Undo on a path nobody has open is "nothing to undo", not an error and not an
/// implicit open — opening here would mark the path live with no socket whose
/// disconnect ever clears it.
#[tokio::test]
async fn undo_without_a_live_room_changes_nothing() {
    let (srv, _alice, _bob) = serve().await;
    let (status, body) = post_undo(&srv, "tok-alice", "never-opened.md", false).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"changed\":false"), "{body}");
    assert!(
        srv.ws.live_paths().await.unwrap().is_empty(),
        "undo opened a room and left the path marked live with no socket to clear it"
    );
}

#[tokio::test]
async fn undo_requires_a_credential() {
    let (srv, _alice, _bob) = serve().await;
    assert_eq!(post_undo_anonymous(&srv, "doc.md").await, 401);
}
