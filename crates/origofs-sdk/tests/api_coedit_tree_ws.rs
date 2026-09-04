//! The structured co-editing surface (#92) end to end: a genuine Yjs client — a
//! raw `yrs::Doc` driven by the stock y-sync `DefaultProtocol` — binds to
//! `/v1/coedit-tree/{path}` over a `Y.XmlFragment`, and the host lands the bytes
//! with its own span map. Requires the `api` + `coedit` features.
//!
//! This client is the **`y-prosemirror`/TipTap** shape, and used to claim it was
//! `@platejs/yjs`'s too. It is not: Plate binds through `@slate-yjs/core`, which
//! roots at a `Y.XmlText`, so this exercises the socket and the checkpoint but
//! says nothing about that binding — which is how the compatibility claim in
//! #152 went unchecked. The Slate half is pinned separately, against bytes from
//! a real `@slate-yjs/core` client, in
//! `origofs-core/tests/coedit_tree_slate.rs`.
#![cfg(all(feature = "api", feature = "coedit"))]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use origofs_sdk::Workspace;
use origofs_sdk::api::{BearerAuth, router};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tower::ServiceExt;
use yrs::sync::{Awareness, DefaultProtocol, Message as Y, Protocol, SyncMessage};
use yrs::types::text::YChange;
use yrs::types::xml::{XmlElementPrelim, XmlFragment, XmlOut, XmlTextPrelim};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Any, Doc, Out, Text, Transact};

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

/// Process one inbound y-sync frame through the vanilla `DefaultProtocol`.
async fn pump(sock: &mut Socket, awareness: &Awareness) {
    let Some(data) = next_frame(sock).await else {
        return;
    };
    let responses = DefaultProtocol.handle(awareness, &data).unwrap();
    if !responses.is_empty() {
        sock.send(Ws::Binary(frame(&responses))).await.unwrap();
    }
}

/// Append a `<p>text</p>` to the client's fragment and return the y-sync update.
fn type_paragraph(awareness: &Awareness, text: &str) -> Vec<u8> {
    let frag = awareness.doc().get_or_insert_xml_fragment("content");
    let mut txn = awareness.doc().transact_mut();
    let p = frag.push_back(&mut txn, XmlElementPrelim::empty("p"));
    p.push_back(&mut txn, XmlTextPrelim::new(text));
    txn.encode_update_v1()
}

/// Every text run this client can see, as `(text, node id)` — what a host reads off
/// `ytext.toDelta()` to build its span map.
fn runs(awareness: &Awareness) -> Vec<(String, Option<String>)> {
    let frag = awareness.doc().get_or_insert_xml_fragment("content");
    let txn = awareness.doc().transact();
    let mut out = Vec::new();
    for node in frag.successors(&txn) {
        let XmlOut::Text(text) = node else { continue };
        for chunk in text.diff(&txn, YChange::identity) {
            let Out::Any(Any::String(piece)) = &chunk.insert else {
                continue;
            };
            let node_id = match chunk.attributes.as_deref().and_then(|a| a.get("n")) {
                Some(Any::String(id)) => Some(id.to_string()),
                _ => None,
            };
            out.push((piece.to_string(), node_id));
        }
    }
    out
}

/// The node id of the run whose text is exactly `text`, waiting for the server's
/// attribution delta to arrive if it has not yet.
async fn node_of(sock: &mut Socket, awareness: &Awareness, text: &str) -> String {
    for _ in 0..8 {
        if let Some((_, Some(id))) = runs(awareness).iter().find(|(t, _)| t == text) {
            return id.clone();
        }
        pump(sock, awareness).await;
    }
    panic!(
        "no node id ever arrived for {text:?}: {:?}",
        runs(awareness)
    );
}

async fn post_json(app: &Router, uri: &str, token: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::post(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// A workspace, a running server, and the same router in-process — the WebSocket
/// needs a real socket, the checkpoint is an ordinary POST, and both must reach the
/// *same* coordinator, which cloning the router preserves (its state is all `Arc`s).
struct Fixture {
    ws: Arc<Workspace>,
    app: Router,
    addr: std::net::SocketAddr,
    alice: i64,
    alice_s: i64,
    bob: i64,
    bob_s: i64,
}

async fn fixture() -> Fixture {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
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

    Fixture {
        ws,
        app,
        addr,
        alice,
        alice_s,
        bob,
        bob_s,
    }
}

// The whole point of #92: two people edit a *tree* through a native binding, and
// each one's exact byte ranges land in blame against the host's serialization —
// with no whole-file text diff anywhere in the loop.
#[tokio::test]
async fn vanilla_yjs_tree_clients_collaborate_and_the_host_lands_the_bytes() {
    let f = fixture().await;
    let url = |tok: &str| format!("ws://{}/v1/coedit-tree/notes.md?token={tok}", f.addr);
    let (mut a, _) = tokio_tungstenite::connect_async(url("tok-alice"))
        .await
        .unwrap();
    let (mut b, _) = tokio_tungstenite::connect_async(url("tok-bob"))
        .await
        .unwrap();
    let a_aware = Awareness::new(Doc::new());
    let b_aware = Awareness::new(Doc::new());

    pump(&mut a, &a_aware).await; // answer the server's SyncStep1 greeting
    pump(&mut b, &b_aware).await;

    // Alice types a paragraph.
    let update = type_paragraph(&a_aware, "hello");
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();

    // Bob converges through the server's fan-out, then types his own.
    for _ in 0..8 {
        if runs(&b_aware).iter().any(|(t, _)| t == "hello") {
            break;
        }
        pump(&mut b, &b_aware).await;
    }
    assert!(
        runs(&b_aware).iter().any(|(t, _)| t == "hello"),
        "Bob never saw Alice's paragraph: {:?}",
        runs(&b_aware)
    );
    let update = type_paragraph(&b_aware, "world");
    b.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();

    // The host reads the node ids off its own client — public Yjs API, no origofs
    // knowledge — and serializes however it likes.
    let hello = node_of(&mut b, &b_aware, "hello").await;
    let world = node_of(&mut b, &b_aware, "world").await;
    let body = "# Notes\n\nhello\n\nworld\n";
    let (status, json) = post_json(
        &f.app,
        "/v1/coedit-tree-checkpoint/notes.md",
        "tok-bob",
        json!({
            "body": body,
            "spans": [
                { "start": 9, "end": 14, "node": hello },
                { "start": 16, "end": 21, "node": world },
            ],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["bytes"], body.len());

    assert_eq!(&f.ws.read("/notes.md").await.unwrap()[..], body.as_bytes());
    let blame = f.ws.blame("/notes.md").await.unwrap();
    assert_eq!(
        blame
            .iter()
            .map(|r| (r.actor.id, r.byte_start, r.byte_end))
            .collect::<Vec<_>>(),
        vec![
            (f.bob, 0, 9),    // the serializer's heading, to the checkpointer
            (f.alice, 9, 14), // "hello"
            (f.bob, 14, 22),  // "world" and the punctuation around it
        ],
        "got {blame:?}"
    );
    assert_eq!(blame[1].session, Some(f.alice_s));
    assert_eq!(blame[0].session, Some(f.bob_s));

    drop(a);
    drop(b);
}

// The server cannot serialize a tree, so it cannot checkpoint one on a timer — but
// it can persist the CRDT, which is what keeps a worker crash from costing the
// editing history. The file stays where the last real checkpoint left it.
#[tokio::test]
async fn the_sweeper_persists_a_tree_room_without_inventing_a_body() {
    use origofs_sdk::api::{ApiOptions, CheckpointPolicy, router_with};

    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alice = ws.create_human("alice", None).await.unwrap();
    let alice_s = ws.create_session(alice, Some("web")).await.unwrap();
    let auth = BearerAuth::new().with_token("tok-alice", alice, Some(alice_s));
    let ws = Arc::new(ws);

    let options = ApiOptions {
        checkpoint: CheckpointPolicy {
            idle_after: Some(Duration::from_millis(50)),
            max_interval: None,
            tick: Duration::from_millis(20),
        },
        ..Default::default()
    };
    let app = router_with(ws.clone(), Arc::new(auth), options);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (mut a, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/v1/coedit-tree/doc.md?token=tok-alice"
    ))
    .await
    .unwrap();
    let aware = Awareness::new(Doc::new());
    pump(&mut a, &aware).await;
    let update = type_paragraph(&aware, "unsaved but not lost");
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();

    // Give the sweeper several ticks with the socket still open.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The file was never invented…
    assert!(
        ws.read("/doc.md").await.is_err(),
        "the server must not serialize a tree it has no schema for"
    );
    // …and the marker does not claim the bytes were crystallized…
    let live = ws.live_doc("/doc.md").await.unwrap().expect("still live");
    assert_eq!(
        live.checkpointed_at, None,
        "persisting a sidecar is not a checkpoint and must not be reported as one"
    );
    // …but the typing is durable: a fresh open resumes it.
    let ctx = origofs_sdk::WriteCtx::session(alice, alice_s);
    let resumed = ws
        .open_coedit_tree(ctx, "/doc.md", "content")
        .await
        .unwrap();
    assert!(
        resumed.resumed(),
        "the sweeper never persisted the room's CRDT"
    );
    assert_eq!(resumed.plain_text(), "unsaved but not lost");
    drop(a);
}

// A malformed span map is refused with an explanation rather than stored as blame
// nobody can render.
#[tokio::test]
async fn a_bad_span_map_is_refused() {
    let f = fixture().await;
    let (status, json) = post_json(
        &f.app,
        "/v1/coedit-tree-checkpoint/doc.md",
        "tok-alice",
        json!({
            "body": "abcd",
            "spans": [
                { "start": 0, "end": 3, "node": "x" },
                { "start": 2, "end": 4, "node": "y" },
            ],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert!(
        json.to_string().contains("non-overlapping"),
        "the error should say what is wrong with the map: {json}"
    );
    // Nothing was written on the way to refusing.
    assert!(f.ws.read("/doc.md").await.is_err());
}

// The checkpoint route names byte ranges and node ids — never an actor. Bob cannot
// make Alice the author of what he wrote by citing her.
#[tokio::test]
async fn the_checkpoint_request_cannot_name_an_author() {
    let f = fixture().await;
    let (mut b, _) = tokio_tungstenite::connect_async(format!(
        "ws://{}/v1/coedit-tree/doc.md?token=tok-bob",
        f.addr
    ))
    .await
    .unwrap();
    let aware = Awareness::new(Doc::new());
    pump(&mut b, &aware).await;
    let update = type_paragraph(&aware, "bob wrote this");
    b.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();
    let node = node_of(&mut b, &aware, "bob wrote this").await;

    // Bob checkpoints, citing his own node *and* trying an id shaped like Alice's
    // identity. Neither can reach her: the node id resolves through the server's
    // own stamps, and an id it never issued falls back to the checkpointer.
    let (status, json) = post_json(
        &f.app,
        "/v1/coedit-tree-checkpoint/doc.md",
        "tok-bob",
        json!({
            "body": "bob wrote thisand this",
            "spans": [
                { "start": 0, "end": 14, "node": node },
                { "start": 14, "end": 22, "node": format!("{},{}", f.alice, f.alice_s) },
            ],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");

    let blame = f.ws.blame("/doc.md").await.unwrap();
    assert!(
        blame.iter().all(|r| r.actor.id == f.bob),
        "a request must not be able to name an author: {blame:?}"
    );
    drop(b);
}
