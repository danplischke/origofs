//! Live co-editing over a real WebSocket (roadmap M8): a genuine Yjs client — a
//! raw `yrs::Doc` driven by the stock y-sync `DefaultProtocol`, exactly what
//! PlateJS and `y-websocket` run — connects to `/v1/coedit/{path}`, and its edits
//! merge, fan out to a second client, and land attributed in blame. Requires the
//! `coedit` feature.
#![cfg(all(feature = "api", feature = "coedit"))]

use futures::{SinkExt, StreamExt};
use origofs_sdk::Workspace;
use origofs_sdk::api::{BearerAuth, router};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use yrs::sync::{Awareness, DefaultProtocol, Message as Y, Protocol, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, Text, Transact};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Encode a batch of y-sync messages into one frame.
fn frame(msgs: &[Y]) -> Vec<u8> {
    let mut e = EncoderV1::new();
    for m in msgs {
        m.encode(&mut e);
    }
    e.to_vec()
}

/// Read the next binary frame (skipping pings/pongs), with a timeout so a stalled
/// test fails instead of hanging forever.
async fn next_frame(sock: &mut Socket) -> Option<Vec<u8>> {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), sock.next()).await {
            Ok(Some(Ok(Ws::Binary(b)))) => return Some(b.to_vec()),
            Ok(Some(Ok(Ws::Close(_)))) | Ok(None) => return None,
            Ok(Some(Ok(_))) => continue, // ping/pong/text — keep reading
            Ok(Some(Err(_))) | Err(_) => return None,
        }
    }
}

/// Process one inbound y-sync frame through the vanilla `DefaultProtocol`, sending
/// any protocol responses back over the socket.
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

// A real Yjs client types into a shared doc; a second client sees the edit through
// the server's fan-out and converges; after both disconnect, the server's
// checkpoint has landed the content attributed to the typist — server-side, from
// a client that knows nothing about origofs authorship.
#[tokio::test]
async fn vanilla_yjs_clients_collaborate_over_websocket() {
    // Server owns identity: actors exist in the DB, tokens map to them.
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

    // Bring up a real server on an ephemeral port.
    let app = router(ws.clone(), Arc::new(auth));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let url = |tok: &str| format!("ws://{addr}/v1/coedit/doc.md?token={tok}");
    let (mut a, _) = tokio_tungstenite::connect_async(url("tok-alice"))
        .await
        .unwrap();
    let (mut b, _) = tokio_tungstenite::connect_async(url("tok-bob"))
        .await
        .unwrap();
    let a_aware = Awareness::new(Doc::new());
    let b_aware = Awareness::new(Doc::new());

    // Handshake: each answers the server's SyncStep1 greeting.
    pump(&mut a, &a_aware).await;
    pump(&mut b, &b_aware).await;

    // Alice types; send it up as a y-sync Update.
    let update = {
        let text = a_aware.doc().get_or_insert_text("content");
        let mut txn = a_aware.doc().transact_mut();
        text.insert(&mut txn, 0, "hello from alice");
        txn.encode_update_v1()
    };
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();

    // Bob converges via the server's fan-out (drain frames until the text arrives).
    for _ in 0..8 {
        if text_of(&b_aware) == "hello from alice" {
            break;
        }
        pump(&mut b, &b_aware).await;
    }
    assert_eq!(text_of(&b_aware), "hello from alice");

    // Both leave; the last-leave checkpoint lands the content in blame.
    a.send(Ws::Close(None)).await.ok();
    b.send(Ws::Close(None)).await.ok();

    let blame = loop_until_blamed(&ws).await;
    assert_eq!(blame.len(), 1);
    assert_eq!(blame[0].actor.id, alice); // attributed to the typist, not the driver
    assert_eq!(blame[0].actor.display_name, "alice");
    assert_eq!((blame[0].byte_start, blame[0].byte_end), (0, 16)); // "hello from alice"
    assert_eq!(blame[0].session, Some(alice_s));
    assert_eq!(&ws.read("/doc.md").await.unwrap()[..], b"hello from alice");
}

/// Poll blame until the checkpoint (async, on last disconnect) has landed.
async fn loop_until_blamed(ws: &Workspace) -> Vec<origofs_sdk::BlameRange> {
    for _ in 0..40 {
        if let Ok(b) = ws.blame("/doc.md").await
            && !b.is_empty()
        {
            return b;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("blame never populated — checkpoint on last-leave did not run");
}

// --- credential transport and per-connection sessions (#98) -----------------

// A browser cannot set headers on a WebSocket upgrade, so the documented answer
// was `?token=`. That works, but a URL is the worst place for a credential: it
// lands in access logs, proxy logs, and Referer-adjacent tooling by default.
// `Sec-WebSocket-Protocol` is the one header a browser *can* set.
#[tokio::test]
async fn a_credential_can_ride_the_websocket_subprotocol() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alice = ws.create_human("alice", None).await.unwrap();
    let alice_s = ws.create_session(alice, Some("web")).await.unwrap();
    let auth = BearerAuth::new().with_token("tok-alice", alice, Some(alice_s));
    let ws = Arc::new(ws);

    let app = router(ws.clone(), Arc::new(auth));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // No `?token=` anywhere in the URL — the credential is in the subprotocol
    // list, exactly as `new WebSocket(url, ["origofs", token])` sends it.
    let req = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://{addr}/v1/coedit/doc.md"))
        .header("host", addr.to_string())
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-protocol", "origofs, tok-alice")
        .body(())
        .unwrap();
    let (mut a, resp) = tokio_tungstenite::connect_async(req).await.unwrap();

    // The server must echo the marker back, or a browser fails the handshake --
    // and it must echo only the marker, never the token beside it.
    let selected = resp
        .headers()
        .get("sec-websocket-protocol")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(selected.as_deref(), Some("origofs"));

    let aware = Awareness::new(Doc::new());
    pump(&mut a, &aware).await;
    let update = {
        let text = aware.doc().get_or_insert_text("content");
        let mut txn = aware.doc().transact_mut();
        text.insert(&mut txn, 0, "typed over a subprotocol");
        txn.encode_update_v1()
    };
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();
    a.send(Ws::Close(None)).await.ok();

    let blame = loop_until_blamed(&ws).await;
    assert_eq!(blame[0].actor.id, alice);
    assert_eq!(blame[0].session, Some(alice_s));
}

#[tokio::test]
async fn a_bogus_subprotocol_credential_is_still_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alice = ws.create_human("alice", None).await.unwrap();
    let auth = BearerAuth::new().with_token("tok-alice", alice, None);

    let app = router(Arc::new(ws), Arc::new(auth));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let req = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://{addr}/v1/coedit/doc.md"))
        .header("host", addr.to_string())
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-protocol", "origofs, not-a-real-token")
        .body(())
        .unwrap();
    assert!(
        tokio_tungstenite::connect_async(req).await.is_err(),
        "an unknown token in the subprotocol must not authenticate"
    );
}

// A connection whose credential names only an actor used to produce edits stamped
// `(actor, session=None)` -- which `revert_session` can never undo, on the surface
// that generates more edits than any other. The room opens a session for it.
#[tokio::test]
async fn a_session_less_credential_gets_a_session_for_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alice = ws.create_human("alice", None).await.unwrap();
    // Bound to an actor and *no* session -- `WriteCtx::actor(..)`, the shape the
    // issue is about.
    let auth = BearerAuth::new().with_token("tok-alice", alice, None);
    let ws = Arc::new(ws);

    let app = router(ws.clone(), Arc::new(auth));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (mut a, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/v1/coedit/doc.md?token=tok-alice"))
            .await
            .unwrap();
    let aware = Awareness::new(Doc::new());
    pump(&mut a, &aware).await;
    let update = {
        let text = aware.doc().get_or_insert_text("content");
        let mut txn = aware.doc().transact_mut();
        text.insert(&mut txn, 0, "live edits are revertible");
        txn.encode_update_v1()
    };
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();
    a.send(Ws::Close(None)).await.ok();

    let blame = loop_until_blamed(&ws).await;
    assert_eq!(blame[0].actor.id, alice);
    let session = blame[0]
        .session
        .expect("a live edit must carry a session, or it can never be reverted");

    // The point of having one: the edit can be undone as a unit.
    let changed = ws.revert_session(alice, session, None).await.unwrap();
    assert_eq!(changed, vec!["/doc.md".to_string()]);
    assert_eq!(&ws.read("/doc.md").await.unwrap()[..], b"");
}

// --- periodic checkpointing (#97) -------------------------------------------

// A room's CRDT lives in memory and used to reach durable storage only when its
// last socket left. A browser tab left open on a document *is* an open room, so
// that could be hours -- and a worker dying in between lost everything since the
// last checkpoint. These drive the policy with short timings so the test doesn't
// have to wait out the defaults.
#[tokio::test]
async fn an_idle_room_is_checkpointed_without_anyone_disconnecting() {
    use origofs_sdk::api::{ApiOptions, CheckpointPolicy, router_with};
    use std::time::Duration;

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

    let (mut a, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/v1/coedit/doc.md?token=tok-alice"))
            .await
            .unwrap();
    let aware = Awareness::new(Doc::new());
    pump(&mut a, &aware).await;
    let update = {
        let text = aware.doc().get_or_insert_text("content");
        let mut txn = aware.doc().transact_mut();
        text.insert(&mut txn, 0, "durable before I disconnect");
        txn.encode_update_v1()
    };
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();

    // The socket stays OPEN throughout -- this is the whole point.
    //
    // Wait for the LAST step of a checkpoint, not the first. `checkpoint_coedit`
    // writes the file, then the sidecar blob, then stamps `checkpointed_at`;
    // breaking as soon as `read` returns bytes can catch the middle of that
    // sequence, and the marker assertion below then fails on a checkpoint that
    // was merely still in progress. Polling the stamp makes the observation
    // ordered rather than racing the writer.
    let mut landed = None;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let stamped = ws
            .live_doc("/doc.md")
            .await
            .unwrap()
            .and_then(|l| l.checkpointed_at)
            .is_some();
        if !stamped {
            continue;
        }
        if let Ok(b) = ws.read("/doc.md").await
            && !b.is_empty()
        {
            landed = Some(b);
            break;
        }
    }
    assert_eq!(
        landed.as_deref(),
        Some(&b"durable before I disconnect"[..]),
        "an idle room was never checkpointed while its socket stayed open"
    );

    // And the marker says when, so a UI can render "last saved N ago" instead of
    // only "this may be stale".
    let live = ws.live_doc("/doc.md").await.unwrap().expect("still live");
    assert!(
        live.checkpointed_at.is_some(),
        "the live marker records that the path is live but not when it was saved"
    );
    drop(a);
}

#[tokio::test]
async fn continuous_editing_is_checkpointed_on_the_interval() {
    use origofs_sdk::api::{ApiOptions, CheckpointPolicy, router_with};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let alice = ws.create_human("alice", None).await.unwrap();
    let alice_s = ws.create_session(alice, Some("web")).await.unwrap();
    let auth = BearerAuth::new().with_token("tok-alice", alice, Some(alice_s));
    let ws = Arc::new(ws);

    // Idle is disabled, so only the interval trigger can fire -- and a client that
    // keeps typing would reset an idle timer forever, which is exactly why the
    // interval trigger exists.
    let options = ApiOptions {
        checkpoint: CheckpointPolicy {
            idle_after: None,
            max_interval: Some(Duration::from_millis(100)),
            tick: Duration::from_millis(20),
        },
        ..Default::default()
    };
    let app = router_with(ws.clone(), Arc::new(auth), options);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (mut a, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/v1/coedit/doc.md?token=tok-alice"))
            .await
            .unwrap();
    let aware = Awareness::new(Doc::new());
    pump(&mut a, &aware).await;

    // Type continuously, never pausing long enough for an idle timer.
    let mut landed = false;
    for i in 0..40 {
        let update = {
            let text = aware.doc().get_or_insert_text("content");
            let mut txn = aware.doc().transact_mut();
            text.insert(&mut txn, 0, "x");
            txn.encode_update_v1()
        };
        a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        if i > 4
            && let Ok(b) = ws.read("/doc.md").await
            && !b.is_empty()
        {
            landed = true;
            break;
        }
    }
    assert!(
        landed,
        "a continuously-edited room never reached durable storage"
    );
    drop(a);
}

#[tokio::test]
async fn a_room_with_no_edits_is_not_rewritten() {
    // The sweeper must not churn: an untouched room has nothing to crystallize,
    // and writing it anyway would put an op-log entry and a blame rewrite behind
    // every tick of every open document.
    use origofs_sdk::api::{ApiOptions, CheckpointPolicy, router_with};
    use std::time::Duration;

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
            idle_after: Some(Duration::from_millis(10)),
            max_interval: Some(Duration::from_millis(10)),
            tick: Duration::from_millis(10),
        },
        ..Default::default()
    };
    let app = router_with(ws.clone(), Arc::new(auth), options);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (mut a, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/v1/coedit/doc.md?token=tok-alice"))
            .await
            .unwrap();
    let aware = Awareness::new(Doc::new());
    pump(&mut a, &aware).await;

    // Many sweeper ticks pass with nobody typing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let ops = ws.edit_ops(alice, Some(alice_s)).await.unwrap();
    assert!(
        ops.is_empty(),
        "the sweeper wrote {} op-log entries for a room nobody edited",
        ops.len()
    );
    drop(a);
}
