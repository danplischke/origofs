//! Live co-editing over a real WebSocket (roadmap M8): a genuine Yjs client — a
//! raw `yrs::Doc` driven by the stock y-sync `DefaultProtocol`, exactly what
//! PlateJS and `y-websocket` run — connects to `/coedit/{path}`, and its edits
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

    let url = |tok: &str| format!("ws://{addr}/coedit/doc.md?token={tok}");
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
