//! Cross-worker live co-editing (roadmap M8): two API workers backed by the same
//! Postgres database (and a shared content store) each host a socket editing the
//! same document. An edit on one worker must reach the client on the other —
//! proving the coordinator's Postgres relay (publish on edit, drain + fan out on
//! peers) closes the multi-worker gap.
//!
//! Self-skips unless `ORIGOFS_PG_TEST_URL` points at a reachable database.
//! Requires the `coedit` feature.
#![cfg(feature = "coedit")]

use futures::{SinkExt, StreamExt};
use origofs_api::{router, BearerAuth};
use origofs_sdk::{MemStore, Workspace};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
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

/// Process one inbound y-sync frame through the vanilla `DefaultProtocol`, sending
/// any responses back. Returns false if the socket closed.
async fn pump(sock: &mut Socket, awareness: &Awareness) -> bool {
    let Some(data) = next_frame(sock).await else {
        return false;
    };
    let responses = DefaultProtocol.handle(awareness, &data).unwrap();
    if !responses.is_empty() {
        sock.send(Ws::Binary(frame(&responses))).await.unwrap();
    }
    true
}

fn text_of(awareness: &Awareness) -> String {
    let text = awareness.doc().get_or_insert_text("content");
    text.get_string(&awareness.doc().transact())
}

async fn reset_schema(dsn: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let h = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .unwrap();
    drop(client);
    let _ = h.await;
}

/// Bring up an API server for `ws` on an ephemeral port; return its address.
async fn spawn_worker(ws: Arc<Workspace>, auth: Arc<BearerAuth>) -> std::net::SocketAddr {
    let app = router(ws, auth);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edit_on_one_worker_reaches_a_client_on_another() {
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!(
            "skipping edit_on_one_worker_reaches_a_client_on_another: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    reset_schema(&dsn).await;

    // Two workers over the SAME Postgres DB and content store — as two processes
    // behind a load balancer would be.
    let content = Arc::new(MemStore::new());
    let ws_a = Arc::new(Workspace::open_pg(&dsn, content.clone()).await.unwrap());
    let alice = ws_a.create_human("alice", None).await.unwrap();
    let alice_s = ws_a.create_session(alice, Some("web")).await.unwrap();
    let bob = ws_a.create_human("bob", None).await.unwrap();
    let bob_s = ws_a.create_session(bob, Some("web")).await.unwrap();
    let ws_b = Arc::new(Workspace::open_pg(&dsn, content.clone()).await.unwrap());

    let auth = Arc::new(
        BearerAuth::new()
            .with_token("alice", alice, Some(alice_s))
            .with_token("bob", bob, Some(bob_s)),
    );
    let addr_a = spawn_worker(ws_a.clone(), auth.clone()).await;
    let addr_b = spawn_worker(ws_b.clone(), auth.clone()).await;

    // Alice on worker A, Bob on worker B — same document.
    let (mut a, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr_a}/coedit/doc.md?token=alice"))
            .await
            .unwrap();
    let (mut b, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr_b}/coedit/doc.md?token=bob"))
            .await
            .unwrap();
    let a_aware = Awareness::new(Doc::new());
    let b_aware = Awareness::new(Doc::new());
    // Handshake both (creates the room on each worker, so each hosts the doc).
    pump(&mut a, &a_aware).await;
    pump(&mut b, &b_aware).await;

    // Alice types on worker A.
    let update = {
        let text = a_aware.doc().get_or_insert_text("content");
        let mut txn = a_aware.doc().transact_mut();
        text.insert(&mut txn, 0, "hi from worker A");
        txn.encode_update_v1()
    };
    a.send(Ws::Binary(frame(&[Y::Sync(SyncMessage::Update(update))])))
        .await
        .unwrap();

    // Bob — on the *other* worker — converges via the Postgres relay.
    for _ in 0..40 {
        if text_of(&b_aware) == "hi from worker A" {
            break;
        }
        if !pump(&mut b, &b_aware).await {
            break;
        }
    }
    assert_eq!(
        text_of(&b_aware),
        "hi from worker A",
        "the edit did not cross workers over the relay"
    );

    // And it's attributed to Alice after a checkpoint (Bob leaves last).
    a.send(Ws::Close(None)).await.ok();
    b.send(Ws::Close(None)).await.ok();
    let mut blame = vec![];
    for _ in 0..40 {
        if let Ok(bl) = ws_a.blame("/doc.md").await {
            if !bl.is_empty() {
                blame = bl;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(blame.len(), 1);
    assert_eq!(blame[0].actor.id, alice);
    assert_eq!((blame[0].byte_start, blame[0].byte_end), (0, 16));
}
