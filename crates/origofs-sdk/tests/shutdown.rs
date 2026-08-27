//! Graceful shutdown: a stop signal closes the listener but lets requests already
//! in flight finish.
//!
//! There was no signal handling anywhere in the workspace — `api::serve` was a
//! bare `axum::serve` — so a `SIGTERM` (an ordinary Kubernetes rollout, a
//! `docker stop`) severed every in-flight request wherever it had reached. Content
//! is written before the metadata referencing it, so a write cut mid-way leaves
//! durable orphaned chunks and a client that never learns whether it landed.
#![cfg(feature = "api")]

use origofs_sdk::api::{Authenticator, Principal};
use origofs_sdk::{Workspace, api};
use std::sync::Arc;

struct AllowAll(i64);
#[async_trait::async_trait]
impl Authenticator for AllowAll {
    async fn authenticate(&self, _h: &axum::http::HeaderMap) -> Option<Principal> {
        Some(Principal {
            actor: self.0,
            session: None,
        })
    }
}

/// A request in flight when the signal arrives must still complete, and the
/// server must then return rather than hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_requests_finish_after_the_stop_signal() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("m.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let actor = ws.create_human("dan", None).await.unwrap();
    // A body big enough that the response is still streaming when we signal.
    let body = vec![b'x'; 8 * 1024 * 1024];
    ws.write("/big.bin", &body).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let auth: Arc<dyn Authenticator> = Arc::new(AllowAll(actor));
    let server = tokio::spawn(async move {
        api::serve_until(Arc::new(ws), addr, auth, async {
            let _ = rx.await;
        })
        .await
    });

    // Wait for the listener to be up.
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Start a read and deliberately read *slowly*: take a first small chunk, then
    // stop. The body is far larger than the socket buffers, so the server is still
    // mid-write when the signal arrives — which is what makes this a drain test
    // rather than a race the response usually wins. (With a fast reader an 8 MiB
    // body completes in under a millisecond on loopback and an abrupt shutdown
    // looks identical.)
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /v1/files/big.bin HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut head = vec![0u8; 1024];
    let n = stream.read(&mut head).await.unwrap();
    assert!(n > 0, "expected the response to start");
    let text = String::from_utf8_lossy(&head[..n.min(64)]).to_string();
    assert!(text.starts_with("HTTP/1.1 200"), "got: {text}");

    // Signal shutdown mid-response, then keep reading.
    tx.send(()).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // The discriminating assertion. `serve_until` must still be running: a drain
    // is exactly the promise that the server does not consider itself stopped
    // while a request is outstanding. Without `with_graceful_shutdown` it returns
    // straight away — which is what lets a supervisor conclude the process is
    // done and kill it, severing the response.
    assert!(
        !server.is_finished(),
        "serve_until must not report itself stopped while a response is in flight"
    );

    let mut rest = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        stream.read_to_end(&mut rest),
    )
    .await
    .expect("reading the rest must not hang")
    .expect("the in-flight response must not be severed by shutdown");

    let total = n + rest.len();
    assert!(
        total > 8 * 1024 * 1024,
        "the whole body should have been delivered despite the shutdown, got {total} bytes"
    );

    // ...and only then does the server stop, of its own accord.
    tokio::time::timeout(std::time::Duration::from_secs(30), server)
        .await
        .expect("serve_until must return after draining")
        .unwrap()
        .unwrap();
}

/// A server with nothing in flight stops promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_server_stops_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("m.db"), dir.path().join("cas"))
        .await
        .unwrap();
    let actor = ws.create_human("dan", None).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let auth: Arc<dyn Authenticator> = Arc::new(AllowAll(actor));
    let server = tokio::spawn(async move {
        api::serve_until(Arc::new(ws), addr, auth, async {
            let _ = rx.await;
        })
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .expect("an idle server must stop promptly")
        .unwrap()
        .unwrap();
}
