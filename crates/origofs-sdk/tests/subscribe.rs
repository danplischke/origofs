//! `Workspace::subscribe` — the push change feed over Postgres, driven through
//! the SDK (so it also guards that the PG constructors keep their typed handle).
//! Self-skips unless `ORIGOFS_PG_TEST_URL` points at a reachable database.

use origofs_sdk::{MemStore, Workspace};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

fn dsn() -> Option<String> {
    std::env::var("ORIGOFS_PG_TEST_URL").ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_pushes_writes_over_postgres() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping subscribe_pushes_writes_over_postgres: ORIGOFS_PG_TEST_URL unset");
        return;
    };

    // open_pg must retain the concrete Postgres handle for subscribe to work.
    let ws = Workspace::open_pg(&dsn, Arc::new(MemStore::new()))
        .await
        .unwrap();

    // Subscribe at the current tail (LISTEN is active once subscribe returns).
    let cursor = ws
        .watch(0)
        .await
        .unwrap()
        .last()
        .map(|e| e.seq)
        .unwrap_or(0);
    let mut sub = ws.subscribe(cursor, None).await.unwrap();

    // A write emits a "write" event; the NOTIFY wakes recv().
    ws.write("/live-sdk.txt", b"hi").await.unwrap();
    let batch = timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .expect("recv errored");
    assert!(
        batch.iter().any(|e| e.path == "/live-sdk.txt"),
        "expected the write to be pushed, got {batch:?}"
    );
    assert!(batch.iter().all(|e| e.seq > cursor));
}

#[tokio::test]
async fn subscribe_errors_without_postgres() {
    // A SQLite-backed workspace has no push feed; subscribe must fail (not panic).
    let d = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(d.path().join("m.db"), d.path().join("cas"))
        .await
        .unwrap();
    assert!(ws.subscribe(0, None).await.is_err());
}
