//! Cross-worker co-editing relay (roadmap M8): the Postgres-backed bus that
//! carries attributed update deltas between workers, so replicas of a document
//! living in different processes converge. The engine primitive under the axum /
//! FastAPI coordinators.
//!
//! Self-skips unless `ORIGOFS_PG_TEST_URL` points at a reachable database.
//! Requires the `coedit` feature.
#![cfg(feature = "coedit")]

use origofs_core::{CoeditDoc, MetadataStore, PostgresMetadataStore, WriteCtx};
use std::sync::OnceLock;
use std::time::Duration;

/// Serializes these tests: they share one database and each resets the schema, so
/// they must not overlap (cargo runs a binary's tests concurrently).
fn pg_lock() -> &'static tokio::sync::Mutex<()> {
    static L: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Drop and recreate the public schema, then a migrated store — a clean slate.
async fn fresh_store(dsn: &str) -> PostgresMetadataStore {
    let (reset, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let h = tokio::spawn(async move {
        let _ = conn.await;
    });
    reset
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .unwrap();
    drop(reset);
    let _ = h.await;
    let store = PostgresMetadataStore::connect(dsn).await.unwrap();
    store.init().await.unwrap();
    store
}

/// A worker attributes a client's edit and publishes the delta; a second worker
/// receives it over the relay, applies it *without* re-attribution, and converges
/// — authorship intact. A third worker catches up purely by replaying.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_carries_attributed_updates_between_workers() {
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!(
            "skipping relay_carries_attributed_updates_between_workers: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let _guard = pg_lock().lock().await;
    let store = fresh_store(&dsn).await;
    store.coedit_relay_init().await.unwrap();

    // Worker B subscribes first, so it's listening when A publishes.
    let mut sub_b = store.coedit_subscribe().await.unwrap();

    // Worker A's room ingests a client's edit and produces a broadcast frame —
    // exactly what the transport publishes.
    let room_a = CoeditDoc::new();
    let client = CoeditDoc::new();
    client.insert(WriteCtx::session(7, 7), 0, "cross-worker hello");
    let greeting = room_a.sync_start();
    let answer = client
        .handle_sync(WriteCtx::session(7, 7), &greeting)
        .unwrap();
    let out = room_a
        .handle_sync(WriteCtx::session(7, 7), &answer.reply)
        .unwrap();
    assert_eq!(room_a.text(), "cross-worker hello");
    assert!(!out.broadcast.is_empty());

    store
        .coedit_publish("/doc.md", "worker-A", &out.broadcast)
        .await
        .unwrap();

    // Worker B receives the op and merges it into its own replica.
    let notes = tokio::time::timeout(Duration::from_secs(5), sub_b.recv())
        .await
        .expect("a relayed op should arrive within 5s")
        .unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].origin, "worker-A");
    assert_eq!(notes[0].path, "/doc.md");

    let room_b = CoeditDoc::new();
    room_b.apply_relayed(&notes[0].delta).unwrap();
    assert_eq!(room_b.text(), "cross-worker hello");
    // A's authorship survived the hop — still attributed to actor 7, not re-stamped.
    let (_t, spans) = room_b.snapshot();
    assert_eq!(spans, vec![(7, 7, "cross-worker hello".len() as u64)]);

    // A worker that starts hosting the doc later catches up by replaying.
    let replay = store.coedit_replay("/doc.md").await.unwrap();
    assert_eq!(replay.len(), 1);
    let room_c = CoeditDoc::new();
    room_c.apply_relayed(&replay[0].delta).unwrap();
    assert_eq!(room_c.text(), "cross-worker hello");
}

/// The relay wakes a live subscriber (a real `LISTEN`, not a poll): recv blocks,
/// then returns as soon as a peer publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_recv_wakes_on_publish() {
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!("skipping relay_recv_wakes_on_publish: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    let store = fresh_store(&dsn).await;
    store.coedit_relay_init().await.unwrap();
    let mut sub = store.coedit_subscribe().await.unwrap();

    // Nothing yet: recv must block. Publish from another task, then recv wakes.
    let publisher = {
        let dsn = dsn.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let s = PostgresMetadataStore::connect(&dsn).await.unwrap();
            s.coedit_publish("/late.md", "worker-X", b"\x00\x01\x00")
                .await
                .unwrap();
        })
    };

    let notes = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv should wake on the publish")
        .unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].path, "/late.md");
    assert_eq!(notes[0].origin, "worker-X");
    let _ = publisher.await;
}
