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

/// A per-run-unique token so a test can pick out only its own events on a shared
/// Postgres workspace (these tests may run concurrently against one database).
fn unique_token(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{label}-{nanos}")
}

/// Collect events matching `prefix` off a live subscription until `want` of them
/// have arrived, re-`recv`ing across coalesced wakeups. Fails on timeout or if
/// the feed closes early (recv yielding an empty batch) before `want` is reached.
async fn collect_matching(
    sub: &mut origofs_sdk::EventSubscription,
    prefix: &str,
    want: usize,
) -> Vec<origofs_sdk::Event> {
    let mut got = Vec::new();
    while got.len() < want {
        let batch = timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("recv timed out waiting for the change feed")
            .expect("recv errored");
        assert!(
            !batch.is_empty(),
            "the change feed closed after {} of {want} expected events",
            got.len()
        );
        got.extend(batch.into_iter().filter(|e| e.path.starts_with(prefix)));
    }
    got
}

// A5 (issue #70): the reconnect story. A client's subscription is torn down (the
// dedicated LISTEN connection drops), writes land *during the gap*, then the
// client re-subscribes from the cursor it last saw. Because the durable
// `fs_event` table — not the ephemeral NOTIFY — is the source of truth on every
// drain, none of the gap writes may be lost or duplicated across the reconnect.
// This is exactly the loop a resilient client runs when its socket drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_reconnect_recovers_events_written_during_the_gap() {
    let Some(dsn) = dsn() else {
        eprintln!(
            "skipping subscribe_reconnect_recovers_events_written_during_the_gap: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let tok = unique_token("gap");
    let prefix = format!("/{tok}-");
    let ws = Workspace::open_pg(&dsn, Arc::new(MemStore::new()))
        .await
        .unwrap();

    // Start at the current tail so only our own writes appear past the cursor.
    let tail = ws
        .watch(0)
        .await
        .unwrap()
        .last()
        .map(|e| e.seq)
        .unwrap_or(0);
    let mut sub = ws.subscribe(tail, None).await.unwrap();

    // One write on the live subscription; remember the cursor we've consumed to.
    ws.write(&format!("{prefix}a"), b"a").await.unwrap();
    let seen = collect_matching(&mut sub, &prefix, 1).await;
    let resume = seen[0].seq;
    assert!(resume > tail);

    // Drop the subscription — the LISTEN connection and its driver task tear down.
    drop(sub);

    // Writes that happen while nothing is listening must still be durable.
    for name in ["b", "c", "d"] {
        ws.write(&format!("{prefix}{name}"), name.as_bytes())
            .await
            .unwrap();
    }

    // Reconnect from the last cursor we consumed and catch up on the gap writes.
    let mut sub2 = ws.subscribe(resume, None).await.unwrap();
    let recovered = collect_matching(&mut sub2, &prefix, 3).await;

    let paths: Vec<&str> = recovered.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            format!("{prefix}b"),
            format!("{prefix}c"),
            format!("{prefix}d"),
        ],
        "every write during the disconnect must be recovered, in order"
    );
    // Strictly increasing and strictly past the resume cursor: no loss, no dupes,
    // no replay of the pre-gap event.
    assert!(recovered[0].seq > resume);
    assert!(recovered.windows(2).all(|w| w[0].seq < w[1].seq));
}

// A5 (issue #70): a burst of writes coalesces the capacity-1 wakeup channel — a
// pile of NOTIFYs collapses into a single "re-drain" wakeup. Because each drain
// reads every row past the cursor from the durable table, coalescing must never
// drop an event: all N writes arrive, in seq order, exactly once (L5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_coalesced_burst_delivers_every_event_in_order() {
    let Some(dsn) = dsn() else {
        eprintln!(
            "skipping subscribe_coalesced_burst_delivers_every_event_in_order: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let tok = unique_token("burst");
    let prefix = format!("/{tok}-");
    let ws = Workspace::open_pg(&dsn, Arc::new(MemStore::new()))
        .await
        .unwrap();

    let tail = ws
        .watch(0)
        .await
        .unwrap()
        .last()
        .map(|e| e.seq)
        .unwrap_or(0);
    let mut sub = ws.subscribe(tail, None).await.unwrap();

    // Fire a tight burst — far more than the capacity-1 wakeup buffer — so many
    // NOTIFYs coalesce while the subscriber is still parked.
    const N: usize = 25;
    for i in 0..N {
        ws.write(&format!("{prefix}{i:02}"), b"x").await.unwrap();
    }

    let got = collect_matching(&mut sub, &prefix, N).await;
    let paths: Vec<&str> = got.iter().map(|e| e.path.as_str()).collect();
    let expected: Vec<String> = (0..N).map(|i| format!("{prefix}{i:02}")).collect();
    assert_eq!(paths, expected, "coalesced NOTIFYs must not drop any event");
    // Strictly increasing seqs past the tail: in order, no duplicates.
    assert!(got[0].seq > tail);
    assert!(got.windows(2).all(|w| w[0].seq < w[1].seq));
}
