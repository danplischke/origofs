//! Live collaboration storage primitives: the cursor-ordered change feed and
//! presence with staleness, plus (against a live Postgres) the LISTEN/NOTIFY
//! push that lets consumers skip polling.

use origofs_core::{ActorInit, EventInit, MetadataStore, SqliteMetadataStore};

async fn store() -> SqliteMetadataStore {
    let m = SqliteMetadataStore::open_in_memory().unwrap();
    m.init().await.unwrap();
    m
}

fn event(actor: Option<i64>, kind: &str, path: &str) -> EventInit {
    EventInit {
        actor_id: actor,
        session_id: None,
        kind: kind.to_string(),
        path: path.to_string(),
        detail: None,
        branch: None,
    }
}

#[tokio::test]
async fn event_feed_is_cursor_ordered() {
    let m = store().await;
    let alice = m
        .create_actor(ActorInit::human("alice", None))
        .await
        .unwrap();

    let s1 = m
        .append_event(event(Some(alice), "write", "/a"), 100)
        .await
        .unwrap();
    let s2 = m
        .append_event(event(None, "mkdir", "/d"), 101)
        .await
        .unwrap();
    assert!(s2 > s1, "seq is monotonic");

    let all = m.events_since(0, 100).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!((all[0].seq, all[0].kind.as_str()), (s1, "write"));
    assert_eq!(all[0].actor_id, Some(alice));
    assert_eq!(all[1].kind, "mkdir");

    // Tailing from a cursor only yields later events.
    let after = m.events_since(s1, 100).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].seq, s2);

    // The limit is respected.
    let capped = m.events_since(0, 1).await.unwrap();
    assert_eq!(capped.len(), 1);
}

#[tokio::test]
async fn presence_reflects_heartbeats_and_staleness() {
    let m = store().await;
    let alice = m
        .create_actor(ActorInit::human("alice", None))
        .await
        .unwrap();
    let bot = m
        .create_actor(ActorInit::agent("claude", "opus", None))
        .await
        .unwrap();
    let sa = m.create_session(alice, Some("cli"), 0).await.unwrap();
    let sb = m.create_session(bot, Some("mcp"), 0).await.unwrap();

    m.touch_presence(sa, alice, Some("/a.txt"), 1000)
        .await
        .unwrap();
    m.touch_presence(sb, bot, None, 500).await.unwrap();

    // Both are active since 400; ordered by last_seen descending.
    let both = m.active_presence(400).await.unwrap();
    assert_eq!(both.len(), 2);
    assert_eq!(both[0].display_name, "alice");
    assert_eq!(both[0].path.as_deref(), Some("/a.txt"));
    assert_eq!(both[0].kind.as_str(), "human");
    assert_eq!(both[1].display_name, "claude");
    assert_eq!(both[1].kind.as_str(), "agent");

    // Raising the window past the bot's last_seen drops it.
    let recent = m.active_presence(600).await.unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].display_name, "alice");

    // A fresh heartbeat upserts (moves it back in and updates the path).
    m.touch_presence(sb, bot, Some("/x"), 2000).await.unwrap();
    let now = m.active_presence(1500).await.unwrap();
    assert_eq!(now.len(), 1);
    assert_eq!(now[0].display_name, "claude");
    assert_eq!(now[0].path.as_deref(), Some("/x"));
}

/// Against a live Postgres, appending an event must fire `NOTIFY origofs_events`
/// with the new seq — so consumers can be pushed instead of polling.
/// Self-skips unless `ORIGOFS_PG_TEST_URL` is set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_append_fires_notify() {
    use futures::{StreamExt, future, stream};
    use origofs_core::{EVENT_CHANNEL, PostgresMetadataStore};
    use std::time::Duration;

    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!("skipping postgres_append_fires_notify: ORIGOFS_PG_TEST_URL unset");
        return;
    };

    // Clean schema, then a store that migrates it.
    let (reset, reset_conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let reset_handle = tokio::spawn(async move {
        let _ = reset_conn.await;
    });
    reset
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .unwrap();
    drop(reset);
    let _ = reset_handle.await;

    let store = PostgresMetadataStore::connect(&dsn).await.unwrap();
    store.init().await.unwrap();

    // A dedicated LISTEN connection, pumping notifications into a channel.
    let (client, mut connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let pump = stream::poll_fn(move |cx| connection.poll_message(cx)).for_each(move |msg| {
        if let Ok(tokio_postgres::AsyncMessage::Notification(n)) = msg {
            let _ = tx.send(n.payload().to_string());
        }
        future::ready(())
    });
    tokio::spawn(pump);
    client
        .batch_execute(&format!("LISTEN {EVENT_CHANNEL}"))
        .await
        .unwrap();

    let seq = store
        .append_event(event(None, "write", "/pushed.txt"), 42)
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a NOTIFY should arrive within 5s")
        .expect("channel open");
    assert_eq!(payload, seq.to_string(), "NOTIFY carries the new seq");
}
