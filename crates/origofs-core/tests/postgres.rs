//! Postgres backend: the same metadata + engine behavior as SQLite, plus a
//! concurrent-writers check, atomic-create serialization, and the NOTIFY helper.
//!
//! Self-skips unless `ORIGOFS_PG_TEST_URL` points at a reachable database, e.g.
//!   ORIGOFS_PG_TEST_URL="host=/tmp/origofs-pg/sock port=5433 user=postgres dbname=origofs"

use origofs_core::{
    EventInit, FileKind, Fs, InodeInit, MemStore, MetadataStore, PostgresMetadataStore,
    SuggestionStatus, WriteCtx,
};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::timeout;

fn dsn() -> Option<String> {
    std::env::var("ORIGOFS_PG_TEST_URL").ok()
}

/// Serializes the PG tests: they share one database and each resets the schema,
/// so they must not overlap (cargo runs tests in a binary concurrently).
fn pg_lock() -> &'static tokio::sync::Mutex<()> {
    static L: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Drop and recreate the `public` schema so each run starts clean.
async fn reset(dsn: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect for reset");
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .expect("reset public schema");
    drop(client);
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_backend() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping postgres_backend: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;

    // --- metadata-store level ------------------------------------------------
    let meta = PostgresMetadataStore::connect(&dsn).await.unwrap();
    meta.init().await.unwrap();
    meta.init().await.unwrap(); // idempotent

    let root = meta.get_inode(1).await.unwrap().expect("root inode");
    assert_eq!(root.kind, FileKind::Dir);

    let ino = meta
        .create_inode(InodeInit {
            kind: FileKind::File,
            mode: 0o100644,
        })
        .await
        .unwrap();
    assert!(ino > 1, "identity sequence must not collide with root");
    meta.add_dentry(1, "hello", ino).await.unwrap();
    assert_eq!(meta.lookup(1, "hello").await.unwrap(), Some(ino));
    // duplicate name is rejected
    assert!(meta.add_dentry(1, "hello", ino).await.is_err());
    assert_eq!(meta.child_count(1).await.unwrap(), 1);
    let entries = meta.list_dir(1).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "hello");

    meta.set_symlink(ino, "/target").await.unwrap();
    assert_eq!(
        meta.get_symlink(ino).await.unwrap().as_deref(),
        Some("/target")
    );

    meta.remove_dentry(1, "hello").await.unwrap();
    meta.delete_inode(ino).await.unwrap();
    assert!(meta.get_inode(ino).await.unwrap().is_none());

    // --- engine over Postgres (same code path as SQLite) --------------------
    let content = Arc::new(MemStore::new());
    let fs = Fs::new(PostgresMetadataStore::connect(&dsn).await.unwrap(), content);
    fs.init().await.unwrap();
    fs.mkdir_p("/a/b").await.unwrap();
    fs.write("/a/f.txt", b"hello pg").await.unwrap();
    assert_eq!(&fs.read("/a/f.txt").await.unwrap()[..], b"hello pg");
    fs.rename("/a/f.txt", "/a/g.txt").await.unwrap();
    assert!(fs.read("/a/f.txt").await.is_err());
    assert_eq!(&fs.read("/a/g.txt").await.unwrap()[..], b"hello pg");

    // --- concurrent writers to different inodes don't block/deadlock --------
    let fs = Arc::new(fs);
    let mut handles = Vec::new();
    for i in 0..20 {
        let fs = fs.clone();
        handles.push(tokio::spawn(async move {
            fs.write(&format!("/a/c{i:02}.txt"), format!("data-{i}").as_bytes())
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    // b, g.txt, and 20 concurrent files
    assert_eq!(fs.ls("/a").await.unwrap().len(), 22);
    assert_eq!(&fs.read("/a/c07.txt").await.unwrap()[..], b"data-7");

    // --- versioning over Postgres (same engine, PG-backed refs/config) ------
    let commit = fs.commit("tester", "snapshot on pg").await.unwrap();
    let log = fs.log().await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].hash, commit);
    fs.create_branch("dev").await.unwrap();
    assert!(
        fs.list_branches()
            .await
            .unwrap()
            .iter()
            .any(|(n, _)| n == "dev")
    );

    // --- attribution over Postgres ------------------------------------------
    let human = fs.create_human("dev-human", Some("dev@x")).await.unwrap();
    let agent = fs
        .create_agent("dev-agent", "m", Some(human))
        .await
        .unwrap();
    let sess = fs.create_session(agent, None).await.unwrap();
    fs.write_as(WriteCtx::session(agent, sess), "/a/attr.txt", b"one\ntwo\n")
        .await
        .unwrap();
    assert_eq!(fs.blame("/a/attr.txt").await.unwrap()[0].actor.id, agent);
    assert_eq!(
        fs.get_actor(agent)
            .await
            .unwrap()
            .unwrap()
            .controller_actor_id,
        Some(human)
    );

    // External-identity mapping over Postgres (V9 unique index): the human was
    // registered with auth_subject "dev@x", so it resolves and find-or-create is
    // idempotent; a fresh subject creates a distinct actor.
    assert_eq!(
        fs.actor_by_subject("dev@x").await.unwrap().unwrap().id,
        human
    );
    assert_eq!(
        fs.find_or_create_human("dev@x", "again").await.unwrap(),
        human
    );
    let other = fs
        .find_or_create_human("someone-else", "Sam")
        .await
        .unwrap();
    assert_ne!(other, human);
    assert!(fs.actor_by_subject("nobody").await.unwrap().is_none());
    assert_eq!(fs.edit_ops(agent, Some(sess)).await.unwrap().len(), 1);

    // --- agent-suggestion review queue over Postgres ------------------------
    let sug = fs
        .suggest(
            origofs_core::WriteCtx::session(agent, sess),
            "/a/attr.txt",
            b"one\ntwo\nthree\n",
            Some("append a line"),
        )
        .await
        .unwrap();
    let pending = fs
        .list_suggestions(Some(SuggestionStatus::Pending), None)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, sug);
    assert!(fs.suggestion_diff(sug).await.unwrap().contains("+three"));
    // working tree untouched until accept
    assert_eq!(&fs.read("/a/attr.txt").await.unwrap()[..], b"one\ntwo\n");
    fs.accept_suggestion(sug, origofs_core::WriteCtx::actor(human))
        .await
        .unwrap();
    assert_eq!(
        &fs.read("/a/attr.txt").await.unwrap()[..],
        b"one\ntwo\nthree\n"
    );
    assert_eq!(
        fs.get_suggestion(sug).await.unwrap().unwrap().status,
        SuggestionStatus::Accepted
    );

    // --- NOTIFY helper ------------------------------------------------------
    let pg = PostgresMetadataStore::connect(&dsn).await.unwrap();
    pg.notify("origofs_changes", "hello").await.unwrap();
}

/// Await the next batch with a timeout so a stuck subscription fails loudly.
async fn recv_batch(sub: &mut origofs_core::EventSubscription) -> Vec<origofs_core::Event> {
    timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .expect("recv errored")
}

/// The `LISTEN`-backed push subscription: a committed change wakes `recv()` and
/// yields the new events in order; coalesced changes arrive in one batch; and a
/// branch filter delivers only that branch's events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_change_feed_push() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping postgres_change_feed_push: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = PostgresMetadataStore::connect(&dsn).await.unwrap();
    meta.init().await.unwrap();

    let ev = |kind: &str, path: &str, branch: &str| EventInit {
        actor_id: None,
        session_id: None,
        kind: kind.to_string(),
        path: path.to_string(),
        detail: None,
        branch: Some(branch.to_string()),
    };

    // Subscribe from the start; a separate handle appends -> we get pushed.
    let mut sub = meta.subscribe(0, None).await.unwrap();
    let writer = PostgresMetadataStore::connect(&dsn).await.unwrap();
    writer
        .append_event(ev("write", "/a", "main"), 100)
        .await
        .unwrap();

    let batch = recv_batch(&mut sub).await;
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].path, "/a");
    assert_eq!(batch[0].branch.as_deref(), Some("main"));

    // Two changes before the next recv coalesce into one ordered batch.
    writer
        .append_event(ev("write", "/b", "main"), 101)
        .await
        .unwrap();
    writer
        .append_event(ev("mkdir", "/c", "feature"), 102)
        .await
        .unwrap();
    let batch = recv_batch(&mut sub).await;
    let paths: Vec<&str> = batch.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, vec!["/b", "/c"], "ordered by seq, both delivered");
    drop(sub);

    // A branch-filtered subscription only ever sees its own branch.
    let mut feat = meta
        .subscribe(0, Some("feature".to_string()))
        .await
        .unwrap();
    let seen = recv_batch(&mut feat).await; // /c already exists on feature
    assert!(seen.iter().all(|e| e.branch.as_deref() == Some("feature")));
    assert!(seen.iter().any(|e| e.path == "/c"));

    writer
        .append_event(ev("write", "/d", "main"), 103)
        .await
        .unwrap();
    writer
        .append_event(ev("write", "/e", "feature"), 104)
        .await
        .unwrap();
    let batch = recv_batch(&mut feat).await;
    assert!(batch.iter().all(|e| e.branch.as_deref() == Some("feature")));
    assert!(batch.iter().any(|e| e.path == "/e"));
    assert!(
        batch.iter().all(|e| e.path != "/d"),
        "main change filtered out"
    );
}

/// A from-zero / lagging subscriber pages the backlog in bounded batches
/// instead of loading every event into memory at once (drain has a LIMIT).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_drain_is_bounded() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping postgres_drain_is_bounded: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = PostgresMetadataStore::connect(&dsn).await.unwrap();
    meta.init().await.unwrap();

    // Append more than one drain batch (the internal LIMIT is 1024).
    for i in 0..1100i64 {
        meta.append_event(
            EventInit {
                actor_id: None,
                session_id: None,
                kind: "write".into(),
                path: format!("/f{i}"),
                detail: None,
                branch: None,
            },
            100 + i,
        )
        .await
        .unwrap();
    }

    let mut sub = meta.subscribe(0, None).await.unwrap();
    let first = recv_batch(&mut sub).await;
    assert_eq!(
        first.len(),
        1024,
        "first drain is capped at the batch limit"
    );
    let second = recv_batch(&mut sub).await;
    assert_eq!(second.len(), 76, "the remainder pages on the next drain");
    // ordered + contiguous across pages
    assert_eq!(first[0].seq + 1, first[1].seq);
    assert_eq!(first.last().unwrap().seq + 1, second[0].seq);
}

/// C1/H11: many writers racing to create the *same* path serialize on the unique
/// dentry index. Each create + link is one transaction, so a loser's inode rolls
/// back instead of being orphaned. We assert that directly by counting inode
/// rows — exactly root + the one file, no leaked inodes from lost create races.
/// (Before the transaction primitive, the promised advisory-lock serialization
/// was structurally broken and unused, so losers left orphaned inodes.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_concurrent_same_path_create_leaves_no_orphans() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping postgres_concurrent_same_path_create: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;

    let content = Arc::new(MemStore::new());
    let fs = Arc::new(Fs::new(
        PostgresMetadataStore::connect(&dsn).await.unwrap(),
        content,
    ));
    fs.init().await.unwrap();

    let mut handles = Vec::new();
    for i in 0..16 {
        let fs = fs.clone();
        handles.push(tokio::spawn(async move {
            fs.write("/race.txt", format!("writer-{i}").as_bytes())
                .await
        }));
    }
    let mut ok = 0;
    for h in handles {
        if h.await.unwrap().is_ok() {
            ok += 1;
        }
    }

    // At least one writer wins; the file exists, is readable and consistent, and
    // there is exactly one dentry for it.
    assert!(ok >= 1, "at least one concurrent create must succeed");
    let body = fs.read("/race.txt").await.unwrap();
    assert!(
        body.starts_with(b"writer-"),
        "content is one writer's bytes"
    );
    let root = fs.ls("/").await.unwrap();
    assert_eq!(
        root.iter().filter(|e| e.name == "race.txt").count(),
        1,
        "exactly one race.txt entry"
    );

    // The decisive check: no orphaned inodes. Only root (ino 1) and race.txt.
    let (client, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let handle = tokio::spawn(async move {
        let _ = conn.await;
    });
    let n: i64 = client
        .query_one("SELECT count(*) FROM inode", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        n, 2,
        "root + one file; lost create races must not orphan inodes"
    );
    drop(client);
    let _ = handle.await;
}

/// H6: change-feed appends serialize on the feed advisory lock so `seq` commits
/// in assignment order — the property that stops a tailer from advancing past a
/// still-uncommitted lower seq and silently dropping it. Deterministic: hold the
/// feed lock in a side transaction and prove `append_event` blocks until it's
/// released (before the fix it took no lock and returned immediately).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_append_event_serializes_on_feed_lock() {
    let Some(dsn) = dsn() else {
        eprintln!(
            "skipping postgres_append_event_serializes_on_feed_lock: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    meta.init().await.unwrap();

    // Hold the feed advisory lock in a side transaction on a separate connection.
    let (blocker, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let blocker_conn = tokio::spawn(async move {
        let _ = conn.await;
    });
    blocker.batch_execute("BEGIN").await.unwrap();
    blocker
        .execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&origofs_core::postgres::FEED_LOCK_KEY],
        )
        .await
        .unwrap();

    // An append must now block on the feed lock, not complete.
    let m = meta.clone();
    let append = tokio::spawn(async move {
        m.append_event(
            EventInit {
                actor_id: None,
                session_id: None,
                kind: "write".into(),
                path: "/x".into(),
                detail: None,
                branch: None,
            },
            1,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !append.is_finished(),
        "append_event must block while the feed lock is held (H6 serialization)"
    );

    // Release the lock (commit the side txn); the append now completes.
    blocker.batch_execute("COMMIT").await.unwrap();
    let seq = timeout(Duration::from_secs(5), append)
        .await
        .expect("append did not finish after the feed lock was released")
        .unwrap()
        .unwrap();
    assert!(seq >= 1);

    drop(blocker);
    let _ = blocker_conn.await;
}

/// Under concurrent appends the feed stays gapless (every seq assigned exactly
/// once, contiguous) and a from-zero subscriber sees every event exactly once in
/// increasing seq order — no drops, no duplicates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_concurrent_appends_deliver_every_event_once() {
    let Some(dsn) = dsn() else {
        eprintln!(
            "skipping postgres_concurrent_appends_deliver_every_event_once: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    meta.init().await.unwrap();

    const K: usize = 50;
    let mut handles = Vec::new();
    for i in 0..K {
        let m = meta.clone();
        handles.push(tokio::spawn(async move {
            m.append_event(
                EventInit {
                    actor_id: None,
                    session_id: None,
                    kind: "write".into(),
                    path: format!("/f{i}"),
                    detail: None,
                    branch: None,
                },
                i as i64,
            )
            .await
        }));
    }
    let mut seqs = Vec::new();
    for h in handles {
        seqs.push(h.await.unwrap().unwrap());
    }
    seqs.sort_unstable();
    assert_eq!(seqs.len(), K);
    for w in seqs.windows(2) {
        assert_eq!(
            w[1],
            w[0] + 1,
            "seqs must be contiguous — no gaps or duplicates"
        );
    }

    // Every appended event is delivered exactly once, in increasing seq order.
    let mut sub = meta.subscribe(0, None).await.unwrap();
    let mut got: Vec<i64> = Vec::new();
    while got.len() < K {
        for e in recv_batch(&mut sub).await {
            got.push(e.seq);
        }
    }
    assert_eq!(got.len(), K, "every event delivered exactly once");
    for w in got.windows(2) {
        assert!(w[1] > w[0], "delivered in strictly increasing seq order");
    }
}

/// The multi-workspace migrations on **Postgres** use a different mechanism than
/// SQLite: V11/V13 `ALTER TABLE … DROP CONSTRAINT … ADD PRIMARY KEY` (not a
/// table rebuild) and a `setval` on the workspace-id sequence. Those DDL paths run
/// only against a *populated* store and are never exercised by the fresh-store PG
/// tests. This drives a real V10→latest upgrade of a data-bearing PG database and
/// asserts every row survives, backfilled into the default workspace, and that both
/// the workspace and inode identity sequences were advanced past the existing rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_upgrade_preserves_data_and_backfills_default_workspace() {
    let Some(dsn) = dsn() else {
        eprintln!(
            "skipping postgres_upgrade_preserves_data_and_backfills_default_workspace: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;

    // Bring a clean DB to schema V10 by hand: apply each ≤V10 Postgres migration
    // and record it, exactly as the runner would but stopping short of V11.
    {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS schema_meta(version BIGINT PRIMARY KEY, applied_at BIGINT NOT NULL);",
            )
            .await
            .unwrap();
        for m in origofs_core::migrations::MIGRATIONS
            .iter()
            .filter(|m| m.version <= 10)
        {
            client.batch_execute(m.postgres).await.unwrap();
            client
                .execute(
                    "INSERT INTO schema_meta(version, applied_at) VALUES ($1, 0)",
                    &[&m.version],
                )
                .await
                .unwrap();
        }
        // Old-shape rows (pre-V11: no workspace_id). One per table the migrations
        // touch — especially the ones V11/V13 rekey via ALTER … ADD PRIMARY KEY.
        client
            .batch_execute(
                "INSERT INTO inode(ino, kind, mode, nlink, size, content_hash, mtime, ctime)
                     VALUES (1, 'dir', 16877, 1, 0, NULL, 0, 0);
                 INSERT INTO inode(ino, kind, mode, nlink, size, content_hash, mtime, ctime)
                     VALUES (2, 'file', 33188, 1, 3, 'hash-x', 7, 7);
                 INSERT INTO ref(name, value) VALUES ('refs/heads/main', 'commit-abc');
                 INSERT INTO config(key, value) VALUES ('versioning', 'native');
                 INSERT INTO conflict(path, kind) VALUES ('/c.txt', 'text');
                 INSERT INTO file_lock(path, owner, acquired_at) VALUES ('/l.bin', 'bob', 42);
                 INSERT INTO blob_blame(content_hash, runs) VALUES ('hash-x', 'blame-runs');
                 INSERT INTO suggestion(actor_id, path, status, created_ts)
                     VALUES (5, '/s.txt', 'pending', 9);",
            )
            .await
            .unwrap();
        drop(client);
        let _ = handle.await;
    }

    // Run the remaining migrations (V11–V13) through the real store init.
    let meta = PostgresMetadataStore::connect(&dsn).await.unwrap();
    meta.init().await.unwrap();
    meta.init().await.unwrap(); // idempotent
    assert_eq!(
        meta.schema_version().await.unwrap(),
        origofs_core::latest_schema_version()
    );

    // Every row survived the ALTER path, tagged into the default workspace (id 1).
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "SELECT workspace_id FROM ref WHERE name='refs/heads/main'", // V11 rekey
        "SELECT workspace_id FROM config WHERE key='versioning'",    // V11 rekey
        "SELECT workspace_id FROM conflict WHERE path='/c.txt'",     // V11 rekey
        "SELECT workspace_id FROM file_lock WHERE path='/l.bin'",    // V11 rekey
        "SELECT workspace_id FROM blob_blame WHERE content_hash='hash-x'", // V13 rekey
        "SELECT workspace_id FROM inode WHERE ino=2",                // V11 ADD COLUMN
        "SELECT workspace_id FROM suggestion WHERE path='/s.txt'",   // V12 ADD COLUMN
    ] {
        let ws: i64 = client.query_one(sql, &[]).await.unwrap().get(0);
        assert_eq!(ws, 1, "row not backfilled to default workspace: {sql}");
    }
    // Values, not just the new column, are intact through the table ALTERs.
    let v: String = client
        .query_one("SELECT value FROM ref WHERE name='refs/heads/main'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(v, "commit-abc");
    let runs: String = client
        .query_one(
            "SELECT runs FROM blob_blame WHERE content_hash='hash-x'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(runs, "blame-runs");
    // The default workspace registry row now exists at id 1.
    let name: String = client
        .query_one("SELECT name FROM workspace WHERE id=1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, "default");
    drop(client);
    let _ = handle.await;

    // The sequence fixes: the next workspace gets id 2 (setval bumped the workspace
    // id sequence past the explicit default row), and its root inode is a fresh id
    // past the hand-inserted max (2) — both identity sequences advanced, so neither
    // collides with pre-migration rows.
    let (id, root) = meta.create_workspace("second").await.unwrap();
    assert_eq!(
        id, 2,
        "workspace id sequence was not advanced past the default row"
    );
    assert!(
        root > 2,
        "new workspace root inode {root} collided with a pre-migration inode"
    );
}

/// Directory pagination on Postgres (M16, issue #75): the keyset `list_dir_page`
/// and the batched `get_inodes` are separate hand-written SQL per backend, so the
/// SQLite coverage in `tests/readdir_paging.rs` does not speak for this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_readdir_paging() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping postgres_readdir_paging: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;

    let meta = PostgresMetadataStore::connect(&dsn).await.unwrap();
    meta.init().await.unwrap();

    // Creation order deliberately differs from name order, and the set includes
    // names that differ only by a trailing byte plus non-ASCII ones — each is a
    // potential page boundary for the `name > $2` cursor.
    let names: Vec<String> = (0..120)
        .map(|i| format!("f{:03}", (i * 47) % 120))
        .chain(["a", "a ", "aa", "é", "éa", "日本", "日本語"].map(str::to_string))
        .collect();
    let mut inos = Vec::new();
    for n in &names {
        let ino = meta
            .create_inode(InodeInit {
                kind: FileKind::File,
                mode: 0o100644,
            })
            .await
            .unwrap();
        meta.add_dentry(1, n, ino).await.unwrap();
        inos.push(ino);
    }

    // The paged walk reproduces the full listing exactly, under Postgres's own
    // collation (which is why this compares against `list_dir`, not a hardcoded
    // order).
    let full: Vec<String> = meta
        .list_dir(1)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(full.len(), names.len());
    for page_size in [1, 7, 120, 1000] {
        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = meta
                .list_dir_page(1, cursor.as_deref(), page_size)
                .await
                .unwrap();
            assert!(page.len() <= page_size);
            if page.is_empty() {
                break;
            }
            cursor = Some(page.last().unwrap().name.clone());
            seen.extend(page.into_iter().map(|e| e.name));
            assert!(seen.len() <= names.len(), "paging did not terminate");
        }
        assert_eq!(seen, full, "page size {page_size}");
    }

    // Batched attrs: `= ANY($1)` tolerates a missing ino and a duplicate, and
    // agrees with the one-at-a-time `get_inode`.
    assert!(meta.get_inodes(&[]).await.unwrap().is_empty());
    let mut asked = inos.clone();
    asked.push(9_999_999);
    asked.push(inos[0]);
    let batched = meta.get_inodes(&asked).await.unwrap();
    assert_eq!(batched.len(), inos.len(), "duplicates must coalesce");
    for b in &batched {
        let one = meta.get_inode(b.ino).await.unwrap().expect("inode");
        assert_eq!((one.kind, one.mode, one.size), (b.kind, b.mode, b.size));
    }
    assert!(!batched.iter().any(|i| i.ino == 9_999_999));

    // `dentry_name` is the inverse of `lookup` — the NFS cookie bridge.
    assert_eq!(
        meta.dentry_name(1, inos[0]).await.unwrap().as_deref(),
        Some(names[0].as_str())
    );
    assert_eq!(meta.dentry_name(1, 9_999_999).await.unwrap(), None);
}

/// Every `MetaTxn` opens at a *stated* isolation level, not an inherited one.
///
/// A bare `BEGIN` takes whatever `default_transaction_isolation` happens to be —
/// a server or pooler setting an operator can change without ever seeing the
/// engine's code, which would silently move the level that every cross-row
/// invariant is argued against. READ COMMITTED is the level the design wants:
/// `MetaTxn` exposes no plain reads, so nothing depends on two statements sharing
/// a snapshot, while a conditional `UPDATE … WHERE value = $expected` re-evaluates
/// against the latest committed row after waiting — exactly what `cas_ref` needs.
#[tokio::test]
async fn a_transaction_opens_at_a_stated_isolation_level() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping isolation check: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    let meta = PostgresMetadataStore::connect(&dsn).await.unwrap();
    assert_eq!(
        meta.begin_isolation_self().await.unwrap(),
        "read committed",
        "the level the server actually applies must match what `begin` asks for"
    );
    // And the level is genuinely asked for, not inherited from the default.
    assert!(
        origofs_core::postgres::BEGIN_TXN.contains("ISOLATION LEVEL"),
        "a bare BEGIN would inherit `default_transaction_isolation`"
    );
}

/// The `rmdir`-races-`mkdir` guarantee on the backend that can actually run the
/// two transactions at the same time.
///
/// SQLite serializes writers outright, so its version of this test only proves
/// the existence check. Postgres runs both concurrently, which is where the
/// contention has to come from the parent row: `add_dentry` claims it, so the
/// conditional delete either loses the row or re-evaluates and finds the child.
/// Asserted against a live server rather than argued from MVCC semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn postgres_rmdir_racing_mkdir_never_orphans_a_dentry() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping pg rmdir race: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;
    let meta = Arc::new(PostgresMetadataStore::connect(&dsn).await.unwrap());
    let fs = Arc::new(Fs::new(meta.clone(), Arc::new(MemStore::new())));
    fs.init().await.unwrap();

    for round in 0..40u64 {
        let d = format!("/dir{round}");
        fs.mkdir_p(&d).await.unwrap();
        let dir = meta
            .lookup(1, &format!("dir{round}"))
            .await
            .unwrap()
            .unwrap();

        let (a, b) = (Arc::clone(&fs), Arc::clone(&fs));
        let (d1, d2) = (d.clone(), format!("{d}/child"));
        let remove = tokio::spawn(async move { a.remove(&d1).await });
        let create = tokio::spawn(async move { b.mkdir_p(&d2).await });
        let removed = remove.await.unwrap();
        let created = create.await.unwrap();

        let children = meta.child_count(dir).await.unwrap();
        let parent_gone = meta.get_inode(dir).await.unwrap().is_none();
        assert!(
            !(parent_gone && children > 0),
            "round {round}: {d} deleted with {children} orphaned child dentr(ies) \
             (remove: {removed:?}, mkdir: {created:?})"
        );
    }
}
