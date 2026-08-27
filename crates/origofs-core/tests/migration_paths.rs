//! Upgrade-path coverage: **every** `vN → latest` migration path, per engine.
//!
//! The migration list is forward-only and append-only, and `migrations.rs`'s unit
//! test pins its shape (contiguous, sorted, non-empty). What that test cannot see
//! is whether the *runner* can actually carry a store sitting at an arbitrary older
//! version all the way to `latest_schema_version()` — a step that rebuilds a table
//! (V11, V13) or does a bare `ADD COLUMN` (V6, V10, V12) can only fail against a
//! real database at a real starting version.
//!
//! The existing V10→latest test (`sqlite::tests::upgrade_preserves_data_and_backfills_default_workspace`)
//! covers one such pair in depth. This file covers *all* of them, in breadth: for
//! each `N` in `1..=latest`, build a store stopped at schema `vN`, populate it with
//! rows that exist at that version, run the real runner, and assert it lands on
//! `latest` with the data intact and the store still usable.
//!
//! The two dialects diverge (SQLite rebuilds tables where Postgres alters in
//! place), so each engine is exercised separately. The Postgres leg self-skips
//! unless `ORIGOFS_PG_TEST_URL` points at a reachable database, matching
//! `tests/postgres.rs`.

use origofs_core::migrations::MIGRATIONS;
use origofs_core::{
    FileKind, InodeInit, MetadataStore, PostgresMetadataStore, SqliteMetadataStore,
    latest_schema_version,
};
use std::sync::OnceLock;

/// Rows we seed into a store stopped at version `at`, using only the columns that
/// exist at that version. Everything here must survive the upgrade to `latest`.
///
/// * `inode`/`dentry` exist from V1 and are only ever *widened* afterwards
///   (V11 adds `inode.workspace_id` with a default), so the V1 column list is
///   valid at every version.
/// * `ref`/`config` arrive in V2 and are **rebuilt** by V11 (create-copy-drop-rename
///   on SQLite), which is exactly the step most likely to silently drop rows.
const SEED_INODES_SQLITE: &str = "
INSERT INTO inode(ino, kind, mode, nlink, size, content_hash, mtime, ctime)
    VALUES (1, 'dir', 16877, 1, 0, NULL, 0, 0);
INSERT INTO inode(ino, kind, mode, nlink, size, content_hash, mtime, ctime)
    VALUES (2, 'file', 33188, 1, 11, '1111111111111111111111111111111111111111111111111111111111111111', 7, 7);
INSERT INTO dentry(parent_ino, name, ino) VALUES (1, 'keep.txt', 2);
";

/// The manifest hash seeded above, as the store parses it back out.
const SEED_CONTENT_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";

const SEED_REFS: &str = "
INSERT INTO ref(name, value) VALUES ('refs/heads/main', 'commit-abc');
INSERT INTO config(key, value) VALUES ('versioning', 'native');
";

/// Assertions common to both engines once a store seeded at `from` has been
/// migrated: it reports `latest`, the seeded rows survived, and it still works.
async fn assert_upgraded(store: &dyn MetadataStore, from: i64) {
    assert_eq!(
        store.schema_version().await.unwrap(),
        latest_schema_version(),
        "a store at v{from} must migrate all the way to latest"
    );

    // The seeded working tree survived the upgrade.
    let ino = store
        .lookup(1, "keep.txt")
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("v{from}→latest: seeded dentry 'keep.txt' was lost"));
    assert_eq!(
        ino, 2,
        "v{from}→latest: dentry now points at the wrong inode"
    );
    let inode = store
        .get_inode(2)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("v{from}→latest: seeded inode 2 was lost"));
    assert_eq!(inode.kind, FileKind::File);
    assert_eq!(inode.size, 11, "v{from}→latest: inode size changed");
    assert_eq!(
        inode.content.map(|h| h.to_hex()).as_deref(),
        Some(SEED_CONTENT_HEX),
        "v{from}→latest: inode lost its content address"
    );

    // The root the runner ensures is present and is a directory.
    let root = store
        .get_inode(1)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("v{from}→latest: root inode was lost"));
    assert_eq!(root.kind, FileKind::Dir);

    // The namespace-keyed tables V11 rebuilds kept their rows, now scoped to the
    // `default` workspace (which is what the migrated store reads by default).
    if from >= 2 {
        assert_eq!(
            store.get_ref("refs/heads/main").await.unwrap().as_deref(),
            Some("commit-abc"),
            "v{from}→latest: V11's ref rebuild dropped the branch"
        );
        assert_eq!(
            store.get_config("versioning").await.unwrap().as_deref(),
            Some("native"),
            "v{from}→latest: V11's config rebuild dropped the setting"
        );
    }
    // A store that migrated but can't be written to is not actually upgraded.
    let fresh = store
        .create_inode(InodeInit::new(FileKind::File, 0o100644))
        .await
        .unwrap();
    assert!(
        fresh > 2,
        "v{from}→latest: new inode {fresh} collides with the seeded ids"
    );
    store.add_dentry(1, "after.txt", fresh).await.unwrap();
    let names: Vec<String> = store
        .list_dir(1)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(
        names.iter().any(|n| n == "keep.txt") && names.iter().any(|n| n == "after.txt"),
        "v{from}→latest: directory listing is {names:?}"
    );

    // Migrating an already-current store is a no-op, not a re-run.
    store.init().await.unwrap();
    assert_eq!(
        store.schema_version().await.unwrap(),
        latest_schema_version()
    );
}

// --- SQLite ---------------------------------------------------------------

/// Every `vN → latest` upgrade path on SQLite. Each iteration builds a store
/// stopped at `vN` by applying exactly the first `N` migrations and recording them
/// in `schema_meta` — the same bookkeeping the runner writes — then hands it to the
/// real `init()` runner.
#[tokio::test]
async fn sqlite_upgrades_from_every_version_to_latest() {
    let dir = tempfile::tempdir().unwrap();

    for from in 1..=latest_schema_version() {
        let path = dir.path().join(format!("v{from}.db"));
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_meta(version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL);",
            )
            .unwrap();
            for m in MIGRATIONS.iter().filter(|m| m.version <= from) {
                conn.execute_batch(m.sqlite)
                    .unwrap_or_else(|e| panic!("applying SQLite migration v{}: {e}", m.version));
                conn.execute(
                    "INSERT INTO schema_meta(version, applied_at) VALUES (?1, 0)",
                    rusqlite::params![m.version],
                )
                .unwrap();
            }
            conn.execute_batch(SEED_INODES_SQLITE).unwrap();
            if from >= 2 {
                conn.execute_batch(SEED_REFS).unwrap();
            }
        }

        let store = SqliteMetadataStore::open(&path).unwrap();
        assert_eq!(
            store.schema_version().await.unwrap(),
            from,
            "hand-built store should sit at exactly v{from}"
        );
        store
            .init()
            .await
            .unwrap_or_else(|e| panic!("SQLite v{from}→latest migration failed: {e}"));
        assert_upgraded(&store, from).await;
    }
}

/// A store built by the runner itself at `vN` (rather than by hand) also upgrades:
/// this catches a migration whose *own* effects — not just its DDL — the next step
/// depends on. Equivalent to stopping a real deployment mid-history and resuming.
#[tokio::test]
async fn sqlite_upgrade_is_stepwise_from_every_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stepwise.db");
    let store = SqliteMetadataStore::open(&path).unwrap();

    // Walk the whole history one version at a time, letting the runner do each
    // step, and check the reported version after every one. A step that silently
    // fails to record itself (or records the wrong version) shows up here.
    store.init().await.unwrap();
    assert_eq!(
        store.schema_version().await.unwrap(),
        latest_schema_version()
    );

    // Now rewind the bookkeeping one version at a time and re-run: every suffix
    // `vN..=latest` of the migration list must be re-appliable onto a store whose
    // schema is already current. This is the crash-recovery shape (H9) generalized
    // to every version, not just V6's ADD COLUMN.
    for from in (1..=latest_schema_version()).rev() {
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "DELETE FROM schema_meta WHERE version >= ?1",
                rusqlite::params![from],
            )
            .unwrap();
        }
        let store = SqliteMetadataStore::open(&path).unwrap();
        assert_eq!(store.schema_version().await.unwrap(), from - 1);
        store.init().await.unwrap_or_else(|e| {
            panic!("SQLite re-apply of migrations v{from}..=latest failed: {e}")
        });
        assert_eq!(
            store.schema_version().await.unwrap(),
            latest_schema_version()
        );
    }
}

// --- Postgres -------------------------------------------------------------

fn dsn() -> Option<String> {
    std::env::var("ORIGOFS_PG_TEST_URL").ok()
}

/// Serializes the PG tests in this binary: they share one database and each resets
/// the schema, so they must not overlap.
fn pg_lock() -> &'static tokio::sync::Mutex<()> {
    static L: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Connect, run `sql`, disconnect. Panics with context on failure.
async fn pg_exec(dsn: &str, sql: &str, what: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect for {what}: {e}"));
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{what}: {e}"));
    drop(client);
    let _ = handle.await;
}

/// Every `vN → latest` upgrade path on Postgres. The dialects diverge — Postgres
/// alters the PK-changed tables in place where SQLite rebuilds them — so this is
/// not redundant with the SQLite leg.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_upgrades_from_every_version_to_latest() {
    let Some(dsn) = dsn() else {
        eprintln!(
            "skipping postgres_upgrades_from_every_version_to_latest: ORIGOFS_PG_TEST_URL unset"
        );
        return;
    };
    let _guard = pg_lock().lock().await;

    for from in 1..=latest_schema_version() {
        pg_exec(
            &dsn,
            "DROP SCHEMA public CASCADE; CREATE SCHEMA public;",
            "reset public schema",
        )
        .await;

        // Build the store at exactly vN, mirroring the runner's bookkeeping.
        let mut sql = String::from(
            "CREATE TABLE IF NOT EXISTS schema_meta(version BIGINT PRIMARY KEY, applied_at BIGINT NOT NULL);\n",
        );
        for m in MIGRATIONS.iter().filter(|m| m.version <= from) {
            sql.push_str(m.postgres);
            sql.push('\n');
            sql.push_str(&format!(
                "INSERT INTO schema_meta(version, applied_at) VALUES ({}, 0);\n",
                m.version
            ));
        }
        sql.push_str(SEED_INODES_SQLITE); // plain INSERTs; valid in both dialects
        if from >= 2 {
            sql.push_str(SEED_REFS);
        }
        pg_exec(&dsn, &sql, &format!("build a Postgres store at v{from}")).await;

        let store = PostgresMetadataStore::connect(&dsn).await.unwrap();
        assert_eq!(
            store.schema_version().await.unwrap(),
            from,
            "hand-built store should sit at exactly v{from}"
        );
        store
            .init()
            .await
            .unwrap_or_else(|e| panic!("Postgres v{from}→latest migration failed: {e}"));
        assert_upgraded(&store, from).await;
    }
}
