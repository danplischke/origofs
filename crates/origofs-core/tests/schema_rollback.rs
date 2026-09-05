//! What happens to a metadata database when the binaries move — forward, and
//! back again.
//!
//! Content in the object store is immutable and versioned per object, so a format
//! change there mints new objects and leaves old ones readable
//! (`origofs_core::format`). The metadata database is the opposite: migrations
//! rewrite it in place, forward only, and there are no down-migrations by design
//! — a step that dropped the column a newer build had been filling would destroy
//! whatever was written since the upgrade, silently and irreversibly. `docs/DESIGN.md`
//! §4b spells the trade out.
//!
//! What that leaves is a rollback story with exactly two requirements, both
//! tested here:
//!
//! 1. **An older binary must refuse a newer database, loudly, before touching
//!    it.** `Fs::init` runs the migration runner, which applies every step absent
//!    from `schema_meta` and would otherwise happily proceed against a schema it
//!    does not know — including past V11 and V13, which changed primary keys.
//! 2. **The refusal must leave the database exactly as it found it**, or "roll
//!    back the binaries, then roll forward again" would not be a recovery at all.
//!
//! The supported downgrade is therefore: restore the metadata backup taken before
//! the migration (`origofs migrate --backup`, `origofs backup`, `pg_dump`). The
//! content store needs no rollback — the old binary reads the objects a newer one
//! wrote, unless the store descriptor says otherwise.

use origofs_core::{Fs, MemStore, SqliteMetadataStore, StoreLifecycle, latest_schema_version};
use std::sync::Arc;

/// A SQLite store migrated to `latest`, then stamped with one more version than
/// this build knows — exactly what a newer origofs leaves behind.
async fn from_the_future(path: &std::path::Path) -> SqliteMetadataStore {
    let store = SqliteMetadataStore::open(path).unwrap();
    store.init().await.unwrap();
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute(
        "INSERT INTO schema_meta(version, applied_at) VALUES (?1, 0)",
        rusqlite::params![latest_schema_version() + 1],
    )
    .unwrap();
    drop(conn);
    store
}

fn schema_meta_rows(path: &std::path::Path) -> Vec<(i64, i64)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT version, applied_at FROM schema_meta ORDER BY version")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[tokio::test]
async fn an_older_binary_refuses_a_newer_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.db");
    let store = from_the_future(&path).await;

    let fs = Fs::new(store, Arc::new(MemStore::new()));
    let e = match fs.init().await {
        Err(e) => e,
        Ok(()) => panic!("a database from a newer origofs must not open"),
    };

    // "Upgrade origofs", not "your data is damaged" — the same distinction the
    // content store's version byte makes, and for the same reason: the bytes are
    // fine and restoring a backup is the wrong reflex.
    assert_eq!(e.code(), "unsupported_version");
    assert!(e.is_unsupported_version());
    let msg = e.to_string();
    assert!(msg.contains("metadata schema"), "{msg}");
    assert!(msg.contains("upgrade origofs"), "{msg}");
}

/// The refusal is a *precondition*, not a failure part-way through: nothing about
/// the database may change. Otherwise rolling the binaries back and forward again
/// would leave a store neither version fully understands.
#[tokio::test]
async fn refusing_leaves_the_database_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.db");
    let store = from_the_future(&path).await;
    let before = schema_meta_rows(&path);

    let fs = Fs::new(store, Arc::new(MemStore::new()));
    assert!(fs.init().await.is_err());

    assert_eq!(schema_meta_rows(&path), before);
    // And the newer build can still open it, which is what makes rolling forward
    // again a recovery rather than a repair.
    assert_eq!(
        SqliteMetadataStore::open(&path)
            .unwrap()
            .schema_version()
            .await
            .unwrap(),
        latest_schema_version() + 1
    );
}

/// The ordinary directions still work: a store at `latest` opens, and one behind
/// migrates forward. Without this the guard could pass its own tests by refusing
/// everything.
#[tokio::test]
async fn current_and_older_databases_still_open() {
    let dir = tempfile::tempdir().unwrap();

    let current = dir.path().join("current.db");
    let fs = Fs::new(
        SqliteMetadataStore::open(&current).unwrap(),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    fs.init()
        .await
        .expect("re-opening a current store is idempotent");

    // A store that has recorded nothing is version 0 — the fresh case, which must
    // never be mistaken for "from the future".
    let fresh = dir.path().join("fresh.db");
    let store = SqliteMetadataStore::open(&fresh).unwrap();
    assert_eq!(store.schema_version().await.unwrap(), 0);
    let fs = Fs::new(store, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    assert_eq!(
        SqliteMetadataStore::open(&fresh)
            .unwrap()
            .schema_version()
            .await
            .unwrap(),
        latest_schema_version()
    );
}
