//! The schema-version / migrate surface: a freshly-opened workspace is already
//! at the latest schema version, and `migrate()` is an idempotent no-op there.

use origofs_sdk::Workspace;

#[tokio::test]
async fn schema_version_reports_latest_and_migrate_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();

    let latest = ws.latest_schema_version();
    assert!(latest >= 9, "expected at least the V9 migrations to exist");

    // A normal open already runs migrations, so the store is current.
    assert_eq!(ws.schema_version().await.unwrap(), latest);

    // migrate() is a no-op when already current, and reports (from == to).
    let (from, to) = ws.migrate().await.unwrap();
    assert_eq!(from, latest);
    assert_eq!(to, latest);
    assert_eq!(ws.schema_version().await.unwrap(), latest);
}
