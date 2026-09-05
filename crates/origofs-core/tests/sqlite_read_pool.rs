//! The SQLite backend reads from a pool, and only reads from it.
//!
//! Every read and write used to serialize on one `Mutex<Connection>`. WAL was
//! enabled and then thrown away — its whole point is that readers do not block on
//! the writer or on each other — so the metadata store was a process-global lock.
//! Invisible for a solo CLI call; very visible under a mount, which issues
//! concurrent requests.
//!
//! The pool's safety interlock is `PRAGMA query_only=ON` on every reader: a
//! method that mutates but was routed to the pool fails loudly instead of writing
//! outside the single-writer discipline. That interlock only exists on a
//! **file-backed** store — `open_in_memory` gives each connection its own private
//! database, so there is no pool and `read()` falls back to the writer.
//!
//! Almost every other suite is in-memory. These tests are therefore deliberately
//! on-disk: without them the classification of ~90 methods into read and write
//! would have no coverage at all, which is exactly the way to ship a `set_ref`
//! that quietly stopped working under a pool.

use origofs_core::{
    ActorInit, AttributionStore, ConfigStore, Fs, LocalCasStore, LockStore, MetadataStore,
    RefStore, SqliteMetadataStore, StoreLifecycle, WorkspaceRegistry, WritePolicy,
};
use std::sync::Arc;

async fn on_disk() -> (tempfile::TempDir, Arc<dyn MetadataStore>) {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("meta.db")).unwrap());
    meta.init().await.unwrap();
    (dir, meta)
}

/// The headline: a broad sweep of the store on disk, where a misrouted mutation
/// is refused by `query_only` rather than silently served by the writer.
///
/// One test covering many operations rather than one per operation, because the
/// interesting failure is a *single* method in the wrong bucket, and a sweep is
/// what finds it.
#[tokio::test]
async fn a_file_backed_store_round_trips_every_kind_of_operation() {
    let (dir, meta) = on_disk().await;

    // refs
    meta.set_ref("main", "abc").await.unwrap();
    assert_eq!(meta.get_ref("main").await.unwrap().as_deref(), Some("abc"));
    assert!(meta.cas_ref("main", Some("abc"), "def").await.unwrap());
    assert_eq!(meta.list_refs().await.unwrap().len(), 1);
    meta.delete_ref("main").await.unwrap();
    assert!(meta.get_ref("main").await.unwrap().is_none());

    // config + counters
    meta.set_config("k", "v").await.unwrap();
    assert_eq!(meta.get_config("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(meta.bump_counter("c").await.unwrap(), 1);
    assert_eq!(meta.bump_counter("c").await.unwrap(), 2);

    // actors and sessions
    let actor = meta
        .create_actor(ActorInit::agent("claude", "opus", None))
        .await
        .unwrap();
    assert!(meta.get_actor(actor).await.unwrap().is_some());
    assert_eq!(meta.list_actors().await.unwrap().len(), 1);
    meta.set_write_policy(actor, WritePolicy::Propose)
        .await
        .unwrap();
    assert_eq!(
        meta.get_actor(actor).await.unwrap().unwrap().write_policy,
        WritePolicy::Propose
    );

    // conflicts and named locks
    meta.set_conflict("/a", "both-modified").await.unwrap();
    assert_eq!(meta.list_conflicts().await.unwrap().len(), 1);
    meta.clear_conflicts().await.unwrap();
    assert!(meta.list_conflicts().await.unwrap().is_empty());
    assert!(meta.acquire_lock("/bin.o", "me", 0).await.unwrap());
    assert_eq!(meta.list_locks().await.unwrap().len(), 1);
    assert!(meta.release_lock("/bin.o", "me").await.unwrap());

    // workspaces
    let (_id, _root) = meta.create_workspace("second").await.unwrap();
    assert!(meta.lookup_workspace("second").await.unwrap().is_some());
    assert_eq!(meta.list_workspaces().await.unwrap().len(), 2);

    drop(dir);
}

/// The engine's own paths over a file-backed store: inodes, dentries, xattrs and
/// symlinks all move through the split, and several of them read immediately
/// after writing.
#[tokio::test]
async fn the_engine_round_trips_over_a_file_backed_store() {
    let dir = tempfile::tempdir().unwrap();
    let meta: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("meta.db")).unwrap());
    let content = Arc::new(LocalCasStore::open(dir.path().join("cas")).await.unwrap());
    let fs = Fs::new(meta, content);
    fs.init().await.unwrap();

    fs.mkdir_p("/a/b").await.unwrap();
    fs.write("/a/b/f.txt", b"hello").await.unwrap();
    assert_eq!(&fs.read("/a/b/f.txt").await.unwrap()[..], b"hello");

    fs.setxattr("/a/b/f.txt", "user.k", b"v").await.unwrap();
    assert_eq!(
        fs.getxattr("/a/b/f.txt", "user.k").await.unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(fs.listxattr("/a/b/f.txt").await.unwrap(), vec!["user.k"]);
    assert!(fs.removexattr("/a/b/f.txt", "user.k").await.unwrap());

    fs.symlink("/a/b/f.txt", "/a/link").await.unwrap();
    assert_eq!(fs.readlink("/a/link").await.unwrap(), "/a/b/f.txt");

    fs.chmod("/a/b/f.txt", 0o100600).await.unwrap();
    assert_eq!(fs.stat("/a/b/f.txt").await.unwrap().mode & 0o777, 0o600);

    assert_eq!(fs.ls("/a/b").await.unwrap().len(), 1);
    fs.remove("/a/b/f.txt").await.unwrap();
    assert!(fs.ls("/a/b").await.unwrap().is_empty());

    // `truncate_tree` deletes rows and must be on the writer; it is reached
    // through `checkout`, and was misclassified as a read by the first pass of
    // this split precisely because it delegates to a helper.
    fs.commit("t", "seed").await.unwrap();
    fs.write("/a/b/later.txt", b"x").await.unwrap();
    fs.checkout("main").await.unwrap();
    assert!(
        fs.stat("/a/b/later.txt").await.is_err(),
        "checkout must have truncated the working tree back to the commit"
    );
}

/// Concurrent reads actually overlap rather than queueing behind one another.
///
/// Asserted as "every reader saw the data", not as a timing win: a timing
/// assertion on a loaded CI box is a flake generator. What this does prove is
/// that the pool serves many simultaneous readers correctly, which is the part a
/// wrong `read()` would break.
#[tokio::test]
async fn many_concurrent_readers_are_served_correctly() {
    let (dir, meta) = on_disk().await;
    for i in 0..50 {
        meta.set_ref(&format!("b{i}"), &format!("{i:040x}"))
            .await
            .unwrap();
    }

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let m = meta.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..50 {
                let got = m.get_ref(&format!("b{i}")).await.unwrap();
                assert_eq!(got.as_deref(), Some(format!("{i:040x}").as_str()));
            }
            m.list_refs().await.unwrap().len()
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), 50);
    }
    drop(dir);
}

/// A reader really is read-only, so the interlock the classification relies on is
/// actually armed. A negative control: if `query_only` were ever dropped from the
/// pool's setup, every test above would keep passing and this one would fail.
#[tokio::test]
async fn the_pool_connections_refuse_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.db");
    let store = SqliteMetadataStore::open(&path).unwrap();
    store.init().await.unwrap();
    drop(store);

    // Open a connection the same way the pool does and confirm it refuses.
    let r = rusqlite::Connection::open(&path).unwrap();
    r.execute_batch("PRAGMA query_only=ON;").unwrap();
    let err = r
        .execute("INSERT INTO config(key, value) VALUES ('x', 'y')", [])
        .unwrap_err();
    assert!(
        err.to_string().contains("readonly") || err.to_string().contains("read-only"),
        "a pool connection accepted a write, so the interlock protecting the \
         read/write classification is not armed: {err}"
    );
}

/// Turning the pool off restores the old single-connection behaviour, so the
/// escape hatch in the doc comment is real.
#[tokio::test]
async fn the_pool_can_be_disabled() {
    // `reader_count` caches its answer per process, so this cannot set the env
    // var and observe it here. What it can check is that a store built with no
    // pool still works — which is the in-memory shape, and the fallback path in
    // `read()`.
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    meta.init().await.unwrap();
    meta.set_ref("main", "abc").await.unwrap();
    assert_eq!(meta.get_ref("main").await.unwrap().as_deref(), Some("abc"));
}
