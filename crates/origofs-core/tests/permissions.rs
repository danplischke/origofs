//! `chmod`/`chown` and inode ownership (migration V17, `docs/PERMISSIONS.md` §3a;
//! issues #121, #122).
//!
//! Two things are being pinned here, and the second is the one that bites.
//!
//! **That a mode change lands at all.** Before V17 there was no `chmod` in the
//! engine: both mounts bound their `setattr` mode argument, discarded it, and
//! replied with the *unchanged* attributes — so `chmod` reported success and did
//! nothing, and mode was write-once at creation.
//!
//! **That it lands on the permission bits only.** The stored `mode` carries the
//! file-type bits too (`S_IFREG`/`S_IFDIR`, set in `vfs_create`/`vfs_mkdir`), and
//! those bits are what the committed tree entry and the git exporter's exec-bit
//! check read. A `chmod` implemented as a plain assignment passes a naive
//! "did the mode change?" assertion and silently turns every file into kind 0.
//! That is why every case below asserts the *whole* mode word, never `& 0o7777`.
//!
//! The Postgres leg is not optional. `set_mode`/`set_owner` have a second
//! implementation with different bitwise-operator and parameter-binding syntax,
//! and this repository has been bitten before by a maintenance path that was only
//! ever tested on SQLite (the GC lease that could not store a NUL byte in a
//! Postgres `text` column — see `gc.rs`). Self-skips without `ORIGOFS_PG_TEST_URL`.

use origofs_core::{
    Fs, LocalCasStore, MemStore, MetadataStore, PostgresMetadataStore, SqliteMetadataStore,
};
use std::sync::Arc;
use std::sync::OnceLock;
use tempfile::TempDir;

const S_IFREG: u32 = 0o100000;
const S_IFDIR: u32 = 0o040000;

async fn fixture() -> (Fs<SqliteMetadataStore, LocalCasStore>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let meta = SqliteMetadataStore::open(dir.path().join("meta.db")).unwrap();
    let content = LocalCasStore::open(dir.path().join("cas")).await.unwrap();
    let fs = Fs::new(meta, content);
    fs.init().await.unwrap();
    (fs, dir)
}

#[tokio::test]
async fn chmod_changes_permission_bits_and_keeps_the_file_type() {
    let (fs, _dir) = fixture().await;
    fs.write("/build.sh", b"#!/bin/sh\n").await.unwrap();
    assert_eq!(fs.stat("/build.sh").await.unwrap().mode, S_IFREG | 0o644);

    let after = fs.chmod("/build.sh", 0o755).await.unwrap();

    // The whole word, not just the low bits: an implementation that assigned the
    // mode outright would leave 0o755 here and lose `S_IFREG`.
    assert_eq!(after.mode, S_IFREG | 0o755);
    assert_eq!(fs.stat("/build.sh").await.unwrap().mode, S_IFREG | 0o755);
}

#[tokio::test]
async fn chmod_on_a_directory_keeps_s_ifdir() {
    let (fs, _dir) = fixture().await;
    fs.mkdir_p("/private").await.unwrap();
    assert_eq!(fs.stat("/private").await.unwrap().mode, S_IFDIR | 0o755);

    let after = fs.chmod("/private", 0o700).await.unwrap();
    assert_eq!(after.mode, S_IFDIR | 0o700);
}

#[tokio::test]
async fn chmod_ignores_bits_above_the_permission_field() {
    let (fs, _dir) = fixture().await;
    fs.write("/f", b"x").await.unwrap();

    // A caller passing a whole mode word (type bits included) must not be able to
    // change the file's *kind* through `chmod` — the low 12 bits are the contract.
    fs.chmod("/f", S_IFDIR | 0o600).await.unwrap();
    assert_eq!(fs.stat("/f").await.unwrap().mode, S_IFREG | 0o600);
}

#[tokio::test]
async fn chmod_and_chown_on_a_missing_path_are_not_found() {
    let (fs, _dir) = fixture().await;
    // The whole point of the change is that a mode request is never accepted-and-
    // ignored. A bare `UPDATE … WHERE ino = ?` matching zero rows would return
    // `Ok(())` and reproduce exactly the silent success this replaced.
    assert!(fs.chmod("/nope", 0o600).await.is_err());
    assert!(fs.chown("/nope", Some(1), Some(1)).await.is_err());
}

#[tokio::test]
async fn new_inodes_are_root_owned_and_chown_sets_both_halves() {
    let (fs, _dir) = fixture().await;
    fs.write("/f", b"x").await.unwrap();

    // V17 defaults, so the migration is behaviour-preserving for existing stores.
    let st = fs.stat("/f").await.unwrap();
    assert_eq!((st.uid, st.gid), (0, 0));

    let after = fs.chown("/f", Some(1000), Some(100)).await.unwrap();
    assert_eq!((after.uid, after.gid), (1000, 100));
    let st = fs.stat("/f").await.unwrap();
    assert_eq!((st.uid, st.gid), (1000, 100));
}

#[tokio::test]
async fn chown_leaves_an_omitted_half_alone() {
    let (fs, _dir) = fixture().await;
    fs.write("/f", b"x").await.unwrap();
    fs.chown("/f", Some(1000), Some(100)).await.unwrap();

    // `chown :group` and `chown user` are both legal; writing back a value the
    // caller never supplied would silently reassign the other half.
    let after = fs.chown("/f", None, Some(42)).await.unwrap();
    assert_eq!((after.uid, after.gid), (1000, 42));

    let after = fs.chown("/f", Some(7), None).await.unwrap();
    assert_eq!((after.uid, after.gid), (7, 42));

    // Neither half named: a no-op, not a reset to 0.
    let after = fs.chown("/f", None, None).await.unwrap();
    assert_eq!((after.uid, after.gid), (7, 42));
}

#[tokio::test]
async fn chmod_and_chown_do_not_disturb_content_or_size() {
    let (fs, _dir) = fixture().await;
    fs.write("/f", b"hello world").await.unwrap();
    let before = fs.stat("/f").await.unwrap();

    fs.chmod("/f", 0o600).await.unwrap();
    fs.chown("/f", Some(5), Some(5)).await.unwrap();

    let after = fs.stat("/f").await.unwrap();
    assert_eq!(after.content, before.content);
    assert_eq!(after.size, before.size);
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"hello world");
}

#[tokio::test]
async fn an_exec_bit_set_by_chmod_survives_a_commit_round_trip() {
    let (fs, _dir) = fixture().await;
    fs.write("/build.sh", b"#!/bin/sh\n").await.unwrap();
    fs.chmod("/build.sh", 0o755).await.unwrap();

    // Mode is encoded into the committed tree entry, and the git exporter reads
    // its exec bit. A `chmod` that only moved the working-tree row — or that lost
    // `S_IFREG` on the way — would not survive being crystallized and restored.
    fs.commit("alice", "mark it executable").await.unwrap();

    // Diverge on another branch, then come back: the mode has to be rebuilt from
    // the committed tree rather than read from the row `chmod` last touched.
    fs.create_branch("dev").await.unwrap();
    fs.checkout("dev").await.unwrap();
    fs.chmod("/build.sh", 0o600).await.unwrap();
    fs.commit("alice", "lock it down").await.unwrap();
    assert_eq!(fs.stat("/build.sh").await.unwrap().mode, S_IFREG | 0o600);

    fs.checkout("main").await.unwrap();
    assert_eq!(fs.stat("/build.sh").await.unwrap().mode, S_IFREG | 0o755);
}

// --- Postgres -------------------------------------------------------------
// The second implementation of `set_mode`/`set_owner`. See the module header.

fn dsn() -> Option<String> {
    std::env::var("ORIGOFS_PG_TEST_URL").ok()
}

/// Serializes the PG tests: they share one database and each resets the schema.
fn pg_lock() -> &'static tokio::sync::Mutex<()> {
    static L: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

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
        .expect("reset schema");
    drop(client);
    let _ = handle.await;
}

#[tokio::test]
async fn postgres_chmod_and_chown_match_the_sqlite_behaviour() {
    let Some(dsn) = dsn() else {
        eprintln!("skipping: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let _guard = pg_lock().lock().await;
    reset(&dsn).await;

    let meta = PostgresMetadataStore::connect(&dsn).await.unwrap();
    meta.init().await.unwrap();
    let fs = Fs::new(Arc::new(meta), Arc::new(MemStore::new()));
    fs.init().await.unwrap();

    fs.write("/build.sh", b"#!/bin/sh\n").await.unwrap();
    let st = fs.stat("/build.sh").await.unwrap();
    assert_eq!(st.mode, S_IFREG | 0o644);
    assert_eq!((st.uid, st.gid), (0, 0));

    // `(mode & ~4095) | ($1 & 4095)` in Postgres, not SQLite — different operator
    // precedence rules and a different parameter syntax, so it is genuinely a
    // second implementation rather than the same string run twice.
    let after = fs.chmod("/build.sh", 0o755).await.unwrap();
    assert_eq!(after.mode, S_IFREG | 0o755);

    fs.mkdir_p("/d").await.unwrap();
    assert_eq!(fs.chmod("/d", 0o700).await.unwrap().mode, S_IFDIR | 0o700);

    let after = fs.chown("/build.sh", Some(1000), Some(100)).await.unwrap();
    assert_eq!((after.uid, after.gid), (1000, 100));

    // COALESCE, the half that is easy to get wrong in either dialect.
    let after = fs.chown("/build.sh", None, Some(42)).await.unwrap();
    assert_eq!((after.uid, after.gid), (1000, 42));

    assert!(fs.chmod("/nope", 0o600).await.is_err());
}
