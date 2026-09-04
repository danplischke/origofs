//! Encryption at rest for the **production** backend combinations.
//!
//! It used to be wired only for SQLite + a local directory, and the CLI refused
//! outright for anything else — so the deployment `deploy/config.example.toml`
//! recommends, Postgres over an object store, could not have encryption at all.
//! `EncryptedStore` always composed over any `ContentStore`; what was missing was
//! somewhere to keep the key-derivation salt that survives losing the metadata
//! database and that garbage collection cannot sweep.

use origofs_sdk::{
    ContentStore, LocalCasStore, MetadataStore, ObjectContentStore, PackStore, SqliteMetadataStore,
    Workspace,
};
use std::sync::Arc;

/// A packed object store — the layout `deploy/config.example.toml` recommends —
/// encrypted, with the salt living in the bucket beside the content.
#[tokio::test]
async fn encryption_works_over_a_packed_object_store() {
    let dir = tempfile::tempdir().unwrap();
    // One shared in-memory bucket across both opens, standing in for S3.
    let bucket: Arc<dyn ContentStore> = Arc::new(ObjectContentStore::in_memory());
    let index: Arc<dyn ContentStore> =
        Arc::new(LocalCasStore::open(dir.path().join("index")).await.unwrap());

    let backend: Arc<dyn ContentStore> = Arc::new(PackStore::new(bucket.clone(), index.clone()));
    let meta: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("m.db")).unwrap());
    let ws = Workspace::open_encrypted(meta, backend, "correct horse battery staple")
        .await
        .unwrap();

    let dan = ws.create_human("dan", None).await.unwrap();
    ws.write_as(
        origofs_sdk::WriteCtx::actor(dan),
        "/secret.txt",
        b"plaintext here",
    )
    .await
    .unwrap();
    ws.flush().await.unwrap();
    assert_eq!(
        &ws.read("/secret.txt").await.unwrap()[..],
        b"plaintext here"
    );

    // Reopening with the same passphrase works: the salt was found, not regenerated.
    let backend2: Arc<dyn ContentStore> = Arc::new(PackStore::new(bucket.clone(), index.clone()));
    let meta2: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("m.db")).unwrap());
    let ws2 = Workspace::open_encrypted(meta2, backend2, "correct horse battery staple")
        .await
        .unwrap();
    assert_eq!(
        &ws2.read("/secret.txt").await.unwrap()[..],
        b"plaintext here"
    );

    // The wrong passphrase fails loudly rather than returning garbage.
    let backend3: Arc<dyn ContentStore> = Arc::new(PackStore::new(bucket.clone(), index.clone()));
    let meta3: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("m.db")).unwrap());
    let ws3 = Workspace::open_encrypted(meta3, backend3, "wrong passphrase")
        .await
        .unwrap();
    assert!(
        ws3.read("/secret.txt").await.is_err(),
        "a wrong passphrase must fail, not return garbage"
    );
}

/// The salt must survive garbage collection. If GC could sweep it, every object
/// in the store would become permanently undecryptable — the worst outcome the
/// system has.
#[tokio::test]
async fn gc_cannot_sweep_the_encryption_salt() {
    let dir = tempfile::tempdir().unwrap();
    let bucket: Arc<dyn ContentStore> = Arc::new(ObjectContentStore::in_memory());
    let meta: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("m.db")).unwrap());
    let ws = Workspace::open_encrypted(meta, bucket.clone(), "pass")
        .await
        .unwrap();

    let dan = ws.create_human("dan", None).await.unwrap();
    ws.write_as(
        origofs_sdk::WriteCtx::actor(dan),
        "/keep.txt",
        b"still here",
    )
    .await
    .unwrap();
    ws.commit("dan", "base").await.unwrap();
    // Churn, so the sweep has something to actually delete.
    ws.write_as(origofs_sdk::WriteCtx::actor(dan), "/churn.txt", b"garbage")
        .await
        .unwrap();
    ws.remove("/churn.txt").await.unwrap();

    // Aggressive: no grace at all, so nothing unreferenced is spared by age.
    ws.gc_with_grace(0).await.unwrap();

    // The salt is untouched, so the store still reads.
    assert_eq!(&ws.read("/keep.txt").await.unwrap()[..], b"still here");
    assert!(
        bucket.get_sidecar("keysalt").await.unwrap().is_some(),
        "the salt sidecar must survive a sweep"
    );

    // And a fresh handle with the same passphrase still derives the same key.
    let meta2: Arc<dyn MetadataStore> =
        Arc::new(SqliteMetadataStore::open(dir.path().join("m.db")).unwrap());
    let ws2 = Workspace::open_encrypted(meta2, bucket, "pass")
        .await
        .unwrap();
    assert_eq!(&ws2.read("/keep.txt").await.unwrap()[..], b"still here");
}

/// A local encrypted workspace written before the salt moved to a sidecar must
/// still open: the local sidecar path *is* `<cas_dir>/keysalt`, where it always was.
#[tokio::test]
async fn an_existing_local_encrypted_workspace_still_opens() {
    let dir = tempfile::tempdir().unwrap();
    let cas = dir.path().join("cas");
    std::fs::create_dir_all(&cas).unwrap();
    // The on-disk layout the old code produced.
    std::fs::write(cas.join("keysalt"), b"0123456789abcdef").unwrap();

    let ws = Workspace::open_local_encrypted(dir.path().join("m.db"), &cas, "pass")
        .await
        .unwrap();
    ws.write("/f.txt", b"hello").await.unwrap();
    assert_eq!(&ws.read("/f.txt").await.unwrap()[..], b"hello");
    // The pre-existing salt was adopted, not replaced.
    assert_eq!(
        std::fs::read(cas.join("keysalt")).unwrap(),
        b"0123456789abcdef"
    );
}

/// The Argon2id cost a store predating the `kdf` descriptor is read at.
///
/// The parameters used to come from `argon2::Params::default()`, a constant the
/// crate owns and has moved before. What replaces it has to answer one question
/// correctly forever: a store that already holds objects was keyed at the *old*
/// cost, and must go on being keyed at it even after this build's default is
/// raised. A salt with no descriptor beside it is exactly that store.
///
/// The assertion is on the recorded value rather than on a decryption failure,
/// because `LEGACY` and `current()` are equal today — this is the guard that fails
/// the day someone raises `current()` and drops the distinction, which would
/// re-key every existing store at once and report it as a wrong passphrase.
#[tokio::test]
async fn a_store_predating_the_kdf_descriptor_keeps_the_legacy_cost() {
    let dir = tempfile::tempdir().unwrap();
    let cas = dir.path().join("cas");
    std::fs::create_dir_all(&cas).unwrap();
    // A store as an older origofs left it: a salt, and nothing describing the cost.
    std::fs::write(cas.join("keysalt"), b"0123456789abcdef").unwrap();
    assert!(!cas.join("kdf").exists());

    let ws = Workspace::open_local_encrypted(dir.path().join("m.db"), &cas, "pass")
        .await
        .unwrap();
    ws.write("/f.txt", b"hello").await.unwrap();
    assert_eq!(&ws.read("/f.txt").await.unwrap()[..], b"hello");

    // Made explicit rather than changed: the store is now self-describing, and it
    // describes what it always was.
    let recorded = std::fs::read(cas.join("kdf")).unwrap();
    assert_eq!(
        origofs_sdk::KdfParams::decode(&recorded).unwrap(),
        origofs_sdk::KdfParams::LEGACY,
        "a store that predates the descriptor must be read at the legacy cost"
    );
}

/// A store with neither a salt nor a descriptor is genuinely new, and is the only
/// kind that may be created at this build's current cost.
#[tokio::test]
async fn a_fresh_store_records_the_current_kdf_cost() {
    let dir = tempfile::tempdir().unwrap();
    let cas = dir.path().join("cas");

    let ws = Workspace::open_local_encrypted(dir.path().join("m.db"), &cas, "pass")
        .await
        .unwrap();
    ws.write("/f.txt", b"hello").await.unwrap();

    let recorded = std::fs::read(cas.join("kdf")).unwrap();
    assert_eq!(
        origofs_sdk::KdfParams::decode(&recorded).unwrap(),
        origofs_sdk::KdfParams::current()
    );

    // And it is stable across reopens — a second open must adopt what is there,
    // never re-derive.
    drop(ws);
    let ws2 = Workspace::open_local_encrypted(dir.path().join("m.db"), &cas, "pass")
        .await
        .unwrap();
    assert_eq!(&ws2.read("/f.txt").await.unwrap()[..], b"hello");
    assert_eq!(std::fs::read(cas.join("kdf")).unwrap(), recorded);
}
