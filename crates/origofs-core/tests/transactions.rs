//! The `MetadataStore` transaction primitive (C1, Phase 2 of #34).
//!
//! A logical filesystem write is several statements; before this primitive they
//! ran as independent autocommits, so a failure between any two left the store
//! corrupt — a dangling dentry, an orphaned inode, or content/blame out of sync.
//! `begin` groups them so they commit all-or-nothing and a failed or dropped
//! transaction rolls back in full. These tests exercise the guarantee directly
//! (the engine's write paths, which now route through it, are covered by the
//! roundtrip/attribution/nfs suites).

use origofs_core::{FileKind, InodeInit, MetadataStore, OrigoFSError, SqliteMetadataStore};

fn file() -> InodeInit {
    InodeInit {
        kind: FileKind::File,
        mode: 0o100644,
    }
}

/// Committing a multi-step transaction persists every mutation together.
#[tokio::test]
async fn commit_persists_all_mutations() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    store.init().await.unwrap();

    let ino = {
        let mut tx = store.begin().await.unwrap();
        let ino = tx.create_inode(file()).await.unwrap();
        tx.add_dentry(1, "f", ino).await.unwrap();
        tx.set_content(ino, None, 0).await.unwrap();
        tx.commit().await.unwrap();
        ino
    };

    assert_eq!(store.lookup(1, "f").await.unwrap(), Some(ino));
    assert!(store.get_inode(ino).await.unwrap().is_some());
}

/// Dropping a transaction without committing rolls back everything — including
/// the freshly created inode, so a failed create can't orphan one (M6).
#[tokio::test]
async fn drop_rolls_back_including_the_inode() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    store.init().await.unwrap();

    let ino = {
        let mut tx = store.begin().await.unwrap();
        let ino = tx.create_inode(file()).await.unwrap();
        tx.add_dentry(1, "ghost", ino).await.unwrap();
        ino
        // `tx` drops here without `commit` -> ROLLBACK
    };

    assert!(
        store.lookup(1, "ghost").await.unwrap().is_none(),
        "the dentry must not survive a rolled-back transaction"
    );
    assert!(
        store.get_inode(ino).await.unwrap().is_none(),
        "the inode must not be left orphaned by a rolled-back create"
    );
}

/// A failure part-way through a transaction rolls back the *whole* logical
/// write — modeling `write_as` hitting an error after it has already created the
/// inode, linked it, and set content. Nothing partial persists, and unrelated
/// data committed earlier is untouched.
#[tokio::test]
async fn a_failed_step_rolls_back_the_whole_transaction() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    store.init().await.unwrap();

    // A pre-existing committed file whose name we'll collide with.
    {
        let mut tx = store.begin().await.unwrap();
        let a = tx.create_inode(file()).await.unwrap();
        tx.add_dentry(1, "taken", a).await.unwrap();
        tx.commit().await.unwrap();
    }

    let ino = {
        let mut tx = store.begin().await.unwrap();
        let ino = tx.create_inode(file()).await.unwrap();
        tx.add_dentry(1, "new", ino).await.unwrap();
        tx.set_content(ino, None, 0).await.unwrap();
        // The failing step: linking a name that already exists.
        let err = tx.add_dentry(1, "taken", ino).await;
        assert!(
            matches!(err, Err(OrigoFSError::AlreadyExists(_))),
            "duplicate dentry must error, got {err:?}"
        );
        ino
        // `tx` drops here on the error path -> ROLLBACK
    };

    // The partial write is entirely gone: no "new" dentry, no orphaned inode.
    assert!(store.lookup(1, "new").await.unwrap().is_none());
    assert!(store.get_inode(ino).await.unwrap().is_none());
    // The earlier committed file is intact.
    assert!(store.lookup(1, "taken").await.unwrap().is_some());
}
