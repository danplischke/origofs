//! Backing up the metadata store, and restoring from it.
//!
//! `CLAUDE.md`, `DESIGN.md` §7, and `README.md` all say the metadata database is
//! the thing to back up — `fsck --rebuild` reconstructs committed files,
//! directories, symlinks, and branches from the content store alone, but blame,
//! the audit log, the actor registry, and every uncommitted edit exist *only* in
//! the database. There was no command, no procedure, and no test.

use origofs_sdk::{Workspace, WriteCtx};
use std::path::Path;

async fn open(dir: &Path) -> Workspace {
    Workspace::open_local(dir.join("meta.db"), dir.join("cas"))
        .await
        .unwrap()
}

/// The whole point: destroy the metadata database, restore the snapshot, and get
/// back the things the content store could never have told you.
#[tokio::test]
async fn a_restored_backup_brings_back_blame_and_the_audit_log() {
    let dir = tempfile::tempdir().unwrap();
    let backup = dir.path().join("backups/meta-1.db");

    let committed;
    {
        let ws = open(dir.path()).await;
        let dan = ws.create_human("dan", None).await.unwrap();
        let agent = ws.create_agent("claude", "opus", None).await.unwrap();

        ws.write_as(WriteCtx::actor(dan), "/notes.txt", b"human line\n")
            .await
            .unwrap();
        committed = ws.commit("dan", "first").await.unwrap();
        // An *uncommitted* edit by a second actor: content the object graph has,
        // but whose authorship and working-tree state live only in the database.
        ws.write_as(
            WriteCtx::actor(agent),
            "/notes.txt",
            b"human line\nagent line\n",
        )
        .await
        .unwrap();

        let what = ws.backup_metadata(&backup).await.unwrap();
        assert!(what.contains("sqlite online backup"), "got: {what}");
    }

    // Lose the database — the failure the backup exists for. Content is untouched.
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(dir.path().join(format!("meta.db{suffix}")));
    }
    {
        let ws = open(dir.path()).await;
        assert!(
            ws.read("/notes.txt").await.is_err(),
            "a fresh database knows nothing about the workspace"
        );
    }

    // Restore.
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(dir.path().join(format!("meta.db{suffix}")));
    }
    std::fs::copy(&backup, dir.path().join("meta.db")).unwrap();

    let ws = open(dir.path()).await;
    // The uncommitted edit is back, with its authorship.
    assert_eq!(
        &ws.read("/notes.txt").await.unwrap()[..],
        b"human line\nagent line\n"
    );
    let blame = ws.blame("/notes.txt").await.unwrap();
    let authors: Vec<String> = blame.iter().map(|r| r.actor.display_name.clone()).collect();
    assert!(
        authors.iter().any(|a| a == "dan") && authors.iter().any(|a| a == "claude"),
        "both authors must survive the restore, got {authors:?}"
    );
    // And the history.
    let log = ws.log().await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].hash, committed);
    // And the actor registry.
    let actors = ws.list_actors().await.unwrap();
    assert_eq!(actors.len(), 2, "the actor registry must survive");
}

/// A snapshot can be taken while the workspace is being written to — otherwise it
/// is not a backup anyone will actually take.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_can_be_taken_with_writers_running() {
    let dir = tempfile::tempdir().unwrap();
    let ws = std::sync::Arc::new(open(dir.path()).await);
    let dan = ws.create_human("dan", None).await.unwrap();
    ws.write_as(WriteCtx::actor(dan), "/seed.txt", b"seed")
        .await
        .unwrap();

    let writer = {
        let ws = ws.clone();
        tokio::spawn(async move {
            for i in 0..200u64 {
                let body = format!("line {i}\n").repeat(8);
                ws.write_as(
                    WriteCtx::actor(dan),
                    &format!("/f{}.txt", i % 4),
                    body.as_bytes(),
                )
                .await?;
            }
            Ok::<_, origofs_sdk::OrigoFSError>(())
        })
    };

    let backup = dir.path().join("live.db");
    ws.backup_metadata(&backup)
        .await
        .expect("a snapshot must be possible without stopping writers");
    writer.await.unwrap().expect("writers keep working");

    // The snapshot is a usable database, not a torn file.
    let restored = tempfile::tempdir().unwrap();
    std::fs::copy(&backup, restored.path().join("meta.db")).unwrap();
    // Point it at the *original* content store: the snapshot is metadata only.
    let ws2 = Workspace::open_local(restored.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    assert_eq!(&ws2.read("/seed.txt").await.unwrap()[..], b"seed");
    assert!(ws2.schema_version().await.unwrap() > 0);
}

/// Refuse to overwrite: a backup command that clobbers silently can destroy the
/// only good copy.
#[tokio::test]
async fn backup_refuses_to_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let ws = open(dir.path()).await;
    let dest = dir.path().join("b.db");
    ws.backup_metadata(&dest).await.unwrap();
    let err = ws.backup_metadata(&dest).await;
    assert!(
        matches!(err, Err(origofs_sdk::OrigoFSError::AlreadyExists(_))),
        "a second backup to the same path must be refused, got {err:?}"
    );
}
