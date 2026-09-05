//! Trash: an uncommitted delete becomes recoverable (issue #115).
//!
//! A committed file can be read back out of history; an **uncommitted** one could
//! not be recovered at all. GC's grace period looks like it might help and does
//! not — it protects in-flight writes from the sweep, which is a correctness guard
//! for the durability barrier, not a user-facing undo.
//!
//! The gap matters more here than for an ordinary filesystem because the users are
//! agents: an agent that shells out to `rm -rf` on a bad path is routine, and "you
//! should have committed first" is no answer when the actor that failed to commit
//! is the one that deleted the tree.

use origofs_core::{
    DEFAULT_TRASH_RETENTION_SECS, Fs, MemStore, MetadataStore, OrigoFSError, SqliteMetadataStore,
    WriteCtx,
};
use std::sync::Arc;

async fn fixture() -> Fs<Arc<dyn MetadataStore>, Arc<MemStore>> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

/// A workspace with trash on, plus an actor to attribute deletions to.
async fn with_trash() -> (Fs<Arc<dyn MetadataStore>, Arc<MemStore>>, i64, i64) {
    let fs = fixture().await;
    fs.set_trash_retention(Some(DEFAULT_TRASH_RETENTION_SECS))
        .await
        .unwrap();
    let actor = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(actor, Some("t")).await.unwrap();
    (fs, actor, session)
}

/// The headline: a deleted file comes back, byte for byte.
#[tokio::test]
async fn a_deleted_file_can_be_restored() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/notes.md", b"important work")
        .await
        .unwrap();

    fs.remove_as(ctx, "/notes.md").await.unwrap();
    assert!(fs.stat("/notes.md").await.is_err(), "the delete happened");

    let entries = fs.list_trash().await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "/notes.md");

    let restored = fs.restore_trash(entries[0].id, ctx).await.unwrap();
    assert_eq!(restored, "/notes.md");
    assert_eq!(
        &fs.read("/notes.md").await.unwrap()[..],
        b"important work",
        "the restored body must be byte-identical"
    );
    assert!(
        fs.list_trash().await.unwrap().is_empty(),
        "a restored entry must leave the trash"
    );
}

/// A trash entry records **who** deleted it and in which session.
///
/// This is what makes trash fit origofs's grain rather than being a port of
/// JuiceFS's `.trash` directory: the deletion is already in the op-log, and the
/// entry beside it names the same actor, so "who deleted this" is answerable
/// without correlating timestamps.
#[tokio::test]
async fn a_trash_entry_records_who_deleted_it() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/f.txt", b"x").await.unwrap();
    fs.remove_as(ctx, "/f.txt").await.unwrap();

    let e = &fs.list_trash().await.unwrap()[0];
    assert_eq!(
        e.actor_id,
        Some(actor),
        "the deleting actor was not recorded"
    );
    assert_eq!(e.session_id, Some(session));
}

/// A restore is attributed to the **restorer**, not to whoever deleted it. It is a
/// new act by a new actor and the op-log should say so; the original deleter stays
/// on the trash entry, so both questions remain answerable.
#[tokio::test]
async fn a_restore_is_attributed_to_the_restorer() {
    let (fs, deleter, session) = with_trash().await;
    let restorer = fs.create_human("dan", None).await.unwrap();
    let del_ctx = WriteCtx::session(deleter, session);

    fs.write_as(del_ctx, "/f.txt", b"line one\n").await.unwrap();
    fs.remove_as(del_ctx, "/f.txt").await.unwrap();
    let id = fs.list_trash().await.unwrap()[0].id;

    fs.restore_trash(id, WriteCtx::actor(restorer))
        .await
        .unwrap();

    let blame = fs.blame("/f.txt").await.unwrap();
    assert!(
        blame.iter().all(|b| b.actor.id == restorer),
        "the restored content should be attributed to whoever restored it"
    );
}

/// Mode and ownership survive the round trip. Without this a restored file comes
/// back with default permissions — a `chmod +x` script silently loses its exec
/// bit, which is the same class of quiet wrongness #121 was about.
#[tokio::test]
async fn a_restore_preserves_mode_and_ownership() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/build.sh", b"#!/bin/sh\n").await.unwrap();
    let ino = fs.stat("/build.sh").await.unwrap().ino;
    fs.vfs_chmod_unchecked(ino, 0o755).await.unwrap();
    fs.vfs_chown_unchecked(ino, Some(1000), Some(2000))
        .await
        .unwrap();

    fs.remove_as(ctx, "/build.sh").await.unwrap();
    let id = fs.list_trash().await.unwrap()[0].id;
    fs.restore_trash(id, ctx).await.unwrap();

    let after = fs.stat("/build.sh").await.unwrap();
    assert_eq!(after.mode & 0o7777, 0o755, "the exec bit was lost");
    assert_eq!((after.uid, after.gid), (1000, 2000), "ownership was lost");
}

/// Restoring over something that already exists is refused. An undo does not get
/// to trade one lost file for another on the user's behalf.
#[tokio::test]
async fn a_restore_will_not_overwrite_a_live_file() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/f.txt", b"old").await.unwrap();
    fs.remove_as(ctx, "/f.txt").await.unwrap();
    let id = fs.list_trash().await.unwrap()[0].id;

    // Something new now occupies the path.
    fs.write_as(ctx, "/f.txt", b"new work").await.unwrap();

    assert!(
        matches!(
            fs.restore_trash(id, ctx).await,
            Err(OrigoFSError::AlreadyExists(_))
        ),
        "restoring must not silently overwrite what is there now"
    );
    assert_eq!(
        &fs.read("/f.txt").await.unwrap()[..],
        b"new work",
        "the live file must be untouched by the refused restore"
    );
    assert_eq!(
        fs.list_trash().await.unwrap().len(),
        1,
        "a refused restore must leave the entry in the trash"
    );
}

/// Symlinks and directories restore too, not just regular files.
#[tokio::test]
async fn symlinks_and_directories_restore() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/target.txt", b"t").await.unwrap();
    fs.symlink_as(ctx, "/target.txt", "/link").await.unwrap();
    fs.mkdir_as(ctx, "/emptydir").await.unwrap();

    fs.remove_as(ctx, "/link").await.unwrap();
    fs.remove_as(ctx, "/emptydir").await.unwrap();

    for e in fs.list_trash().await.unwrap() {
        fs.restore_trash(e.id, ctx).await.unwrap();
    }
    assert_eq!(
        fs.readlink("/link").await.unwrap(),
        "/target.txt",
        "a restored symlink must point where it did"
    );
    assert!(fs.stat("/emptydir").await.unwrap().kind == origofs_core::FileKind::Dir);
}

/// **The GC interaction.** A trashed body is a GC root for as long as its entry is
/// retained. Without that the sweep reclaims its chunks and a restore finds an
/// entry pointing at content that is gone — trash that looks like it works right
/// up until you need it.
#[tokio::test]
async fn a_trashed_body_survives_garbage_collection() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/f.txt", b"content worth keeping")
        .await
        .unwrap();
    fs.remove_as(ctx, "/f.txt").await.unwrap();
    let id = fs.list_trash().await.unwrap()[0].id;

    // A grace of 0 sweeps everything unreachable — the harshest case, and the one
    // that proves the root is doing the work rather than the age gate.
    fs.gc_with_grace(0).await.unwrap();

    fs.restore_trash(id, ctx).await.unwrap();
    assert_eq!(
        &fs.read("/f.txt").await.unwrap()[..],
        b"content worth keeping",
        "gc reclaimed a trashed body that was still restorable"
    );
}

/// A clock the test drives, so retention can be crossed without sleeping. Real
/// time would need a multi-second sleep to clear the one-second granularity of
/// `now_secs`, and would still be a race on a loaded runner.
struct SettableClock(std::sync::atomic::AtomicI64);

impl origofs_core::clock::Clock for SettableClock {
    fn now_secs(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Once an entry expires, it is purged and its content becomes collectable —
/// otherwise trash is a leak rather than a retention window.
#[tokio::test]
async fn an_expired_entry_is_purged_and_its_content_freed() {
    let clock = Arc::new(SettableClock(std::sync::atomic::AtomicI64::new(1_000)));
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::with_clock(meta, Arc::new(MemStore::new()), clock.clone());
    fs.init().await.unwrap();
    fs.set_trash_retention(Some(60)).await.unwrap();
    let actor = fs.create_agent("claude", "opus", None).await.unwrap();
    let ctx = WriteCtx::actor(actor);

    fs.write_as(ctx, "/f.txt", b"transient").await.unwrap();
    fs.remove_as(ctx, "/f.txt").await.unwrap();
    assert_eq!(fs.list_trash().await.unwrap().len(), 1);

    // Still inside the window: gc must leave it alone.
    clock.0.store(1_030, std::sync::atomic::Ordering::Relaxed);
    fs.gc_with_grace(0).await.unwrap();
    assert_eq!(
        fs.list_trash().await.unwrap().len(),
        1,
        "an entry inside its retention window must survive a gc pass"
    );

    // Past it.
    clock.0.store(1_100, std::sync::atomic::Ordering::Relaxed);
    fs.gc_with_grace(0).await.unwrap();
    assert!(
        fs.list_trash().await.unwrap().is_empty(),
        "an expired entry must be purged by the gc pass"
    );
}

/// Trash is **off by default**, so every existing workspace deletes exactly as it
/// did. Turning it on by default would silently change when space is reclaimed for
/// every deployment, and the first anyone would learn of it is a storage bill.
#[tokio::test]
async fn trash_is_off_by_default() {
    let fs = fixture().await;
    let actor = fs.create_human("dan", None).await.unwrap();
    let ctx = WriteCtx::actor(actor);

    assert!(fs.trash_retention().await.unwrap().is_none());
    fs.write_as(ctx, "/f.txt", b"x").await.unwrap();
    fs.remove_as(ctx, "/f.txt").await.unwrap();
    assert!(
        fs.list_trash().await.unwrap().is_empty(),
        "a delete must not be captured when trash is disabled"
    );
}

/// Disabling retention does **not** destroy what is already there. Silently
/// dropping recoverable data as a side effect of a config change would be the
/// opposite of what this feature is for.
#[tokio::test]
async fn disabling_retention_does_not_purge_existing_entries() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/f.txt", b"x").await.unwrap();
    fs.remove_as(ctx, "/f.txt").await.unwrap();

    fs.set_trash_retention(None).await.unwrap();
    assert_eq!(
        fs.list_trash().await.unwrap().len(),
        1,
        "turning trash off must not destroy entries captured while it was on"
    );
    // ...and gc must not quietly finish the job either.
    fs.gc_with_grace(0).await.unwrap();
    assert_eq!(fs.list_trash().await.unwrap().len(), 1);
}

/// Internal machinery does not fill the trash. `remove` is also the demolition
/// primitive for checkout and merge materialization, and trashing those would fill
/// the trash with entries nobody deleted while pinning their content as GC roots.
#[tokio::test]
async fn internal_removes_do_not_fill_the_trash() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/f.txt", b"x").await.unwrap();

    // The raw, unattributed primitive — what checkout and merge use.
    fs.remove("/f.txt").await.unwrap();

    assert!(
        fs.list_trash().await.unwrap().is_empty(),
        "the internal remove primitive must not capture into the trash"
    );
}

/// The user-facing unattributed delete *does* capture, with no actor recorded —
/// the mounts have no actor context, and losing a mount delete entirely would be
/// the exact failure mode this feature exists to prevent.
#[tokio::test]
async fn an_unattributed_user_delete_is_captured_without_an_actor() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.write_as(ctx, "/f.txt", b"x").await.unwrap();

    fs.remove_trashing("/f.txt").await.unwrap();

    let entries = fs.list_trash().await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].actor_id, None,
        "an unattributed delete has no actor to record, and must say so rather \
         than inventing one"
    );
}

/// Purging one entry by hand works, and leaves the others alone.
#[tokio::test]
async fn entries_can_be_purged_individually_or_wholesale() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    for n in 0..3 {
        let p = format!("/f{n}.txt");
        fs.write_as(ctx, &p, b"x").await.unwrap();
        fs.remove_as(ctx, &p).await.unwrap();
    }
    let entries = fs.list_trash().await.unwrap();
    assert_eq!(entries.len(), 3);

    assert!(fs.purge_trash(entries[0].id).await.unwrap());
    assert!(
        !fs.purge_trash(entries[0].id).await.unwrap(),
        "purging twice must report that it was already gone"
    );
    assert_eq!(fs.list_trash().await.unwrap().len(), 2);

    assert_eq!(fs.empty_trash().await.unwrap(), 2);
    assert!(fs.list_trash().await.unwrap().is_empty());
}

/// A delete through a **mount** is captured too, with the path reconstructed from
/// the inode. The mount surfaces address everything by inode number, so nothing on
/// that call path already knows a path — and `rm` through a mount is exactly the
/// failure mode trash exists for, so not capturing it would leave the biggest hole
/// open.
///
/// A mount has no actor context (a deliberate bypass per `CLAUDE.md`), so the
/// entry records none — strictly better than not capturing at all.
#[tokio::test]
async fn a_delete_through_the_mount_surface_is_captured() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.mkdir_as(ctx, "/deep").await.unwrap();
    fs.mkdir_as(ctx, "/deep/nested").await.unwrap();
    fs.write_as(ctx, "/deep/nested/f.txt", b"mount data")
        .await
        .unwrap();

    let parent = fs.stat("/deep/nested").await.unwrap().ino;
    fs.vfs_unlink_unchecked(parent, "f.txt").await.unwrap();

    let entries = fs.list_trash().await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].path, "/deep/nested/f.txt",
        "the path must be reconstructed from the inode, not left as an inode number"
    );
    assert_eq!(entries[0].actor_id, None, "a mount has no actor to record");

    fs.restore_trash(entries[0].id, ctx).await.unwrap();
    assert_eq!(
        &fs.read("/deep/nested/f.txt").await.unwrap()[..],
        b"mount data"
    );
}

/// `vfs_path_of` answers for the root and refuses to loop.
#[tokio::test]
async fn vfs_path_of_resolves_a_nested_inode() {
    let (fs, actor, session) = with_trash().await;
    let ctx = WriteCtx::session(actor, session);
    fs.mkdir_as(ctx, "/a").await.unwrap();
    fs.mkdir_as(ctx, "/a/b").await.unwrap();
    fs.write_as(ctx, "/a/b/c.txt", b"x").await.unwrap();

    let ino = fs.stat("/a/b/c.txt").await.unwrap().ino;
    assert_eq!(
        fs.vfs_path_of_unchecked(ino).await.unwrap().as_deref(),
        Some("/a/b/c.txt")
    );
    assert_eq!(
        fs.vfs_path_of_unchecked(origofs_core::INO_ROOT)
            .await
            .unwrap()
            .as_deref(),
        Some("/")
    );
    // An inode that never existed is not reachable, and says so rather than
    // fabricating a path.
    assert_eq!(fs.vfs_path_of_unchecked(999_999).await.unwrap(), None);
}
