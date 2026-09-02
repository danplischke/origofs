//! POSIX advisory locks over a shared metadata store (issue #119).
//!
//! The range arithmetic is unit-tested in `src/posixlock.rs`, where it needs no
//! database. These tests are about everything that module deliberately does not
//! know: that the decision is applied atomically, that it is visible to a *second*
//! mount, that a dead holder stops blocking, and that none of it happens at all
//! until the workspace opts in.

use origofs_core::posixlock::{LOCK_EOF, LockAnswer, LockKind, LockRequest, PosixLock};
use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, Perms, SqliteMetadataStore, WriteCtx,
};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

fn req(owner: &str, holder: &str, start: i64, end: i64, kind: LockKind) -> LockRequest {
    LockRequest {
        owner: owner.into(),
        holder: holder.into(),
        pid: 42,
        start,
        end,
        kind,
    }
}

/// One workspace holding `/data.bin`, with advisory locking switched on.
async fn fixture() -> (TestFs, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.write("/data.bin", b"0123456789").await.unwrap();
    fs.set_posix_locks_enabled(true).await.unwrap();
    let ino = fs.stat("/data.bin").await.unwrap().ino;
    (fs, ino)
}

/// Two engines over **one** metadata store — what two mounts of a workspace are.
async fn two_mounts() -> (TestFs, TestFs, i64) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let a = Fs::new(meta.clone(), Arc::new(MemStore::new()));
    a.init().await.unwrap();
    a.write("/data.bin", b"0123456789").await.unwrap();
    a.set_posix_locks_enabled(true).await.unwrap();
    let ino = a.stat("/data.bin").await.unwrap().ino;
    let b = Fs::new(meta, Arc::new(MemStore::new()));
    (a, b, ino)
}

// --- the switch ------------------------------------------------------------

/// Off by default, and inert while off.
///
/// This is the whole safety story for the rollout: a mount that does not answer
/// `setlk` gets the kernel's own local locking, which is what every deployment has
/// today. If the engine answered anyway, upgrading would silently move every
/// existing mount onto this code.
#[tokio::test]
async fn locking_is_off_until_the_workspace_opts_in() {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;

    assert!(
        !fs.posix_locks_enabled().await.unwrap(),
        "default must be off"
    );
    let r = req("o", "h", 0, LOCK_EOF, LockKind::Exclusive);
    assert_eq!(
        fs.vfs_setlk_as(None, ino, &r).await.unwrap(),
        LockAnswer::NotEnabled
    );
    assert_eq!(
        fs.vfs_getlk_as(None, ino, &r).await.unwrap(),
        LockAnswer::NotEnabled
    );
    // And nothing was recorded on the way to saying no.
    assert!(fs.posix_locks(ino).await.unwrap().is_empty());

    fs.set_posix_locks_enabled(true).await.unwrap();
    assert_eq!(
        fs.vfs_setlk_as(None, ino, &r).await.unwrap(),
        LockAnswer::Free
    );
}

// --- the point of the feature ---------------------------------------------

/// The headline property: a lock taken on one mount is seen by another.
///
/// Local kernel locking already handles one mount. If this test could pass with
/// per-process state, the feature would not be worth having.
#[tokio::test]
async fn a_lock_taken_on_one_mount_blocks_another() {
    let (a, b, ino) = two_mounts().await;

    let taken = a
        .vfs_setlk_as(None, ino, &req("o1", "mount-a", 0, 99, LockKind::Exclusive))
        .await
        .unwrap();
    assert_eq!(taken, LockAnswer::Free);

    // The second mount is a different process with its own owner ids.
    let blocked = b
        .vfs_setlk_as(
            None,
            ino,
            &req("o2", "mount-b", 50, 149, LockKind::Exclusive),
        )
        .await
        .unwrap();
    match blocked {
        LockAnswer::Held(l) => {
            assert_eq!(l.owner, "o1");
            assert_eq!((l.start, l.end), (0, 99));
            assert!(l.exclusive);
        }
        other => panic!("expected to be blocked, got {other:?}"),
    }

    // A range past the held one is free, so the refusal is about bytes and not
    // about the file.
    assert_eq!(
        b.vfs_setlk_as(
            None,
            ino,
            &req("o2", "mount-b", 100, 199, LockKind::Exclusive)
        )
        .await
        .unwrap(),
        LockAnswer::Free
    );
}

#[tokio::test]
async fn readers_share_and_a_writer_does_not() {
    let (a, b, ino) = two_mounts().await;
    for (fs, owner, holder) in [(&a, "r1", "mount-a"), (&b, "r2", "mount-b")] {
        assert_eq!(
            fs.vfs_setlk_as(None, ino, &req(owner, holder, 0, 99, LockKind::Shared))
                .await
                .unwrap(),
            LockAnswer::Free,
            "shared locks must coexist"
        );
    }
    assert!(matches!(
        b.vfs_setlk_as(None, ino, &req("w", "mount-b", 0, 99, LockKind::Exclusive))
            .await
            .unwrap(),
        LockAnswer::Held(_)
    ));
}

#[tokio::test]
async fn releasing_lets_the_next_owner_in() {
    let (a, b, ino) = two_mounts().await;
    a.vfs_setlk_as(None, ino, &req("o1", "mount-a", 0, 99, LockKind::Exclusive))
        .await
        .unwrap();
    a.vfs_setlk_as(None, ino, &req("o1", "mount-a", 0, 99, LockKind::Unlock))
        .await
        .unwrap();
    assert_eq!(
        b.vfs_setlk_as(None, ino, &req("o2", "mount-b", 0, 99, LockKind::Exclusive))
            .await
            .unwrap(),
        LockAnswer::Free
    );
}

/// A split decided by the pure resolver has to survive the round trip through SQL.
#[tokio::test]
async fn a_range_split_is_persisted() {
    let (fs, ino) = fixture().await;
    fs.vfs_setlk_as(None, ino, &req("o", "h", 0, 99, LockKind::Shared))
        .await
        .unwrap();
    fs.vfs_setlk_as(None, ino, &req("o", "h", 40, 59, LockKind::Exclusive))
        .await
        .unwrap();
    let mut held: Vec<(i64, i64, bool)> = fs
        .posix_locks(ino)
        .await
        .unwrap()
        .into_iter()
        .map(|l| (l.start, l.end, l.exclusive))
        .collect();
    held.sort();
    assert_eq!(held, vec![(0, 39, false), (40, 59, true), (60, 99, false)]);
}

// --- surviving a crash -----------------------------------------------------

/// A mount that dies must not hold a range forever.
///
/// Driven at the store, because the engine derives `expires_at` from its own clock
/// and this would otherwise be a test that sleeps for a minute.
#[tokio::test]
async fn an_expired_lease_stops_blocking() {
    // Through an `Fs` first, because that is what creates the schema; the store is
    // then driven directly so the lease timestamps can be chosen rather than waited
    // out — otherwise this test would sleep for a minute to prove one `WHERE`.
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta.clone(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;
    let dead = req("gone", "crashed-mount", 0, 99, LockKind::Exclusive);
    // Taken at t=1000, leased to t=1060.
    assert!(
        meta.apply_posix_lock(ino, &dead, 1060, 1000)
            .await
            .unwrap()
            .is_none()
    );
    // Still inside the lease: a live blocker.
    assert!(
        meta.apply_posix_lock(
            ino,
            &req("other", "mount-b", 0, 99, LockKind::Exclusive),
            1100,
            1040
        )
        .await
        .unwrap()
        .is_some()
    );
    // Past it: gone, and the range is takeable.
    assert!(
        meta.apply_posix_lock(
            ino,
            &req("other", "mount-b", 0, 99, LockKind::Exclusive),
            1200,
            1100
        )
        .await
        .unwrap()
        .is_none()
    );
    // The dead row was cleared rather than merely ignored.
    let owners: Vec<String> = meta
        .posix_locks(ino, 1100)
        .await
        .unwrap()
        .into_iter()
        .map(|l| l.owner)
        .collect();
    assert_eq!(owners, vec!["other".to_string()]);
}

/// A clean unmount drops its rows immediately rather than leaving them to expire.
#[tokio::test]
async fn unmounting_releases_that_mount_and_only_that_mount() {
    let (a, b, ino) = two_mounts().await;
    a.vfs_setlk_as(None, ino, &req("o1", "mount-a", 0, 49, LockKind::Exclusive))
        .await
        .unwrap();
    b.vfs_setlk_as(
        None,
        ino,
        &req("o2", "mount-b", 50, 99, LockKind::Exclusive),
    )
    .await
    .unwrap();

    assert_eq!(
        a.release_posix_locks_for_holder("mount-a").await.unwrap(),
        1
    );
    let left: Vec<PosixLock> = b.posix_locks(ino).await.unwrap();
    assert_eq!(left.len(), 1, "only the unmounted holder's rows go");
    assert_eq!(left[0].holder, "mount-b");
}

/// Renewal pushes the lease out, which is what keeps a long-held lock alive.
///
/// Driven at the store for the same reason as the expiry test: the engine derives
/// `expires_at` from its own clock, so a renewal it performed would be
/// indistinguishable from the original within the same second. Choosing the
/// timestamps is what makes the movement observable.
#[tokio::test]
async fn renewal_extends_the_lease() {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta.clone(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;

    let held = req("o", "mount-a", 0, 99, LockKind::Exclusive);
    meta.apply_posix_lock(ino, &held, 1060, 1000).await.unwrap();

    // Without a renewal this lock is gone by t=1500.
    assert_eq!(meta.renew_posix_lease("mount-a", 2000).await.unwrap(), 1);
    let live = meta.posix_locks(ino, 1500).await.unwrap();
    assert_eq!(live.len(), 1, "a renewed lease must still be live");
    assert_eq!(live[0].owner, "o");

    // Renewal is per holder: another mount's lease is untouched.
    assert_eq!(meta.renew_posix_lease("mount-b", 3000).await.unwrap(), 0);
    assert!(meta.posix_locks(ino, 2500).await.unwrap().is_empty());
}

// --- authorization ---------------------------------------------------------

/// An exclusive lock is a writer's claim, so it takes the write check; letting go
/// never does.
#[tokio::test]
async fn an_exclusive_lock_needs_write_and_releasing_never_does() {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.mkdir_p("/src").await.unwrap();
    fs.write("/src/secret.rs", b"x").await.unwrap();
    fs.set_posix_locks_enabled(true).await.unwrap();
    let ino = fs.stat("/src/secret.rs").await.unwrap().ino;

    let agent = fs.create_agent("claude", "opus", None).await.unwrap();
    let session = fs.create_session(agent, None).await.unwrap();
    fs.set_acl_default_deny(true).await.unwrap();
    let ctx = Some(WriteCtx::session(agent, session));

    let err = fs
        .vfs_setlk_as(ctx, ino, &req("o", "h", 0, 99, LockKind::Exclusive))
        .await
        .unwrap_err();
    assert!(
        matches!(err, OrigoFSError::Denied(_)),
        "expected Denied, got {err:?}"
    );

    // Releasing is never refused: an actor whose grant was revoked mid-flight must
    // still be able to let go, or the range stays stuck until the lease runs out.
    assert_eq!(
        fs.vfs_setlk_as(ctx, ino, &req("o", "h", 0, 99, LockKind::Unlock))
            .await
            .unwrap(),
        LockAnswer::Free
    );

    // With the grant, the same request is allowed.
    fs.grant(agent, "/src", Perms::READ | Perms::WRITE, None)
        .await
        .unwrap();
    assert_eq!(
        fs.vfs_setlk_as(ctx, ino, &req("o", "h", 0, 99, LockKind::Exclusive))
            .await
            .unwrap(),
        LockAnswer::Free
    );
}
