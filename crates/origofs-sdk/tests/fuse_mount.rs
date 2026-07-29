//! Mount an origofs workspace via FUSE and exercise it with ordinary `std::fs`
//! syscalls. Self-skips where a FUSE mount isn't possible (needs root + /dev/fuse).
#![cfg(all(unix, feature = "fuse"))]

use origofs_sdk::Workspace;
use origofs_sdk::fuse::{mountable, spawn};
use std::io::{Read, Seek, SeekFrom};
use std::time::{Duration, Instant};

/// The entry/attr TTL the mount hands the kernel (`fuse::TTL`).
///
/// Cached *names* become fresh again on their own after this, so a test that
/// only checked "the change eventually shows up" would pass with or without any
/// notification. The genuine regression guard here is the page cache, which no
/// TTL ever repairs — see [`fuse_mount_sees_remote_write`].
const MOUNT_TTL: Duration = Duration::from_secs(1);

/// How long to wait before giving up on a change becoming visible.
const GIVE_UP: Duration = Duration::from_secs(10);

#[test]
fn fuse_mount_read_write_rename_delete() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    // Build + seed the workspace on a throwaway runtime, then hand it to FUSE.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap();
        ws.write("/hello.txt", b"hi from origofs\n").await.unwrap();
        ws
    });
    drop(rt);

    let session = spawn(ws, &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300)); // let the mount settle

    // read a pre-existing file
    assert_eq!(
        std::fs::read(mnt.join("hello.txt")).unwrap(),
        b"hi from origofs\n"
    );

    // create + write + read back
    std::fs::write(mnt.join("new.txt"), b"written via fuse\n").unwrap();
    assert_eq!(
        std::fs::read(mnt.join("new.txt")).unwrap(),
        b"written via fuse\n"
    );

    // mkdir + nested write + readdir
    std::fs::create_dir(mnt.join("sub")).unwrap();
    std::fs::write(mnt.join("sub/a"), b"x").unwrap();
    let mut names: Vec<String> = std::fs::read_dir(&mnt)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["hello.txt", "new.txt", "sub"]);
    assert_eq!(std::fs::read(mnt.join("sub/a")).unwrap(), b"x");

    // truncate via set_len
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(mnt.join("new.txt"))
        .unwrap();
    f.set_len(3).unwrap();
    drop(f);
    assert_eq!(std::fs::read(mnt.join("new.txt")).unwrap(), b"wri");

    // rename + delete
    std::fs::rename(mnt.join("new.txt"), mnt.join("renamed.txt")).unwrap();
    assert!(mnt.join("renamed.txt").exists());
    assert!(!mnt.join("new.txt").exists());
    std::fs::remove_file(mnt.join("renamed.txt")).unwrap();
    assert!(!mnt.join("renamed.txt").exists());

    drop(session); // unmounts
}

/// A remote writer changing a file's *bytes* must reach a reader on the mount.
///
/// This is the case a TTL cannot cover: the file is held open, so reads are
/// served straight from the kernel's page cache, which is not on a timer at all.
/// The replacement content is deliberately the **same length** as the original,
/// so a size change can't smuggle the answer past a missing invalidation — only
/// a real `inval_inode` makes the new bytes visible.
#[test]
fn fuse_mount_sees_remote_write() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap();
        ws.write("/live.txt", b"AAAA").await.unwrap();
        ws
    });

    // The mount gets its own handle; `ws` here plays the *other* writer (a second
    // process, the HTTP API, an agent over MCP — the mount can't see their writes).
    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300)); // let the mount settle

    // Populate the page cache and keep the fd open: subsequent `pread`s are then
    // answered from cache without a revalidating lookup/getattr.
    let mut f = std::fs::File::open(mnt.join("live.txt")).unwrap();
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"AAAA");

    let wrote_at = Instant::now();
    rt.block_on(ws.write("/live.txt", b"BBBB")).unwrap();

    loop {
        f.seek(SeekFrom::Start(0)).unwrap();
        f.read_exact(&mut buf).unwrap();
        if &buf == b"BBBB" {
            break;
        }
        assert!(
            wrote_at.elapsed() < GIVE_UP,
            "mount kept serving stale bytes {:?} after a remote write",
            std::str::from_utf8(&buf),
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // Attributes too, and this one is timed: the kernel would refresh a cached
    // `stat` by itself once [`MOUNT_TTL`] lapsed, so seeing the new size well
    // inside that window can only be the notification's doing.
    assert_eq!(f.metadata().unwrap().len(), 4); // caches the attrs
    let grew_at = Instant::now();
    rt.block_on(ws.write("/live.txt", b"CCCCCCCC")).unwrap();
    while f.metadata().unwrap().len() != 8 {
        assert!(
            grew_at.elapsed() < GIVE_UP,
            "mount kept serving a stale size after a remote write"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let took = grew_at.elapsed();
    assert!(
        took < MOUNT_TTL * 3 / 4,
        "the new size only showed up after {took:?}, i.e. plausibly by the \
         {MOUNT_TTL:?} attribute TTL lapsing rather than by an invalidation"
    );

    drop(f); // an fd still open on the mount would make the unmount EBUSY
    drop(session);
}

/// A remote *create* and a remote *delete* must both become visible on the
/// mount, through `stat` and `readdir`.
///
/// Names, unlike bytes, are covered by [`MOUNT_TTL`] on their own — the mount
/// deliberately does not send the dentry-forgetting notification that would beat
/// it (`fuse::invalidate` documents the deadlock that ruled it out). So this
/// pins the *bounded* guarantee: the change lands within a small multiple of the
/// TTL, not eventually-or-never.
#[test]
fn fuse_mount_sees_remote_create_and_delete() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap();
        ws.write("/doomed.txt", b"here for now\n").await.unwrap();
        ws
    });

    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    // Prime the kernel's dentry + attr cache for the doomed path.
    assert!(mnt.join("doomed.txt").exists());

    let removed_at = Instant::now();
    rt.block_on(ws.remove("/doomed.txt")).unwrap();
    while mnt.join("doomed.txt").exists() {
        assert!(
            removed_at.elapsed() < GIVE_UP,
            "mount kept resolving a remotely deleted path"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let took = removed_at.elapsed();
    assert!(
        took < MOUNT_TTL * 3,
        "the deletion took {took:?} to become visible; the mount's entry TTL is \
         {MOUNT_TTL:?}, so this is unbounded staleness, not cache expiry"
    );

    // And a remote create becomes visible through both stat and readdir.
    let created_at = Instant::now();
    rt.block_on(ws.write("/remote.txt", b"from another writer\n"))
        .unwrap();
    while !mnt.join("remote.txt").exists() {
        assert!(
            created_at.elapsed() < GIVE_UP,
            "a remotely created file never appeared on the mount"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        std::fs::read(mnt.join("remote.txt")).unwrap(),
        b"from another writer\n"
    );
    let names: Vec<String> = std::fs::read_dir(&mnt)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, vec!["remote.txt".to_string()]);

    drop(session);
}

/// Unmounting must take the change-feed watcher with it — the guard's `Drop`
/// tears it down, so nothing keeps polling (or holding a `LISTEN` connection)
/// after the mount is gone.
#[test]
fn fuse_unmount_stops_the_watcher() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap();
        ws.write("/a.txt", b"a").await.unwrap();
        ws
    });

    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(std::fs::read(mnt.join("a.txt")).unwrap(), b"a");
    drop(session); // unmounts; the filesystem (and with it the watcher) is dropped

    // Give the session thread time to finish dropping the filesystem, then keep
    // writing: a leaked watcher would go on notifying a dead kernel channel. The
    // workspace must stay perfectly usable, and the process must not wedge.
    std::thread::sleep(Duration::from_millis(500));
    for i in 0..20 {
        rt.block_on(ws.write("/a.txt", format!("a{i}").as_bytes()))
            .unwrap();
    }
    assert_eq!(rt.block_on(ws.read("/a.txt")).unwrap().as_ref(), b"a19");
    // The mountpoint is a plain empty directory again.
    assert_eq!(std::fs::read_dir(&mnt).unwrap().count(), 0);
}

/// The same invalidation, over the **push** feed: a Postgres-backed workspace
/// takes the `subscribe` (`LISTEN/NOTIFY`) branch rather than polling.
/// Self-skips unless `ORIGOFS_PG_TEST_URL` points at a reachable database.
///
/// The Postgres test database is shared between tests, so this only touches a
/// per-run-unique path and never asserts on directory listings.
#[test]
fn fuse_mount_sees_remote_write_over_postgres() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let Ok(dsn) = std::env::var("ORIGOFS_PG_TEST_URL") else {
        eprintln!("skipping: ORIGOFS_PG_TEST_URL unset");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let tag = format!(
        "fuse-notify-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let path = format!("/{tag}.txt");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        let ws = Workspace::open_pg(&dsn, std::sync::Arc::new(origofs_sdk::MemStore::new()))
            .await
            .unwrap();
        ws.write(&path, b"AAAA").await.unwrap();
        ws
    });
    assert!(ws.is_postgres(), "expected the push-feed backend");

    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    let mut f = std::fs::File::open(mnt.join(format!("{tag}.txt"))).unwrap();
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"AAAA");

    let wrote_at = Instant::now();
    rt.block_on(ws.write(&path, b"BBBB")).unwrap();
    loop {
        f.seek(SeekFrom::Start(0)).unwrap();
        f.read_exact(&mut buf).unwrap();
        if &buf == b"BBBB" {
            break;
        }
        assert!(
            wrote_at.elapsed() < GIVE_UP,
            "mount kept serving stale bytes {:?} after a remote write",
            std::str::from_utf8(&buf),
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    drop(f); // an fd still open on the mount would make the unmount EBUSY
    drop(session);
    let _ = rt.block_on(ws.remove(&path));
}
