//! The mount guard: the watcher stops **before** the unmount (issue #75).
//!
//! # What this is really testing
//!
//! The change-feed watcher issues kernel notifications, and a notification is only
//! safe while the session can still answer requests. The watcher used to be owned
//! by the filesystem, which the session owned, so it was stopped *by* the unmount —
//! that is, after it. The window between "session torn down" and "watcher noticed"
//! is exactly where a notification has nobody left to answer it.
//!
//! That ordering is the prerequisite #75 names for issuing
//! `FUSE_NOTIFY_INVAL_ENTRY`, which takes the parent directory's `i_rwsem`
//! exclusively and parks in uninterruptible `D` state if the mount cannot answer —
//! a state that survives `SIGKILL` and leaves the mount behind. An earlier revision
//! that issued it wedged the whole process roughly one run in eight.
//!
//! So the loops below are deliberately hostile: mount, generate concurrent traffic
//! from both sides, and tear down *while it is still going*, repeatedly. A
//! regression here does not look like a failed assertion — it looks like the test
//! binary hanging forever in `D` state. `mount_teardown_under_traffic_does_not_hang`
//! therefore watches a worker thread from a **timeout thread**, so a hang is
//! reported as a failure instead of pinning CI until it is killed.
#![cfg(all(unix, feature = "fuse"))]

use origofs_sdk::Workspace;
use origofs_sdk::fuse::{mountable, spawn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Generous enough that a slow runner is not mistaken for a deadlock, short
/// enough that a real one is reported rather than hanging the suite.
const HANG_BUDGET: Duration = Duration::from_secs(90);

/// How many mount/teardown cycles to run. The historical failure showed at
/// roughly one run in eight, and the fix was validated over 20 consecutive runs,
/// so this matches that bar rather than guessing at a lower one.
const CYCLES: usize = 20;

fn workspace(dir: &std::path::Path) -> Workspace {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        Workspace::open_local(dir.join("meta.db"), dir.join("cas"))
            .await
            .unwrap()
    });
    drop(rt);
    ws
}

/// A mount can be torn down while both the kernel and a remote writer are
/// actively working it, twenty times in a row, without wedging.
///
/// The traffic matters: a teardown of an idle mount exercises nothing. Each cycle
/// keeps a reader hammering `readdir`/`stat`/`read` through the kernel while a
/// second thread makes *namespace* changes through the `Workspace` API — creates,
/// renames and deletes, which are precisely the events that drive dentry
/// invalidation — and then drops the guard mid-flight.
#[test]
fn mount_teardown_under_traffic_does_not_hang() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }

    let finished = Arc::new(AtomicBool::new(false));
    let done = Arc::clone(&finished);

    let worker = std::thread::spawn(move || {
        for cycle in 0..CYCLES {
            let dir = tempfile::tempdir().unwrap();
            let mnt = dir.path().join("mnt");
            std::fs::create_dir_all(&mnt).unwrap();
            let ws = workspace(dir.path());

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                ws.mkdir_p("/d").await.unwrap();
                for i in 0..8 {
                    ws.write(&format!("/d/f{i}.txt"), b"seed").await.unwrap();
                }
            });

            let mut mount = spawn(ws.clone(), &mnt).expect("mount");

            let stop = Arc::new(AtomicBool::new(false));

            // Kernel-side traffic: the syscalls that hold the parent directory's
            // lock while waiting for the mount to answer.
            let reader_stop = Arc::clone(&stop);
            let rmnt = mnt.clone();
            let reader = std::thread::spawn(move || {
                while !reader_stop.load(Ordering::Relaxed) {
                    if let Ok(entries) = std::fs::read_dir(rmnt.join("d")) {
                        for e in entries.flatten() {
                            let _ = std::fs::metadata(e.path());
                            let _ = std::fs::read(e.path());
                        }
                    }
                }
            });

            // Remote-side traffic: namespace churn, which is what produces the
            // events that would drive `inval_entry`.
            let writer_stop = Arc::clone(&stop);
            let wws = ws.clone();
            let writer = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let mut n = 0u32;
                while !writer_stop.load(Ordering::Relaxed) {
                    n += 1;
                    let p = format!("/d/remote{n}.txt");
                    rt.block_on(async {
                        let _ = wws.write(&p, b"remote").await;
                        let _ = wws.rename(&p, &format!("/d/moved{n}.txt")).await;
                        let _ = wws.remove(&format!("/d/moved{n}.txt")).await;
                    });
                }
            });

            // Let both sides get going, then tear down mid-flight. This is the
            // moment the old ordering could strand a notification.
            std::thread::sleep(Duration::from_millis(120));
            mount.unmount();

            stop.store(true, Ordering::Relaxed);
            reader.join().expect("reader thread");
            writer.join().expect("writer thread");
            eprintln!("teardown cycle {} of {CYCLES} clean", cycle + 1);
        }
        done.store(true, Ordering::Relaxed);
    });

    // A hang here is `D` state, which no signal clears — so report it as a
    // failure from a thread that is still alive rather than waiting forever.
    let deadline = std::time::Instant::now() + HANG_BUDGET;
    while std::time::Instant::now() < deadline {
        if finished.load(Ordering::Relaxed) {
            worker.join().expect("worker thread");
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "mount teardown did not finish {CYCLES} cycles within {HANG_BUDGET:?}; a kernel \
         notification is very likely parked in D state waiting for a session that is \
         already gone — the exact hazard the teardown ordering exists to prevent"
    );
}

/// `unmount` is idempotent, and dropping afterwards is not a second unmount.
///
/// Both `Mount::unmount` and `Drop` stop the watcher and drop the session, so a
/// caller that does both must not double-stop or double-unmount.
#[test]
fn unmount_is_idempotent() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();
    let ws = workspace(dir.path());

    let mut mount = spawn(ws, &mnt).expect("mount");
    mount.unmount();
    mount.unmount();
    drop(mount); // and Drop runs it a third time
}

/// The mount still works normally with the guard in place — the ordering change
/// must not have cost the invalidation it exists to protect.
///
/// This is the property `fuse_mount.rs` already covers in depth; repeated here
/// only to catch a guard that "fixes" teardown by never starting the watcher.
#[test]
fn the_watcher_still_runs_under_the_guard() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();
    let ws = workspace(dir.path());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { ws.write("/f.txt", b"first").await.unwrap() });

    let _mount = spawn(ws.clone(), &mnt).expect("mount");
    // Prime the page cache.
    assert_eq!(std::fs::read(mnt.join("f.txt")).unwrap(), b"first");

    // A remote write. Only the watcher's `inval_inode` makes this visible — no
    // TTL repairs a stale page cache.
    rt.block_on(async { ws.write("/f.txt", b"second").await.unwrap() });

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::read(mnt.join("f.txt")).unwrap() == b"second" {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the mount never saw the remote write; the guard appears to have \
             stopped the watcher from running at all"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The same teardown stress, with dentry invalidation **on**.
///
/// This is the case the guard exists to make safe, and the one that historically
/// wedged: `FUSE_NOTIFY_INVAL_ENTRY` takes the parent's `i_rwsem` exclusively, so
/// a notification stranded by a torn-down session parks in `D` state forever.
///
/// It runs under `ORIGOFS_FUSE_INVAL_ENTRY`, which is read once into a
/// `OnceLock`, so it is spawned as a **subprocess** rather than set in-process —
/// setting it here would race every sibling test in this binary, and the value
/// would leak into whichever test happened to touch the lock first.
///
/// Ignored by default: it is a kernel-deadlock probe whose failure mode is a hang
/// rather than an assertion, and the knob it exercises is off by default. Run it
/// deliberately, on the kernel you care about, when gathering the evidence that
/// would justify flipping that default:
///
/// ```text
/// cargo test -p origofs-sdk --features full --test fuse_teardown -- --ignored
/// ```
#[test]
#[ignore = "kernel-deadlock probe for ORIGOFS_FUSE_INVAL_ENTRY; run deliberately (see #75)"]
fn teardown_with_dentry_invalidation_does_not_hang() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let exe = std::env::current_exe().expect("test binary path");
    let out = std::process::Command::new(exe)
        .args([
            "mount_teardown_under_traffic_does_not_hang",
            "--exact",
            "--nocapture",
        ])
        .env("ORIGOFS_FUSE_INVAL_ENTRY", "1")
        .output()
        .expect("re-run this binary with dentry invalidation on");

    assert!(
        out.status.success(),
        "teardown under traffic failed with ORIGOFS_FUSE_INVAL_ENTRY=1:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
