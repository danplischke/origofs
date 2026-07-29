//! Mount an origofs workspace via FUSE and exercise it with ordinary `std::fs`
//! syscalls. Self-skips where a FUSE mount isn't possible (needs root + /dev/fuse).
#![cfg(all(unix, feature = "fuse"))]

use origofs_sdk::Workspace;
use origofs_sdk::fuse::{mountable, spawn};
use std::time::Duration;

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
