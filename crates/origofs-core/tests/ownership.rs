//! POSIX ownership and mode changes (issues #121, #122).
//!
//! Two gaps, closed together because they are the same code path.
//!
//! **#121 — `chmod` silently succeeded and did nothing.** Both mounts accepted a
//! mode change and discarded it, then reported success: FUSE's `setattr` bound
//! `mode`/`uid`/`gid` with leading underscores and replied with freshly-read
//! (unchanged) attributes, and NFS documented the no-op in a comment. A script
//! running `chmod +x build.sh` and checking the return code proceeded on a false
//! premise. It also persisted: mode is encoded into committed tree objects and
//! read by git export to set the exec bit, so the mode a file happened to be
//! *created* with was the mode it carried into history and out through
//! `git clone origofs://…`, with no way to correct it.
//!
//! **#122 — there was no ownership at all.** No `uid`, no `gid`, on `Inode` or in
//! any migration, so both mounts hardcoded `uid: 0, gid: 0`. That was coherent
//! rather than broken — the FUSE mount sets `DefaultPermissions`, asking the
//! kernel to run real POSIX checks against what origofs reports, and
//! `fuse_mountable()` requires root, so every check passed — but it is exactly why
//! `allow_other` and non-root mounts could not work.
//!
//! These buy no authorization on their own; see `docs/PERMISSIONS.md` §2 for why a
//! uid must not become the principal. They stop the mount lying.

use origofs_core::{
    FileKind, Fs, INO_ROOT, MemStore, MetadataStore, Owner, SqliteMetadataStore, VersioningMode,
};
use std::sync::Arc;

async fn fixture() -> Fs<Arc<dyn MetadataStore>, Arc<MemStore>> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

/// The headline of #121: a mode change sticks, and is visible on the next read.
///
/// Fails against the old behaviour, where the mode never moved.
#[tokio::test]
async fn chmod_actually_changes_the_mode() {
    let fs = fixture().await;
    let f = fs
        .vfs_create_unchecked(INO_ROOT, "build.sh", 0o644, Owner::ROOT)
        .await
        .unwrap();
    assert_eq!(f.mode & 0o7777, 0o644, "created with the requested mode");

    // `chmod +x`
    let after = fs.vfs_chmod_unchecked(f.ino, 0o755).await.unwrap();
    assert_eq!(
        after.mode & 0o7777,
        0o755,
        "chmod must change the mode, not report success and discard it"
    );

    // And it is durable, not just reflected in the returned value.
    let reread = fs.vfs_getattr_unchecked(f.ino).await.unwrap();
    assert_eq!(reread.mode & 0o7777, 0o755, "mode did not persist");
}

/// `chmod` changes only the permission bits — the format bits are the inode's
/// kind, and a caller must not be able to turn a file into a directory by
/// passing a mode word with different type bits.
#[tokio::test]
async fn chmod_cannot_rewrite_the_file_type() {
    let fs = fixture().await;
    let d = fs
        .vfs_mkdir_unchecked(INO_ROOT, "sub", 0o755, Owner::ROOT)
        .await
        .unwrap();
    let before_type = d.mode & !0o7777;

    // A whole mode word claiming S_IFREG. Only the low bits may land.
    let after = fs.vfs_chmod_unchecked(d.ino, 0o100600).await.unwrap();

    assert_eq!(
        after.mode & !0o7777,
        before_type,
        "chmod rewrote the file-type bits"
    );
    assert_eq!(after.mode & 0o7777, 0o600, "permission bits did not apply");
    assert_eq!(after.kind, FileKind::Dir, "the inode is still a directory");
}

/// setuid/setgid/sticky are permission bits and must survive a chmod that sets
/// them — masking with `0o777` instead of `0o7777` would silently drop them.
#[tokio::test]
async fn chmod_preserves_the_high_permission_bits() {
    let fs = fixture().await;
    let f = fs
        .vfs_create_unchecked(INO_ROOT, "helper", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let after = fs.vfs_chmod_unchecked(f.ino, 0o4755).await.unwrap();
    assert_eq!(
        after.mode & 0o7777,
        0o4755,
        "setuid bit was dropped; the mask must be 0o7777, not 0o777"
    );
}

/// A chmod of something that does not exist is an error, not a silent no-op.
///
/// The whole point of #121 is that silence is worse than the gap, so the fix must
/// not introduce a new silence of its own: a zero-row `UPDATE` reported as success
/// would be the same bug in a different place.
#[tokio::test]
async fn chmod_of_a_missing_inode_is_an_error() {
    let fs = fixture().await;
    assert!(
        fs.vfs_chmod_unchecked(999_999, 0o755).await.is_err(),
        "chmod of a nonexistent inode must fail, not silently succeed"
    );
    assert!(
        fs.vfs_chown_unchecked(999_999, Some(1), Some(1))
            .await
            .is_err(),
        "chown of a nonexistent inode must fail, not silently succeed"
    );
}

/// The headline of #122: an inode carries a real uid/gid, and reports it.
#[tokio::test]
async fn inodes_carry_ownership() {
    let fs = fixture().await;
    let f = fs
        .vfs_create_unchecked(INO_ROOT, "owned.txt", 0o644, Owner::new(1000, 2000))
        .await
        .unwrap();
    assert_eq!(
        (f.uid, f.gid),
        (1000, 2000),
        "create did not record ownership"
    );
    assert_eq!(f.owner(), Owner::new(1000, 2000));

    let reread = fs.vfs_getattr_unchecked(f.ino).await.unwrap();
    assert_eq!(
        (reread.uid, reread.gid),
        (1000, 2000),
        "ownership did not persist"
    );
}

/// Ownership defaults to root, so a workspace that predates the migration — and
/// every inode the internal machinery creates — reports exactly what it did
/// before. This is what makes V17 behaviour-preserving.
#[tokio::test]
async fn ownership_defaults_to_root() {
    let fs = fixture().await;
    // The path API (not a mount) has no requesting process, so it creates as root.
    fs.write("/plain.txt", b"hi").await.unwrap();
    let i = fs.stat("/plain.txt").await.unwrap();
    assert_eq!(
        (i.uid, i.gid),
        (0, 0),
        "a non-mount write must still create root-owned inodes"
    );

    // As does the root directory itself.
    let root = fs.vfs_getattr_unchecked(INO_ROOT).await.unwrap();
    assert_eq!((root.uid, root.gid), (0, 0));
}

/// `chown` sets both halves, and either half alone — `None` is chown(2)'s `-1`
/// ("leave this one alone"), which is how `chgrp` and a uid-only `chown` arrive.
#[tokio::test]
async fn chown_sets_each_half_independently() {
    let fs = fixture().await;
    let f = fs
        .vfs_create_unchecked(INO_ROOT, "f", 0o644, Owner::new(1, 2))
        .await
        .unwrap();

    // Both.
    let a = fs
        .vfs_chown_unchecked(f.ino, Some(10), Some(20))
        .await
        .unwrap();
    assert_eq!((a.uid, a.gid), (10, 20));

    // uid only — gid must not move.
    let b = fs.vfs_chown_unchecked(f.ino, Some(11), None).await.unwrap();
    assert_eq!(
        (b.uid, b.gid),
        (11, 20),
        "a uid-only chown must leave the gid alone"
    );

    // gid only — uid must not move (the `chgrp` case).
    let c = fs.vfs_chown_unchecked(f.ino, None, Some(21)).await.unwrap();
    assert_eq!(
        (c.uid, c.gid),
        (11, 21),
        "a gid-only chown must leave the uid alone"
    );

    // Neither: a no-op, not a reset to zero.
    let d = fs.vfs_chown_unchecked(f.ino, None, None).await.unwrap();
    assert_eq!(
        (d.uid, d.gid),
        (11, 21),
        "chown(None, None) must change nothing"
    );
}

/// Mode and ownership are independent: changing one must not disturb the other.
/// Both are written by the same `setattr` on a mount, so a shared-UPDATE
/// implementation could easily clobber one with a stale read of the other.
#[tokio::test]
async fn mode_and_ownership_do_not_clobber_each_other() {
    let fs = fixture().await;
    let f = fs
        .vfs_create_unchecked(INO_ROOT, "f", 0o600, Owner::new(5, 6))
        .await
        .unwrap();

    let after_chmod = fs.vfs_chmod_unchecked(f.ino, 0o755).await.unwrap();
    assert_eq!(
        (after_chmod.uid, after_chmod.gid),
        (5, 6),
        "chmod reset the ownership"
    );

    let after_chown = fs
        .vfs_chown_unchecked(f.ino, Some(7), Some(8))
        .await
        .unwrap();
    assert_eq!(after_chown.mode & 0o7777, 0o755, "chown reset the mode");
}

/// Both apply to directories and symlinks, not only regular files.
#[tokio::test]
async fn ownership_and_mode_apply_to_every_kind() {
    let fs = fixture().await;
    let d = fs
        .vfs_mkdir_unchecked(INO_ROOT, "d", 0o700, Owner::new(3, 4))
        .await
        .unwrap();
    assert_eq!((d.uid, d.gid), (3, 4), "mkdir did not record ownership");
    assert_eq!(
        fs.vfs_chmod_unchecked(d.ino, 0o750).await.unwrap().mode & 0o7777,
        0o750
    );

    let l = fs
        .vfs_symlink_unchecked(INO_ROOT, "l", "/d", Owner::new(3, 4))
        .await
        .unwrap();
    assert_eq!((l.uid, l.gid), (3, 4), "symlink did not record ownership");
    assert_eq!(
        fs.vfs_chown_unchecked(l.ino, Some(9), None)
            .await
            .unwrap()
            .uid,
        9,
        "chown must work on a symlink inode"
    );
}

/// Ownership is **working-tree state and is deliberately not committed**, which is
/// the one place mode and ownership diverge.
///
/// `TreeEntry` carries `mode` and nothing else, so a checkout rebuilds inodes as
/// root-owned. That is intentional and matches git, which tracks the exec bit and
/// refuses to track ownership — a uid is meaningful only on the machine that
/// issued it, so baking one into a shared commit would hand every other checkout a
/// number that means something different or nothing at all. It also keeps the
/// `git` versioning mode honest, since a real git tree has nowhere to put it.
///
/// Pinned as a test rather than left implicit: without this, someone reading #122
/// could reasonably "fix" the asymmetry by widening the tree object format, and
/// that is a format change made for the wrong reason.
#[tokio::test]
async fn ownership_is_not_carried_through_a_commit() {
    let fs = fixture().await;
    fs.set_versioning_mode(VersioningMode::Native)
        .await
        .unwrap();
    fs.write("/f.txt", b"x").await.unwrap();
    let ino = fs.stat("/f.txt").await.unwrap().ino;
    fs.vfs_chown_unchecked(ino, Some(1000), Some(1000))
        .await
        .unwrap();
    fs.vfs_chmod_unchecked(ino, 0o755).await.unwrap();
    fs.commit("tester", "own it").await.unwrap();

    fs.create_branch("side").await.unwrap();
    fs.checkout("side").await.unwrap();
    fs.checkout("main").await.unwrap();

    let after = fs.stat("/f.txt").await.unwrap();
    assert_eq!(
        (after.uid, after.gid),
        (0, 0),
        "ownership is working-tree state; a checkout rebuilds inodes root-owned"
    );
    assert_eq!(
        after.mode & 0o7777,
        0o755,
        "mode, unlike ownership, *is* committed"
    );
}

/// A mode change survives a commit and reaches the committed tree.
///
/// This is the half of #121 that outlives the working tree: `TreeEntry.mode` is
/// what git export reads to set the exec bit, so a `chmod +x` that did not reach a
/// commit would still leave `git clone origofs://…` handing out a non-executable
/// script.
#[tokio::test]
async fn a_mode_change_reaches_the_committed_tree() {
    let fs = fixture().await;
    fs.set_versioning_mode(VersioningMode::Native)
        .await
        .unwrap();
    fs.write("/build.sh", b"#!/bin/sh\necho hi\n")
        .await
        .unwrap();

    let ino = fs.stat("/build.sh").await.unwrap().ino;
    fs.vfs_chmod_unchecked(ino, 0o755).await.unwrap();
    fs.commit("tester", "make it executable").await.unwrap();

    // Round-trip through the object graph rather than just re-reading the working
    // tree — committing does not touch the working-tree inode, so asserting on it
    // here would pass even if `TreeEntry.mode` never carried the change. `checkout`
    // truncates the tree and rebuilds every inode from the committed tree objects,
    // so what survives this is what the commit actually holds.
    fs.create_branch("elsewhere").await.unwrap();
    fs.checkout("elsewhere").await.unwrap();
    fs.checkout("main").await.unwrap();

    let after = fs.stat("/build.sh").await.unwrap();
    assert_eq!(
        after.mode & 0o7777,
        0o755,
        "the executable bit did not survive into the commit"
    );
}
