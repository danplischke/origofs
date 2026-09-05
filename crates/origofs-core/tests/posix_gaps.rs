//! Hard links, extended attributes, `statfs`, `du`, and quotas (issues #119, #116).
//!
//! #119 was a checklist of POSIX holes that were **absent** rather than stubbed, so
//! each failed confusingly instead of cleanly: no `link` op at all (though `nlink`,
//! its column, and the whole decrement path already existed — `adjust_nlink` had
//! only ever been called with `-1`), no `statfs` anywhere in the tree, and no
//! xattrs outside `sandbox.rs`'s overlayfs whiteout detection.
//!
//! #116 was the accounting underneath: `child_count` was the only per-directory
//! stat, with no recursive stats, no `du`, and no quota — and `statfs` needs
//! something to report, which is why the two landed together.

use origofs_core::{
    Fs, INO_ROOT, MAX_XATTR_LEN, MemStore, MetadataStore, NamespaceStore, OrigoFSError, Quota,
    SqliteMetadataStore,
};
use std::sync::Arc;

async fn fixture() -> Fs<Arc<dyn MetadataStore>, Arc<MemStore>> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

// --- hard links ---------------------------------------------------------

/// The headline of the `link` half of #119: a second name for one inode, with
/// `nlink` raised. This is the first caller ever to pass `adjust_nlink` a positive
/// delta.
#[tokio::test]
async fn a_hard_link_gives_one_inode_two_names() {
    let fs = fixture().await;
    fs.write("/a.txt", b"shared").await.unwrap();
    let a = fs.stat("/a.txt").await.unwrap();
    assert_eq!(a.nlink, 1);

    let linked = fs
        .vfs_link_unchecked(a.ino, INO_ROOT, "b.txt")
        .await
        .unwrap();
    assert_eq!(linked.nlink, 2, "link must raise nlink");

    let b = fs.stat("/b.txt").await.unwrap();
    assert_eq!(b.ino, a.ino, "both names must resolve to one inode");
    assert_eq!(&fs.read("/b.txt").await.unwrap()[..], b"shared");
}

/// Unlinking one name of a linked pair keeps the file; unlinking the last removes
/// it. This is the property that makes the increment side worth having — without
/// it the first `rm` would destroy content still reachable by another name.
#[tokio::test]
async fn unlinking_one_name_of_a_link_keeps_the_file() {
    let fs = fixture().await;
    fs.write("/a.txt", b"shared").await.unwrap();
    let ino = fs.stat("/a.txt").await.unwrap().ino;
    fs.vfs_link_unchecked(ino, INO_ROOT, "b.txt").await.unwrap();

    fs.vfs_unlink_unchecked(INO_ROOT, "a.txt").await.unwrap();
    assert!(fs.stat("/a.txt").await.is_err(), "the name is gone");
    assert_eq!(
        &fs.read("/b.txt").await.unwrap()[..],
        b"shared",
        "the content must survive while another name holds it"
    );
    assert_eq!(
        fs.stat("/b.txt").await.unwrap().nlink,
        1,
        "nlink came back down"
    );

    fs.vfs_unlink_unchecked(INO_ROOT, "b.txt").await.unwrap();
    assert!(
        fs.vfs_getattr_unchecked(ino).await.is_err(),
        "the last unlink must remove the inode"
    );
}

/// A write through one name is visible through the other — they are one file, not
/// a copy.
#[tokio::test]
async fn a_hard_link_shares_content() {
    let fs = fixture().await;
    fs.write("/a.txt", b"one").await.unwrap();
    let ino = fs.stat("/a.txt").await.unwrap().ino;
    fs.vfs_link_unchecked(ino, INO_ROOT, "b.txt").await.unwrap();

    fs.write("/a.txt", b"two").await.unwrap();
    assert_eq!(
        &fs.read("/b.txt").await.unwrap()[..],
        b"two",
        "a hard link must observe writes through the other name"
    );
}

/// POSIX forbids hard links to directories, and so must this: a directory link
/// would let the dentry graph form a cycle unreachable from the root, which
/// nothing here — gc, commit, the recursive walks — is written to survive.
#[tokio::test]
async fn hard_links_to_directories_are_refused() {
    let fs = fixture().await;
    fs.mkdir_p("/d").await.unwrap();
    let d = fs.stat("/d").await.unwrap();
    assert!(
        matches!(
            fs.vfs_link_unchecked(d.ino, INO_ROOT, "d2").await,
            Err(OrigoFSError::Denied(_))
        ),
        "a hard link to a directory must be refused"
    );
}

/// A link onto an existing name is `EEXIST`, and a poisoned name is rejected by
/// the same `validate_component` rule every other inode-oriented op enforces.
#[tokio::test]
async fn link_rejects_a_taken_or_poisoned_name() {
    let fs = fixture().await;
    fs.write("/a.txt", b"x").await.unwrap();
    fs.write("/taken.txt", b"y").await.unwrap();
    let ino = fs.stat("/a.txt").await.unwrap().ino;

    assert!(
        matches!(
            fs.vfs_link_unchecked(ino, INO_ROOT, "taken.txt").await,
            Err(OrigoFSError::AlreadyExists(_))
        ),
        "linking onto an existing name must not silently replace it"
    );
    for bad in ["..", ".", "", "a/b", "a\0b"] {
        assert!(
            fs.vfs_link_unchecked(ino, INO_ROOT, bad).await.is_err(),
            "vfs_link must reject the component {bad:?}"
        );
    }
}

// --- extended attributes ------------------------------------------------

/// Set, get, list, remove — the whole xattr surface, on one file.
#[tokio::test]
async fn xattrs_round_trip() {
    let fs = fixture().await;
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;

    assert_eq!(
        fs.vfs_listxattr_unchecked(ino).await.unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        fs.vfs_getxattr_unchecked(ino, "user.tag").await.unwrap(),
        None
    );

    fs.vfs_setxattr_unchecked(ino, "user.tag", b"blue")
        .await
        .unwrap();
    fs.vfs_setxattr_unchecked(ino, "user.other", b"green")
        .await
        .unwrap();

    assert_eq!(
        fs.vfs_getxattr_unchecked(ino, "user.tag")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"blue"[..])
    );
    // Listed in name order, so a caller's output is stable.
    assert_eq!(
        fs.vfs_listxattr_unchecked(ino).await.unwrap(),
        vec!["user.other".to_string(), "user.tag".to_string()]
    );

    // Setting an existing name replaces rather than duplicating.
    fs.vfs_setxattr_unchecked(ino, "user.tag", b"red")
        .await
        .unwrap();
    assert_eq!(
        fs.vfs_getxattr_unchecked(ino, "user.tag")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"red"[..])
    );
    assert_eq!(fs.vfs_listxattr_unchecked(ino).await.unwrap().len(), 2);

    assert!(fs.vfs_removexattr_unchecked(ino, "user.tag").await.unwrap());
    assert_eq!(
        fs.vfs_getxattr_unchecked(ino, "user.tag").await.unwrap(),
        None
    );
    assert_eq!(
        fs.vfs_listxattr_unchecked(ino).await.unwrap(),
        vec!["user.other"]
    );
}

/// Removing a name that was never set reports `false`, which is what lets the FUSE
/// surface answer `ENODATA` instead of reporting success for a removal that
/// removed nothing.
#[tokio::test]
async fn removing_an_absent_xattr_reports_that_it_was_absent() {
    let fs = fixture().await;
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;
    assert!(
        !fs.vfs_removexattr_unchecked(ino, "user.nope")
            .await
            .unwrap(),
        "removal of an unset name must be distinguishable from a real one"
    );
}

/// Xattrs are per-inode, not per-content: two files with identical bytes dedup to
/// one manifest, and must **not** share attributes. An xattr describes this file
/// at this path — a label, a resource fork — not the bytes.
#[tokio::test]
async fn xattrs_are_per_inode_not_per_content() {
    let fs = fixture().await;
    fs.write("/a", b"identical").await.unwrap();
    fs.write("/b", b"identical").await.unwrap();
    let a = fs.stat("/a").await.unwrap();
    let b = fs.stat("/b").await.unwrap();
    assert_eq!(a.content, b.content, "the two files should dedup");
    assert_ne!(a.ino, b.ino);

    fs.vfs_setxattr_unchecked(a.ino, "user.tag", b"only-a")
        .await
        .unwrap();
    assert_eq!(
        fs.vfs_getxattr_unchecked(b.ino, "user.tag").await.unwrap(),
        None,
        "deduplicated content must not share extended attributes"
    );
}

/// An oversized value is refused. This is the metadata/content split being
/// enforced: without the cap, `setfattr` would be a supported way to write
/// unbounded, un-deduplicated, un-chunked bytes straight into the metadata DB.
#[tokio::test]
async fn an_oversized_xattr_is_refused() {
    let fs = fixture().await;
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;

    // At the limit: fine.
    let ok = vec![b'a'; MAX_XATTR_LEN];
    fs.vfs_setxattr_unchecked(ino, "user.big", &ok)
        .await
        .unwrap();

    // One byte over: refused, and as TooLarge rather than a backend error.
    let too_big = vec![b'a'; MAX_XATTR_LEN + 1];
    assert!(
        matches!(
            fs.vfs_setxattr_unchecked(ino, "user.toobig", &too_big)
                .await,
            Err(OrigoFSError::TooLarge(_))
        ),
        "an xattr past the cap must be refused; the metadata DB never holds large bytes"
    );
}

/// Xattrs die with their inode. They are keyed by inode number, and inode numbers
/// are reused from a sequence — so a leaked row would eventually reattach itself
/// to an unrelated future file.
#[tokio::test]
async fn xattrs_are_removed_with_their_inode() {
    // Hold the store directly: this test needs to look for a leaked row *behind*
    // the engine, which checks the inode exists first.
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta.clone(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;
    fs.vfs_setxattr_unchecked(ino, "user.tag", b"v")
        .await
        .unwrap();

    fs.remove("/f").await.unwrap();

    // Read straight through the store: the engine's accessors check the inode
    // exists first, which would mask a leaked row rather than reveal it.
    assert_eq!(
        meta.get_xattr(ino, "user.tag").await.unwrap(),
        None,
        "an xattr outlived its inode; inode ids are reused, so it would resurface \
         attached to an unrelated file"
    );
}

// --- statfs / du / quota ------------------------------------------------

/// `statfs` answers with real numbers, and the used figure tracks actual content.
#[tokio::test]
async fn statfs_reports_real_usage() {
    let fs = fixture().await;
    let before = fs.statfs().await.unwrap();
    assert_eq!(before.block_size, 4096);
    assert!(
        before.free_blocks > 0,
        "an empty workspace must not look full; `df` showing 100% makes some \
         installers refuse to run"
    );

    fs.write("/big", &vec![b'x'; 100_000]).await.unwrap();
    let after = fs.statfs().await.unwrap();
    assert!(
        after.free_blocks < before.free_blocks,
        "writing 100 KB must consume free space ({} -> {})",
        before.free_blocks,
        after.free_blocks
    );
    assert!(
        after.free_inodes < before.free_inodes,
        "creating a file must consume an inode"
    );
}

/// Free space must keep *moving* as the workspace grows, in both axes.
///
/// Guarding a specific mistake this made twice while being written. With no quota
/// there is no real capacity to report, so the total is synthesized — and the
/// obvious synthesis, `used + headroom`, makes free a constant: `df` then shows
/// the same free space forever no matter how much is written, which is worse than
/// the missing `statfs` it replaced, because it looks like it works. The inode axis
/// then reintroduced it separately, by sharing a byte-sized headroom constant that
/// dwarfed the inode nominal and pushed that axis permanently onto the same branch.
#[tokio::test]
async fn free_space_keeps_moving_as_the_workspace_grows() {
    let fs = fixture().await;
    let mut last = fs.statfs().await.unwrap();
    for i in 0..4 {
        fs.write(&format!("/f{i}"), &vec![b'x'; 200_000])
            .await
            .unwrap();
        let now = fs.statfs().await.unwrap();
        assert!(
            now.free_blocks < last.free_blocks,
            "free blocks stalled at {} after write {i}; the synthesized total must \
             not track usage 1:1",
            now.free_blocks
        );
        assert!(
            now.free_inodes < last.free_inodes,
            "free inodes stalled at {} after write {i}",
            now.free_inodes
        );
        last = now;
    }
}

/// `usage` and `du` agree with what was written, and `du` is scoped to its subtree.
#[tokio::test]
async fn du_measures_a_subtree() {
    let fs = fixture().await;
    fs.mkdir_p("/proj/src").await.unwrap();
    fs.write("/proj/src/a", &vec![b'a'; 1000]).await.unwrap();
    fs.write("/proj/src/b", &vec![b'b'; 2000]).await.unwrap();
    fs.write("/outside", &vec![b'c'; 9000]).await.unwrap();

    let src = fs.du("/proj/src").await.unwrap();
    assert_eq!(
        src.bytes, 3000,
        "du must sum the subtree, and only the subtree"
    );
    // /proj/src itself plus its two files.
    assert_eq!(src.inodes, 3);

    let proj = fs.du("/proj").await.unwrap();
    assert_eq!(proj.bytes, 3000, "a parent includes its children");
    assert_eq!(proj.inodes, 4, "and one more inode for /proj/src's parent");

    let all = fs.usage().await.unwrap();
    assert_eq!(all.bytes, 12_000, "workspace usage includes /outside");
}

/// A hard-linked inode is counted once by `du`, as `du` itself does — the
/// recursion unions inode ids rather than accumulating per dentry.
#[tokio::test]
async fn du_counts_a_hard_linked_inode_once() {
    let fs = fixture().await;
    fs.mkdir_p("/d").await.unwrap();
    fs.write("/d/a", &vec![b'x'; 5000]).await.unwrap();
    let ino = fs.stat("/d/a").await.unwrap().ino;
    fs.vfs_link_unchecked(ino, fs.stat("/d").await.unwrap().ino, "b")
        .await
        .unwrap();

    let d = fs.du("/d").await.unwrap();
    assert_eq!(
        d.bytes, 5000,
        "a file reachable by two names must be counted once, not twice"
    );
}

/// The headline of the quota half of #116: a write past the limit is refused, and
/// the workspace is left exactly as it was.
#[tokio::test]
async fn a_byte_quota_refuses_the_write_that_would_breach_it() {
    let fs = fixture().await;
    fs.set_quota(Quota {
        bytes: Some(10_000),
        inodes: None,
    })
    .await
    .unwrap();

    fs.write("/ok", &vec![b'x'; 9_000]).await.unwrap();

    let err = fs.write("/too-big", &vec![b'x'; 5_000]).await;
    assert!(
        matches!(err, Err(OrigoFSError::TooLarge(_))),
        "a write past the quota must be refused, got {err:?}"
    );
    assert!(
        fs.stat("/too-big").await.is_err(),
        "the refused write must not have created the file"
    );
    assert_eq!(fs.usage().await.unwrap().bytes, 9_000, "usage is unchanged");
}

/// A quota bounds *growth*, not every write: overwriting an existing file inside a
/// full workspace must still work, because what the write adds is the delta. A
/// naive implementation charging the whole new size would refuse to let a user
/// shrink or even rewrite a file once the quota was reached — leaving them stuck.
#[tokio::test]
async fn a_quota_charges_the_delta_not_the_whole_body() {
    let fs = fixture().await;
    fs.write("/f", &vec![b'x'; 9_000]).await.unwrap();
    fs.set_quota(Quota {
        bytes: Some(10_000),
        inodes: None,
    })
    .await
    .unwrap();

    // Same size: adds nothing, so it must be allowed even though a fresh 9 KB
    // write would not fit alongside the existing one.
    fs.write("/f", &vec![b'y'; 9_000]).await.unwrap();
    // Smaller: frees space.
    fs.write("/f", &vec![b'z'; 1_000]).await.unwrap();
    assert_eq!(fs.usage().await.unwrap().bytes, 1_000);
    // Now there is room again.
    fs.write("/g", &vec![b'w'; 8_000]).await.unwrap();
}

/// The inode limit is enforced independently of the byte limit.
#[tokio::test]
async fn an_inode_quota_is_enforced() {
    let fs = fixture().await;
    let start = fs.usage().await.unwrap().inodes;
    fs.set_quota(Quota {
        bytes: None,
        inodes: Some(start + 2),
    })
    .await
    .unwrap();

    fs.write("/a", b"x").await.unwrap();
    fs.write("/b", b"x").await.unwrap();
    assert!(
        matches!(fs.write("/c", b"x").await, Err(OrigoFSError::TooLarge(_))),
        "the third file must exceed an inode quota of two"
    );
}

/// Setting a quota below current usage is allowed and is not retroactive: nothing
/// is deleted, existing files stay readable, and only further growth is refused.
/// The alternative — refusing to set it — would make a quota impossible to
/// introduce on a workspace that already has data, which is the only case that
/// matters.
#[tokio::test]
async fn a_quota_below_current_usage_is_allowed_and_not_retroactive() {
    let fs = fixture().await;
    fs.write("/f", &vec![b'x'; 10_000]).await.unwrap();
    fs.set_quota(Quota {
        bytes: Some(1_000),
        inodes: None,
    })
    .await
    .unwrap();

    assert_eq!(
        fs.read("/f").await.unwrap().len(),
        10_000,
        "an over-quota file must stay readable"
    );
    assert!(fs.write("/g", b"more").await.is_err(), "growth is refused");

    // And `statfs` reports zero free rather than underflowing to an enormous one.
    let st = fs.statfs().await.unwrap();
    assert_eq!(st.free_blocks, 0, "over-quota must report no free space");
}

/// A quota can be cleared again, restoring unlimited growth. Without this, setting
/// one would be a one-way door.
#[tokio::test]
async fn a_quota_can_be_cleared() {
    let fs = fixture().await;
    fs.set_quota(Quota {
        bytes: Some(100),
        inodes: None,
    })
    .await
    .unwrap();
    assert!(fs.write("/f", &vec![b'x'; 5_000]).await.is_err());

    fs.set_quota(Quota::default()).await.unwrap();
    assert!(
        fs.quota().await.unwrap().is_unlimited(),
        "clearing must leave no limit behind"
    );
    fs.write("/f", &vec![b'x'; 5_000]).await.unwrap();
}

/// No quota is the default, and it costs nothing: every existing workspace keeps
/// writing exactly as before.
#[tokio::test]
async fn no_quota_is_the_default() {
    let fs = fixture().await;
    assert!(fs.quota().await.unwrap().is_unlimited());
    fs.write("/f", &vec![b'x'; 5_000_000]).await.unwrap();
}
