//! The inode-oriented (FUSE/NFS) write path under concurrency.
//!
//! `vfs_write` and `vfs_truncate` are read-modify-write of the *whole* body, so
//! two writes to different offsets of one file each rewrite everything. Applied
//! unconditionally, the second erases the first — a lost update on precisely the
//! surface where concurrent writers to one file are the norm, because that is what
//! a mount is. These pin the compare-and-set that prevents it.

use origofs_core::{Fs, MemStore, OrigoFSError, SqliteMetadataStore};
use std::sync::Arc;

/// A shared on-disk store, so several `Fs` handles genuinely contend.
async fn fixture() -> (
    tempfile::TempDir,
    Fs<Arc<SqliteMetadataStore>, Arc<MemStore>>,
    Arc<SqliteMetadataStore>,
    Arc<MemStore>,
) {
    let dir = tempfile::tempdir().unwrap();
    let meta = Arc::new(SqliteMetadataStore::open(dir.path().join("meta.db")).unwrap());
    let content = Arc::new(MemStore::new());
    let fs = Fs::new(meta.clone(), content.clone());
    fs.init().await.unwrap();
    (dir, fs, meta, content)
}

/// Two writers touching disjoint byte ranges must both survive.
///
/// Before the CAS, each `vfs_write` read the whole body, patched its own range,
/// and wrote the result back unconditionally — so whichever committed second
/// silently reverted the other's range to what it had read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_to_disjoint_ranges_do_not_lose_each_other() {
    let (_dir, fs, meta, content) = fixture().await;

    // A file of zeros, big enough that the two writes land in different chunks.
    let ino = {
        fs.write("/f.bin", &vec![0u8; 64 * 1024]).await.unwrap();
        fs.stat("/f.bin").await.unwrap().ino
    };

    let a = {
        let fs = Fs::new(meta.clone(), content.clone());
        tokio::spawn(async move { fs.vfs_write(ino, 0, &[0xAA; 4096]).await })
    };
    let b = {
        let fs = Fs::new(meta.clone(), content.clone());
        tokio::spawn(async move { fs.vfs_write(ino, 32 * 1024, &[0xBB; 4096]).await })
    };

    a.await.unwrap().unwrap();
    b.await.unwrap().unwrap();

    let body = fs.read("/f.bin").await.unwrap();
    assert_eq!(body.len(), 64 * 1024);
    assert!(
        body[0..4096].iter().all(|&b| b == 0xAA),
        "writer A's range was lost"
    );
    assert!(
        body[32 * 1024..32 * 1024 + 4096].iter().all(|&b| b == 0xBB),
        "writer B's range was lost"
    );
}

/// Many concurrent single-byte writes at distinct offsets must all land.
///
/// A stronger form of the above: with N writers the unconditional version keeps
/// roughly one of them, so this fails loudly rather than marginally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_concurrent_writer_survives() {
    let (_dir, fs, meta, content) = fixture().await;

    const N: usize = 8;
    fs.write("/g.bin", &[0u8; N]).await.unwrap();
    let ino = fs.stat("/g.bin").await.unwrap().ino;

    let mut tasks = Vec::new();
    for i in 0..N {
        let fs = Fs::new(meta.clone(), content.clone());
        tasks.push(tokio::spawn(async move {
            fs.vfs_write(ino, i as u64, &[(i + 1) as u8]).await
        }));
    }
    for t in tasks {
        t.await.unwrap().unwrap();
    }

    let body = fs.read("/g.bin").await.unwrap();
    let expected: Vec<u8> = (1..=N as u8).collect();
    assert_eq!(
        &body[..],
        &expected[..],
        "concurrent writers overwrote each other"
    );
}

/// A truncate racing a write must not resurrect the truncated bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_and_write_do_not_interleave_destructively() {
    let (_dir, fs, meta, content) = fixture().await;

    fs.write("/h.bin", &vec![0xFFu8; 8192]).await.unwrap();
    let ino = fs.stat("/h.bin").await.unwrap().ino;

    let t = {
        let fs = Fs::new(meta.clone(), content.clone());
        tokio::spawn(async move { fs.vfs_truncate(ino, 1024).await })
    };
    let w = {
        let fs = Fs::new(meta.clone(), content.clone());
        tokio::spawn(async move { fs.vfs_write(ino, 0, &[0x11; 512]).await })
    };
    t.await.unwrap().unwrap();
    w.await.unwrap().unwrap();

    // Whichever order they serialized in, the result must be one of the two
    // legal outcomes — never a torn mix of a stale read and a fresh write.
    let body = fs.read("/h.bin").await.unwrap();
    let stat = fs.stat("/h.bin").await.unwrap();
    assert_eq!(
        body.len() as u64,
        stat.size,
        "inode size disagrees with the body it points at"
    );
    assert!(
        body.len() == 1024 || body.len() == 8192,
        "unexpected length {} — neither the truncate's nor the write's result",
        body.len()
    );
    assert!(
        body[0..512].iter().all(|&b| b == 0x11),
        "the write was lost even though it reported success"
    );
}

/// A write whose inode is unlinked mid-flight must fail, not report success.
///
/// `set_content` used to discard its affected-row count, so an `UPDATE` matching
/// zero rows committed happily and the caller was told the write had landed.
#[tokio::test]
async fn writing_to_an_unlinked_inode_is_an_error() {
    let (_dir, fs, _meta, _content) = fixture().await;

    fs.write("/doomed.txt", b"hello").await.unwrap();
    let ino = fs.stat("/doomed.txt").await.unwrap().ino;
    fs.remove("/doomed.txt").await.unwrap();

    let err = fs
        .vfs_write(ino, 0, b"bytes with nowhere to go")
        .await
        .unwrap_err();
    assert!(
        matches!(err, OrigoFSError::NotFound(_)),
        "expected NotFound for a removed inode, got {err:?}"
    );
}
