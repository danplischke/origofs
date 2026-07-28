//! The NFSv3 adapter: exercise every operation against a real workspace by
//! calling the `NFSFileSystem` methods directly (a live kernel mount needs
//! `nfs-utils`, absent in CI), plus a server-bind smoke test.

use nfsserve::nfs::{
    fattr3, ftype3, nfsstat3, nfsstring, sattr3, set_atime, set_gid3, set_mode3, set_mtime,
    set_size3, set_uid3,
};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{NFSFileSystem, VFSCapabilities};
use origofs_nfs::{serve, OrigoFSNfs};
use origofs_sdk::Workspace;

fn fname(s: &str) -> nfsstring {
    nfsstring::from(s.as_bytes())
}

fn no_attrs() -> sattr3 {
    sattr3 {
        mode: set_mode3::Void,
        uid: set_uid3::Void,
        gid: set_gid3::Void,
        size: set_size3::Void,
        atime: set_atime::DONT_CHANGE,
        mtime: set_mtime::DONT_CHANGE,
    }
}

fn entry_names(r: &nfsserve::vfs::ReadDirResult) -> Vec<String> {
    r.entries
        .iter()
        .map(|e| String::from_utf8(e.name.0.clone()).unwrap())
        .collect()
}

async fn seeded() -> OrigoFSNfs {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    ws.mkdir_p("/docs").await.unwrap();
    ws.write("/docs/readme.txt", b"hello nfs\n").await.unwrap();
    OrigoFSNfs::new(ws)
}

#[tokio::test]
async fn maps_reads_and_lookups() {
    let nfs = seeded().await;
    assert!(matches!(nfs.capabilities(), VFSCapabilities::ReadWrite));
    assert_eq!(nfs.root_dir(), 1);

    let docs = nfs.lookup(1, &fname("docs")).await.unwrap();
    assert!(matches!(
        nfs.getattr(docs).await.unwrap().ftype,
        ftype3::NF3DIR
    ));

    let file = nfs.lookup(docs, &fname("readme.txt")).await.unwrap();
    let a: fattr3 = nfs.getattr(file).await.unwrap();
    assert!(matches!(a.ftype, ftype3::NF3REG));
    assert_eq!(a.size, 10);

    let (bytes, eof) = nfs.read(file, 0, 100).await.unwrap();
    assert_eq!(bytes, b"hello nfs\n");
    assert!(eof);
    let (part, eof) = nfs.read(file, 0, 5).await.unwrap();
    assert_eq!(part, b"hello");
    assert!(!eof);

    // missing name -> NOENT
    assert!(matches!(
        nfs.lookup(docs, &fname("nope")).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));
}

#[tokio::test]
async fn create_write_mkdir_symlink_rename_remove() {
    let nfs = seeded().await;
    let docs = nfs.lookup(1, &fname("docs")).await.unwrap();

    // create + write + read back
    let (nf, _) = nfs
        .create(docs, &fname("new.txt"), no_attrs())
        .await
        .unwrap();
    nfs.write(nf, 0, b"written via nfs").await.unwrap();
    assert_eq!(nfs.read(nf, 0, 100).await.unwrap().0, b"written via nfs");

    // mkdir
    let (_sub, sa) = nfs.mkdir(docs, &fname("sub")).await.unwrap();
    assert!(matches!(sa.ftype, ftype3::NF3DIR));

    // readdir shows everything
    let rd = nfs.readdir(docs, 0, 100).await.unwrap();
    let mut names = entry_names(&rd);
    names.sort();
    assert_eq!(names, vec!["new.txt", "readme.txt", "sub"]);
    assert!(rd.end);

    // symlink + readlink
    let (lnk, la) = nfs
        .symlink(docs, &fname("link"), &fname("readme.txt"), &no_attrs())
        .await
        .unwrap();
    assert!(matches!(la.ftype, ftype3::NF3LNK));
    assert_eq!(nfs.readlink(lnk).await.unwrap().0, b"readme.txt");

    // rename + remove
    nfs.rename(docs, &fname("new.txt"), docs, &fname("moved.txt"))
        .await
        .unwrap();
    assert!(nfs.lookup(docs, &fname("moved.txt")).await.is_ok());
    assert!(matches!(
        nfs.lookup(docs, &fname("new.txt")).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));
    nfs.remove(docs, &fname("moved.txt")).await.unwrap();
    assert!(matches!(
        nfs.lookup(docs, &fname("moved.txt")).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));
}

#[tokio::test]
async fn setattr_truncates() {
    let nfs = seeded().await;
    let docs = nfs.lookup(1, &fname("docs")).await.unwrap();
    let (f, _) = nfs.create(docs, &fname("t.txt"), no_attrs()).await.unwrap();
    nfs.write(f, 0, b"0123456789").await.unwrap();

    let mut sa = no_attrs();
    sa.size = set_size3::size(4);
    let after = nfs.setattr(f, sa).await.unwrap();
    assert_eq!(after.size, 4);
    assert_eq!(nfs.read(f, 0, 100).await.unwrap().0, b"0123");
}

#[tokio::test]
async fn readdir_paginates_by_cookie() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    for n in ["a", "b", "c", "d", "e"] {
        ws.write(&format!("/{n}"), b"x").await.unwrap();
    }
    let nfs = OrigoFSNfs::new(ws);

    let page1 = nfs.readdir(1, 0, 2).await.unwrap();
    assert_eq!(page1.entries.len(), 2);
    assert!(!page1.end);

    let cookie = page1.entries.last().unwrap().fileid;
    let page2 = nfs.readdir(1, cookie, 2).await.unwrap();
    assert!(page2.entries.iter().all(|e| e.fileid > cookie));

    // walking pages covers all five, no overlap
    let mut all = entry_names(&page1);
    all.extend(entry_names(&page2));
    let last = page2.entries.last().unwrap().fileid;
    all.extend(entry_names(&nfs.readdir(1, last, 2).await.unwrap()));
    all.sort();
    assert_eq!(all, vec!["a", "b", "c", "d", "e"]);

    // an unknown cookie is rejected, not silently restarted
    assert!(matches!(
        nfs.readdir(1, 999_999, 2).await,
        Err(nfsstat3::NFS3ERR_BAD_COOKIE)
    ));
}

#[tokio::test]
async fn server_binds_a_port() {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
        .await
        .unwrap();
    // Bind on an ephemeral port; the server is ready to accept NFS clients.
    let listener = NFSTcpListener::bind("127.0.0.1:0", OrigoFSNfs::new(ws))
        .await
        .expect("bind NFS server");
    assert!(listener.get_listen_port() > 0);
    // `serve` is the blocking convenience wrapper; just prove it links.
    let _ = serve;
}
