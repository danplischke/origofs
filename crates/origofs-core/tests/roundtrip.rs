//! End-to-end tests for the M0 working-tree engine over SQLite + local CAS.

use origofs_core::{ContentStore, Fs, Hash, LocalCasStore, SqliteMetadataStore};
use tempfile::TempDir;

async fn fixture() -> (Fs<SqliteMetadataStore, LocalCasStore>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let meta = SqliteMetadataStore::open(dir.path().join("meta.db")).unwrap();
    let content = LocalCasStore::open(dir.path().join("cas")).await.unwrap();
    let fs = Fs::new(meta, content);
    fs.init().await.unwrap();
    (fs, dir)
}

#[tokio::test]
async fn write_read_roundtrip() {
    let (fs, _dir) = fixture().await;
    fs.mkdir_p("/notes").await.unwrap();
    fs.write("/notes/a.txt", b"hello world").await.unwrap();

    let got = fs.read("/notes/a.txt").await.unwrap();
    assert_eq!(&got[..], b"hello world");

    let st = fs.stat("/notes/a.txt").await.unwrap();
    assert_eq!(st.size, 11);
}

#[tokio::test]
async fn overwrite_and_empty_file() {
    let (fs, _dir) = fixture().await;
    fs.write("/f", b"first").await.unwrap();
    fs.write("/f", b"second-longer").await.unwrap();
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"second-longer");

    fs.write("/empty", b"").await.unwrap();
    assert_eq!(&fs.read("/empty").await.unwrap()[..], b"");
    assert_eq!(fs.stat("/empty").await.unwrap().size, 0);
}

#[tokio::test]
async fn read_range() {
    let (fs, _dir) = fixture().await;
    fs.write("/f", b"0123456789").await.unwrap();
    assert_eq!(&fs.read_range("/f", 2, 4).await.unwrap()[..], b"2345");
    // range clamps to end of file
    assert_eq!(&fs.read_range("/f", 8, 100).await.unwrap()[..], b"89");
}

#[tokio::test]
async fn directories_and_listing() {
    let (fs, _dir) = fixture().await;
    fs.mkdir_p("/a/b").await.unwrap();
    fs.write("/a/one.txt", b"1").await.unwrap();
    fs.write("/a/two.txt", b"2").await.unwrap();

    let names: Vec<String> = fs
        .ls("/a")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, vec!["b", "one.txt", "two.txt"]);

    // root lists the single top-level dir
    let root: Vec<String> = fs
        .ls("/")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(root, vec!["a"]);
}

#[tokio::test]
async fn rename_and_remove() {
    let (fs, _dir) = fixture().await;
    fs.mkdir_p("/d").await.unwrap();
    fs.write("/d/x", b"data").await.unwrap();

    fs.rename("/d/x", "/d/y").await.unwrap();
    assert!(fs.read("/d/x").await.is_err());
    assert_eq!(&fs.read("/d/y").await.unwrap()[..], b"data");

    fs.unlink("/d/y").await.unwrap();
    assert!(fs.read("/d/y").await.is_err());

    fs.remove("/d").await.unwrap();
    assert!(fs.stat("/d").await.is_err());
}

#[tokio::test]
async fn rmdir_refuses_nonempty() {
    let (fs, _dir) = fixture().await;
    fs.mkdir_p("/d").await.unwrap();
    fs.write("/d/x", b"1").await.unwrap();
    assert!(fs.rmdir("/d").await.is_err());
    fs.unlink("/d/x").await.unwrap();
    fs.rmdir("/d").await.unwrap();
}

#[tokio::test]
async fn symlinks() {
    let (fs, _dir) = fixture().await;
    fs.write("/target.txt", b"payload").await.unwrap();
    fs.symlink("/target.txt", "/link").await.unwrap();
    assert_eq!(fs.readlink("/link").await.unwrap(), "/target.txt");
}

#[tokio::test]
async fn content_is_deduplicated() {
    let (fs, dir) = fixture().await;
    // Two files with identical content share one blob (content addressing).
    fs.write("/p", b"same bytes").await.unwrap();
    fs.write("/q", b"same bytes").await.unwrap();
    assert_eq!(&fs.read("/p").await.unwrap()[..], b"same bytes");
    assert_eq!(&fs.read("/q").await.unwrap()[..], b"same bytes");

    // Exactly one object exists on disk for that content.
    let hash = Hash::of(b"same bytes");
    let hex = hash.to_hex();
    let obj = dir
        .path()
        .join("cas")
        .join("objects")
        .join(&hex[0..2])
        .join(&hex[2..]);
    assert!(obj.exists(), "expected a single shared blob at {obj:?}");
}

#[tokio::test]
async fn content_store_put_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalCasStore::open(dir.path()).await.unwrap();
    let h1 = store.put(b"abc").await.unwrap();
    let h2 = store.put(b"abc").await.unwrap();
    assert_eq!(h1, h2);
    assert!(store.has(&h1).await.unwrap());
    assert_eq!(&store.get(&h1).await.unwrap()[..], b"abc");
}
