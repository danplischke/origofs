//! Store-layer directory pagination (M16, issue #75).
//!
//! `list_dir` hands back the whole directory, so every `readdir` on the FUSE/NFS
//! surfaces used to re-read it and slice in memory. These cover the replacement:
//! the keyset `list_dir_page`, the batched `get_inodes`, the `dentry_name` cookie
//! bridge, and the `Fs::vfs_readdir_page*` wrappers the surfaces now page with.
//!
//! The page order is deliberately checked *against the backend's own*
//! `list_dir` rather than an order hardcoded here: a keyset scan only has to be
//! self-consistent with the collation its `ORDER BY` uses, and SQLite (`BINARY`)
//! and Postgres (column collation) legitimately differ on non-ASCII names.

use origofs_core::{
    FileKind, Fs, INO_ROOT, Ino, InodeInit, MemStore, MetadataStore, SqliteMetadataStore,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

async fn fixture() -> Fs<Arc<SqliteMetadataStore>, Arc<MemStore>> {
    let meta = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs
}

/// Link `names` as regular files directly under `parent`, returning their inodes
/// by name. Goes through the store rather than the engine so a name is exactly
/// the bytes given.
async fn seed<M: MetadataStore>(meta: &M, parent: Ino, names: &[&str]) -> HashMap<String, Ino> {
    let mut out = HashMap::new();
    for n in names {
        let ino = meta
            .create_inode(InodeInit::new(FileKind::File, 0o100644))
            .await
            .unwrap();
        meta.add_dentry(parent, n, ino).await.unwrap();
        out.insert((*n).to_string(), ino);
    }
    out
}

/// Walk `parent` one keyset page at a time, exactly as a surface's `readdir`
/// does, and return every name seen in the order the pages produced them.
async fn walk<M: MetadataStore>(meta: &M, parent: Ino, page_size: usize) -> Vec<String> {
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = meta
            .list_dir_page(parent, cursor.as_deref(), page_size)
            .await
            .unwrap();
        assert!(
            page.len() <= page_size,
            "page returned {} entries for limit {page_size}",
            page.len()
        );
        if page.is_empty() {
            return seen;
        }
        cursor = Some(page.last().unwrap().name.clone());
        seen.extend(page.into_iter().map(|e| e.name));
        // Guard against a cursor that fails to advance turning this into a
        // hang rather than a failure.
        assert!(seen.len() <= 10_000, "paging did not terminate");
    }
}

#[tokio::test]
async fn pages_cover_every_entry_exactly_once() {
    let fs = fixture().await;
    // Names whose creation order is deliberately not their name order, so a page
    // that leaked inode/rowid ordering would show up.
    let names: Vec<String> = (0..250)
        .map(|i| format!("f{:03}", (i * 97) % 250))
        .collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    seed(&fs.meta, INO_ROOT, &refs).await;

    let full: Vec<String> = fs
        .meta
        .list_dir(INO_ROOT)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(full.len(), 250);

    // Every page size — including ones that do and do not divide the directory
    // evenly, and one larger than the whole thing — reproduces the full listing.
    for page_size in [1, 2, 7, 50, 249, 250, 251, 1000] {
        let paged = walk(&fs.meta, INO_ROOT, page_size).await;
        assert_eq!(paged, full, "page size {page_size} diverged from list_dir");
        let unique: HashSet<&String> = paged.iter().collect();
        assert_eq!(
            unique.len(),
            paged.len(),
            "page size {page_size} duplicated"
        );
    }
}

#[tokio::test]
async fn page_boundaries_advance_on_tricky_names() {
    let fs = fixture().await;
    // Names differing only by a trailing character or byte, plus non-ASCII and
    // multi-byte ones — every adjacent pair here is a potential page boundary
    // where a `>` comparison could stall or skip.
    let names = [
        "a",
        "a ",
        "a!",
        "a0",
        "aa",
        "ab",
        "ab ",
        "abc",
        "abc ",
        "abcd",
        "z",
        "zz",
        "é",
        "éa",
        "éb",
        "ée",
        "日本",
        "日本語",
        "日本語!",
        "🙂",
        "🙂🙂",
    ];
    seed(&fs.meta, INO_ROOT, &names).await;

    let full: Vec<String> = fs
        .meta
        .list_dir(INO_ROOT)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(full.len(), names.len());

    // A page size of 1 makes *every* adjacent pair a boundary.
    for page_size in [1, 2, 3, 5] {
        let paged = walk(&fs.meta, INO_ROOT, page_size).await;
        assert_eq!(paged, full, "page size {page_size} diverged from list_dir");
    }

    // Resuming from any single name lands on exactly the tail after it — the
    // property that makes the cursor safe to hand to a client.
    for (i, name) in full.iter().enumerate() {
        let rest = fs
            .meta
            .list_dir_page(INO_ROOT, Some(name), 100)
            .await
            .unwrap();
        let rest: Vec<String> = rest.into_iter().map(|e| e.name).collect();
        assert_eq!(rest, full[i + 1..], "resume after {name:?}");
    }
}

#[tokio::test]
async fn limit_is_respected() {
    let fs = fixture().await;
    seed(&fs.meta, INO_ROOT, &["a", "b", "c", "d", "e"]).await;

    for limit in 0..=7usize {
        let page = fs.meta.list_dir_page(INO_ROOT, None, limit).await.unwrap();
        assert_eq!(page.len(), limit.min(5), "limit {limit}");
    }
    // A limit applies to the tail after the cursor too, not to the directory.
    let page = fs
        .meta
        .list_dir_page(INO_ROOT, Some("c"), 10)
        .await
        .unwrap();
    let got: Vec<&str> = page.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(got, ["d", "e"]);
    // A cursor past every name yields an empty page (end of directory), not an
    // error and not a wrap-around.
    assert!(
        fs.meta
            .list_dir_page(INO_ROOT, Some("zzz"), 10)
            .await
            .unwrap()
            .is_empty()
    );
    // A page in a directory that has none.
    let empty = fs
        .meta
        .create_inode(InodeInit::new(FileKind::Dir, 0o040755))
        .await
        .unwrap();
    assert!(
        fs.meta
            .list_dir_page(empty, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn batched_get_inodes_matches_get_inode() {
    let fs = fixture().await;
    let by_name = seed(&fs.meta, INO_ROOT, &["a", "b", "c", "d"]).await;
    // Give the inodes distinguishable attributes so an accidental row/parameter
    // misalignment can't pass by returning the right *count* of identical rows.
    for (i, name) in ["a", "b", "c", "d"].iter().enumerate() {
        fs.write(&format!("/{name}"), &vec![b'x'; i + 1])
            .await
            .unwrap();
    }

    let mut inos: Vec<Ino> = by_name.values().copied().collect();
    inos.sort_unstable();

    // An empty request is a no-op, not a malformed `IN ()`.
    assert!(fs.meta.get_inodes(&[]).await.unwrap().is_empty());

    // A missing ino is simply absent and a duplicate is coalesced — the batch
    // must not error or return placeholder rows.
    let missing: Ino = 999_999;
    let mut asked = inos.clone();
    asked.push(missing);
    asked.push(inos[0]);
    asked.push(inos[0]);

    let batched = fs.meta.get_inodes(&asked).await.unwrap();
    assert_eq!(batched.len(), inos.len(), "duplicates must coalesce");
    let by_ino: HashMap<Ino, _> = batched.into_iter().map(|i| (i.ino, i)).collect();
    assert!(!by_ino.contains_key(&missing));

    for ino in &inos {
        let one = fs.meta.get_inode(*ino).await.unwrap().expect("inode");
        let many = by_ino.get(ino).expect("batched inode");
        assert_eq!(one.ino, many.ino);
        assert_eq!(one.kind, many.kind);
        assert_eq!(one.mode, many.mode);
        assert_eq!(one.nlink, many.nlink);
        assert_eq!(one.size, many.size);
        assert_eq!(one.content, many.content);
        assert_eq!(one.mtime, many.mtime);
        assert_eq!(one.ctime, many.ctime);
    }
    // Sizes really did differ, so the per-ino join above was meaningful.
    let mut sizes: Vec<u64> = by_ino.values().map(|i| i.size).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [1, 2, 3, 4]);
}

#[tokio::test]
async fn batched_get_inodes_spans_many_chunks() {
    // SQLite binds a bounded number of parameters per statement, so `get_inodes`
    // chunks its IN-list; ask for comfortably more than one chunk.
    let fs = fixture().await;
    let names: Vec<String> = (0..1200).map(|i| format!("n{i:04}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let by_name = seed(&fs.meta, INO_ROOT, &refs).await;

    let mut inos: Vec<Ino> = by_name.values().copied().collect();
    inos.sort_unstable();
    let got = fs.meta.get_inodes(&inos).await.unwrap();
    assert_eq!(got.len(), inos.len());
    let mut got_inos: Vec<Ino> = got.into_iter().map(|i| i.ino).collect();
    got_inos.sort_unstable();
    assert_eq!(got_inos, inos);
}

#[tokio::test]
async fn readdir_page_with_attrs_matches_per_entry_getattr() {
    let fs = fixture().await;
    for i in 0..25 {
        fs.write(&format!("/f{i:02}"), &vec![b'y'; i + 1])
            .await
            .unwrap();
    }
    fs.mkdir_p("/sub").await.unwrap();
    fs.symlink("/f00", "/link").await.unwrap();

    let full = fs.vfs_readdir(INO_ROOT).await.unwrap();

    let mut cursor: Option<String> = None;
    let mut seen = Vec::new();
    loop {
        let page = fs
            .vfs_readdir_page_with_attrs(INO_ROOT, cursor.as_deref(), 4)
            .await
            .unwrap();
        for e in &page.entries {
            // The batched attrs are the same attrs a per-entry getattr returns —
            // the N+1 that item M16 removes.
            let one = fs.vfs_getattr(e.entry.ino).await.unwrap();
            assert_eq!(one.ino, e.inode.ino);
            assert_eq!(one.kind, e.inode.kind);
            assert_eq!(one.size, e.inode.size);
            assert_eq!(one.mode, e.inode.mode);
            assert_eq!(one.content, e.inode.content);
            // The dentry's kind and the inode's agree.
            assert_eq!(e.entry.kind, e.inode.kind);
            seen.push(e.entry.name.clone());
        }
        if page.end {
            assert!(page.entries.len() < 4);
            break;
        }
        cursor = page.next_after.clone();
        assert!(cursor.is_some(), "a non-final page must carry a cursor");
    }

    let expected: Vec<String> = full.into_iter().map(|e| e.name).collect();
    assert_eq!(seen, expected);
    assert!(seen.iter().any(|n| n == "sub"));
    assert!(seen.iter().any(|n| n == "link"));
}

#[tokio::test]
async fn dentry_name_bridges_an_ino_cookie_back_to_a_name() {
    // NFSv3 resumes readdir by fileid; the store pages by name. `dentry_name` is
    // the translation, and it must reject a cookie that isn't a child.
    let fs = fixture().await;
    fs.mkdir_p("/d").await.unwrap();
    let d = fs.vfs_lookup(INO_ROOT, "d").await.unwrap().unwrap().ino;
    let by_name = seed(&fs.meta, d, &["one", "two", "three"]).await;

    for (name, ino) in &by_name {
        assert_eq!(
            fs.vfs_dentry_name(d, *ino).await.unwrap().as_deref(),
            Some(name.as_str())
        );
        // Right ino, wrong parent.
        assert_eq!(fs.vfs_dentry_name(INO_ROOT, *ino).await.unwrap(), None);
    }
    // An ino that doesn't exist at all is `None`, not an error — that is what
    // lets the NFS surface answer BAD_COOKIE.
    assert_eq!(fs.vfs_dentry_name(d, 999_999).await.unwrap(), None);

    // Round-trip: every cookie resumes the page immediately after its own entry.
    let all: Vec<String> = fs
        .vfs_readdir(d)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    for (i, name) in all.iter().enumerate() {
        let ino = by_name[name];
        let cursor = fs.vfs_dentry_name(d, ino).await.unwrap().unwrap();
        let rest: Vec<String> = fs
            .vfs_readdir_page(d, Some(&cursor), 10)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(rest, all[i + 1..]);
    }
}

#[tokio::test]
async fn a_page_is_stable_across_edits_before_the_cursor() {
    // The reason the cursor is a name and not an offset: deleting an entry the
    // scan already passed must not shift the remaining pages.
    let fs = fixture().await;
    seed(&fs.meta, INO_ROOT, &["a", "b", "c", "d", "e"]).await;

    let page1 = fs.vfs_readdir_page(INO_ROOT, None, 2).await.unwrap();
    let got: Vec<&str> = page1.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(got, ["a", "b"]);

    // Remove an already-returned entry, then resume. An OFFSET-based pager would
    // now skip "c"; the keyset cursor cannot.
    fs.meta.remove_dentry(INO_ROOT, "a").await.unwrap();
    let page2 = fs.vfs_readdir_page(INO_ROOT, Some("b"), 2).await.unwrap();
    let got: Vec<&str> = page2.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(got, ["c", "d"]);
}
