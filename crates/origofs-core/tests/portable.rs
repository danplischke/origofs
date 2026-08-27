//! Engine-independent metadata dump and load (issue #117).
//!
//! `backup_to` returns an error on every backend but SQLite, and `CLAUDE.md` is
//! explicit that the DB is the irreplaceable half: `fsck --rebuild` reconstructs
//! committed files, dirs, symlinks and branches from the bucket alone, and **none**
//! of the attribution.
//!
//! The test that matters most is [`a_dump_round_trips_attribution`]: the issue
//! calls the SQLite → Postgres migration path "the one that will be missed first",
//! and what makes it hard is precisely that blame, the audit log, and uncommitted
//! edits do not travel with content.

use origofs_core::{
    Fs, MemStore, MetadataStore, OrigoFSError, Perms, SqliteMetadataStore, VersioningMode, WriteCtx,
};
use std::sync::Arc;

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

/// A fresh workspace sharing `store`, so a dump/load pair can move metadata
/// between two independent metadata DBs over one content store — which is exactly
/// the shape of a backend migration.
async fn fs_sharing(store: Arc<MemStore>) -> TestFs {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

async fn fixture() -> (TestFs, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    (fs_sharing(store.clone()).await, store)
}

fn dump_of(bytes: &[u8]) -> Vec<serde_json::Value> {
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// **The headline.** A dump carries everything the content store cannot rebuild:
/// blame, the actor registry, the op-log, and uncommitted working-tree state.
///
/// The two workspaces share a content store but have entirely separate metadata
/// DBs, which is the shape of a SQLite → Postgres migration.
#[tokio::test]
async fn a_dump_round_trips_attribution() {
    let (src, store) = fixture().await;
    let alice = src.create_human("alice", Some("sub:alice")).await.unwrap();
    let claude = src.create_agent("claude", "opus", None).await.unwrap();
    let session = src.create_session(claude, Some("work")).await.unwrap();

    src.write_as(WriteCtx::actor(alice), "/notes.md", b"from alice\n")
        .await
        .unwrap();
    src.write_as(
        WriteCtx::session(claude, session),
        "/notes.md",
        b"from alice\nfrom claude\n",
    )
    .await
    .unwrap();
    // Uncommitted on purpose: this is the state `fsck --rebuild` cannot recover.
    src.mkdir_p("/sub").await.unwrap();
    src.write_as(WriteCtx::actor(alice), "/sub/draft.txt", b"wip")
        .await
        .unwrap();

    let before = src.blame("/notes.md").await.unwrap();
    assert!(
        before.iter().any(|b| b.actor.id == claude),
        "the fixture should have per-line blame to carry"
    );

    let mut buf = Vec::new();
    let rows = src.dump(&mut buf).await.unwrap();
    assert!(rows > 0);

    // A second, independent metadata DB over the same content store.
    let dst = fs_sharing(store).await;
    let report = dst.load(std::io::Cursor::new(&buf)).await.unwrap();
    assert!(report.total_rows() > 0);
    assert!(
        report.skipped_tables.is_empty(),
        "a dump from this build should have no unknown tables: {:?}",
        report.skipped_tables
    );

    // Content is readable, because the content store is shared and the manifests
    // travelled in the metadata.
    assert_eq!(
        &dst.read("/notes.md").await.unwrap()[..],
        b"from alice\nfrom claude\n"
    );
    assert_eq!(&dst.read("/sub/draft.txt").await.unwrap()[..], b"wip");

    // And the attribution survived, which is the part `resync` does not carry in
    // full and `fsck --rebuild` cannot recover at all.
    let after = dst.blame("/notes.md").await.unwrap();
    assert_eq!(
        after.iter().map(|b| b.actor.id).collect::<Vec<_>>(),
        before.iter().map(|b| b.actor.id).collect::<Vec<_>>(),
        "per-line blame must survive the round trip"
    );
    let actors = dst.list_actors().await.unwrap();
    assert!(
        actors.iter().any(|a| a.display_name == "alice")
            && actors.iter().any(|a| a.display_name == "claude"),
        "the actor registry must survive; got {:?}",
        actors.iter().map(|a| &a.display_name).collect::<Vec<_>>()
    );
}

/// Committed history and branches survive too, so a restored workspace is a
/// workspace and not just a pile of files.
#[tokio::test]
async fn a_dump_round_trips_commits_and_branches() {
    let (src, store) = fixture().await;
    src.set_versioning_mode(VersioningMode::Native)
        .await
        .unwrap();
    src.write("/a.txt", b"one").await.unwrap();
    let c1 = src.commit("dan", "first").await.unwrap();
    src.create_branch("feature").await.unwrap();
    src.write("/b.txt", b"two").await.unwrap();
    src.commit("dan", "second").await.unwrap();

    let mut buf = Vec::new();
    src.dump(&mut buf).await.unwrap();

    let dst = fs_sharing(store).await;
    dst.load(std::io::Cursor::new(&buf)).await.unwrap();

    let branches = dst.list_branches().await.unwrap();
    let names: Vec<&str> = branches.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"main") && names.contains(&"feature"),
        "branches must survive; got {names:?}"
    );
    let log = dst.log().await.unwrap();
    assert_eq!(log.len(), 2, "commit history must survive");
    assert!(log.iter().any(|c| c.hash == c1));
}

/// The newer surface travels: trash entries, ACL grants, quotas, and xattrs are
/// all metadata-only state that nothing else can reconstruct.
#[tokio::test]
async fn a_dump_round_trips_the_newer_metadata() {
    let (src, store) = fixture().await;
    let agent = src.create_agent("claude", "opus", None).await.unwrap();
    let ctx = WriteCtx::actor(agent);

    src.set_trash_retention(Some(3600)).await.unwrap();
    src.set_quota(origofs_core::Quota {
        bytes: Some(1_000_000),
        inodes: None,
    })
    .await
    .unwrap();
    src.grant(agent, "/docs", Perms::WRITE, None).await.unwrap();

    src.write_as(ctx, "/f.txt", b"x").await.unwrap();
    let ino = src.stat("/f.txt").await.unwrap().ino;
    src.vfs_setxattr(ino, "user.tag", b"blue").await.unwrap();
    src.write_as(ctx, "/gone.txt", b"deleted").await.unwrap();
    src.remove_as(ctx, "/gone.txt").await.unwrap();
    assert_eq!(src.list_trash().await.unwrap().len(), 1);

    let mut buf = Vec::new();
    src.dump(&mut buf).await.unwrap();

    let dst = fs_sharing(store).await;
    dst.load(std::io::Cursor::new(&buf)).await.unwrap();

    assert_eq!(dst.trash_retention().await.unwrap(), Some(3600));
    assert_eq!(dst.quota().await.unwrap().bytes, Some(1_000_000));
    assert_eq!(dst.list_grants(Some(agent)).await.unwrap().len(), 1);
    assert_eq!(dst.list_trash().await.unwrap().len(), 1);

    let ino = dst.stat("/f.txt").await.unwrap().ino;
    assert_eq!(
        dst.vfs_getxattr(ino, "user.tag").await.unwrap().as_deref(),
        Some(&b"blue"[..]),
        "extended attributes must survive the round trip"
    );

    // The restored trash entry is genuinely restorable, not just present.
    let id = dst.list_trash().await.unwrap()[0].id;
    dst.restore_trash(id, ctx).await.unwrap();
    assert_eq!(&dst.read("/gone.txt").await.unwrap()[..], b"deleted");
}

/// **Integers survive.** A bare JSON number is a float in most parsers, so an
/// `i64` past 2^53 round-trips wrong — and inode numbers, sizes and timestamps are
/// all `i64` here, so this is the column type that matters most.
#[tokio::test]
async fn large_integers_survive_the_text_format() {
    use origofs_core::{Cell, Row};

    let big = 9_007_199_254_740_993i64; // 2^53 + 1: the first integer a f64 cannot hold
    let (fs, _store) = fixture().await;

    // Round-trip through the same encoder the dump uses, via a table that has a
    // free-form integer column.
    let rows = vec![Row(vec![
        ("workspace_id".into(), Cell::Int(1)),
        ("key".into(), Cell::Text("probe".into())),
        ("value".into(), Cell::Text(big.to_string())),
    ])];
    let _ = rows; // shape check only; the assertion below is the real one

    fs.set_quota(origofs_core::Quota {
        bytes: Some(big as u64),
        inodes: None,
    })
    .await
    .unwrap();

    let mut buf = Vec::new();
    fs.dump(&mut buf).await.unwrap();
    let dst = fs_sharing(Arc::new(MemStore::new())).await;
    dst.load(std::io::Cursor::new(&buf)).await.unwrap();

    assert_eq!(
        dst.quota().await.unwrap().bytes,
        Some(big as u64),
        "a value past 2^53 must not be mangled by the text format"
    );
}

/// The header names the format and the schema version, so a loader can refuse a
/// file that is not a dump rather than misparsing it.
#[tokio::test]
async fn the_header_identifies_the_dump() {
    let (fs, _store) = fixture().await;
    let mut buf = Vec::new();
    fs.dump(&mut buf).await.unwrap();

    let records = dump_of(&buf);
    assert_eq!(records[0]["format"], "origofs-metadata-dump");
    assert_eq!(
        records[0]["schema_version"],
        origofs_core::latest_schema_version()
    );

    // Anything else is refused.
    let dst = fs_sharing(Arc::new(MemStore::new())).await;
    assert!(
        dst.load(std::io::Cursor::new(b"{\"hello\":1}\n".to_vec()))
            .await
            .is_err(),
        "a file that is not a dump must be refused, not misparsed"
    );
    assert!(
        dst.load(std::io::Cursor::new(Vec::new())).await.is_err(),
        "an empty file must be refused"
    );
}

/// A dump from a **newer** schema is refused outright. It may carry columns this
/// build cannot interpret, and loading half of it is worse than not starting.
#[tokio::test]
async fn a_dump_from_a_newer_schema_is_refused() {
    let (fs, _store) = fixture().await;
    let mut buf = Vec::new();
    fs.dump(&mut buf).await.unwrap();

    let mut records = dump_of(&buf);
    records[0]["schema_version"] = serde_json::json!(origofs_core::latest_schema_version() + 100);
    let doctored: String = records
        .iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    let dst = fs_sharing(Arc::new(MemStore::new())).await;
    let err = dst.load(std::io::Cursor::new(doctored.into_bytes())).await;
    assert!(
        matches!(err, Err(OrigoFSError::Metadata(ref m)) if m.contains("upgrade origofs")),
        "a dump from a newer schema must be refused with a clear reason, got {err:?}"
    );
}

/// A dump carrying a table this build does not know is **skipped, not fatal**: an
/// older origofs must still restore everything it understands from a newer dump.
#[tokio::test]
async fn an_unknown_table_is_skipped_rather_than_fatal() {
    let (fs, store) = fixture().await;
    fs.write("/f.txt", b"x").await.unwrap();
    let mut buf = Vec::new();
    fs.dump(&mut buf).await.unwrap();

    let mut text = String::from_utf8(buf).unwrap();
    text.push_str("{\"t\":\"table_from_the_future\",\"r\":{\"x\":\"y\"}}\n");

    let dst = fs_sharing(store).await;
    let report = dst
        .load(std::io::Cursor::new(text.into_bytes()))
        .await
        .unwrap();

    assert_eq!(report.skipped_tables, vec!["table_from_the_future"]);
    assert_eq!(
        &dst.read("/f.txt").await.unwrap()[..],
        b"x",
        "the rows this build *does* understand must still have loaded"
    );
}

/// **A load is a restore, not a merge.** Merging would have to reconcile two
/// independent id spaces — inode numbers, actor ids, session ids are all local
/// sequences — and getting that silently wrong produces blame attributed to the
/// wrong actor, which is the one failure this system exists to prevent.
#[tokio::test]
async fn loading_into_a_populated_workspace_is_refused() {
    let (src, store) = fixture().await;
    src.write("/a.txt", b"x").await.unwrap();
    let mut buf = Vec::new();
    src.dump(&mut buf).await.unwrap();

    let dst = fs_sharing(store).await;
    dst.write("/existing.txt", b"already here").await.unwrap();

    let err = dst.load(std::io::Cursor::new(&buf)).await;
    assert!(
        matches!(err, Err(OrigoFSError::InvalidArgument(ref m)) if m.contains("does not merge")),
        "a load into a populated workspace must be refused, got {err:?}"
    );
    assert_eq!(
        &dst.read("/existing.txt").await.unwrap()[..],
        b"already here",
        "the refused load must not have touched anything"
    );
}

/// The table allowlist is the security boundary: `export_table` interpolates the
/// name into SQL, which is only safe because anything off the list is refused.
#[tokio::test]
async fn a_table_outside_the_allowlist_is_refused() {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta.clone(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();

    for bad in [
        "fs_event",                      // real, but deliberately not dumped
        "sqlite_master",                 // real, and not ours
        "inode\"; DROP TABLE inode; --", // the reason the allowlist exists
        "does_not_exist",
    ] {
        assert!(
            meta.export_table(bad).await.is_err(),
            "export_table must refuse {bad:?}"
        );
        assert!(
            meta.import_table(bad, &[]).await.is_err(),
            "import_table must refuse {bad:?}"
        );
    }
    // The tree is intact after the injection attempt.
    assert!(fs.stat("/").await.is_ok());
}

/// The transient tables are deliberately absent. Restoring a change feed would
/// fire every watcher for changes that already happened; restoring presence or a
/// live-document marker would assert something false about right now.
#[tokio::test]
async fn transient_tables_are_not_dumped() {
    for t in ["fs_event", "presence", "live_doc"] {
        assert!(
            !origofs_core::DUMP_TABLES.contains(&t),
            "{t} is transient and must not be dumped"
        );
    }
    // ...and everything durable is.
    for t in [
        "inode",
        "dentry",
        "blob_blame",
        "actor",
        "edit_op",
        "acl",
        "trash",
    ] {
        assert!(
            origofs_core::DUMP_TABLES.contains(&t),
            "{t} is durable metadata and must be dumped"
        );
    }
}
