//! Regression tests for the failure-surface audit (issue #34, Phase 1).
//! Each test pins a specific fix so the failure mode can't silently return.

use origofs_core::{
    ActorInit, ChunkRef, Fs, Hash, INO_ROOT, Manifest, MemStore, MetadataStore, OrigoFSError,
    SqliteMetadataStore, WriteCtx,
};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    fs
}

// C5: a hostile truncate/write size must be rejected, not abort the process on
// a giant `Vec::resize`. (Reaches the NFS/FUSE surfaces as SETATTR/WRITE.)
#[tokio::test]
async fn oversized_truncate_and_write_are_rejected_not_panic() {
    let fs = fixture().await;
    fs.write("/f", b"hello").await.unwrap();
    let ino = fs.vfs_lookup(INO_ROOT, "f").await.unwrap().unwrap().ino;

    assert!(matches!(
        fs.vfs_truncate(ino, u64::MAX).await,
        Err(OrigoFSError::TooLarge(_))
    ));
    // write at an offset that would overflow / allocate absurdly
    assert!(matches!(
        fs.vfs_write(ino, u64::MAX - 4, b"boom").await,
        Err(OrigoFSError::TooLarge(_))
    ));
    // a normal write still works and the file is intact
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"hello");
}

// H5: a manifest whose declared size doesn't match its chunks is rejected at
// decode — this is what stops a hostile `size` from driving an OOM allocation.
#[test]
fn manifest_with_lying_size_is_rejected() {
    let honest = Manifest {
        size: 5,
        chunks: vec![ChunkRef {
            hash: Hash::of(b"hello"),
            len: 5,
        }],
    };
    // round-trips fine
    assert_eq!(Manifest::decode(&honest.encode()).unwrap(), honest);

    // same chunks, but a wildly inflated size field
    let liar = Manifest {
        size: u64::MAX,
        chunks: honest.chunks.clone(),
    };
    assert!(matches!(
        Manifest::decode(&liar.encode()),
        Err(OrigoFSError::Corrupt(_))
    ));
}

// H3: GC must not reclaim the proposed content of a *pending* suggestion.
#[tokio::test]
async fn gc_keeps_pending_suggestion_content() {
    let fs = fixture().await;
    let actor = fs.create_human("dan", None).await.unwrap();
    let reviewer = fs.create_human("reviewer", None).await.unwrap();
    fs.write("/f.txt", b"one\n").await.unwrap();
    fs.commit("dan", "base").await.unwrap();

    let sid = fs
        .suggest(WriteCtx::actor(actor), "/f.txt", b"one\ntwo\n", Some("add"))
        .await
        .unwrap();

    // A GC pass on the (otherwise quiescent) store must keep the proposed blob.
    fs.gc().await.unwrap();

    assert!(fs.suggestion_diff(sid).await.unwrap().contains("+two"));
    fs.accept_suggestion(sid, WriteCtx::actor(reviewer))
        .await
        .unwrap();
    assert_eq!(&fs.read("/f.txt").await.unwrap()[..], b"one\ntwo\n");
}

// L8: an empty-content suggestion is a real empty file, NOT a deletion; only
// `suggest_delete` removes the path.
#[tokio::test]
async fn empty_suggestion_is_not_a_deletion() {
    let fs = fixture().await;
    let actor = fs.create_human("dan", None).await.unwrap();
    let reviewer = fs.create_human("reviewer", None).await.unwrap();
    fs.write("/e.txt", b"stuff\n").await.unwrap();

    let sid = fs
        .suggest(WriteCtx::actor(actor), "/e.txt", b"", None)
        .await
        .unwrap();
    fs.accept_suggestion(sid, WriteCtx::actor(reviewer))
        .await
        .unwrap();
    // still present, now empty
    assert_eq!(&fs.read("/e.txt").await.unwrap()[..], b"");

    let del = fs
        .suggest_delete(WriteCtx::actor(actor), "/e.txt", None)
        .await
        .unwrap();
    fs.accept_suggestion(del, WriteCtx::actor(reviewer))
        .await
        .unwrap();
    assert!(fs.read("/e.txt").await.is_err());
}

// M12: presence rows can be reaped so the table doesn't grow without bound.
#[tokio::test]
async fn presence_rows_can_be_reaped() {
    let m = SqliteMetadataStore::open_in_memory().unwrap();
    m.init().await.unwrap();
    let actor = m.create_actor(ActorInit::human("h", None)).await.unwrap();
    m.touch_presence(1, actor, Some("/x"), 100).await.unwrap();
    assert_eq!(m.active_presence(0).await.unwrap().len(), 1);

    let reaped = m.reap_presence(200).await.unwrap();
    assert_eq!(reaped, 1);
    assert!(m.active_presence(0).await.unwrap().is_empty());
}

// SEC (security audit #2/#11): traversal/separator path components are rejected
// at every metadata boundary, so a poisoned name (`..`) can never be *stored*
// and later escape during a host materialization — e.g. the sandbox's
// `export_tree` doing `host_dir.join("..")`, which would climb out of `lower/`
// and write arbitrary host files.
#[tokio::test]
async fn traversal_path_components_are_rejected_everywhere() {
    let fs = fixture().await;

    // The path API (origofs-api / MCP / SDK / CLI all funnel through `split`).
    for bad in ["/a/../b", "/../etc/passwd", "/./x", "/a/./b"] {
        assert!(
            matches!(fs.mkdir_p(bad).await, Err(OrigoFSError::InvalidPath(_))),
            "mkdir_p should reject {bad:?}"
        );
    }
    assert!(matches!(
        fs.write("/x/../y", b"z").await,
        Err(OrigoFSError::InvalidPath(_))
    ));

    // The inode-oriented FUSE/NFS boundary (raw name components).
    for bad in ["..", ".", "a/b", "x\0y", ""] {
        assert!(
            matches!(
                fs.vfs_create(INO_ROOT, bad, 0o644).await,
                Err(OrigoFSError::InvalidPath(_))
            ),
            "vfs_create should reject {bad:?}"
        );
        assert!(
            matches!(
                fs.vfs_mkdir(INO_ROOT, bad, 0o755).await,
                Err(OrigoFSError::InvalidPath(_))
            ),
            "vfs_mkdir should reject {bad:?}"
        );
    }

    // rename cannot introduce a traversal destination.
    fs.write("/ok", b"hi").await.unwrap();
    assert!(matches!(
        fs.vfs_rename(INO_ROOT, "ok", INO_ROOT, "..").await,
        Err(OrigoFSError::InvalidPath(_))
    ));

    // a normal nested path still works end to end.
    fs.mkdir_p("/real/dir").await.unwrap();
    fs.write("/real/dir/f", b"ok").await.unwrap();
    assert_eq!(&fs.read("/real/dir/f").await.unwrap()[..], b"ok");
}

// SEC (security audit #4): the object-graph decoders must bound their
// pre-allocation, so a tiny crafted object declaring a hostile entry count
// returns an error instead of aborting the process on a multi-GB
// `Vec::with_capacity`. Without the fix these lines abort the test binary.
#[test]
fn objectgraph_decoders_reject_hostile_counts_without_oom() {
    use origofs_core::{Commit, RefSnapshot, Tree};

    // Tree: magic | count = 0xFFFFFFFF, no entry bytes.
    let mut t = b"ORGT\x01".to_vec();
    t.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(Tree::decode(&t).is_err());

    // Commit: magic | tree(32) | parent_count = 0xFFFFFFFF.
    let mut c = b"ORGC\x01".to_vec();
    c.extend_from_slice(&[0u8; 32]);
    c.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(Commit::decode(&c).is_err());

    // RefSnapshot: magic | generation(8) | count = 0xFFFFFFFF.
    let mut r = b"ORGR\x01".to_vec();
    r.extend_from_slice(&0u64.to_le_bytes());
    r.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(RefSnapshot::decode(&r).is_err());

    // honest objects still round-trip.
    let tree = Tree { entries: vec![] };
    assert_eq!(Tree::decode(&tree.encode()).unwrap(), tree);
}

// H1: concurrent merges must not both "succeed" — a merge that loses the ref
// CAS must error, never report a Merged/FastForward commit that isn't the
// branch head (which would orphan the commit and drop history).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_merges_never_orphan_a_success() {
    let fs = Fs::new(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    // base has all three files; each side MODIFIES a different existing file so
    // the 3-way is a clean, conflict-free merge (Merged), letting the test focus
    // on the concurrent ref-CAS race rather than content conflicts.
    fs.write("/a", b"base\n").await.unwrap();
    fs.write("/b", b"base\n").await.unwrap();
    fs.write("/c", b"base\n").await.unwrap();
    fs.commit("x", "base").await.unwrap();
    fs.create_branch("feature").await.unwrap();
    fs.checkout("feature").await.unwrap();
    fs.write("/b", b"feature\n").await.unwrap();
    let feat = fs.commit("x", "feat").await.unwrap();
    fs.checkout("main").await.unwrap();
    fs.write("/c", b"main\n").await.unwrap();
    fs.commit("x", "main change").await.unwrap();

    let (f1, f2) = (fs.clone(), fs.clone());
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { f1.merge(feat, "x", "m1").await }),
        tokio::spawn(async move { f2.merge(feat, "x", "m2").await }),
    );
    let outcomes = [r1.unwrap(), r2.unwrap()];

    let head = fs.head_commit().await.unwrap();
    for o in &outcomes {
        if let Ok(origofs_core::MergeOutcome::Merged(h))
        | Ok(origofs_core::MergeOutcome::FastForward(h)) = o
        {
            assert_eq!(
                Some(*h),
                head,
                "a merge reported success for a commit that isn't the branch head (orphaned): {outcomes:?}"
            );
        }
    }
    // and the surviving history is well-formed: both changes are reachable.
    assert!(fs.is_ancestor(feat, head.unwrap()).await.unwrap());
}

// SEC (security audit #9): checkout/merge/rebuild replace the working tree in one
// transaction, so a rematerialize that fails partway — here, a commit whose tree
// references an object missing from the content store — rolls back and leaves the
// previous working tree intact, never half-emptied.
#[tokio::test]
async fn checkout_rolls_back_and_keeps_the_tree_on_a_missing_object() {
    use origofs_core::{Commit, ContentStore, Tree, TreeEntry, TreeKind};
    let fs = fixture().await;
    fs.write("/keep.txt", b"hello").await.unwrap();
    fs.commit("dan", "base").await.unwrap();

    // A branch whose commit tree references a file manifest that was never stored.
    let missing = Hash::of(b"a manifest that was never stored");
    let tree = Tree {
        entries: vec![TreeEntry {
            name: "bad.txt".to_string(),
            mode: 0o100644,
            kind: TreeKind::File,
            hash: missing,
        }],
    };
    let tree_hash = fs.content.put(&tree.encode()).await.unwrap();
    let commit = Commit {
        tree: tree_hash,
        parents: vec![],
        author: "x".to_string(),
        message: "broken".to_string(),
        timestamp: 0,
    };
    let commit_hash = fs.content.put(&commit.encode()).await.unwrap();
    fs.meta
        .set_ref("broken", &commit_hash.to_hex())
        .await
        .unwrap();

    // Checkout fails when materialize hits the missing object...
    assert!(fs.checkout("broken").await.is_err());
    // ...and the previous working tree survived intact (the transaction rolled back).
    assert_eq!(&fs.read("/keep.txt").await.unwrap()[..], b"hello");
}

// SEC (security audit #13/#18): accept_suggestion applies the proposed content
// atomically via write_as_expecting — the write only lands if the file is still
// at the base it was proposed against, so a change that slips in after the
// staleness check can't be silently clobbered (optimistic concurrency).
#[tokio::test]
async fn write_as_expecting_is_a_content_cas() {
    let fs = fixture().await;
    let actor = fs.create_human("dan", None).await.unwrap();
    let ctx = WriteCtx::actor(actor);
    fs.write_as(ctx, "/f.txt", b"base").await.unwrap();

    // the file's current content hash — what a suggestion records as its base.
    let base = fs
        .vfs_lookup(INO_ROOT, "f.txt")
        .await
        .unwrap()
        .unwrap()
        .content;

    // expecting the wrong base => Conflict, and the file is left untouched.
    let wrong = Some(Hash::of(b"not the current manifest"));
    assert!(matches!(
        fs.write_as_expecting(ctx, "/f.txt", b"proposed", wrong)
            .await,
        Err(OrigoFSError::Conflict(_))
    ));
    assert_eq!(&fs.read("/f.txt").await.unwrap()[..], b"base");

    // expecting the real base => the write applies.
    fs.write_as_expecting(ctx, "/f.txt", b"proposed", base)
        .await
        .unwrap();
    assert_eq!(&fs.read("/f.txt").await.unwrap()[..], b"proposed");
}

// SEC (security audit #21): mirror_refs bumps its generation via an atomic
// counter, not a read-then-write — so concurrent ref updates get distinct,
// strictly increasing generations and a recovery scan can pick the newest
// snapshot unambiguously.
#[tokio::test]
async fn bump_counter_is_atomic_and_distinct() {
    let m = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    m.init().await.unwrap();

    // sequential increments start at 1 and step by one
    assert_eq!(m.bump_counter("c").await.unwrap(), 1);
    assert_eq!(m.bump_counter("c").await.unwrap(), 2);

    // concurrent bumps never collide: three racers yield {3, 4, 5}
    let (a, b, c) = tokio::join!(
        {
            let m = m.clone();
            async move { m.bump_counter("c").await.unwrap() }
        },
        {
            let m = m.clone();
            async move { m.bump_counter("c").await.unwrap() }
        },
        {
            let m = m.clone();
            async move { m.bump_counter("c").await.unwrap() }
        },
    );
    let mut got = [a, b, c];
    got.sort();
    assert_eq!(got, [3, 4, 5]);
}
