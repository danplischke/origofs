//! Regression tests for the failure-surface audit (issue #34, Phase 1).
//! Each test pins a specific fix so the failure mode can't silently return.

use origofs_core::{
    ActorInit, ChunkRef, Fs, Hash, INO_ROOT, Manifest, MemStore, MetadataStore, OrigoFSError,
    Owner, SqliteMetadataStore, WriteCtx,
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
    assert_eq!(Manifest::decode(&honest.encode().unwrap()).unwrap(), honest);

    // same chunks, but a wildly inflated size field
    let liar = Manifest {
        size: u64::MAX,
        chunks: honest.chunks.clone(),
    };
    assert!(matches!(
        Manifest::decode(&liar.encode().unwrap()),
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
        .suggest(
            WriteCtx::actor(actor),
            "/f.txt",
            b"one\ntwo\n",
            Some("add"),
            None,
        )
        .await
        .unwrap();

    // A GC pass on the (otherwise quiescent) store must keep the proposed blob.
    fs.gc_with_grace(0).await.unwrap();

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
        .suggest(WriteCtx::actor(actor), "/e.txt", b"", None, None)
        .await
        .unwrap();
    fs.accept_suggestion(sid, WriteCtx::actor(reviewer))
        .await
        .unwrap();
    // still present, now empty
    assert_eq!(&fs.read("/e.txt").await.unwrap()[..], b"");

    let del = fs
        .suggest_delete(WriteCtx::actor(actor), "/e.txt", None, None)
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
                fs.vfs_create(INO_ROOT, bad, 0o644, Owner::ROOT).await,
                Err(OrigoFSError::InvalidPath(_))
            ),
            "vfs_create should reject {bad:?}"
        );
        assert!(
            matches!(
                fs.vfs_mkdir(INO_ROOT, bad, 0o755, Owner::ROOT).await,
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

// SEC: ref names are the *other* half of the traversal story. A branch name is
// written to the ref table as a plain key, but the git-interop layer turns it back
// into a host path (`refs/heads/<name>`) and interpolates it into `HEAD` — so an
// absolute or `..`-bearing name writes outside the exported repository, and an
// embedded newline injects a second line into `HEAD`. Reject at the door, so a
// hostile name can never be stored.
#[tokio::test]
async fn hostile_ref_names_are_rejected_everywhere() {
    let fs = fixture().await;
    fs.write("/f", b"hi").await.unwrap();
    fs.commit("a", "c1").await.unwrap();

    let hostile = [
        "/etc/cron.d/pwn", // absolute: `Path::join` discards the base
        "../../../../home/u/.ssh/authorized_keys", // climbs out of refs/heads
        "a/../../b",       // traversal mid-name
        "main\nref: refs/heads/evil", // newline injects a line into HEAD
        "main\0evil",      // NUL
        "-delete",         // reads as a flag to anything forwarding it
        "feature branch",  // whitespace
        "refs/heads/",     // trailing slash -> empty component
        "a//b",            // empty component
        "x.lock",          // git refuses; would collide with its locks
        "he^ad",           // refspec metacharacter
        "v1..v2",          // range syntax
        "@{now}",          // reflog syntax
        ".",
        "..",
        "",
    ];
    for bad in hostile {
        assert!(
            fs.create_branch(bad).await.is_err(),
            "create_branch should reject {bad:?}"
        );
        assert!(
            fs.set_branch(bad, Hash::of(b"x")).await.is_err(),
            "set_branch should reject {bad:?}"
        );
        assert!(
            fs.cas_branch(bad, None, Hash::of(b"x")).await.is_err(),
            "cas_branch should reject {bad:?}"
        );
        assert!(
            fs.checkout(bad).await.is_err(),
            "checkout should reject {bad:?}"
        );
        // Nothing hostile reached the ref table by any of those doors.
        assert!(
            fs.branch_head(bad).await.unwrap().is_none(),
            "{bad:?} must not have been stored"
        );
    }

    // Ordinary names — including the ones that merely *look* like the rejected
    // ones — still work, so the rule isn't over-broad.
    for ok in [
        "dev",
        "feature/login",
        "release-1.2",
        "fix.2",
        "user/x/deep/name",
    ] {
        fs.create_branch(ok)
            .await
            .unwrap_or_else(|e| panic!("create_branch should accept {ok:?}: {e}"));
        fs.checkout(ok).await.unwrap();
    }
    // The internal refs satisfy the same rule, so they need no carve-out.
    origofs_core::validate_ref_name("HEAD").unwrap();
    origofs_core::validate_ref_name("MERGE_HEAD").unwrap();
}

// SEC: the *third* traversal door. Path components are validated on the way in
// and ref names at the ref table — but a tree entry's name arrives from the
// content store, and `materialize_into_txn` fed it straight to `add_dentry`. An
// object's address proves its bytes are what was written, not that the writer was
// honest: a tree from a shared bucket, a `git import`, or a `resync` peer can name
// an entry `..`. Checkout/merge/rebuild must refuse it, and leave the working tree
// alone when they do.
#[tokio::test]
async fn hostile_tree_entry_names_are_rejected_on_materialize() {
    use origofs_core::{Commit, Tree, TreeEntry, TreeKind};

    for bad in ["..", ".", "a/b", "x\0y", ""] {
        let fs = fixture().await;
        fs.write("/legit", b"original").await.unwrap();
        fs.commit("a", "base").await.unwrap();

        // A real manifest for the hostile entry to point at: write a file the
        // ordinary way and reuse its content address.
        fs.write("/payload-src", b"payload").await.unwrap();
        let mhash = fs.stat("/payload-src").await.unwrap().content.unwrap();
        let tree = Tree {
            entries: vec![TreeEntry {
                name: bad.to_string(),
                mode: 0o100644,
                kind: TreeKind::File,
                hash: mhash,
            }],
        };
        let tree_hash = fs.put_object(&tree.encode().unwrap()).await.unwrap();
        let commit = Commit {
            tree: tree_hash,
            parents: vec![],
            author: "attacker".into(),
            message: "poisoned tree".into(),
            timestamp: 0,
        };
        let chash = fs.put_object(&commit.encode().unwrap()).await.unwrap();
        fs.set_branch("evil", chash).await.unwrap();

        let err = fs.checkout("evil").await;
        assert!(
            matches!(err, Err(OrigoFSError::InvalidPath(_))),
            "checkout of a tree naming {bad:?} should be rejected, got {err:?}"
        );

        // The transaction rolled back: the pre-existing tree is intact and the
        // hostile name was never stored.
        assert_eq!(&fs.read("/legit").await.unwrap()[..], b"original");
        let names: Vec<String> = fs
            .ls("/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            names.iter().all(|n| n == "legit" || n == "payload-src"),
            "no entry from the poisoned tree may survive, got {names:?}"
        );
    }
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
    assert_eq!(Tree::decode(&tree.encode().unwrap()).unwrap(), tree);
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
    let tree_hash = fs.content.put(&tree.encode().unwrap()).await.unwrap();
    let commit = Commit {
        tree: tree_hash,
        parents: vec![],
        author: "x".to_string(),
        message: "broken".to_string(),
        timestamp: 0,
    };
    let commit_hash = fs.content.put(&commit.encode().unwrap()).await.unwrap();
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

// A6 (issue #70): path components are opaque, byte-exact UTF-8. The engine does
// NOT Unicode-normalize or case-fold names, so the NFC and NFD encodings of the
// same grapheme are two DISTINCT files, and case variants are distinct too. This
// pins the contract — a future change that silently normalized names would alias
// files and corrupt dedup/attribution keyed on the exact name — and documents the
// boundary with case-insensitive host mounts (e.g. macOS/APFS default, some FUSE
// layers), where it is the OS, not the engine, that may collapse these.
#[tokio::test]
async fn names_are_byte_exact_no_unicode_normalization_or_casefold() {
    let fs = fixture().await;

    // "café.txt" in NFC (U+00E9) vs NFD ("e" + U+0301 combining acute): same
    // rendered glyph, different bytes.
    let nfc = "/caf\u{00e9}.txt";
    let nfd = "/cafe\u{0301}.txt";
    assert_ne!(
        nfc.as_bytes(),
        nfd.as_bytes(),
        "setup: NFC and NFD forms must differ at the byte level"
    );

    fs.write(nfc, b"nfc-body").await.unwrap();
    fs.write(nfd, b"nfd-body").await.unwrap();

    // Two distinct files, each with its own content — no aliasing/normalization.
    assert_eq!(&fs.read(nfc).await.unwrap()[..], b"nfc-body");
    assert_eq!(&fs.read(nfd).await.unwrap()[..], b"nfd-body");
    let nfc_ino = fs
        .vfs_lookup(INO_ROOT, "caf\u{00e9}.txt")
        .await
        .unwrap()
        .unwrap()
        .ino;
    let nfd_ino = fs
        .vfs_lookup(INO_ROOT, "cafe\u{0301}.txt")
        .await
        .unwrap()
        .unwrap()
        .ino;
    assert_ne!(
        nfc_ino, nfd_ino,
        "NFC and NFD names must be separate inodes"
    );

    // Case is significant: "README" and "readme" are different files.
    fs.write("/README", b"upper").await.unwrap();
    fs.write("/readme", b"lower").await.unwrap();
    assert_eq!(&fs.read("/README").await.unwrap()[..], b"upper");
    assert_eq!(&fs.read("/readme").await.unwrap()[..], b"lower");
    let upper = fs
        .vfs_lookup(INO_ROOT, "README")
        .await
        .unwrap()
        .unwrap()
        .ino;
    let lower = fs
        .vfs_lookup(INO_ROOT, "readme")
        .await
        .unwrap()
        .unwrap()
        .ino;
    assert_ne!(upper, lower, "case variants must be separate inodes");
}

// SEC/L: renaming a directory *into itself* must be refused (POSIX `EINVAL`).
//
// `rename("/a", "/a/b/a2")` makes `a` a child of its own child. The subtree is
// then unreachable from the root, so it disappears from `ls` and from
// `build_tree` — and, decisively, from `mark_working`, which is what GC uses to
// decide what is live. So an ordinary `mv` silently destroyed data: the rows
// stayed in the database while GC reclaimed all of the content they pointed at.
#[tokio::test]
async fn rename_into_own_descendant_is_refused() {
    let fs = fixture().await;
    fs.mkdir_p("/a/b/deep").await.unwrap();
    fs.write("/a/payload.txt", b"precious").await.unwrap();

    for (from, to) in [("/a", "/a/b/a2"), ("/a", "/a/a2"), ("/a/b", "/a/b/deep/x")] {
        let err = fs.rename(from, to).await;
        assert!(
            matches!(err, Err(OrigoFSError::InvalidArgument(_))),
            "rename({from:?}, {to:?}) must be refused, got {err:?}"
        );
    }

    // Still reachable and intact, and — the part that mattered — still live as
    // far as GC is concerned.
    assert_eq!(&fs.read("/a/payload.txt").await.unwrap()[..], b"precious");
    fs.gc_with_grace(0).await.unwrap();
    assert_eq!(
        &fs.read("/a/payload.txt").await.unwrap()[..],
        b"precious",
        "content of a subtree that a refused rename would have orphaned"
    );

    // The inode surface (FUSE/NFS `mv`) reaches the same cycle.
    let a = fs.vfs_lookup(INO_ROOT, "a").await.unwrap().unwrap().ino;
    let b = fs.vfs_lookup(a, "b").await.unwrap().unwrap().ino;
    let err = fs.vfs_rename(INO_ROOT, "a", b, "a2").await;
    assert!(
        matches!(err, Err(OrigoFSError::InvalidArgument(_))),
        "vfs_rename into a descendant must be refused, got {err:?}"
    );

    // Ordinary moves — including into a sibling and back out of a subtree — still
    // work, so the guard isn't over-broad.
    fs.mkdir_p("/elsewhere").await.unwrap();
    fs.rename("/a/b", "/elsewhere/b").await.unwrap();
    fs.rename("/elsewhere", "/moved").await.unwrap();
    assert!(fs.stat("/moved/b").await.is_ok());
    assert_eq!(&fs.read("/a/payload.txt").await.unwrap()[..], b"precious");
}
