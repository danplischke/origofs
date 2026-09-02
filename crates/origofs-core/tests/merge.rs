//! Three-way merge: fast-forward, clean text merge, text conflict + resolve,
//! chunk-granular binary merge, binary conflict, modify/delete, and locks.

use origofs_core::{
    Fs, Hash, INTERNAL_DIR, MemStore, MergeOutcome, SqliteMetadataStore, is_internal_path,
};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Random-looking, definitely-not-UTF-8 bytes (forces the binary merge path).
fn binary(len: usize, seed: u64) -> Vec<u8> {
    let mut v = pseudo_random(len, seed);
    v[0] = 0xff;
    v[1] = 0xfe;
    v
}

fn flip(data: &mut [u8], range: std::ops::Range<usize>) {
    for b in &mut data[range] {
        *b ^= 0xff;
    }
}

#[tokio::test]
async fn fast_forward_and_up_to_date() {
    let fs = fixture().await;
    fs.write("/a", b"1").await.unwrap();
    let c1 = fs.commit("a", "v1").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    fs.write("/b", b"2").await.unwrap();
    let c2 = fs.commit("a", "on dev").await.unwrap();

    fs.checkout("main").await.unwrap();
    match fs.merge(c2, "a", "merge").await.unwrap() {
        MergeOutcome::FastForward(h) => assert_eq!(h, c2),
        other => panic!("expected fast-forward, got {other:?}"),
    }
    assert_eq!(fs.head_commit().await.unwrap(), Some(c2));
    assert_eq!(&fs.read("/b").await.unwrap()[..], b"2");

    // merging an ancestor is a no-op
    assert!(matches!(
        fs.merge(c1, "a", "noop").await.unwrap(),
        MergeOutcome::AlreadyUpToDate
    ));
}

async fn diverge_text(
    fs: &Fs<SqliteMetadataStore, Arc<MemStore>>,
    base: &str,
    ours: &str,
    theirs: &str,
) -> (Hash, Hash) {
    fs.write("/f", base.as_bytes()).await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    fs.write("/f", theirs.as_bytes()).await.unwrap();
    let dev = fs.commit("a", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/f", ours.as_bytes()).await.unwrap();
    let main = fs.commit("a", "ours").await.unwrap();
    (main, dev)
}

#[tokio::test]
async fn clean_text_merge_records_two_parents() {
    let fs = fixture().await;
    let (main, dev) = diverge_text(
        &fs,
        "l1\nl2\nl3\n",
        "L1\nl2\nl3\n", // ours changed line 1
        "l1\nl2\nL3\n", // theirs changed line 3
    )
    .await;

    let merged = match fs.merge(dev, "a", "merge dev").await.unwrap() {
        MergeOutcome::Merged(h) => h,
        other => panic!("expected clean merge, got {other:?}"),
    };
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"L1\nl2\nL3\n");
    // both sides are ancestors of the merge commit
    assert!(fs.is_ancestor(main, merged).await.unwrap());
    assert!(fs.is_ancestor(dev, merged).await.unwrap());
    assert!(fs.conflicts().await.unwrap().is_empty());
}

#[tokio::test]
async fn overlapping_text_conflicts_then_resolves() {
    let fs = fixture().await;
    let (main, dev) = diverge_text(
        &fs,
        "a\nb\nc\n",
        "a\nX\nc\n", // ours changed line 2
        "a\nY\nc\n", // theirs changed line 2 differently
    )
    .await;

    match fs.merge(dev, "a", "merge").await.unwrap() {
        MergeOutcome::Conflicts(cs) => {
            assert_eq!(cs.len(), 1);
            assert_eq!(cs[0].path, "/f");
        }
        other => panic!("expected conflict, got {other:?}"),
    }
    // working tree has conflict markers; conflict is recorded
    let body = fs.read("/f").await.unwrap();
    assert!(body.windows(7).any(|w| w == b"<<<<<<<"));
    assert_eq!(fs.conflicts().await.unwrap().len(), 1);

    // resolve and commit -> a real 2-parent merge commit; conflicts cleared
    fs.write("/f", b"a\nRESOLVED\nc\n").await.unwrap();
    let merged = fs.commit("a", "resolve").await.unwrap();
    assert!(fs.is_ancestor(main, merged).await.unwrap());
    assert!(fs.is_ancestor(dev, merged).await.unwrap());
    assert!(fs.conflicts().await.unwrap().is_empty());
}

// Binary files are NOT line-merged over their chunk-hash sequence: doing so
// silently corrupts binaries with repeated chunks (diff3 mis-anchors on equal
// hash-lines and drops/duplicates a chunk). So any divergent binary 3-way is a
// conflict that keeps ours + surfaces theirs as a `.theirs` sibling — even when
// the two edits touch disjoint regions. Safety over convenience.
#[tokio::test]
async fn binary_divergent_edits_conflict_never_corrupt() {
    let fs = fixture().await;
    let base = binary(300_000, 1);
    fs.write("/bin", &base).await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // theirs edits the END
    fs.checkout("dev").await.unwrap();
    let mut theirs = base.clone();
    flip(&mut theirs, 299_968..300_000);
    fs.write("/bin", &theirs).await.unwrap();
    let dev = fs.commit("a", "end").await.unwrap();

    // ours edits the START (disjoint region)
    fs.checkout("main").await.unwrap();
    let mut ours = base.clone();
    flip(&mut ours, 0..32);
    fs.write("/bin", &ours).await.unwrap();
    fs.commit("a", "start").await.unwrap();

    match fs.merge(dev, "a", "merge").await.unwrap() {
        MergeOutcome::Conflicts(cs) => assert!(cs.iter().any(|c| c.path == "/bin")),
        other => panic!("expected binary conflict, got {other:?}"),
    }
    // ours is kept verbatim, theirs is preserved as a sibling — never a spliced,
    // silently-corrupt body.
    assert_eq!(fs.read("/bin").await.unwrap()[..], ours[..]);
    assert_eq!(fs.read("/bin.theirs").await.unwrap()[..], theirs[..]);
}

// The trivially-clean binary case still auto-resolves: if only one side changed
// the binary (the other equals base), take the changed side with no conflict.
#[tokio::test]
async fn binary_one_sided_edit_auto_resolves() {
    let fs = fixture().await;
    let base = binary(120_000, 7);
    fs.write("/bin", &base).await.unwrap();
    fs.write("/other.txt", b"base\n").await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // dev changes /bin; main modifies /other.txt (in the base), so this is a
    // genuine 3-way merge (not a fast-forward). /bin must resolve to theirs
    // (base == ours) with no conflict.
    fs.checkout("dev").await.unwrap();
    let mut theirs = base.clone();
    flip(&mut theirs, 60_000..60_064);
    fs.write("/bin", &theirs).await.unwrap();
    let dev = fs.commit("a", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/other.txt", b"main change\n").await.unwrap();
    fs.commit("a", "ours").await.unwrap();

    let outcome = fs.merge(dev, "a", "merge").await.unwrap();
    assert!(
        matches!(outcome, MergeOutcome::Merged(_)),
        "got {outcome:?}"
    );
    assert_eq!(fs.read("/bin").await.unwrap()[..], theirs[..]);
}

#[tokio::test]
async fn binary_overlapping_conflicts_keeps_both() {
    let fs = fixture().await;
    let base = binary(200_000, 2);
    fs.write("/bin", &base).await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // both edit the SAME start region, differently
    fs.checkout("dev").await.unwrap();
    let mut theirs = base.clone();
    for b in &mut theirs[0..32] {
        *b ^= 0x0f;
    }
    fs.write("/bin", &theirs).await.unwrap();
    let dev = fs.commit("a", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    let mut ours = base.clone();
    flip(&mut ours, 0..32);
    fs.write("/bin", &ours).await.unwrap();
    fs.commit("a", "ours").await.unwrap();

    match fs.merge(dev, "a", "merge").await.unwrap() {
        MergeOutcome::Conflicts(cs) => assert!(cs.iter().any(|c| c.path == "/bin")),
        other => panic!("expected binary conflict, got {other:?}"),
    }
    // never silently corrupts: ours kept, theirs surfaced as a sibling
    assert_eq!(fs.read("/bin").await.unwrap()[..], ours[..]);
    assert_eq!(fs.read("/bin.theirs").await.unwrap()[..], theirs[..]);
}

#[tokio::test]
async fn modify_delete_conflicts() {
    let fs = fixture().await;
    fs.write("/f", b"hi").await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    fs.unlink("/f").await.unwrap();
    let dev = fs.commit("a", "delete").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/f", b"hello").await.unwrap();
    fs.commit("a", "modify").await.unwrap();

    match fs.merge(dev, "a", "merge").await.unwrap() {
        MergeOutcome::Conflicts(cs) => {
            assert_eq!(cs[0].path, "/f");
            assert_eq!(cs[0].kind, "modify/delete");
        }
        other => panic!("expected modify/delete conflict, got {other:?}"),
    }
    // ours is kept
    assert_eq!(&fs.read("/f").await.unwrap()[..], b"hello");
}

/// `merge_base` must return the **fork point**, not some older shared commit.
///
/// Every other three-way test here forks after a single commit, so the only common
/// ancestor is that commit and any ranking rule looks correct. With more than one
/// commit of shared trunk, ranking common ancestors by hop distance picks the root
/// (a common ancestor of every pair) instead of the fork point.
#[tokio::test]
async fn merge_base_is_the_fork_point_not_the_root() {
    let fs = fixture().await;

    fs.write("/f", b"c1").await.unwrap();
    let c1 = fs.commit("a", "c1").await.unwrap();
    fs.write("/f", b"c2").await.unwrap();
    let c2 = fs.commit("a", "c2").await.unwrap();
    fs.write("/f", b"c3").await.unwrap();
    let fork = fs.commit("a", "c3 (fork point)").await.unwrap();

    fs.create_branch("dev").await.unwrap();
    fs.checkout("dev").await.unwrap();
    fs.write("/dev-only", b"d").await.unwrap();
    let theirs = fs.commit("a", "on dev").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/main-only", b"m").await.unwrap();
    let ours = fs.commit("a", "on main").await.unwrap();

    assert_eq!(
        fs.merge_base(ours, theirs).await.unwrap(),
        Some(fork),
        "merge base must be the fork point, not an older shared commit"
    );
    // Sanity: the older trunk commits really are common ancestors, so this is a
    // choice among several and not a one-candidate accident.
    assert!(fs.is_ancestor(c1, ours).await.unwrap());
    assert!(fs.is_ancestor(c1, theirs).await.unwrap());
    assert!(fs.is_ancestor(c2, theirs).await.unwrap());
}

/// The consequence of a stale merge base: a file created on the trunk *before* the
/// fork and deleted on one side is **resurrected** by the merge.
///
/// With the correct base the entry is present in it, so `b == Some(te)` reads as
/// "theirs never touched it, ours deleted it" and the delete wins. With the root as
/// the base the entry is absent, so the same state reads as "they added it and we
/// never had it" and the file comes back.
#[tokio::test]
async fn merge_does_not_resurrect_a_file_deleted_after_the_fork() {
    let fs = fixture().await;

    fs.write("/keep", b"keep").await.unwrap();
    fs.commit("a", "root").await.unwrap();
    // Created on the trunk, one commit *after* the root — so it exists at the fork
    // point but not at the root.
    fs.write("/doomed", b"doomed").await.unwrap();
    fs.commit("a", "add doomed").await.unwrap();

    fs.create_branch("dev").await.unwrap();
    fs.checkout("dev").await.unwrap();
    fs.write("/dev-only", b"d").await.unwrap();
    let theirs = fs.commit("a", "on dev (doomed untouched)").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.remove("/doomed").await.unwrap();
    fs.commit("a", "delete doomed").await.unwrap();

    match fs.merge(theirs, "a", "merge dev").await.unwrap() {
        MergeOutcome::Merged(_) => {}
        other => panic!("expected a clean merge, got {other:?}"),
    }
    assert!(
        fs.stat("/doomed").await.is_err(),
        "a file deleted on our side and untouched on theirs must stay deleted"
    );
    assert_eq!(&fs.read("/dev-only").await.unwrap()[..], b"d");
    assert_eq!(&fs.read("/keep").await.unwrap()[..], b"keep");
}

/// A criss-cross history has several maximal common ancestors. We don't build a
/// virtual recursive base, but the choice must be **deterministic** — the same pair
/// of commits must always produce the same base, or a merge isn't reproducible.
#[tokio::test]
async fn merge_base_is_deterministic_on_a_criss_cross() {
    let fs = fixture().await;

    fs.write("/f", b"root").await.unwrap();
    fs.commit("a", "root").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    fs.write("/d", b"1").await.unwrap();
    let d1 = fs.commit("a", "d1").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/m", b"1").await.unwrap();
    fs.commit("a", "m1").await.unwrap();
    // Cross-merge both ways, so each side has the other as an ancestor.
    fs.merge(d1, "a", "main <- dev").await.unwrap();
    let m2 = fs.head_commit().await.unwrap().unwrap();

    fs.checkout("dev").await.unwrap();
    fs.write("/d", b"2").await.unwrap();
    let d2 = fs.commit("a", "d2").await.unwrap();

    let first = fs.merge_base(m2, d2).await.unwrap();
    assert!(first.is_some(), "a criss-cross still has a common ancestor");
    for _ in 0..8 {
        assert_eq!(
            fs.merge_base(m2, d2).await.unwrap(),
            first,
            "merge base must not depend on hash-set iteration order"
        );
    }
}

#[tokio::test]
async fn locks_are_exclusive() {
    let fs = fixture().await;
    assert!(fs.lock("/f", "alice").await.unwrap());
    assert!(!fs.lock("/f", "bob").await.unwrap(), "already locked");
    assert_eq!(fs.locks().await.unwrap().len(), 1);
    assert!(!fs.unlock("/f", "bob").await.unwrap(), "not bob's lock");
    assert!(fs.unlock("/f", "alice").await.unwrap());
    assert!(fs.locks().await.unwrap().is_empty());
}

// --- origofs's own state under `/.origofs` (issue #142) ---------------------
//
// The sidecars live in the working tree so they are versioned, collected and
// deduplicated like any other file. The price is that machinery written for user
// content reaches them, and `merge` was the case where that showed: a co-edited
// document checkpointed on two branches produced a *binary conflict* on a hidden
// file nobody can read or resolve, plus a `.theirs` sibling nobody will ever
// find.

/// The boundary rule, tested directly because `/.origofs-bench` is a real path
/// (the `perf` bench directory) and a `starts_with` would swallow it.
#[test]
fn internal_paths_match_on_the_directory_boundary() {
    assert!(is_internal_path(INTERNAL_DIR));
    assert!(is_internal_path("/.origofs/ydoc"));
    assert!(is_internal_path("/.origofs/ydoc/2f6e6f7465732e6d64"));

    assert!(!is_internal_path("/.origofs-bench"));
    assert!(!is_internal_path("/.origofs-bench/bin"));
    assert!(!is_internal_path("/.origofsX"));
    assert!(!is_internal_path("/"));
    assert!(
        !is_internal_path("/docs/.origofs"),
        "only the tree root's copy"
    );
}

// The headline property: two branches that both checkpointed a co-edited document
// merge without asking anyone to adjudicate the CRDT state, while the *document*
// conflicts normally.
#[tokio::test]
async fn diverging_internal_state_never_conflicts_and_leaves_no_theirs_sibling() {
    let fs = fixture().await;
    fs.mkdir_p("/.origofs/ydoc").await.unwrap();
    // Distinct seeds: `binary` masks `seed | 1`, so e.g. 2 and 3 collide — which
    // would leave the sidecar identical on both sides and pass this test on a
    // merge that never diverged. The assertion below pins that.
    let (base, theirs, ours) = (binary(4_000, 11), binary(4_000, 21), binary(4_000, 31));
    assert!(
        base != theirs && theirs != ours && base != ours,
        "the three sides must genuinely differ or this test proves nothing"
    );
    fs.write("/.origofs/ydoc/aa", &base).await.unwrap();
    fs.write("/doc.md", b"base\n").await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    fs.write("/.origofs/ydoc/aa", &theirs).await.unwrap();
    fs.write("/doc.md", b"theirs\n").await.unwrap();
    let dev = fs.commit("a", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/.origofs/ydoc/aa", &ours).await.unwrap();
    fs.write("/doc.md", b"ours\n").await.unwrap();
    fs.commit("a", "ours").await.unwrap();

    // The document itself must still conflict — otherwise this test would pass on
    // a merge that never diverged at all.
    match fs.merge(dev, "a", "merge").await.unwrap() {
        MergeOutcome::Conflicts(cs) => {
            assert!(
                cs.iter().any(|c| c.path == "/doc.md"),
                "the user's document must still conflict: {cs:?}"
            );
            assert!(
                !cs.iter().any(|c| is_internal_path(&c.path)),
                "origofs's own state must never be reported as a conflict: {cs:?}"
            );
        }
        other => panic!("expected a conflict on /doc.md, got {other:?}"),
    }

    // Ours is kept verbatim — never a diff3-spliced blob — and no unreachable
    // sibling is left behind in the hidden directory.
    assert_eq!(fs.read("/.origofs/ydoc/aa").await.unwrap()[..], ours[..]);
    assert!(
        fs.read("/.origofs/ydoc/aa.theirs").await.is_err(),
        "a `.theirs` sibling under {INTERNAL_DIR} is unreachable junk"
    );
    assert!(
        !fs.ls("/.origofs/ydoc")
            .await
            .unwrap()
            .iter()
            .any(|e| e.name.ends_with(".theirs")),
        "no `.theirs` entry may be listed under {INTERNAL_DIR}"
    );
}

// The suppression is scoped by path, not by "hidden-looking": a user directory
// whose name merely begins with the reserved one conflicts like any other.
#[tokio::test]
async fn a_lookalike_of_the_internal_dir_still_conflicts() {
    let fs = fixture().await;
    fs.mkdir_p("/.origofs-bench").await.unwrap();
    fs.write("/.origofs-bench/bin", &binary(4_000, 11))
        .await
        .unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    fs.write("/.origofs-bench/bin", &binary(4_000, 21))
        .await
        .unwrap();
    let dev = fs.commit("a", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/.origofs-bench/bin", &binary(4_000, 31))
        .await
        .unwrap();
    fs.commit("a", "ours").await.unwrap();

    match fs.merge(dev, "a", "merge").await.unwrap() {
        MergeOutcome::Conflicts(cs) => assert!(
            cs.iter().any(|c| c.path == "/.origofs-bench/bin"),
            "a lookalike path must conflict normally: {cs:?}"
        ),
        other => panic!("expected a conflict, got {other:?}"),
    }
}

// The suppression must not swallow the other side's entries: `/.origofs` is a
// directory and still recurses, so a document checkpointed only on their branch
// keeps its sidecar through the merge.
#[tokio::test]
async fn internal_state_added_on_one_side_survives_the_merge() {
    let fs = fixture().await;
    fs.mkdir_p("/.origofs/ydoc").await.unwrap();
    fs.write("/.origofs/ydoc/ours", &binary(1_000, 1))
        .await
        .unwrap();
    fs.write("/a.md", b"base\n").await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    let theirs = binary(1_000, 9);
    fs.write("/.origofs/ydoc/theirs", &theirs).await.unwrap();
    let dev = fs.commit("a", "theirs").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/a.md", b"ours\n").await.unwrap();
    fs.commit("a", "ours").await.unwrap();

    let out = fs.merge(dev, "a", "merge").await.unwrap();
    assert!(
        !matches!(out, MergeOutcome::Conflicts(_)),
        "nothing here conflicts: {out:?}"
    );
    assert_eq!(
        fs.read("/.origofs/ydoc/theirs").await.unwrap()[..],
        theirs[..]
    );
    assert!(fs.read("/.origofs/ydoc/ours").await.is_ok());
}

// modify/delete on internal state is the same non-question: resolve it, silently.
#[tokio::test]
async fn internal_state_modify_delete_does_not_conflict() {
    let fs = fixture().await;
    fs.mkdir_p("/.origofs/ydoc").await.unwrap();
    fs.write("/.origofs/ydoc/aa", &binary(1_000, 1))
        .await
        .unwrap();
    fs.write("/keep.md", b"base\n").await.unwrap();
    fs.commit("a", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // They delete the sidecar (a `checkout` on their side would do this too).
    fs.checkout("dev").await.unwrap();
    fs.remove("/.origofs/ydoc/aa").await.unwrap();
    let dev = fs.commit("a", "dropped it").await.unwrap();

    // We re-checkpointed it.
    fs.checkout("main").await.unwrap();
    let ours = binary(1_000, 4);
    fs.write("/.origofs/ydoc/aa", &ours).await.unwrap();
    fs.commit("a", "rewrote it").await.unwrap();

    let out = fs.merge(dev, "a", "merge").await.unwrap();
    assert!(
        !matches!(out, MergeOutcome::Conflicts(_)),
        "a modify/delete on origofs's own state is not the user's to resolve: {out:?}"
    );
    assert_eq!(fs.read("/.origofs/ydoc/aa").await.unwrap()[..], ours[..]);
}
