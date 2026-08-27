//! Three-way merge: fast-forward, clean text merge, text conflict + resolve,
//! chunk-granular binary merge, binary conflict, modify/delete, and locks.

use origofs_core::{Fs, Hash, MemStore, MergeOutcome, SqliteMetadataStore};
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
