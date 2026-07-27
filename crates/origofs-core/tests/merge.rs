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
