//! Logical operations commit all-or-nothing.
//!
//! `docs/DESIGN.md` §7 claims "operations that touch several rows run inside a
//! single `MetaTxn`". That was true of each *step* but not of the operations:
//! `commit`, `checkout`, and all three `merge` paths swapped the working tree in
//! one transaction and then wrote the refs and conflict rows describing it in
//! several more. An interruption between the parts left a workspace in a state no
//! caller could produce deliberately.
//!
//! Crash-level coverage needs process-kill injection at the metadata seam (a
//! later phase). What is deterministic today is the *error* path: make a step
//! fail, and require that nothing before it stuck.

use origofs_core::{
    Commit, ContentStore, Fs, Hash, MemStore, MetadataStore, SqliteMetadataStore, SuggestionStatus,
    Tree, TreeEntry, TreeKind, WriteCtx,
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

/// A commit whose tree names a manifest that was never stored, so materializing
/// it fails partway. The cheapest way to fail a step that runs *after* the ref
/// has been advanced.
async fn commit_with_missing_content(
    fs: &Fs<SqliteMetadataStore, Arc<MemStore>>,
    parents: Vec<Hash>,
) -> Hash {
    let tree = Tree {
        entries: vec![TreeEntry {
            name: "bad.txt".to_string(),
            mode: 0o100644,
            kind: TreeKind::File,
            hash: Hash::of(b"a manifest that was never stored"),
        }],
    };
    let tree_hash = fs.content.put(&tree.encode()).await.unwrap();
    let commit = Commit {
        tree: tree_hash,
        parents,
        author: "x".to_string(),
        message: "unmaterializable".to_string(),
        timestamp: 0,
    };
    fs.content.put(&commit.encode()).await.unwrap()
}

/// A fast-forward that fails while materializing must not leave the branch
/// advanced.
///
/// The ref advance came first, deliberately — a concurrent branch move should
/// abort before the working tree is touched. But it *committed* first too, so a
/// failure afterwards left the branch pointing at a commit whose tree had never
/// been materialized: `log` shows the merge, the files don't. The next commit
/// then snapshots the stale tree on top, silently reverting it.
#[tokio::test]
async fn a_failed_fast_forward_does_not_advance_the_branch() {
    let fs = fixture().await;
    fs.write("/keep.txt", b"hello").await.unwrap();
    let base = fs.commit("dan", "base").await.unwrap();

    // A descendant of `base`, so the merge takes the fast-forward path — but one
    // that cannot be materialized.
    let theirs = commit_with_missing_content(&fs, vec![base]).await;

    assert!(
        fs.merge(theirs, "dan", "ff").await.is_err(),
        "materializing a missing manifest must fail"
    );

    assert_eq!(
        fs.branch_head("main").await.unwrap(),
        Some(base),
        "a failed fast-forward must leave the branch where it was"
    );
    assert_eq!(fs.head_commit().await.unwrap(), Some(base));
    // The working tree is the one that branch describes.
    assert_eq!(&fs.read("/keep.txt").await.unwrap()[..], b"hello");
    assert!(fs.stat("/bad.txt").await.is_err());
}

/// The same invariant for the three-way path: the ref advance and the merged tree
/// are one unit.
///
/// A guard, not a reproduction — here the merge fails while *reading* the trees,
/// which is before the ref advance, so the old code passed this too. It pins the
/// property so a future reordering can't quietly break it.
#[tokio::test]
async fn a_failed_three_way_merge_does_not_advance_the_branch() {
    let fs = fixture().await;
    fs.write("/f.txt", b"base\n").await.unwrap();
    let base = fs.commit("dan", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    // Diverge: a real commit on main, and on "dev" a sibling of it that cannot
    // be materialized — so the merge is three-way, not a fast-forward.
    fs.write("/f.txt", b"ours\n").await.unwrap();
    let ours = fs.commit("dan", "ours").await.unwrap();
    let theirs = commit_with_missing_content(&fs, vec![base]).await;
    fs.meta.set_ref("dev", &theirs.to_hex()).await.unwrap();

    assert!(fs.merge(theirs, "dan", "merge").await.is_err());
    assert_eq!(
        fs.branch_head("main").await.unwrap(),
        Some(ours),
        "a failed merge must leave the branch where it was"
    );
    assert_eq!(&fs.read("/f.txt").await.unwrap()[..], b"ours\n");
}

/// A conflicted merge leaves three pieces of state — the marked-up tree, the
/// conflict rows, and `MERGE_HEAD`. They must appear together or not at all: a
/// tree full of `<<<<<<<` with no `MERGE_HEAD` reads as an ordinary dirty tree,
/// and committing it produces a single-parent commit containing the markers,
/// dropping the other side from history entirely.
///
/// A guard on the happy path. Observing the *torn* version needs a crash between
/// the writes, which arrives with the metadata-seam injection harness; what this
/// pins is that all three are produced and that committing clears all three.
#[tokio::test]
async fn a_conflicted_merge_records_all_of_its_state() {
    let fs = fixture().await;
    fs.write("/f.txt", b"one\ntwo\nthree\n").await.unwrap();
    fs.commit("dan", "base").await.unwrap();
    fs.create_branch("dev").await.unwrap();

    fs.checkout("dev").await.unwrap();
    fs.write("/f.txt", b"one\nDEV\nthree\n").await.unwrap();
    let theirs = fs.commit("dan", "dev").await.unwrap();

    fs.checkout("main").await.unwrap();
    fs.write("/f.txt", b"one\nMAIN\nthree\n").await.unwrap();
    let ours = fs.commit("dan", "main").await.unwrap();

    let outcome = fs.merge(theirs, "dan", "merge").await.unwrap();
    assert!(
        matches!(outcome, origofs_core::MergeOutcome::Conflicts(_)),
        "expected a conflict, got {outcome:?}"
    );

    // All three, together.
    let body = fs.read("/f.txt").await.unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("<<<<<<<"),
        "the working tree should carry conflict markers"
    );
    assert!(
        !fs.conflicts().await.unwrap().is_empty(),
        "conflicts must be recorded"
    );
    assert_eq!(
        fs.meta.get_ref("MERGE_HEAD").await.unwrap(),
        Some(theirs.to_hex()),
        "MERGE_HEAD must record what is being merged"
    );
    // The branch itself did not move; that waits for the user's commit.
    assert_eq!(fs.branch_head("main").await.unwrap(), Some(ours));

    // Resolving and committing clears all of the merge state in one go, and the
    // result is a real two-parent merge commit.
    fs.write("/f.txt", b"one\nRESOLVED\nthree\n").await.unwrap();
    let merged = fs.commit("dan", "resolved").await.unwrap();
    assert!(fs.meta.get_ref("MERGE_HEAD").await.unwrap().is_none());
    assert!(fs.conflicts().await.unwrap().is_empty());
    let c = Commit::decode(&fs.content.get(&merged).await.unwrap()).unwrap();
    assert_eq!(c.parents.len(), 2, "the merge must keep both parents");
}

/// Two reviewers accepting the same suggestion: exactly one wins, and the loser
/// is told so.
///
/// `resolve_suggestion` returns whether the pending→accepted transition applied —
/// it exists to detect precisely this race — and the return value was discarded.
///
/// Sequentially the old code also refused this, via a different route: the second
/// accept trips the staleness check because the file has moved. The check matters
/// for the case that doesn't, where two accepts both apply; this test pins the
/// observable contract (one winner, and the recorded approver is that winner)
/// rather than the internal route to it.
#[tokio::test]
async fn a_second_accept_of_the_same_suggestion_is_refused() {
    let fs = fixture().await;
    let author = fs.create_human("author", None).await.unwrap();
    let r1 = fs.create_human("reviewer-1", None).await.unwrap();
    let r2 = fs.create_human("reviewer-2", None).await.unwrap();
    fs.write("/f.txt", b"one\n").await.unwrap();

    let id = fs
        .suggest(WriteCtx::actor(author), "/f.txt", b"one\ntwo\n", None)
        .await
        .unwrap();

    fs.accept_suggestion(id, WriteCtx::actor(r1)).await.unwrap();
    let second = fs.accept_suggestion(id, WriteCtx::actor(r2)).await;
    assert!(
        second.is_err(),
        "a suggestion must not be accepted twice, got {second:?}"
    );

    let s = fs.get_suggestion(id).await.unwrap().unwrap();
    assert_eq!(s.status, SuggestionStatus::Accepted);
    assert_eq!(
        s.resolved_by,
        Some(r1),
        "the recorded approver must be the one whose accept applied"
    );
    assert_eq!(&fs.read("/f.txt").await.unwrap()[..], b"one\ntwo\n");
}
