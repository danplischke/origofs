//! Per-file history: `log_path` resolves one path by descending each commit's
//! tree, so its cost is flat in repository size where a pairwise `diff` is not.
//!
//! The load-bearing test is [`agrees_with_the_pairwise_diff_baseline`]: the fast
//! path is only worth having if it answers exactly what the slow, obvious one
//! does, and nothing else here would catch it drifting.

use origofs_core::{DiffStatus, Fs, MemStore, SqliteMetadataStore};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

/// The revisions of `path`, computed the obvious expensive way: flatten both
/// sides of every adjacent commit pair.
///
/// Pairwise only — the root commit has no pair, so it is not covered here and
/// the callers assert it separately. Deriving it from `log_path` would make this
/// baseline agree with the thing it exists to check.
async fn pairwise_baseline(fs: &Fs<SqliteMetadataStore, Arc<MemStore>>, path: &str) -> Vec<String> {
    let log = fs.log().await.unwrap();
    let mut out = Vec::new();
    for w in log.windows(2) {
        let d = fs
            .diff(&w[1].hash.to_hex(), &w[0].hash.to_hex())
            .await
            .unwrap();
        if let Some(e) = d.iter().find(|e| e.path == path) {
            out.push(format!("{} {}", e.status.sigil(), w[0].commit.message));
        }
    }
    out
}

fn rendered(revs: &[origofs_core::PathRevision]) -> Vec<String> {
    revs.iter()
        .map(|r| format!("{} {}", r.status.sigil(), r.commit.commit.message))
        .collect()
}

#[tokio::test]
async fn agrees_with_the_pairwise_diff_baseline() {
    let fs = fixture().await;
    fs.mkdir_p("/src/a/b").await.unwrap();
    for i in 0..6 {
        fs.write(&format!("/src/a/b/f{i}.rs"), b"seed\n")
            .await
            .unwrap();
    }
    fs.commit("t", "seed").await.unwrap();
    // Interleave edits to the target with edits to its siblings, so most commits
    // rewrite the tree spine without touching the path.
    for i in 0..12 {
        let p = if i % 3 == 0 {
            "/src/a/b/f2.rs".to_string()
        } else {
            format!("/src/a/b/f{}.rs", i % 6)
        };
        fs.write(&p, format!("edit {i}\n").as_bytes())
            .await
            .unwrap();
        fs.commit("t", &format!("c{i}")).await.unwrap();
    }
    fs.remove("/src/a/b/f2.rs").await.unwrap();
    fs.commit("t", "delete").await.unwrap();
    fs.write("/src/a/b/f2.rs", b"back\n").await.unwrap();
    fs.commit("t", "resurrect").await.unwrap();

    let revs = fs.log_path("/src/a/b/f2.rs", None).await.unwrap();
    // Every revision but the last is one the pairwise walk can also see; the
    // last is the root commit, which has no pair and is asserted below.
    let (root, rest) = revs.split_last().unwrap();
    assert_eq!(
        rendered(rest),
        pairwise_baseline(&fs, "/src/a/b/f2.rs").await
    );
    assert_eq!(root.commit.commit.message, "seed");
    assert_eq!(root.status, DiffStatus::Added);
    // Newest first, and the lifecycle is visible in it.
    assert_eq!(revs.first().unwrap().status, DiffStatus::Added);
    assert_eq!(revs.first().unwrap().commit.commit.message, "resurrect");
    assert_eq!(revs[1].status, DiffStatus::Deleted);
    assert!(revs[1].hash.is_none(), "a deletion has no content address");
    assert_eq!(revs.last().unwrap().status, DiffStatus::Added);
    assert_eq!(revs.last().unwrap().commit.commit.message, "seed");
}

#[tokio::test]
async fn a_sibling_edit_is_not_a_revision() {
    let fs = fixture().await;
    fs.mkdir_p("/src").await.unwrap();
    fs.write("/src/kept.rs", b"one\n").await.unwrap();
    fs.write("/src/other.rs", b"one\n").await.unwrap();
    fs.commit("t", "seed").await.unwrap();
    for i in 0..5 {
        fs.write("/src/other.rs", format!("{i}\n").as_bytes())
            .await
            .unwrap();
        fs.commit("t", &format!("other {i}")).await.unwrap();
    }
    // Every one of those commits rewrote /src and the root tree. Only the commit
    // that changed this path's own hash counts.
    let revs = fs.log_path("/src/kept.rs", None).await.unwrap();
    assert_eq!(rendered(&revs), ["A seed"]);
}

#[tokio::test]
async fn limit_caps_the_revisions_returned() {
    let fs = fixture().await;
    fs.write("/f.txt", b"0\n").await.unwrap();
    fs.commit("t", "seed").await.unwrap();
    for i in 1..10 {
        fs.write("/f.txt", format!("{i}\n").as_bytes())
            .await
            .unwrap();
        fs.commit("t", &format!("c{i}")).await.unwrap();
    }
    let all = fs.log_path("/f.txt", None).await.unwrap();
    assert_eq!(all.len(), 10);
    let capped = fs.log_path("/f.txt", Some(3)).await.unwrap();
    assert_eq!(rendered(&capped), rendered(&all)[..3]);
}

/// The window is `ORIGOFS_FETCH_CONCURRENCY` wide and the carry between windows
/// is where an off-by-one would hide, so walk a history several windows deep.
#[tokio::test]
async fn history_longer_than_one_concurrency_window() {
    let fs = fixture().await;
    fs.write("/f.txt", b"0\n").await.unwrap();
    fs.write("/pad.txt", b"0\n").await.unwrap();
    fs.commit("t", "seed").await.unwrap();
    let mut expected = vec!["A seed".to_string()];
    for i in 1..60 {
        // Two thirds of the commits touch only the sibling.
        let target = i % 3 == 0;
        let p = if target { "/f.txt" } else { "/pad.txt" };
        fs.write(p, format!("{i}\n").as_bytes()).await.unwrap();
        fs.commit("t", &format!("c{i}")).await.unwrap();
        if target {
            expected.push(format!("M c{i}"));
        }
    }
    expected.reverse();
    let revs = fs.log_path("/f.txt", None).await.unwrap();
    assert_eq!(rendered(&revs), expected);
    assert_eq!(
        rendered(revs.split_last().unwrap().1),
        pairwise_baseline(&fs, "/f.txt").await
    );
}

#[tokio::test]
async fn a_path_that_never_existed_has_no_revisions() {
    let fs = fixture().await;
    fs.write("/f.txt", b"0\n").await.unwrap();
    fs.commit("t", "seed").await.unwrap();
    assert!(fs.log_path("/nope.txt", None).await.unwrap().is_empty());
    // A file cannot have children, so nothing is under it either.
    assert!(fs.log_path("/f.txt/under", None).await.unwrap().is_empty());
}

#[tokio::test]
async fn the_root_is_not_a_path() {
    let fs = fixture().await;
    fs.write("/f.txt", b"0\n").await.unwrap();
    fs.commit("t", "seed").await.unwrap();
    assert!(fs.log_path("/", None).await.is_err());
    assert!(fs.log_path("relative", None).await.is_err());
}

#[tokio::test]
async fn directories_have_history_too() {
    let fs = fixture().await;
    fs.mkdir_p("/src/a").await.unwrap();
    fs.write("/src/a/f.rs", b"one\n").await.unwrap();
    fs.write("/top.txt", b"one\n").await.unwrap();
    fs.commit("t", "seed").await.unwrap();
    fs.write("/top.txt", b"two\n").await.unwrap();
    fs.commit("t", "outside").await.unwrap();
    fs.write("/src/a/f.rs", b"two\n").await.unwrap();
    fs.commit("t", "inside").await.unwrap();
    // A directory's tree hash changes exactly when something under it does.
    let revs = fs.log_path("/src", None).await.unwrap();
    assert_eq!(rendered(&revs), ["M inside", "A seed"]);
}

#[tokio::test]
async fn an_unborn_branch_has_no_history() {
    let fs = fixture().await;
    fs.write("/f.txt", b"0\n").await.unwrap();
    assert!(fs.log_path("/f.txt", None).await.unwrap().is_empty());
}

#[tokio::test]
async fn binary_content_is_not_diffed_as_lossy_text() {
    let fs = fixture().await;
    // Invalid UTF-8 that `from_utf8_lossy` would turn into U+FFFD, producing a
    // patch whose "added" bytes were never in the file.
    fs.write("/b.bin", &[0xff, 0xfe, 0x00, 0x01]).await.unwrap();
    fs.commit("t", "one").await.unwrap();
    fs.write("/b.bin", &[0xff, 0xfe, 0x00, 0x02]).await.unwrap();
    fs.commit("t", "two").await.unwrap();
    let log = fs.log().await.unwrap();
    let patch = fs
        .diff_file(&log[1].hash.to_hex(), &log[0].hash.to_hex(), "/b.bin")
        .await
        .unwrap();
    assert_eq!(patch, "Binary files differ: /b.bin\n");
    assert!(!patch.contains('\u{fffd}'));
}
