//! The op-log read *about a file* rather than about an actor (V23's two indexes).
//!
//! The subtle half is which key it reads by. A rename records the destination
//! path against the same inode, so a path-keyed read loses everything the file
//! did under an earlier name; once the inode is gone the recorded path is the
//! only handle left. Both halves are asserted here because neither is visible
//! from the signature.

use origofs_core::{Fs, MemStore, MetadataStore, SqliteMetadataStore, WriteCtx};

/// A `dyn` metadata handle, so the multi-workspace case can `rebind` onto a
/// `with_workspace` view of the same store.
type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;
use std::sync::Arc;

async fn fixture() -> (TestFs, WriteCtx) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    let actor = fs.create_agent("agent", "opus", None).await.unwrap();
    (fs, WriteCtx::actor(actor))
}

fn ops(v: &[origofs_core::EditOp]) -> Vec<(String, String)> {
    v.iter().map(|o| (o.op.clone(), o.path.clone())).collect()
}

#[tokio::test]
async fn follows_the_file_across_a_rename() {
    let (fs, ctx) = fixture().await;
    fs.write_as(ctx, "/a.txt", b"one\n").await.unwrap();
    fs.write_as(ctx, "/a.txt", b"two\n").await.unwrap();
    fs.rename_as(ctx, "/a.txt", "/b.txt").await.unwrap();
    fs.write_as(ctx, "/b.txt", b"three\n").await.unwrap();

    // Newest first, and the two writes made under the old name are in it — they
    // are rows whose recorded `path` is `/a.txt`, which a path-keyed read of
    // `/b.txt` would never match.
    let got = fs.edit_ops_at("/b.txt", None).await.unwrap();
    assert_eq!(
        ops(&got),
        [
            ("write".into(), "/b.txt".to_string()),
            ("rename".into(), "/b.txt".into()),
            ("write".into(), "/a.txt".into()),
            ("write".into(), "/a.txt".into()),
        ]
    );
    // The old path is not the file any more, and resolving it finds nothing. What
    // is left there is the rows recorded under that name.
    assert_eq!(
        ops(&fs.edit_ops_at("/a.txt", None).await.unwrap()),
        [
            ("write".into(), "/a.txt".to_string()),
            ("write".into(), "/a.txt".into())
        ]
    );
}

#[tokio::test]
async fn a_deleted_file_still_answers_from_its_recorded_path() {
    let (fs, ctx) = fixture().await;
    fs.write_as(ctx, "/gone.txt", b"one\n").await.unwrap();
    fs.remove_or_propose(ctx, "/gone.txt", None).await.unwrap();

    // The inode is gone, so there is nothing to resolve; the delete is exactly the
    // row somebody asking about a vanished file wants, and it is still here.
    let got = fs.edit_ops_at("/gone.txt", None).await.unwrap();
    assert_eq!(
        ops(&got),
        [
            ("remove".into(), "/gone.txt".to_string()),
            ("write".into(), "/gone.txt".into())
        ]
    );
    assert!(
        got[0].pre_hash.is_some(),
        "a delete names what it destroyed"
    );
    assert!(got[0].post_hash.is_none());
}

#[tokio::test]
async fn two_files_that_shared_a_path_are_not_merged() {
    let (fs, ctx) = fixture().await;
    // One inode lives at /slot, moves away, and a *different* file takes the name.
    fs.write_as(ctx, "/slot", b"first\n").await.unwrap();
    fs.rename_as(ctx, "/slot", "/moved").await.unwrap();
    fs.write_as(ctx, "/slot", b"second\n").await.unwrap();
    fs.write_as(ctx, "/slot", b"second again\n").await.unwrap();

    // Reading by inode is what keeps these apart: a path-keyed read of `/slot`
    // would return the first file's write too, and the two are unrelated files
    // that merely shared a name.
    assert_eq!(
        ops(&fs.edit_ops_at("/slot", None).await.unwrap()),
        [
            ("write".into(), "/slot".to_string()),
            ("write".into(), "/slot".into())
        ]
    );
    assert_eq!(
        ops(&fs.edit_ops_at("/moved", None).await.unwrap()),
        [
            ("rename".into(), "/moved".to_string()),
            ("write".into(), "/slot".into())
        ]
    );
}

#[tokio::test]
async fn limit_takes_the_newest() {
    let (fs, ctx) = fixture().await;
    for i in 0..5 {
        fs.write_as(ctx, "/f.txt", format!("{i}\n").as_bytes())
            .await
            .unwrap();
    }
    let all = fs.edit_ops_at("/f.txt", None).await.unwrap();
    assert_eq!(all.len(), 5);
    let capped = fs.edit_ops_at("/f.txt", Some(2)).await.unwrap();
    assert_eq!(capped.len(), 2);
    assert_eq!(capped[0].id, all[0].id, "newest first, not oldest");
    assert_eq!(capped[1].id, all[1].id);
}

#[tokio::test]
async fn an_unattributed_write_records_nothing() {
    let (fs, ctx) = fixture().await;
    fs.write_as(ctx, "/f.txt", b"attributed\n").await.unwrap();
    fs.write("/f.txt", b"anonymous\n").await.unwrap();
    // The plain write is the mount's path too. It leaves no row, which is the
    // documented limit of this view rather than a bug in it.
    assert_eq!(fs.edit_ops_at("/f.txt", None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_path_with_no_ops_is_empty_and_a_bad_path_is_an_error() {
    let (fs, ctx) = fixture().await;
    fs.write_as(ctx, "/f.txt", b"one\n").await.unwrap();
    assert!(fs.edit_ops_at("/never.txt", None).await.unwrap().is_empty());
    assert!(fs.edit_ops_at("relative", None).await.is_err());
}

/// Ops are per workspace, like every other row in this table.
#[tokio::test]
async fn one_workspace_does_not_see_another_s_ops() {
    let (fs, ctx) = fixture().await;
    fs.write_as(ctx, "/f.txt", b"one\n").await.unwrap();
    let (id, root) = fs.meta.create_workspace("other").await.unwrap();
    let other = fs.rebind(fs.meta.with_workspace(id), root);
    other.init().await.unwrap();
    other.write_as(ctx, "/f.txt", b"theirs\n").await.unwrap();
    // Same path, same actor, a different workspace: the V23 indexes lead with
    // `workspace_id` for the same reason every other index in this table does.
    assert_eq!(other.edit_ops_at("/f.txt", None).await.unwrap().len(), 1);
    assert_eq!(fs.edit_ops_at("/f.txt", None).await.unwrap().len(), 1);
}
