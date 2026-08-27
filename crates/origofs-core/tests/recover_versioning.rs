//! Recovery must not silently under-restore when the store holds objects written
//! by a **newer** origofs.
//!
//! `scan` used to classify objects by "try `Commit::decode`, fall through on any
//! error", which made "a commit I'm too old to read" indistinguishable from "not
//! a commit". An old binary rebuilding a newer store would then report success
//! having quietly dropped every commit it couldn't parse — the one failure mode a
//! recovery tool must not have. Objects are now classified by their 4-byte type
//! tag first, and a load-bearing unreadable object fails the rebuild.

use origofs_core::{
    ChunkRef, Commit, ContentStore, Fs, Manifest, MemStore, OrigoFSError, RefSnapshot,
    SqliteMetadataStore, Tree, TreeEntry, TreeKind, types::Hash,
};
use std::sync::Arc;

/// Hand-build `"hi" -> manifest -> tree -> commit` and return the commit's bytes
/// (unstored, so a caller can plant them at whatever version it wants).
async fn commit_bytes(store: &MemStore, message: &str) -> Vec<u8> {
    let chunk = store.put(b"hi").await.unwrap();
    let manifest = store
        .put(
            &Manifest {
                size: 2,
                chunks: vec![ChunkRef {
                    hash: chunk,
                    len: 2,
                }],
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
    let tree = store
        .put(
            &Tree {
                entries: vec![TreeEntry {
                    name: "greet.txt".into(),
                    mode: 0o644,
                    kind: TreeKind::File,
                    hash: manifest,
                }],
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
    Commit {
        tree,
        parents: vec![],
        author: "dan".into(),
        message: message.into(),
        timestamp: 1,
    }
    .encode()
    .unwrap()
}

/// Rewrite an object's format-version byte, simulating a write by a future origofs.
fn as_version(mut bytes: Vec<u8>, version: u8) -> Vec<u8> {
    bytes[4] = version;
    bytes
}

async fn fresh(store: Arc<MemStore>) -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let fs = Fs::new(SqliteMetadataStore::open_in_memory().unwrap(), store);
    fs.init().await.unwrap();
    fs
}

fn assert_too_new(err: OrigoFSError, kind: &str) {
    assert!(
        err.is_unsupported_version(),
        "expected UnsupportedVersion, got {err}"
    );
    assert_eq!(err.code(), "unsupported_version");
    assert!(
        matches!(&err, OrigoFSError::UnsupportedVersion { kind: k, found: 2, .. } if *k == kind),
        "wrong payload: {err:?}"
    );
}

/// No mirror + an unreadable commit: head inference over a partial DAG would
/// invent branches from whatever it happened to understand. Refuse instead.
#[tokio::test]
async fn rebuild_refuses_when_a_commit_is_too_new_and_there_is_no_mirror() {
    let store = Arc::new(MemStore::new());
    store
        .put(&as_version(
            commit_bytes(&store, "from the future").await,
            2,
        ))
        .await
        .unwrap();

    let fs = fresh(store).await;
    assert_too_new(fs.rebuild_from_content().await.unwrap_err(), "commit");
}

/// The dry run is the diagnostic an operator reaches for *before* upgrading, so it
/// must still work on the old binary — and say what it couldn't read.
#[tokio::test]
async fn scan_reports_too_new_objects_instead_of_failing() {
    let store = Arc::new(MemStore::new());
    store
        .put(&as_version(
            commit_bytes(&store, "from the future").await,
            2,
        ))
        .await
        .unwrap();

    let fs = fresh(store).await;
    let report = fs.scan_content().await.unwrap();

    assert_eq!(report.commits_found, 0, "the v2 commit is not decodable");
    assert_eq!(report.unsupported, 1);
    assert_eq!(report.unsupported_kinds, vec![("commit".to_string(), 2)]);
    assert_eq!(report.corrupt, 0, "too-new is not corruption");
}

/// A branch tip this build can't decode is the sharpest case: the mirror names it,
/// so recovering "successfully" means silently dropping that branch's history.
#[tokio::test]
async fn rebuild_refuses_when_a_branch_tip_is_too_new() {
    let store = Arc::new(MemStore::new());
    let tip = store
        .put(&as_version(
            commit_bytes(&store, "from the future").await,
            2,
        ))
        .await
        .unwrap();
    store
        .put(
            &RefSnapshot {
                generation: 1,
                refs: vec![
                    ("main".into(), tip.to_hex()),
                    ("HEAD".into(), "ref:main".into()),
                ],
            }
            .encode(),
        )
        .await
        .unwrap();

    let fs = fresh(store).await;
    assert_too_new(fs.rebuild_from_content().await.unwrap_err(), "commit");
}

/// An unreadable mirror blocks even when a readable one exists: `generation` — the
/// field that would prove which is newer — is inside the bytes we can't parse, so
/// picking the readable one could silently roll the store back.
#[tokio::test]
async fn rebuild_refuses_when_a_ref_mirror_is_too_new() {
    let store = Arc::new(MemStore::new());
    let tip = store.put(&commit_bytes(&store, "c1").await).await.unwrap();
    let mirror = RefSnapshot {
        generation: 1,
        refs: vec![
            ("main".into(), tip.to_hex()),
            ("HEAD".into(), "ref:main".into()),
        ],
    }
    .encode();
    store.put(&mirror).await.unwrap();
    store.put(&as_version(mirror, 2)).await.unwrap();

    let fs = fresh(store).await;
    assert_too_new(fs.rebuild_from_content().await.unwrap_err(), "ref snapshot");
}

/// The refusal is deliberately narrow. A raw data chunk can begin with an object
/// tag by coincidence, so an unreadable object that no readable mirror points at
/// is counted in the report but must not block a rebuild that is otherwise whole.
#[tokio::test]
async fn an_unreferenced_too_new_object_is_reported_but_does_not_block() {
    let store = Arc::new(MemStore::new());
    let tip = store.put(&commit_bytes(&store, "c1").await).await.unwrap();
    store
        .put(
            &RefSnapshot {
                generation: 1,
                refs: vec![
                    ("main".into(), tip.to_hex()),
                    ("HEAD".into(), "ref:main".into()),
                ],
            }
            .encode(),
        )
        .await
        .unwrap();
    // A blob that merely *looks* like a commit from the future — no branch names it.
    let stray = as_version(commit_bytes(&store, "unreferenced").await, 2);
    store.put(&stray).await.unwrap();

    let fs = fresh(store).await;
    let report = fs.rebuild_from_content().await.unwrap();

    assert!(report.used_mirror);
    assert_eq!(report.branches, vec![("main".to_string(), tip.to_hex())]);
    assert_eq!(report.unsupported, 1, "still surfaced to the operator");
    assert_eq!(&fs.read("/greet.txt").await.unwrap()[..], b"hi");
}

/// A chunk that happens to start with a *supported* version but isn't a commit
/// stays invisible — the tag alone is a claim, not proof, and this is the check
/// that stops ordinary file content from polluting the recovery report.
#[tokio::test]
async fn a_chunk_that_merely_starts_with_a_tag_is_not_counted() {
    let store = Arc::new(MemStore::new());
    let tip = store.put(&commit_bytes(&store, "c1").await).await.unwrap();
    let mut junk = b"ORGC\x01".to_vec();
    junk.extend_from_slice(&[0u8; 64]); // a plausible header, nonsense payload
    store.put(&junk).await.unwrap();
    // A commit-shaped blob whose tree object isn't in the store: decodes, but the
    // existing tree-presence guard rejects it.
    let orphan = Commit {
        tree: Hash::from_array([0x99; 32]),
        parents: vec![],
        author: "nobody".into(),
        message: "orphan".into(),
        timestamp: 1,
    }
    .encode()
    .unwrap();
    store.put(&orphan).await.unwrap();

    let fs = fresh(store).await;
    let report = fs.scan_content().await.unwrap();

    assert_eq!(report.unsupported, 0, "neither blob is a version problem");
    assert_eq!(report.commits_found, 1, "only the real commit counts");
    assert_eq!(report.branches, vec![("main".to_string(), tip.to_hex())]);
}
