//! CRDT co-editing (roadmap M8): a live `yrs` document whose interleaved,
//! character-level authorship checkpoints losslessly into the byte-range blame
//! index — the "live co-editing" half of M8. Requires the `coedit` feature.
#![cfg(feature = "coedit")]

use origofs_core::{CoeditDoc, Fs, MemStore, SqliteMetadataStore, WriteCtx};
use std::sync::Arc;

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

// A human and an agent type into the same buffer; the checkpoint lands each
// author's exact character spans in blame — including two authors on one line,
// which the old per-line model could never express.
#[tokio::test]
async fn coedit_checkpoint_preserves_each_authors_spans() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_c = fs.create_session(claude, None).await.unwrap();

    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(alice, s_a), 0, "hello "); // alice: [0,6)
    doc.insert(WriteCtx::session(claude, s_c), 6, "world"); // claude: [6,11)
    doc.insert(WriteCtx::session(alice, s_a), 11, "!"); // alice: [11,12)
    assert_eq!(doc.text(), "hello world!");

    // Checkpoint, driven here by the agent's session — the driver does not change
    // authorship: the CRDT's per-span authors are authoritative.
    fs.checkpoint_coedit(WriteCtx::session(claude, s_c), "/doc", &doc)
        .await
        .unwrap();
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"hello world!");

    let b = fs.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 3);
    assert_eq!(
        (b[0].actor.id, b[0].byte_start, b[0].byte_end),
        (alice, 0, 6)
    );
    assert_eq!(
        (b[1].actor.id, b[1].byte_start, b[1].byte_end),
        (claude, 6, 11)
    );
    assert_eq!(
        (b[2].actor.id, b[2].byte_start, b[2].byte_end),
        (alice, 11, 12)
    );
    // All one line, so a line-only view would collapse them; byte ranges do not.
    assert!(b.iter().all(|r| r.line_start == 1 && r.line_end == 1));
    assert_eq!(b[1].session, Some(s_c));
}

// Two peers exchange opaque update blobs and converge, and per-span authorship
// rides along in the CRDT — so a checkpoint after a sync is still exact.
#[tokio::test]
async fn coedit_updates_sync_and_carry_authorship() {
    let a = CoeditDoc::new();
    let b = CoeditDoc::new();

    a.insert(WriteCtx::session(1, 10), 0, "abc"); // actor 1
    b.apply_update(&a.state_update()).unwrap();
    assert_eq!(b.text(), "abc");

    b.insert(WriteCtx::session(2, 20), 3, "XYZ"); // actor 2 appends
    a.apply_update(&b.state_update()).unwrap();
    assert_eq!(a.text(), "abcXYZ");

    // Authorship survived the round-trip on peer `a`.
    let (text, spans) = a.snapshot();
    assert_eq!(text, "abcXYZ");
    assert_eq!(spans, vec![(1, 10, 3), (2, 20, 3)]);
}

// A co-edit session is durable: after a checkpoint, the CRDT is persisted as a
// sidecar, so it can be reopened (as if in a fresh process) and edited further —
// with the original authorship fully intact, not just the flat text.
#[tokio::test]
async fn coedit_session_persists_and_resumes() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_c = fs.create_session(claude, None).await.unwrap();

    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(alice, s_a), 0, "hello ");
    doc.insert(WriteCtx::session(claude, s_c), 6, "world");
    fs.checkpoint_coedit(WriteCtx::session(alice, s_a), "/doc", &doc)
        .await
        .unwrap();

    // Reopen from storage — the live CRDT is restored, not just the text.
    let resumed = fs
        .open_coedit(WriteCtx::session(alice, s_a), "/doc")
        .await
        .unwrap();
    assert_eq!(resumed.text(), "hello world");

    // Keep editing on the resumed doc, then checkpoint again.
    resumed.insert(WriteCtx::session(claude, s_c), 11, "!");
    fs.checkpoint_coedit(WriteCtx::session(claude, s_c), "/doc", &resumed)
        .await
        .unwrap();

    // Original + resumed authorship both intact: alice "hello ", claude "world!".
    let b = fs.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!(
        (b[0].actor.id, b[0].byte_start, b[0].byte_end),
        (alice, 0, 6)
    );
    assert_eq!(
        (b[1].actor.id, b[1].byte_start, b[1].byte_end),
        (claude, 6, 12)
    );
}
