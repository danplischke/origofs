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

// The server is the sole authority on authorship. A client answers the server's
// SyncStep1 with its content; the server ingests it under the *authenticated*
// identity and re-stamps it, overriding whatever author the client's bytes named.
// (A client that sets no author at all — the common case — takes the same
// format-after-apply path; see `coedit_vanilla_yjs_client_is_attributed`.)
#[test]
fn coedit_ysync_attributes_client_content_server_side() {
    let server = CoeditDoc::new();
    let client = CoeditDoc::new();
    client.insert(WriteCtx::session(999, 999), 0, "typed on the client"); // a lie

    let greeting = server.sync_start(); // server → client: SyncStep1
    let answer = client
        .handle_sync(WriteCtx::session(999, 999), &greeting)
        .unwrap();
    assert!(!answer.reply.is_empty()); // client → server: SyncStep2(content)

    let out = server
        .handle_sync(WriteCtx::session(1, 7), &answer.reply)
        .unwrap();
    assert_eq!(server.text(), "typed on the client");

    // Attributed to the authenticated actor 1, not the client-claimed 999.
    let (_t, spans) = server.snapshot();
    assert_eq!(spans, vec![(1, 7, "typed on the client".len() as u64)]);
    // The attributed delta is what fans out to peers (not the raw inbound bytes).
    assert!(!out.broadcast.is_empty());
}

// One inbound update that inserts at several positions attributes each inserted
// range to the actor — not one coarse span — while leaving untouched text with
// its original author.
#[test]
fn coedit_apply_update_as_attributes_multiple_regions() {
    let server = CoeditDoc::new();
    server.insert(WriteCtx::session(1, 1), 0, "HELLO"); // actor 1 owns the base
    let client = CoeditDoc::load(&server.state_update()).unwrap();

    // A vanilla client edits at two spots (author unknown to us).
    client.insert(WriteCtx::session(0, 0), 0, "a"); // -> "aHELLO"
    client.insert(WriteCtx::session(0, 0), 6, "b"); // -> "aHELLOb"

    server
        .apply_update_as(WriteCtx::session(42, 8), &client.state_update())
        .unwrap();
    assert_eq!(server.text(), "aHELLOb");

    // Two 1-byte inserts credited to actor 42; the 5-byte middle stays actor 1.
    let (_t, spans) = server.snapshot();
    let actors_lens: Vec<(i64, u64)> = spans.iter().map(|&(a, _, l)| (a, l)).collect();
    assert_eq!(actors_lens, vec![(42, 1), (1, 5), (42, 1)]);
}

// A re-sent update carrying content the server already has changes nothing and is
// not re-attributed or re-broadcast (idempotence).
#[test]
fn coedit_apply_update_as_is_idempotent() {
    let server = CoeditDoc::new();
    let client = CoeditDoc::new();
    client.insert(WriteCtx::session(3, 3), 0, "once");

    let update = client.state_update();
    let first = server
        .apply_update_as(WriteCtx::session(7, 7), &update)
        .unwrap();
    assert!(!first.is_empty());
    let second = server
        .apply_update_as(WriteCtx::session(7, 7), &update)
        .unwrap();
    assert!(second.is_empty()); // nothing new -> no relay

    let (_t, spans) = server.snapshot();
    assert_eq!(spans, vec![(7, 7, 4)]); // still one span, not doubled
}

// The headline compatibility claim: a *completely unmodified* Yjs client — a raw
// `yrs::Doc` speaking the stock y-sync `DefaultProtocol`, with no notion of
// origofs authorship — connects, converges with the server, and has its content
// attributed server-side to the authenticated identity.
#[test]
fn coedit_vanilla_yjs_client_is_attributed() {
    use std::collections::HashMap;
    use yrs::sync::{Awareness, DefaultProtocol, Protocol};
    use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
    use yrs::{Doc, Text, Transact};

    // A stock Yjs client types into the shared "content" text — no author attr.
    let client_doc = Doc::new();
    {
        let text = client_doc.get_or_insert_text("content");
        let mut txn = client_doc.transact_mut();
        text.insert(&mut txn, 0, "vanilla");
    }
    let client = Awareness::new(client_doc);
    let protocol = DefaultProtocol;

    // Server already holds a line by actor 2 — exercises a real merge, not a load.
    let server = CoeditDoc::new();
    server.insert(WriteCtx::session(2, 2), 0, "seed\n");

    // Server greets with SyncStep1; the vanilla client answers with its state,
    // which the server ingests as the authenticated actor 5.
    let greeting = server.sync_start();
    let responses = protocol.handle(&client, &greeting).unwrap();
    let mut buf = EncoderV1::new();
    for m in &responses {
        m.encode(&mut buf);
    }
    server
        .handle_sync(WriteCtx::session(5, 5), &buf.to_vec())
        .unwrap();

    // Converged, and attributed by author: 7 bytes ("vanilla") to the vanilla
    // client's authenticated actor 5, the 5 bytes ("seed\n") still to actor 2 —
    // regardless of the concurrent merge order.
    assert!(server.text().contains("vanilla") && server.text().contains("seed"));
    let (_t, spans) = server.snapshot();
    let mut by_actor: HashMap<i64, u64> = HashMap::new();
    for (a, _s, l) in spans {
        *by_actor.entry(a).or_insert(0) += l;
    }
    assert_eq!(by_actor.get(&5), Some(&7));
    assert_eq!(by_actor.get(&2), Some(&5));
}
