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

// `from_blamed` is the exact structural inverse of `snapshot`: rebuild a doc from
// a snapshot's text + spans and it snapshots identically — text and per-span
// authorship. This is what lets a stale sidecar be discarded and the live doc
// resurrected from the durable truth (the file + its blame) without loss.
#[test]
fn coedit_from_blamed_round_trips_snapshot() {
    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(1, 10), 0, "hello ");
    doc.insert(WriteCtx::session(2, 20), 6, "world");
    let (text, spans) = doc.snapshot();

    let rebuilt = CoeditDoc::from_blamed(&text, &spans).unwrap();
    assert_eq!(rebuilt.text(), "hello world");
    assert_eq!(rebuilt.snapshot(), (text, spans)); // authorship preserved exactly
}

// The gap the sidecar-coherence check closes: a change made to a co-edited file by
// another mechanism (here an accepted suggestion — an attributed write outside the
// CRDT) must not be silently reverted when the doc is reopened. The stale sidecar
// is detected and discarded, the live doc rebuilt from the file + blame, and a
// further checkpoint keeps the suggestion (credited to its proposer) rather than
// resurrecting the pre-suggestion text.
#[tokio::test]
async fn coedit_reopen_after_accepted_suggestion_rebuilds_from_file() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_b = fs.create_session(bob, None).await.unwrap();

    // Alice co-edits "hello" and checkpoints (flat text + a sidecar hashing "hello").
    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(alice, s_a), 0, "hello");
    fs.checkpoint_coedit(WriteCtx::session(alice, s_a), "/doc", &doc)
        .await
        .unwrap();

    // Bob proposes replacing it; alice (a different reviewer) accepts. The accept
    // writes "HELLO WORLD" to /doc attributed to bob — outside the CRDT, so the
    // sidecar is now stale.
    let sid = fs
        .suggest(
            WriteCtx::session(bob, s_b),
            "/doc",
            b"HELLO WORLD",
            Some("shout"),
            None,
        )
        .await
        .unwrap();
    fs.accept_suggestion(sid, WriteCtx::session(alice, s_a))
        .await
        .unwrap();
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"HELLO WORLD");

    // Reopen: the live doc reflects the accepted suggestion, not the stale "hello".
    let reopened = fs
        .open_coedit(WriteCtx::session(alice, s_a), "/doc")
        .await
        .unwrap();
    assert_eq!(reopened.text(), "HELLO WORLD");

    // A further checkpoint keeps the suggestion — credited to bob (its proposer),
    // recovered from blame — instead of reverting to "hello".
    fs.checkpoint_coedit(WriteCtx::session(alice, s_a), "/doc", &reopened)
        .await
        .unwrap();
    assert_eq!(&fs.read("/doc").await.unwrap()[..], b"HELLO WORLD");
    let b = fs.blame("/doc").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].actor.id, bob);
    assert_eq!((b[0].byte_start, b[0].byte_end), (0, 11));
}

// Regression: the convergence-ordering bug. An author edits *alone* — before any
// peer has joined the room — so the server's attribution (formatting the inserted
// range with the author mark) adds CRDT items to the server's replica that the
// author's own replica has never seen. If the server does not echo that attributed
// delta back to the sender, the author diverges structurally, and a *later* peer's
// edit — positioned in the CRDT against those unseen author-mark items — can never
// integrate on the author (it waits, pending, on origins the author is missing).
// The earlier Rust tests missed this because they always had both peers exchange
// state before the single edit; this pins the solo-author-first ordering that the
// live browser E2E exposed.
#[test]
fn coedit_solo_author_converges_with_a_later_peer_edit() {
    let server = CoeditDoc::new();
    let a = CoeditDoc::new(); // the author — connects and edits first, alone
    let b = CoeditDoc::new(); // a peer — joins only afterwards
    let ctx_a = WriteCtx::session(1, 10);
    let ctx_b = WriteCtx::session(2, 20);

    // A connects to an empty room and types "hello" as a vanilla client would:
    // unattributed locally, the server is the authority. A hands its content to the
    // server (framed as the SyncStep2 answering the server's greeting); the server
    // attributes it to actor 1.
    a.insert(WriteCtx::session(0, 0), 0, "hello");
    let a_push = a
        .handle_sync(WriteCtx::session(0, 0), &server.sync_start())
        .unwrap();
    let after_a = server.handle_sync(ctx_a, &a_push.reply).unwrap();
    assert_eq!(server.text(), "hello");
    // The fix: the server echoes the attributed delta back to the *sender*, so A's
    // replica gains the server's author-mark items. Without the echo, this is empty.
    assert!(
        !after_a.reply.is_empty(),
        "server must echo the attributed delta back to the author"
    );
    a.apply_relayed(&after_a.reply).unwrap();
    assert_eq!(a.text(), "hello");

    // B joins later and syncs up from the server: it sends SyncStep1, the server
    // answers with the state B lacks (A's "hello" *and* the author-mark items).
    let srv_state = server.handle_sync(ctx_b, &b.sync_start()).unwrap();
    b.apply_relayed(&srv_state.reply).unwrap();
    assert_eq!(b.text(), "hello");

    // B appends " world" — its CRDT position references the server's author-mark
    // items, the very items A must also hold for this edit to integrate on A.
    b.insert(WriteCtx::session(0, 0), 5, " world");
    let b_push = b
        .handle_sync(WriteCtx::session(0, 0), &server.sync_start())
        .unwrap();
    let after_b = server.handle_sync(ctx_b, &b_push.reply).unwrap();
    assert_eq!(server.text(), "hello world");
    assert!(!after_b.broadcast.is_empty()); // fans out to the room's other peer, A

    // The payoff: A converges. Before the fix, A could not integrate B's insert
    // (it referenced author-mark origins A never received) and stayed at "hello".
    a.apply_relayed(&after_b.broadcast).unwrap();
    assert_eq!(a.text(), "hello world");

    // And authorship is exact on the server: "hello" → actor 1, " world" → actor 2.
    let (_t, spans) = server.snapshot();
    let actors_lens: Vec<(i64, u64)> = spans.iter().map(|&(x, _, l)| (x, l)).collect();
    assert_eq!(actors_lens, vec![(1, 5), (2, 6)]);
}

// With no prior sidecar, opening an existing file promotes its text — preserving
// whatever authorship the file already carries (a prior attributed write), not
// re-crediting it to whoever opens it.
#[tokio::test]
async fn coedit_open_existing_file_preserves_prior_authorship() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_b = fs.create_session(bob, None).await.unwrap();

    // Alice writes the file directly (attributed) — never co-edited, no sidecar.
    fs.write_as(WriteCtx::session(alice, s_a), "/notes", b"alice wrote this")
        .await
        .unwrap();

    // Bob opens it for co-editing and checkpoints: alice's authorship is preserved,
    // not reassigned to bob.
    let doc = fs
        .open_coedit(WriteCtx::session(bob, s_b), "/notes")
        .await
        .unwrap();
    assert_eq!(doc.text(), "alice wrote this");
    fs.checkpoint_coedit(WriteCtx::session(bob, s_b), "/notes", &doc)
        .await
        .unwrap();
    let b = fs.blame("/notes").await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].actor.id, alice);
}

// --- document index units (UTF-8 bytes, not UTF-16) -------------------------
//
// `yrs` picks its index unit per document (`OffsetKind`), and origofs pins it to
// bytes because every consumer downstream — snapshot spans, the blame index,
// `write_as_blamed` — speaks bytes. These tests exist because the two units are
// indistinguishable on ASCII: the whole suite was green while non-ASCII text was
// being misattributed, and a `yrs` default flip would be just as silent.

// Pin the unit itself. Formatting `0..6` must cover exactly "héllo" (6 UTF-8
// bytes, 5 UTF-16 code units) — if the document ever indexes UTF-16 again, this
// range would reach into the following space instead.
#[test]
fn coedit_indices_are_utf8_bytes() {
    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(1, 1), 0, "héllo world");
    let (_, spans) = doc.snapshot();
    assert_eq!(
        spans,
        vec![(1, 1, 12)],
        "the whole string is one authored run"
    );

    // A second author appends at the end. The end is byte 12, not UTF-16 unit 11
    // — passing 11 here lands the "!" before the "d", which is precisely the
    // off-by-one a UTF-16 index produces against this byte-indexed document.
    doc.insert(WriteCtx::session(2, 2), 12, "!");
    let (text, spans) = doc.snapshot();
    assert_eq!(text, "héllo world!");
    assert_eq!(spans, vec![(1, 1, 12), (2, 2, 1)]);
}

// The user-visible consequence, through the real Yjs-client path: two authors
// typing non-ASCII must be credited their exact byte counts, with nothing
// orphaned to actor 0. Before the unit fix this produced [(1,1,5), (2,2,5),
// (0,0,1)] — alice short by a byte, and that byte attributed to nobody (which a
// checkpoint then silently credits to whoever happened to checkpoint).
#[test]
fn coedit_non_ascii_authorship_is_byte_exact() {
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, GetString, ReadTxn, Text, Transact, Update};

    let alice = WriteCtx::session(1, 1);
    let bob = WriteCtx::session(2, 2);
    let server = CoeditDoc::new();

    let client = Doc::new();
    let ctext = client.get_or_insert_text("content");

    let sv = client.transact().state_vector();
    ctext.insert(&mut client.transact_mut(), 0, "héllo"); // 6 bytes, 5 UTF-16 units
    let u = client.transact().encode_state_as_update_v1(&sv);
    let relay = server.apply_update_as(alice, &u).unwrap();
    client
        .transact_mut()
        .apply_update(Update::decode_v1(&relay).unwrap())
        .unwrap();

    let sv = client.transact().state_vector();
    let end = ctext.get_string(&client.transact()).len() as u32;
    ctext.insert(&mut client.transact_mut(), end, "wörld"); // 6 bytes too
    let u = client.transact().encode_state_as_update_v1(&sv);
    server.apply_update_as(bob, &u).unwrap();

    let (text, spans) = server.snapshot();
    assert_eq!(text, "héllowörld");
    assert_eq!(
        spans,
        vec![(1, 1, 6), (2, 2, 6)],
        "each author must own exactly the bytes they typed, with none orphaned"
    );
}

// `from_blamed` reconstructs by inserting at byte offsets; with a UTF-16 cursor
// every span after the first non-ASCII one landed at the wrong index.
#[test]
fn coedit_from_blamed_round_trips_non_ascii() {
    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(1, 10), 0, "héllo ");
    doc.insert(WriteCtx::session(2, 20), 7, "wörld");
    doc.insert(WriteCtx::session(3, 30), 13, " 🎉"); // astral: 4 UTF-8 bytes, 2 UTF-16
    let (text, spans) = doc.snapshot();
    assert_eq!(text, "héllo wörld 🎉");

    let rebuilt = CoeditDoc::from_blamed(&text, &spans).unwrap();
    assert_eq!(rebuilt.text(), text);
    assert_eq!(rebuilt.snapshot(), (text, spans));
}

// `reconcile_with` folds an out-of-band write into a live doc by applying the
// difference as attributed CRDT ops — same indexing hazard, different direction.
#[test]
fn coedit_reconcile_with_handles_non_ascii() {
    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(1, 1), 0, "héllo wörld");

    // An out-of-band write replaced a multibyte word.
    let target = "héllo 🌍";
    doc.reconcile_with(target, &[(1, 1, 7), (2, 2, 4)]).unwrap();

    assert_eq!(doc.text(), target);
    let (text, spans) = doc.snapshot();
    assert_eq!(text, target);
    let total: u64 = spans.iter().map(|s| s.2).sum();
    assert_eq!(
        total,
        target.len() as u64,
        "spans must tile the text exactly"
    );
}

// --- the server owns authorship, on every apply ------------------------------
//
// `coedit_ysync_attributes_client_content_server_side` above covers the case
// where a client's forged author rides in the SAME update as its insert: the
// text diff sees the insert, so the stamp is overwritten. That is the only shape
// the suite tested, and it is not the shape an attacker uses.
//
// A client can instead send a SECOND, content-free update that only *formats*
// existing text with a different author. No characters change, so an insert-only
// stamping pass produced no ranges and the forged value stood — then flowed
// through `snapshot` into the durable blame index with the file bytes and their
// content hash unchanged. No byte-level check can see it, and for a co-edited
// file the CRDT attribute is the only record of authorship there is.

/// A vanilla Yjs client that formats `content[start..end]` with `a = value` and
/// nothing else — the forgery, as ~46 bytes on the wire.
fn forge_author_frame(client: &yrs::Doc, start: u32, end: u32, value: &str) -> Vec<u8> {
    use yrs::{ReadTxn, Text, Transact};
    let text = client.get_or_insert_text("content");
    let sv = client.transact().state_vector();
    {
        let mut txn = client.transact_mut();
        let attrs = yrs::types::Attrs::from([("a".into(), yrs::Any::from(value.to_string()))]);
        text.format(&mut txn, start, end - start, attrs);
    }
    client.transact().encode_state_as_update_v1(&sv)
}

#[test]
fn coedit_a_format_only_update_cannot_restamp_text_the_client_did_not_write() {
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, ReadTxn, Text, Transact, Update};

    let mallory = WriteCtx::session(2, 2);
    let server = CoeditDoc::new();

    let client = Doc::new();
    let ctext = client.get_or_insert_text("content");
    let sv = client.transact().state_vector();
    ctext.insert(&mut client.transact_mut(), 0, "evil");
    let u = client.transact().encode_state_as_update_v1(&sv);
    let relay = server.apply_update_as(mallory, &u).unwrap();
    // The client catches up on the server's stamp, as a real socket would.
    client
        .transact_mut()
        .apply_update(Update::decode_v1(&relay).unwrap())
        .unwrap();
    assert_eq!(server.snapshot().1, vec![(2, 2, 4)], "mallory typed it");

    // Now the forgery: content-free, claiming alice (actor 1, session 1).
    let forge = forge_author_frame(&client, 0, 4, "1,1");
    let out = server.apply_update_as(mallory, &forge).unwrap();

    assert_eq!(server.text(), "evil", "the text must be untouched");
    assert_eq!(
        server.snapshot().1,
        vec![(2, 2, 4)],
        "authorship must survive a format-only update from the same socket"
    );

    // The repair has to travel with the delta, or peers keep the forged value
    // and diverge from the server on the next checkpoint.
    assert!(!out.is_empty(), "the repair must be relayed");
    let peer = CoeditDoc::new();
    peer.apply_update(&server.state_update()).unwrap();
    assert_eq!(peer.snapshot().1, vec![(2, 2, 4)]);
}

// The same forgery packed into ONE update alongside a real insert: the insert is
// credited to the sender, and the forged re-stamp of the surrounding text is not.
#[test]
fn coedit_a_forgery_riding_with_a_real_insert_is_still_refused() {
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, ReadTxn, Text, Transact, Update};

    let alice = WriteCtx::session(1, 1);
    let mallory = WriteCtx::session(2, 2);
    let server = CoeditDoc::new();

    // Alice writes the base text.
    let client = Doc::new();
    let ctext = client.get_or_insert_text("content");
    let sv = client.transact().state_vector();
    ctext.insert(&mut client.transact_mut(), 0, "alices words");
    let u = client.transact().encode_state_as_update_v1(&sv);
    let relay = server.apply_update_as(alice, &u).unwrap();
    client
        .transact_mut()
        .apply_update(Update::decode_v1(&relay).unwrap())
        .unwrap();

    // Mallory inserts AND re-stamps alice's text as his own, in one update.
    let sv = client.transact().state_vector();
    {
        let mut txn = client.transact_mut();
        ctext.insert(&mut txn, 12, "!");
        let attrs = yrs::types::Attrs::from([("a".into(), yrs::Any::from("2,2".to_string()))]);
        ctext.format(&mut txn, 0, 12, attrs);
    }
    let u = client.transact().encode_state_as_update_v1(&sv);
    server.apply_update_as(mallory, &u).unwrap();

    assert_eq!(server.text(), "alices words!");
    assert_eq!(
        server.snapshot().1,
        vec![(1, 1, 12), (2, 2, 1)],
        "mallory gets exactly the byte he typed; alice keeps hers"
    );
}

// Laundering attempts: values the server would never write must not be believed
// just because they parse. `"0,0"` is the unattributed sentinel (which a
// checkpoint credits to the checkpointer), and `"1,1,junk"` parses to a
// plausible (1,1) — a parsed comparison would accept both.
#[test]
fn coedit_malformed_forged_authors_are_normalised_away() {
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, ReadTxn, Text, Transact, Update};

    for forged in ["0,0", "1,1,junk", "", "not-a-number"] {
        let mallory = WriteCtx::session(2, 2);
        let server = CoeditDoc::new();
        let client = Doc::new();
        let ctext = client.get_or_insert_text("content");
        let sv = client.transact().state_vector();
        ctext.insert(&mut client.transact_mut(), 0, "mine");
        let u = client.transact().encode_state_as_update_v1(&sv);
        let relay = server.apply_update_as(mallory, &u).unwrap();
        client
            .transact_mut()
            .apply_update(Update::decode_v1(&relay).unwrap())
            .unwrap();

        let forge = forge_author_frame(&client, 0, 4, forged);
        server.apply_update_as(mallory, &forge).unwrap();
        assert_eq!(
            server.snapshot().1,
            vec![(2, 2, 4)],
            "forged value {forged:?} was believed"
        );
    }
}

// Deleting a victim's text and retyping different text credits the retyper —
// they did type those bytes — and never lets an arbitrary actor be named.
#[test]
fn coedit_delete_and_reinsert_credits_the_reinserter() {
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, ReadTxn, Text, Transact, Update};

    let alice = WriteCtx::session(1, 1);
    let mallory = WriteCtx::session(2, 2);
    let server = CoeditDoc::new();

    let client = Doc::new();
    let ctext = client.get_or_insert_text("content");
    let sv = client.transact().state_vector();
    ctext.insert(&mut client.transact_mut(), 0, "aaaaaa");
    let u = client.transact().encode_state_as_update_v1(&sv);
    let relay = server.apply_update_as(alice, &u).unwrap();
    client
        .transact_mut()
        .apply_update(Update::decode_v1(&relay).unwrap())
        .unwrap();

    // Replace it with text sharing no characters, so the diff cannot align any
    // of it as surviving (see the caveat test below).
    let sv = client.transact().state_vector();
    {
        let mut txn = client.transact_mut();
        ctext.remove_range(&mut txn, 0, 6);
        ctext.insert(&mut txn, 0, "XYZXYZ");
    }
    let u = client.transact().encode_state_as_update_v1(&sv);
    server.apply_update_as(mallory, &u).unwrap();

    assert_eq!(server.text(), "XYZXYZ");
    assert_eq!(server.snapshot().1, vec![(2, 2, 6)]);
}

// The limitation this design does NOT close, pinned so it is a known property
// rather than a surprise.
//
// Authorship is carried across an update by `similar`'s *character* diff, not by
// CRDT item identity — `yrs` does not expose the item ids that would make it
// identity-correct. So characters the attacker types that happen to align with
// characters already present read as "surviving" and keep the previous author.
// Here mallory replaces alice's "alices" with "mallory"; the shared "al" aligns
// and stays credited to alice.
//
// This is unchanged from the insert-only stamping that preceded total
// enforcement — `inserted_ranges` used the same diff — so it is neither
// introduced nor worsened here. It is strictly weaker than the forgery this
// commit closes: an attacker cannot *name* an actor, only cause a victim to be
// credited for characters that coincide with the victim's own. Closing it needs
// item-identity tracking; see the note on `apply_update_as`.
#[test]
fn coedit_authorship_carries_by_text_diff_not_crdt_identity() {
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, ReadTxn, Text, Transact, Update};

    let alice = WriteCtx::session(1, 1);
    let mallory = WriteCtx::session(2, 2);
    let server = CoeditDoc::new();

    let client = Doc::new();
    let ctext = client.get_or_insert_text("content");
    let sv = client.transact().state_vector();
    ctext.insert(&mut client.transact_mut(), 0, "alices");
    let u = client.transact().encode_state_as_update_v1(&sv);
    let relay = server.apply_update_as(alice, &u).unwrap();
    client
        .transact_mut()
        .apply_update(Update::decode_v1(&relay).unwrap())
        .unwrap();

    let sv = client.transact().state_vector();
    {
        let mut txn = client.transact_mut();
        ctext.remove_range(&mut txn, 0, 6);
        ctext.insert(&mut txn, 0, "mallory");
    }
    let u = client.transact().encode_state_as_update_v1(&sv);
    server.apply_update_as(mallory, &u).unwrap();

    assert_eq!(server.text(), "mallory");
    let spans = server.snapshot().1;
    // "m" + "al" (aligned with alice's) + "lory".
    assert_eq!(spans, vec![(2, 2, 1), (1, 1, 2), (2, 2, 4)]);
    // The point: alice is never credited with a run mallory *named* her for, and
    // the bulk still lands on mallory.
    let alice_bytes: u64 = spans.iter().filter(|s| s.0 == 1).map(|s| s.2).sum();
    assert!(
        alice_bytes < 3,
        "diff alignment should be incidental, not wholesale"
    );
}

// --- co-edit authorship reaches the op-log, not only the blame index ---------
//
// A checkpoint used to record exactly one op-log row: a whole-file "write" by
// whoever ran it. Every other contributor got none. That left the CRDT's own
// attribute as the *only* record of who wrote which bytes of a co-edited file —
// nothing to rebuild blame from and nothing to cross-check it against — and it
// left `revert_session`, which discovers files through the op-log, unable to
// find a contributor's work at all.

#[tokio::test]
async fn coedit_checkpoint_records_a_row_for_every_contributor() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_c = fs.create_session(claude, None).await.unwrap();

    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(alice, s_a), 0, "hello ");
    doc.insert(WriteCtx::session(claude, s_c), 6, "world");

    // Claude checkpoints; alice never does.
    fs.checkpoint_coedit(WriteCtx::session(claude, s_c), "/doc", &doc)
        .await
        .unwrap();

    let a_ops = fs.edit_ops(alice, Some(s_a)).await.unwrap();
    assert_eq!(a_ops.len(), 1, "alice authored bytes but has no op-log row");
    assert_eq!(a_ops[0].op, "coedit");
    assert_eq!(a_ops[0].path, "/doc");
    assert_eq!((a_ops[0].byte_start, a_ops[0].byte_len), (0, 6));

    // The checkpointer keeps their whole-file row *and* gains a precise one.
    let c_ops = fs.edit_ops(claude, Some(s_c)).await.unwrap();
    assert!(c_ops.iter().any(|o| o.op == "write" && o.byte_start == 0));
    assert!(
        c_ops
            .iter()
            .any(|o| o.op == "coedit" && (o.byte_start, o.byte_len) == (6, 5))
    );
}

// The headline consequence: a co-editor who never ran a checkpoint can now be
// reverted. Before this, `revert_session` returned Ok(vec![]) and changed
// nothing — silently — even though the co-editing sockets deliberately open a
// session per connection so that reverting one would work (#98).
#[tokio::test]
async fn coedit_a_contributor_who_never_checkpointed_can_be_reverted() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_c = fs.create_session(claude, None).await.unwrap();

    // Whole lines, because `revert_session` keeps any line with mixed authorship
    // rather than splitting it (#33) — that limitation is separate from this one.
    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(alice, s_a), 0, "alice line\n");
    doc.insert(WriteCtx::session(claude, s_c), 11, "claude line\n");

    fs.checkpoint_coedit(WriteCtx::session(claude, s_c), "/doc", &doc)
        .await
        .unwrap();
    assert_eq!(
        &fs.read("/doc").await.unwrap()[..],
        b"alice line\nclaude line\n"
    );

    let changed = fs.revert_session(alice, s_a, None).await.unwrap();
    assert_eq!(
        changed,
        vec!["/doc".to_string()],
        "alice's file was not found"
    );
    assert_eq!(
        &fs.read("/doc").await.unwrap()[..],
        b"claude line\n",
        "alice's contribution was not removed"
    );
}

// The sweeper writes on a timer, so a checkpoint that introduces no new
// authorship must add no rows — otherwise an open document accrues op-log rows
// forever.
#[tokio::test]
async fn coedit_a_repeat_checkpoint_records_no_new_rows() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();

    let doc = CoeditDoc::new();
    doc.insert(WriteCtx::session(alice, s_a), 0, "stable");
    let ctx = WriteCtx::session(alice, s_a);
    fs.checkpoint_coedit(ctx, "/doc", &doc).await.unwrap();
    let after_first = fs.edit_ops(alice, Some(s_a)).await.unwrap().len();

    for _ in 0..3 {
        fs.checkpoint_coedit(ctx, "/doc", &doc).await.unwrap();
    }
    let after_repeats = fs.edit_ops(alice, Some(s_a)).await.unwrap().len();

    // The whole-file "write" row is unconditional (it records that a checkpoint
    // ran); the per-contributor rows must not repeat.
    let coedit_rows = fs
        .edit_ops(alice, Some(s_a))
        .await
        .unwrap()
        .iter()
        .filter(|o| o.op == "coedit")
        .count();
    assert_eq!(
        coedit_rows, 1,
        "unchanged authorship re-recorded per checkpoint"
    );
    assert_eq!(after_repeats - after_first, 3, "only the write rows repeat");
}

// A string-valued embed must not be mistaken for text.
//
// `Text::diff` renders `ItemContent::Embed(Any::String(s))` as
// `Out::Any(Any::String(s))` — byte-for-byte the shape real text has — while yrs
// indexes *every* embed as exactly one position. Counting such a chunk's bytes as
// its index length inflated every index after it, so the repair landed past its
// target and a forged stamp survived all the way into durable blame. It also put
// the embed's bytes into the file, which `text()` (GetString) excludes — so
// `snapshot()` and `text()` disagreed. Reachable from a stock Yjs client through
// `ytext.insertEmbed(index, "a string", attrs)`.
#[test]
fn coedit_a_string_embed_cannot_carry_a_forged_author() {
    use yrs::{Any, Doc, OffsetKind, Options, ReadTxn, Text, Transact};

    let bob = WriteCtx::session(2, 2);
    let server = CoeditDoc::new();

    let client = Doc::with_options(Options {
        offset_kind: OffsetKind::Bytes,
        ..Default::default()
    });
    let ct = client.get_or_insert_text("content");
    let sv = client.transact().state_vector();
    {
        let mut txn = client.transact_mut();
        // A 20-byte string embed, then text stamped as the victim (actor 1).
        let mine = yrs::types::Attrs::from([("a".into(), Any::from("2,2".to_string()))]);
        ct.insert_embed_with_attributes(&mut txn, 0, Any::from("XXXXXXXXXXXXXXXXXXXX"), mine);
        let victim = yrs::types::Attrs::from([("a".into(), Any::from("1,1".to_string()))]);
        ct.insert_with_attributes(&mut txn, 1, "FORGED", victim);
    }
    let u = client.transact().encode_state_as_update_v1(&sv);
    server.apply_update_as(bob, &u).unwrap();

    let (text, spans) = server.snapshot();
    // The embed contributes no bytes, exactly as `GetString` reports.
    assert_eq!(text, "FORGED");
    assert_eq!(text, server.text(), "snapshot() and text() must agree");
    assert_eq!(
        spans,
        vec![(2, 2, 6)],
        "the victim was credited bob's bytes"
    );
}
