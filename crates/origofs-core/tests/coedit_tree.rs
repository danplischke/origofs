//! Structured (tree-shaped) co-editing, issue #92: a `Y.XmlFragment` document a
//! rich-text editor binds to natively, whose per-node authorship the host projects
//! onto its own serialized bytes with a span map. Requires the `coedit` feature.
#![cfg(feature = "coedit")]

use origofs_core::{
    CoeditTreeDoc, Fs, MemStore, OrigoFSError, SqliteMetadataStore, TreeSpan, WriteCtx,
};
use std::sync::Arc;
use yrs::types::text::YChange;
use yrs::types::xml::{Xml, XmlElementPrelim, XmlFragment, XmlOut, XmlTextPrelim};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Any, Doc, Out, ReadTxn, StateVector, Text, Transact, Update};

const ROOT: &str = "content";

async fn fixture() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();
    fs
}

/// A stand-in for a real Yjs editor: a plain `yrs` document with the same
/// `XmlFragment` root, speaking nothing but opaque updates — exactly what
/// `@platejs/yjs` or `y-prosemirror` would put on the wire.
struct Client {
    doc: Doc,
    frag: yrs::types::xml::XmlFragmentRef,
    /// What this client has already sent, so `pending` yields only the new part.
    sent: StateVector,
}

impl Client {
    fn new() -> Self {
        let doc = Doc::new();
        let frag = doc.get_or_insert_xml_fragment(ROOT);
        Self {
            doc,
            frag,
            sent: StateVector::default(),
        }
    }

    /// Append a `<p>` holding `text` and return nothing — the editor's "type a new
    /// paragraph".
    fn add_paragraph(&self, text: &str) {
        let mut txn = self.doc.transact_mut();
        let p = self.frag.push_back(&mut txn, XmlElementPrelim::empty("p"));
        p.push_back(&mut txn, XmlTextPrelim::new(text));
    }

    /// Append `text` to the end of the `index`-th paragraph's text node — the case
    /// that matters most, because Yjs makes the insert inherit whatever formatting
    /// attributes (including someone else's author stamp) sit at that position.
    fn append_to_paragraph(&self, index: u32, text: &str) {
        let mut txn = self.doc.transact_mut();
        let p = self
            .frag
            .get(&txn, index)
            .and_then(XmlOut::into_xml_element)
            .expect("paragraph");
        let Some(XmlOut::Text(t)) = p.get(&txn, 0) else {
            panic!("paragraph has no text node")
        };
        let at = t.len(&txn);
        t.insert(&mut txn, at, text);
    }

    /// The update carrying everything this client has not sent yet.
    fn pending(&mut self) -> Vec<u8> {
        let txn = self.doc.transact();
        let update = txn.encode_state_as_update_v1(&self.sent);
        self.sent = txn.state_vector();
        update
    }

    /// Send everything new to the server as `ctx`, and merge back the attributed
    /// delta — one full round trip, exactly as a socket does.
    fn sync(&mut self, doc: &CoeditTreeDoc, ctx: WriteCtx) {
        let update = self.pending();
        let back = doc.apply_update_as(ctx, &update).unwrap();
        if !back.is_empty() {
            self.receive(&back);
        }
    }

    /// Merge what the server sent back.
    fn receive(&self, update: &[u8]) {
        self.doc
            .transact_mut()
            .apply_update(Update::decode_v1(update).unwrap())
            .unwrap();
    }

    /// Every text run this client can see, as `(text, node id)` — the exact reading
    /// a host does (`ytext.toDelta()`) to build its span map.
    fn runs(&self) -> Vec<(String, Option<String>)> {
        let txn = self.doc.transact();
        let mut out = Vec::new();
        for node in self.frag.successors(&txn) {
            let XmlOut::Text(text) = node else { continue };
            for chunk in text.diff(&txn, YChange::identity) {
                let Out::Any(Any::String(piece)) = &chunk.insert else {
                    continue;
                };
                let node_id = match chunk.attributes.as_deref().and_then(|a| a.get("n")) {
                    Some(Any::String(id)) => Some(id.to_string()),
                    _ => None,
                };
                out.push((piece.to_string(), node_id));
            }
        }
        out
    }
}

/// The node id for the run whose text is exactly `text`.
fn node_of(runs: &[(String, Option<String>)], text: &str) -> String {
    runs.iter()
        .find(|(t, _)| t == text)
        .unwrap_or_else(|| panic!("no run {text:?} in {runs:?}"))
        .1
        .clone()
        .unwrap_or_else(|| panic!("run {text:?} carries no node id"))
}

// The headline property: two people edit a *tree*, and their exact byte ranges land
// in blame against the host's own serialization — no whole-file text diff anywhere.
#[tokio::test]
async fn a_hosts_span_map_lands_each_authors_exact_bytes_in_blame() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let claude = fs.create_agent("claude", "m", Some(alice)).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_c = fs.create_session(claude, None).await.unwrap();

    let doc = fs
        .open_coedit_tree(WriteCtx::session(alice, s_a), "/notes.md", ROOT)
        .await
        .unwrap();
    assert!(!doc.resumed(), "nothing to resume from yet");
    assert!(doc.is_empty());

    // Alice types a paragraph; Claude adds a second one.
    let mut editor = Client::new();
    editor.add_paragraph("hello");
    editor.sync(&doc, WriteCtx::session(alice, s_a));

    editor.add_paragraph("world");
    editor.sync(&doc, WriteCtx::session(claude, s_c));

    assert_eq!(doc.plain_text(), "helloworld");

    // The host serializes its tree its own way — origofs never sees the schema —
    // and says which bytes came from which node. The "# Notes\n\n", the paragraph
    // break and the trailing newline are the *serializer's* bytes, covered by no
    // span.
    let runs = editor.runs();
    let body = b"# Notes\n\nhello\n\nworld\n";
    let spans = [
        TreeSpan::new(9, 14, node_of(&runs, "hello")),
        TreeSpan::new(16, 21, node_of(&runs, "world")),
    ];
    fs.checkpoint_coedit_tree(
        WriteCtx::session(claude, s_c),
        "/notes.md",
        &doc,
        body,
        &spans,
    )
    .await
    .unwrap();

    assert_eq!(&fs.read("/notes.md").await.unwrap()[..], body);
    let blame = fs.blame("/notes.md").await.unwrap();
    let ranges: Vec<_> = blame
        .iter()
        .map(|r| (r.actor.id, r.byte_start, r.byte_end))
        .collect();
    assert_eq!(
        ranges,
        vec![
            (claude, 0, 9), // "# Notes\n\n" — serializer output, to the checkpointer
            (alice, 9, 14), // "hello"
            // "\n\n" + "world" + "\n": Claude's paragraph and the serializer bytes
            // either side of it are one author, so they land as one range rather
            // than three — the blame index stays compact.
            (claude, 14, 22),
        ],
        "got {blame:?}"
    );
    assert_eq!(blame[1].session, Some(s_a));
}

// Yjs makes an insert inherit the formatting attributes at its position, so text
// typed against the end of Alice's run arrives already wearing Alice's stamp. The
// server must see through that — this is the reason attribution is a content diff
// rather than "which runs lack an author".
#[tokio::test]
async fn text_typed_against_another_authors_run_is_not_credited_to_them() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_b = fs.create_session(bob, None).await.unwrap();

    let doc = fs
        .open_coedit_tree(WriteCtx::session(alice, s_a), "/n.md", ROOT)
        .await
        .unwrap();

    let mut editor = Client::new();
    editor.add_paragraph("hello");
    editor.sync(&doc, WriteCtx::session(alice, s_a));

    // Bob types directly onto the end of Alice's run, inheriting her attributes.
    editor.append_to_paragraph(0, " there");
    editor.sync(&doc, WriteCtx::session(bob, s_b));
    assert_eq!(doc.plain_text(), "hello there");

    let runs = editor.runs();
    let body = b"hello there";
    let spans = [
        TreeSpan::new(0, 5, node_of(&runs, "hello")),
        TreeSpan::new(5, 11, node_of(&runs, " there")),
    ];
    fs.checkpoint_coedit_tree(WriteCtx::session(alice, s_a), "/n.md", &doc, body, &spans)
        .await
        .unwrap();

    let blame = fs.blame("/n.md").await.unwrap();
    assert_eq!(
        blame
            .iter()
            .map(|r| (r.actor.id, r.byte_start, r.byte_end))
            .collect::<Vec<_>>(),
        vec![(alice, 0, 5), (bob, 5, 11)],
        "the inherited stamp must not have carried Bob's text to Alice"
    );
}

// The server is the sole authority on authorship: an author a client writes onto
// content it is introducing is overwritten, not believed.
#[tokio::test]
async fn a_client_cannot_name_the_author_of_its_own_content() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let mallory = fs.create_human("mallory", None).await.unwrap();
    let s_m = fs.create_session(mallory, None).await.unwrap();

    let doc = CoeditTreeDoc::new(ROOT);

    // Mallory's client stamps Alice as the author of its own paragraph and its own
    // text run, using exactly the attribute keys origofs uses.
    let client = Doc::new();
    let frag = client.get_or_insert_xml_fragment(ROOT);
    {
        let mut txn = client.transact_mut();
        let p = frag.push_back(&mut txn, XmlElementPrelim::empty("p"));
        p.insert_attribute(&mut txn, "a", format!("{alice},0"));
        p.insert_attribute(&mut txn, "n", "forged-element");
        let t = p.push_back(&mut txn, XmlTextPrelim::new("innocuous"));
        let attrs = yrs::types::Attrs::from([
            ("a".into(), Any::from(format!("{alice},0"))),
            ("n".into(), Any::from("forged-run".to_string())),
        ]);
        t.format(&mut txn, 0, 9, attrs);
    }
    let update = client
        .transact()
        .encode_state_as_update_v1(&StateVector::default());
    doc.apply_update_as(WriteCtx::session(mallory, s_m), &update)
        .unwrap();

    let authors = doc.authors();
    assert!(
        !authors.values().any(|&(actor, _)| actor == alice),
        "no node may resolve to the actor the client named: {authors:?}"
    );
    assert!(
        authors.values().all(|&(actor, _)| actor == mallory),
        "every stamped node belongs to the connection's actor: {authors:?}"
    );
    // The forged ids resolve to nothing, so citing one in a span map falls back to
    // the checkpointer rather than to Alice.
    assert!(!authors.contains_key("forged-run"));
    assert!(!authors.contains_key("forged-element"));
}

// A tree cannot be reconstructed from flat bytes without the host's schema, so a
// write that lands around the live document is refused rather than clobbered.
#[tokio::test]
async fn an_out_of_band_write_is_refused_instead_of_silently_clobbered() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let bob = fs.create_human("bob", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let s_b = fs.create_session(bob, None).await.unwrap();
    let ctx_a = WriteCtx::session(alice, s_a);

    fs.write_as(ctx_a, "/n.md", b"original\n").await.unwrap();
    let doc = fs.open_coedit_tree(ctx_a, "/n.md", ROOT).await.unwrap();

    let mut editor = Client::new();
    editor.add_paragraph("edited");
    doc.apply_update_as(ctx_a, &editor.pending()).unwrap();

    // Bob writes the file directly, outside the co-editing session.
    fs.write_as(WriteCtx::session(bob, s_b), "/n.md", b"bob was here\n")
        .await
        .unwrap();

    let err = fs
        .checkpoint_coedit_tree(ctx_a, "/n.md", &doc, b"edited\n", &[])
        .await
        .unwrap_err();
    assert!(matches!(err, OrigoFSError::Conflict(_)), "got {err:?}");
    assert_eq!(
        &fs.read("/n.md").await.unwrap()[..],
        b"bob was here\n",
        "the foreign write survives"
    );
}

// A session survives a restart: the sidecar resumes the CRDT — and stops resuming
// it the moment the file moves underneath, because then it would be a lie.
#[tokio::test]
async fn the_sidecar_resumes_a_session_and_declines_to_when_it_is_stale() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let ctx = WriteCtx::session(alice, s_a);

    let doc = fs.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    let mut editor = Client::new();
    editor.add_paragraph("hello");
    editor.sync(&doc, ctx);
    let runs = editor.runs();
    let spans = [TreeSpan::new(0, 5, node_of(&runs, "hello"))];
    fs.checkpoint_coedit_tree(ctx, "/n.md", &doc, b"hello", &spans)
        .await
        .unwrap();
    fs.end_coedit("/n.md").await.unwrap();

    // Re-open: same tree, same node ids, same authors.
    let again = fs.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    assert!(again.resumed());
    assert_eq!(again.plain_text(), "hello");
    assert_eq!(again.authors(), doc.authors());

    // A different root is a different document, so it must not be resumed into.
    let other = fs.load_coedit_tree("/n.md", "prosemirror").await.unwrap();
    assert!(!other.resumed());
    assert!(other.is_empty());

    // A plain write moves the file: the sidecar no longer describes it.
    fs.write_as(ctx, "/n.md", b"rewritten").await.unwrap();
    let stale = fs.load_coedit_tree("/n.md", ROOT).await.unwrap();
    assert!(
        !stale.resumed() && stale.is_empty(),
        "a stale sidecar must not be resumed; the host reseeds from the file"
    );
}

// Only the host can serialize a tree, so the server persists the CRDT on its own —
// enough that a crashed worker loses no editing history — without pretending the
// durable file moved.
#[tokio::test]
async fn persisting_keeps_the_session_recoverable_without_touching_the_file() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let ctx = WriteCtx::session(alice, s_a);

    let doc = fs.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();
    let mut editor = Client::new();
    editor.add_paragraph("unsaved");
    doc.apply_update_as(ctx, &editor.pending()).unwrap();

    fs.persist_coedit_tree("/n.md", &doc).await.unwrap();

    // The file is untouched and still reads as absent…
    assert!(matches!(
        fs.read("/n.md").await,
        Err(OrigoFSError::NotFound(_))
    ));
    // …and the marker does not claim the bytes were crystallized.
    let live = fs.live_doc("/n.md").await.unwrap().unwrap();
    assert_eq!(live.checkpointed_at, None);
    // …but the typing survives a restart.
    let resumed = fs.load_coedit_tree("/n.md", ROOT).await.unwrap();
    assert!(resumed.resumed());
    assert_eq!(resumed.plain_text(), "unsaved");
}

// The wire path a browser actually uses: y-sync frames in, attributed frames out.
#[tokio::test]
async fn the_y_sync_handshake_drives_a_tree_room() {
    let fs = fixture().await;
    let alice = fs.create_human("alice", None).await.unwrap();
    let s_a = fs.create_session(alice, None).await.unwrap();
    let ctx = WriteCtx::session(alice, s_a);
    let doc = fs.open_coedit_tree(ctx, "/n.md", ROOT).await.unwrap();

    // The server greets with SyncStep1; the client answers with its whole state.
    let greeting = doc.sync_start();
    assert!(!greeting.is_empty());

    let mut editor = Client::new();
    editor.add_paragraph("typed over the wire");
    let mut encoder = EncoderV1::new();
    yrs::sync::Message::Sync(yrs::sync::SyncMessage::Update(editor.pending())).encode(&mut encoder);
    let frame = encoder.to_vec();
    let out = doc.handle_sync(ctx, &frame).unwrap();

    assert_eq!(doc.plain_text(), "typed over the wire");
    assert!(
        !out.broadcast.is_empty() && !out.reply.is_empty(),
        "peers get the content and the sender gets our attribution items back"
    );
    // Re-applying the same frame is a no-op, so nothing is relayed twice.
    let again = doc.handle_sync(ctx, &frame).unwrap();
    assert!(again.broadcast.is_empty());
}
