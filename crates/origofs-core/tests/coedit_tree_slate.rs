//! The tree room against a **real** `@slate-yjs/core` peer (issue #152).
//!
//! #152 reported the tree room (`CoeditTreeDoc`, a `Y.XmlFragment`) as
//! *wire-incompatible* with `@platejs/yjs`, on the grounds that the PlateJS
//! binding — `@slate-yjs/core` — roots its document at a `Y.XmlText` instead, so
//! "a client bound that way and a server serving `CoeditTreeDoc` never converge".
//!
//! The root-type observation is correct: `@slate-yjs/core@1.0.2` really does
//! `doc.get("content", Y.XmlText)`, and yrs cannot even *create* an `XmlText`
//! root — `XmlTextRef` does not implement `RootRef`, and the crate says why
//! ("not bound to be used as root-level types"). So there is no `XmlText`-rooted
//! room to build, on the pinned yrs or any version #144 leaves reachable.
//!
//! But the conclusion drawn from it does not hold, and this file is why. Yjs keys
//! root types by **name**; `doc.get(name, T)` binds a view of whatever branch is
//! already there rather than asserting a type the peer must match. An
//! `XmlFragment`-rooted server and an `XmlText`-rooted client therefore address
//! the same branch and converge — the document round-trips, and origofs's
//! per-run attribution survives it intact.
//!
//! These bytes come from that client. They are not a model of one: they were
//! produced by `yjs@13` + `@slate-yjs/core@1.0.2` driving
//! `slateNodesToInsertDelta`, captured verbatim, and the Slate value below is
//! what `yTextToSlateElement` reconstructs from the server's state afterwards.
//! `tests/fixtures/slate_yjs_client.mjs` regenerates them. Hand-writing a client
//! is exactly the mistake that let this compatibility claim go unchecked in the
//! first place — `api_coedit_tree_ws.rs` calls a raw `yrs::Doc` "what PlateJS
//! runs", and for Plate that is the ProseMirror shape, not Slate's.
//!
//! What a Slate host does have to know is in `coedit_tree.rs`'s module docs: the
//! `a`/`n` stamps arrive as ordinary Yjs formatting attributes, which on this
//! binding means **two extra marks on every Slate text node**.

#![cfg(feature = "coedit")]

use origofs_core::CoeditTreeDoc;
use origofs_core::attribution::WriteCtx;

/// `Y.encodeStateAsUpdate` of a fresh `@slate-yjs/core` document holding
/// two paragraphs: `["hello ", bold "world"]` and `["second para"]`.
const SLATE_INITIAL: &str = concat!(
    "0109f5c18dd70100070107636f6e74656e74062800f5c18dd7010004747970650177097061726167726170680400f5c1",
    "8dd701000668656c6c6f2086f5c18dd7010704626f6c64047472756584f5c18dd7010805776f726c6486f5c18dd7010d",
    "04626f6c64046e756c6c87f5c18dd70100062800f5c18dd7010f04747970650177097061726167726170680400f5c18d",
    "d7010f0b7365636f6e64207061726100",
);

/// The same client typing ` TYPED-BY-CLIENT` at the end of the first paragraph,
/// encoded against the state vector it had already synced.
const SLATE_SECOND_EDIT: &str =
    "01018da2d6970600c4f5c18dd7010df5c18dd7010e102054595045442d42592d434c49454e5400";

fn bytes(hex: &str) -> Vec<u8> {
    (0..hex.len() / 2)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

/// The headline: a real Slate document lands in the tree room, with its text
/// intact and every run attributed to the connection that delivered it.
#[test]
fn a_real_slate_document_lands_in_the_tree_room_attributed() {
    let doc = CoeditTreeDoc::new("content");
    doc.apply_update_as(WriteCtx::actor(7), &bytes(SLATE_INITIAL))
        .expect("an @slate-yjs/core update must apply to the tree room");

    assert!(!doc.is_empty(), "the document must not resume empty");
    assert_eq!(doc.plain_text(), "hello worldsecond para");

    let runs = doc.runs();
    assert_eq!(
        runs.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
        ["hello ", "world", "second para"],
        "the Slate marks split the runs, which is what makes them separately blameable"
    );
    assert!(
        runs.iter().all(|r| r.actor == 7),
        "every run is stamped with the connection's actor, not one the client named"
    );
    assert!(
        runs.iter().all(|r| r.node.is_some()),
        "every run carries a server-issued node id for the host's span map"
    );
}

/// The property the whole tree shape exists for, on this binding: two
/// connections editing one document are told apart per run.
///
/// This is what "mirror a flat `Y.Text`" cannot do — a whole-file text diff
/// collapses concurrent edits in different paragraphs into one replaced span.
#[test]
fn two_connections_are_attributed_separately_across_a_slate_document() {
    let doc = CoeditTreeDoc::new("content");
    doc.apply_update_as(WriteCtx::actor(7), &bytes(SLATE_INITIAL))
        .unwrap();
    // The same document, edited over a second connection belonging to someone else.
    doc.apply_update_as(WriteCtx::actor(9), &bytes(SLATE_SECOND_EDIT))
        .unwrap();

    assert_eq!(doc.plain_text(), "hello world TYPED-BY-CLIENTsecond para");

    let by_actor: Vec<(String, i64)> = doc.runs().into_iter().map(|r| (r.text, r.actor)).collect();
    assert_eq!(
        by_actor,
        vec![
            ("hello ".to_string(), 7),
            ("world".to_string(), 7),
            (" TYPED-BY-CLIENT".to_string(), 9),
            ("second para".to_string(), 7),
        ],
        "the second connection's insert is blamed on actor 9 and nothing else moved"
    );
}

/// The server's state goes back out as something the Slate side can read.
///
/// Asserted here as the structure origofs controls — the node ids and author
/// stamps it issued are present as formatting attributes, which is precisely how
/// they reach `yTextToSlateElement` as marks. The JS half of this round trip is
/// in `tests/fixtures/slate_yjs_client.mjs`; running it against this state yields
///
/// ```json
/// {"children":[
///   {"type":"paragraph","children":[
///      {"a":"7,0","n":"…0","text":"hello "},
///      {"a":"7,0","n":"…0","bold":true,"text":"world"}]},
///   {"type":"paragraph","children":[{"a":"7,0","n":"…1","text":"second para"}]}]}
/// ```
///
/// — the original Slate value, plus the two stamps.
#[test]
fn the_servers_state_carries_the_stamps_a_slate_host_reads_as_marks() {
    let doc = CoeditTreeDoc::new("content");
    doc.apply_update_as(WriteCtx::actor(7), &bytes(SLATE_INITIAL))
        .unwrap();

    let state = doc.state_update();
    assert!(!state.is_empty());

    // Reloading the emitted state must reproduce the same document: this is the
    // sidecar path, and a state a peer cannot rebuild from is the "does not
    // reconstruct" failure #152 describes.
    let reloaded = CoeditTreeDoc::load("content", &state).expect("state must reload");
    assert_eq!(reloaded.plain_text(), "hello worldsecond para");
    assert_eq!(
        reloaded.runs().len(),
        3,
        "the run split -- and so the blame -- survives a sidecar round trip"
    );

    // Every stamp origofs resolves back to the actor that made it.
    let authors = reloaded.authors();
    assert!(!authors.is_empty(), "node ids must resolve to authors");
    assert!(
        authors.values().all(|&(actor, _)| actor == 7),
        "an id origofs issued resolves to the connection that earned it"
    );
}
