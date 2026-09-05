//! Per-actor undo/redo on a live co-edited document (#146).
//!
//! The unit is one **actor**, not the room and not a session: undo pops what the
//! person pressing the key typed, and is not reachable to anyone else's work.
//! That scoping is `yrs`'s tracked-origin filter, fed by the origins
//! `coedit.rs` stamps on every attributed transaction — see
//! `tests/coedit_origin.rs` for the `yrs` behaviour underneath it.
//!
//! The assertions that matter most here are the attribution ones. An undo is an
//! ordinary forward edit, so it must not become a way to rewrite who wrote what:
//! undoing a deletion has to give the text back to its **original** author, not
//! to whoever pressed Ctrl+Z. `checkpoint_coedit` reads exactly what these tests
//! read, so a regression here is a regression in the durable blame index.

#![cfg(feature = "coedit")]

use origofs_core::WriteCtx;
use origofs_core::coedit::CoeditDoc;

/// Drive `server` as a vanilla Yjs client would: `who` types `chunk`, and the
/// server attributes it. Returns the update the room would relay.
fn types(server: &CoeditDoc, who: WriteCtx, at: u32, chunk: &str) -> Vec<u8> {
    let client = CoeditDoc::new();
    client
        .apply_update(&server.state_update())
        .expect("catch up");
    client.insert(who, at, chunk);
    server
        .apply_update_as(who, &client.state_update())
        .expect("apply")
}

/// Wrap a raw update in the y-sync `Update` message `apply_relayed` reads.
fn sync_frame(update: &[u8]) -> Vec<u8> {
    use yrs::sync::{Message, SyncMessage};
    use yrs::updates::encoder::Encode;
    Message::Sync(SyncMessage::Update(update.to_vec())).encode_v1()
}

const ALICE: WriteCtx = WriteCtx {
    actor: 1,
    session: Some(10),
    tool_call: None,
};
const BOB: WriteCtx = WriteCtx {
    actor: 2,
    session: Some(20),
    tool_call: None,
};

#[test]
fn an_actor_undoes_their_own_typing() {
    let doc = CoeditDoc::new();
    doc.track_undo(ALICE);

    types(&doc, ALICE, 0, "hello");
    assert_eq!(doc.text(), "hello");
    assert!(doc.can_undo(1));

    // A peer holding the pre-undo state, as every other socket in the room does.
    let peer = CoeditDoc::new();
    peer.apply_update(&doc.state_update()).expect("peer");
    assert_eq!(peer.text(), "hello");

    let relay = doc.undo_as(ALICE).expect("undo");
    assert_eq!(doc.text(), "");
    assert!(
        !relay.is_empty(),
        "an undo that changed the document must relay"
    );

    // The frame carries the undo to that peer through the room's ordinary
    // fan-out — which is why undo needs no new message type on the y-sync wire.
    peer.apply_relayed(&relay).expect("fan out the undo");
    assert_eq!(
        peer.text(),
        "",
        "the undo did not reach a peer through the frame the room broadcasts"
    );
}

#[test]
fn an_actor_cannot_undo_someone_elses_typing() {
    let doc = CoeditDoc::new();
    doc.track_undo(ALICE);
    doc.track_undo(BOB);

    types(&doc, BOB, 0, "bob wrote this");
    assert!(
        !doc.can_undo(1),
        "alice can reach bob's edit — the origin scoping is not holding"
    );
    assert_eq!(doc.undo_as(ALICE).expect("undo"), Vec::<u8>::new());
    assert_eq!(doc.text(), "bob wrote this");

    // Bob can, and only back to his own work.
    assert!(doc.can_undo(2));
    doc.undo_as(BOB).expect("undo");
    assert_eq!(doc.text(), "");
}

/// The trap #146 was really about: an edit that reached this worker over the
/// cross-worker relay was made on a machine this process never spoke to, and
/// must never be poppable here.
#[test]
fn a_relayed_edit_is_not_undoable() {
    let doc = CoeditDoc::new();
    doc.track_undo(ALICE);

    let elsewhere = CoeditDoc::new();
    elsewhere.insert(BOB, 0, "typed on another worker");
    doc.apply_relayed(&sync_frame(&elsewhere.state_update()))
        .expect("relay");

    assert_eq!(doc.text(), "typed on another worker");
    assert!(
        !doc.can_undo(1),
        "a relayed frame landed on an undo stack — Ctrl+Z would pop an edit from \
         another worker"
    );
}

/// Undoing a deletion gives the text back to **its** author, not to whoever
/// pressed the key. This is the property that keeps `checkpoint_coedit` honest:
/// the blame it writes comes from exactly this authorship map.
#[test]
fn undoing_a_deletion_restores_the_original_author() {
    let doc = CoeditDoc::new();
    doc.track_undo(BOB);

    types(&doc, ALICE, 0, "alice wrote this");
    let (_, spans) = doc.snapshot();
    assert_eq!(spans, vec![(1, 10, 16)], "alice should own all 16 bytes");

    // Bob deletes Alice's sentence, then thinks better of it.
    let client = CoeditDoc::new();
    client.apply_update(&doc.state_update()).expect("catch up");
    client.remove(0, 16);
    doc.apply_update_as(BOB, &client.state_update())
        .expect("apply");
    assert_eq!(doc.text(), "");

    doc.undo_as(BOB).expect("undo");
    assert_eq!(doc.text(), "alice wrote this");

    let (text, spans) = doc.snapshot();
    assert_eq!(text, "alice wrote this");
    assert_eq!(
        spans,
        vec![(1, 10, 16)],
        "undoing a deletion re-credited the text to the actor who undid it — an \
         undo must not be a way to take authorship of someone else's work"
    );
}

#[test]
fn redo_restores_the_undone_edit_and_its_authorship() {
    let doc = CoeditDoc::new();
    doc.track_undo(ALICE);

    types(&doc, ALICE, 0, "draft");
    doc.undo_as(ALICE).expect("undo");
    assert_eq!(doc.text(), "");
    assert!(doc.can_redo(1));

    let relay = doc.redo_as(ALICE).expect("redo");
    assert_eq!(doc.text(), "draft");
    assert!(!relay.is_empty());
    assert_eq!(
        doc.snapshot().1,
        vec![(1, 10, 5)],
        "a redone edit must come back attributed to its author"
    );
}

/// One person, two tabs: one stack. Each session contributes its origin to the
/// actor's manager, so undo means "my last edit" rather than "my last edit in
/// this tab".
#[test]
fn two_sessions_of_one_actor_share_a_stack() {
    let doc = CoeditDoc::new();
    let tab_a = ALICE;
    let tab_b = WriteCtx {
        actor: 1,
        session: Some(11),
        tool_call: None,
    };
    doc.track_undo(tab_a);
    doc.track_undo(tab_b);

    types(&doc, tab_a, 0, "from tab a");
    // Undo it from the *other* tab.
    assert!(doc.can_undo(1));
    doc.undo_as(tab_b).expect("undo");
    assert_eq!(doc.text(), "");
}

/// A manager only sees transactions that commit after it exists, so tracking has
/// to happen when a socket opens rather than when somebody presses the key. If
/// this ever stops being true the API can get simpler; until then it is the
/// reason `track_undo` is a separate call.
#[test]
fn edits_before_tracking_are_not_undoable() {
    let doc = CoeditDoc::new();
    types(&doc, ALICE, 0, "typed before the socket was tracked");
    doc.track_undo(ALICE);

    assert!(!doc.can_undo(1));
    assert_eq!(doc.undo_as(ALICE).expect("undo"), Vec::<u8>::new());
}

/// Undo does not outlive the room: it is an editor affordance, not history.
#[test]
fn untracking_drops_the_stack() {
    let doc = CoeditDoc::new();
    doc.track_undo(ALICE);
    types(&doc, ALICE, 0, "hello");
    assert!(doc.can_undo(1));

    doc.untrack_undo(1);
    assert!(!doc.can_undo(1));
    assert_eq!(doc.undo_as(ALICE).expect("undo"), Vec::<u8>::new());
    assert_eq!(
        doc.text(),
        "hello",
        "untracking must not change the document"
    );
}

/// Nothing to undo is an empty relay, not an error — a client asking too early
/// (or after reconnecting onto a fresh room) is a benign race, not a failure.
#[test]
fn an_empty_stack_is_not_an_error() {
    let doc = CoeditDoc::new();
    doc.track_undo(ALICE);
    assert_eq!(doc.undo_as(ALICE).expect("undo"), Vec::<u8>::new());
    assert_eq!(doc.redo_as(ALICE).expect("redo"), Vec::<u8>::new());
    // An actor that was never tracked at all takes the same path.
    assert_eq!(doc.undo_as(BOB).expect("undo"), Vec::<u8>::new());
}
