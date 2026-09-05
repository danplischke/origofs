//! Per-actor undo/redo on the **tree** shape (#146).
//!
//! `tests/coedit_undo.rs` covers the shared rulings on the flat shape. What is
//! different here, and why this file exists rather than a couple of extra cases
//! there, is the **node ids**.
//!
//! origofs does not own a tree document's schema, so `checkpoint_coedit_tree`
//! takes the host's serialized bytes plus a span map citing the `n` (node id)
//! origofs stamped on each run — and resolves each id to the author it recorded
//! itself. An undo that restored content under *fresh* ids would produce a
//! checkpoint whose spans name nodes nobody issued, and every one of them would
//! resolve to nobody: the file would land with its authorship silently erased,
//! with no error anywhere. So the assertions below are about ids and authors
//! surviving a round trip, not only about text coming back.

#![cfg(feature = "coedit")]

use origofs_core::WriteCtx;
use origofs_core::coedit_tree::CoeditTreeDoc;

const ROOT: &str = "content";

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

/// A client replica caught up to `server`, for driving edits the way a real
/// editor does — through an update, not through the in-process seeding path.
fn replica(server: &CoeditTreeDoc) -> CoeditTreeDoc {
    let client = CoeditTreeDoc::new(ROOT);
    client
        .apply_update(&server.state_update())
        .expect("catch up");
    client
}

/// Delete the first `n` top-level nodes on `doc`'s replica and push the result to
/// `server` as `who` — a client removing a paragraph.
fn deletes(server: &CoeditTreeDoc, who: WriteCtx, n: u32) {
    use yrs::Transact;
    use yrs::types::xml::XmlFragment;
    let client = replica(server);
    {
        let mut txn = client.doc().transact_mut();
        client.fragment().remove_range(&mut txn, 0, n);
    }
    server
        .apply_update_as(who, &client.state_update())
        .expect("apply");
}

fn node_count(doc: &CoeditTreeDoc) -> u32 {
    use yrs::Transact;
    use yrs::types::xml::XmlFragment;
    doc.fragment().len(&doc.doc().transact())
}

/// The property the whole file is about: undoing a deletion restores the nodes
/// with **the same ids and the same authors**, so a host's span map still
/// resolves and the checkpoint lands with authorship intact.
#[test]
fn undoing_a_deletion_restores_node_ids_and_authors() {
    let doc = CoeditTreeDoc::new(ROOT);
    doc.track_undo(BOB);

    let node = doc.append_text(ALICE, "p", "alice wrote this");
    let before = doc.authors();
    assert_eq!(
        before.get(&node),
        Some(&(1, 10)),
        "the seeded run should be alice's"
    );

    // Bob deletes alice's paragraph, then thinks better of it.
    deletes(&doc, BOB, 1);
    assert_eq!(node_count(&doc), 0);
    assert!(doc.authors().is_empty());

    assert!(doc.can_undo(2));
    let frame = doc.undo_as(BOB).expect("undo");
    assert!(
        !frame.is_empty(),
        "an undo that changed the tree must relay"
    );

    assert_eq!(node_count(&doc), 1);
    assert_eq!(
        doc.authors(),
        before,
        "undoing a deletion changed the node ids or their authors — a host's span \
         map would now cite ids origofs never issued, and the checkpoint would \
         land with authorship erased"
    );
}

/// The scoping holds on this shape too: bob cannot reach alice's work.
#[test]
fn an_actor_cannot_undo_someone_elses_work() {
    let doc = CoeditTreeDoc::new(ROOT);
    doc.track_undo(ALICE);
    doc.track_undo(BOB);

    // Bob types; only bob can take it back.
    let client = replica(&doc);
    client.append_text(BOB, "p", "bob wrote this");
    doc.apply_update_as(BOB, &client.state_update())
        .expect("apply");
    assert_eq!(node_count(&doc), 1);

    assert!(!doc.can_undo(1), "alice can reach bob's edit");
    assert!(doc.undo_as(ALICE).expect("undo").is_empty());
    assert_eq!(node_count(&doc), 1);

    assert!(doc.can_undo(2));
    doc.undo_as(BOB).expect("undo");
    assert_eq!(node_count(&doc), 0);
}

/// A relayed frame is another worker's work and must stay off local stacks — the
/// trap #146 was really about, asserted on this shape as well.
#[test]
fn a_relayed_edit_is_not_undoable() {
    let doc = CoeditTreeDoc::new(ROOT);
    doc.track_undo(ALICE);

    let elsewhere = CoeditTreeDoc::new(ROOT);
    elsewhere.append_text(BOB, "p", "typed on another worker");
    doc.apply_relayed(&sync_frame(&elsewhere.state_update()))
        .expect("relay");

    assert_eq!(node_count(&doc), 1);
    assert!(
        !doc.can_undo(1),
        "a relayed frame landed on an undo stack — Ctrl+Z would pop an edit from \
         another worker"
    );
}

#[test]
fn redo_restores_the_undone_edit_with_its_stamps() {
    let doc = CoeditTreeDoc::new(ROOT);
    doc.track_undo(BOB);

    doc.append_text(ALICE, "p", "alice wrote this");
    let before = doc.authors();

    deletes(&doc, BOB, 1);
    doc.undo_as(BOB).expect("undo");
    assert_eq!(doc.authors(), before);

    // Redo puts the deletion back.
    assert!(doc.can_redo(2));
    let frame = doc.redo_as(BOB).expect("redo");
    assert!(!frame.is_empty());
    assert_eq!(node_count(&doc), 0);
}

/// The tree shape's apply is genuinely two transactions — the update, then the
/// authorship reconcile, which cannot share one because the walk borrows a read
/// transaction. They carry the same origin so `yrs` merges them into a single
/// stack item; if they ever stopped, an undo would take back the content and
/// leave the repair, or vice versa.
#[test]
fn an_apply_and_its_authorship_repair_undo_together() {
    let doc = CoeditTreeDoc::new(ROOT);
    doc.track_undo(BOB);

    // A client that stamps a *false* author, which the reconcile must overwrite —
    // so the apply definitely produces a repair transaction as well as content.
    let client = CoeditTreeDoc::new(ROOT);
    client.append_text(ALICE, "p", "bob typed this claiming to be alice");
    doc.apply_update_as(BOB, &client.state_update())
        .expect("apply");

    // Server-side attribution won: the run is bob's despite the claim.
    assert!(
        doc.authors().values().all(|&(actor, _)| actor == 2),
        "the reconcile did not overwrite the client's claimed author: {:?}",
        doc.authors()
    );

    // One undo takes back both halves, leaving nothing behind.
    doc.undo_as(BOB).expect("undo");
    assert_eq!(node_count(&doc), 0);
    assert!(
        doc.authors().is_empty(),
        "the content was undone but an authorship stamp survived it"
    );
    assert!(
        !doc.can_undo(2),
        "the repair was a second, separate stack item"
    );
}

/// Wrap a raw update in the y-sync `Update` message `apply_relayed` reads.
fn sync_frame(update: &[u8]) -> Vec<u8> {
    use yrs::sync::{Message, SyncMessage};
    use yrs::updates::encoder::Encode;
    Message::Sync(SyncMessage::Update(update.to_vec())).encode_v1()
}
