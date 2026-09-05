//! What per-actor undo does when one actor has a socket on **two workers** (#146).
//!
//! A room is per-worker in-memory state, and so is an undo stack. Behind a load
//! balancer one person with two tabs can land on two workers, each holding its
//! own manager over its own replica. This file is what that actually does at the
//! **engine** level, measured rather than reasoned about — the first three
//! properties are better than they sound, and the fourth is the defect that
//! `claim_undo_stack` exists to make unreachable.
//!
//! Nothing here goes through the claim, deliberately: these drive `CoeditDoc`
//! directly, which is the layer that has no idea other workers exist. The
//! coordinator is what refuses the second worker a stack
//! (`coedit_undo_claim.rs` for the claim itself, `api_coedit_undo.rs` for it
//! holding end to end). Keeping this file claim-free is what keeps it an honest
//! record of *why* the claim is there — delete the claim and these still pass,
//! which is exactly the point.
//!
//! The stacks are **disjoint**, which is what makes the first three work: an
//! edit reaches the other worker over the relay, and `apply_relayed` is
//! deliberately origin-less, so it is never captured there (see
//! `coedit_origin.rs`). Each worker's stack therefore holds exactly what was
//! typed through it.

#![cfg(feature = "coedit")]

use origofs_core::WriteCtx;
use origofs_core::coedit::CoeditDoc;

const ALICE: WriteCtx = WriteCtx {
    actor: 1,
    session: Some(10),
    tool_call: None,
};

/// Wrap a raw update in the y-sync `Update` message `apply_relayed` reads.
fn sync_frame(update: &[u8]) -> Vec<u8> {
    use yrs::sync::{Message, SyncMessage};
    use yrs::updates::encoder::Encode;
    Message::Sync(SyncMessage::Update(update.to_vec())).encode_v1()
}

/// `who` types `chunk` at `at` through `worker`, and the edit reaches `peer`
/// over the cross-worker relay — the round trip a load-balanced deployment makes
/// on every keystroke.
fn types_through(worker: &CoeditDoc, peer: &CoeditDoc, who: WriteCtx, at: u32, chunk: &str) {
    let client = CoeditDoc::new();
    client
        .apply_update(&worker.state_update())
        .expect("catch up");
    client.insert(who, at, chunk);
    worker
        .apply_update_as(who, &client.state_update())
        .expect("apply");
    peer.apply_relayed(&sync_frame(&worker.state_update()))
        .expect("relay");
}

/// Each worker's stack holds only what was typed through it, so a Ctrl+Z in one
/// tab takes back that tab's edit and not the other's.
///
/// This is the property that makes the split survivable at all. It is not the
/// "undo my last edit anywhere" a single-worker deployment gives — the stack
/// granularity follows the load balancer's routing, which is worth knowing — but
/// it is coherent, and it is what a non-collaborative editor's per-window undo
/// does anyway.
#[test]
fn each_workers_stack_holds_only_what_was_typed_through_it() {
    let (w1, w2) = (CoeditDoc::new(), CoeditDoc::new());
    w1.track_undo(ALICE);
    w2.track_undo(ALICE);

    types_through(&w1, &w2, ALICE, 0, "one ");
    types_through(&w2, &w1, ALICE, 4, "two");
    assert_eq!(w1.text(), "one two");
    assert_eq!(w2.text(), "one two");

    // Tab 1 takes back its own word, not tab 2's.
    let frame = w1.undo_as(ALICE).expect("undo");
    w2.apply_relayed(&frame).expect("relay");
    assert_eq!(w1.text(), "two");
    assert_eq!(w2.text(), "two", "the undo did not reach the peer worker");

    // Tab 2 takes back its own.
    let frame = w2.undo_as(ALICE).expect("undo");
    w1.apply_relayed(&frame).expect("relay");
    assert_eq!(w1.text(), "");
    assert_eq!(w2.text(), "");
}

/// Redo across the split is symmetric, and the replicas stay converged.
#[test]
fn redo_on_one_worker_reaches_the_other() {
    let (w1, w2) = (CoeditDoc::new(), CoeditDoc::new());
    w1.track_undo(ALICE);
    w2.track_undo(ALICE);

    types_through(&w1, &w2, ALICE, 0, "draft");
    let frame = w1.undo_as(ALICE).expect("undo");
    w2.apply_relayed(&frame).expect("relay");
    assert_eq!(w2.text(), "");

    let frame = w1.redo_as(ALICE).expect("redo");
    w2.apply_relayed(&frame).expect("relay");
    assert_eq!(w1.text(), "draft");
    assert_eq!(w2.text(), "draft");
}

/// Two stacks popping items that touch the same content still **converge** —
/// undo is an ordinary CRDT update, so the usual guarantees hold and there is no
/// split-brain to repair.
#[test]
fn overlapping_stacks_converge() {
    let (w1, w2) = (CoeditDoc::new(), CoeditDoc::new());
    w1.track_undo(ALICE);
    w2.track_undo(ALICE);

    types_through(&w1, &w2, ALICE, 0, "hello");

    // Tab 2 deletes what tab 1 typed.
    let client = CoeditDoc::new();
    client.apply_update(&w2.state_update()).expect("catch up");
    client.remove(0, 5);
    w2.apply_update_as(ALICE, &client.state_update())
        .expect("apply");
    w1.apply_relayed(&sync_frame(&w2.state_update()))
        .expect("relay");
    assert_eq!(w1.text(), "");

    // Tab 1 undoes its insert (of content already deleted), then tab 2 undoes
    // its delete. Two independent stacks popping overlapping items.
    let frame = w1.undo_as(ALICE).expect("undo");
    w2.apply_relayed(&frame).expect("relay");
    let frame = w2.undo_as(ALICE).expect("undo");
    w1.apply_relayed(&frame).expect("relay");

    assert_eq!(w1.text(), w2.text(), "the replicas diverged");
}

/// **The defect the claim exists to prevent, pinned so the reason cannot be
/// forgotten.**
///
/// In the interleaving above, the restored text comes back **unattributed**.
///
/// The mechanism: origofs's author stamp is a formatting attribute written by
/// `apply_update_as`'s repair, in the *same transaction* as the insert it
/// describes — so it is part of the same undo stack item. When tab 1 undoes its
/// insert, the stamp goes with it. Tab 2's stack knows nothing of that, and its
/// undo restores the content items alone. `yrs` behaves correctly throughout;
/// nothing here is a CRDT bug.
///
/// **Why it matters:** `checkpoint_coedit` resolves an unattributed span to the
/// checkpointer, so the next checkpoint credits those bytes to whoever happened
/// to trigger it. In a filesystem whose premise is per-actor attribution that is
/// the harm class that matters, even at this frequency — it needs one actor with
/// two concurrent sockets on one path on two workers, plus this interleaving.
///
/// It is **not** reachable on a single worker: there, one manager owns both
/// items and `yrs` will not restore content whose insert it has itself popped
/// (`single_worker_keeps_authorship_across_the_same_interleaving` below).
///
/// **It is not reachable through a coordinator either, and that is the fix.**
/// `Fs::claim_undo_stack` gives at most one worker an actor's stack for a path,
/// so the two managers below cannot both exist in a running deployment. This
/// test constructs them by hand precisely because the layer it exercises has no
/// claim in it: it is the measurement the claim is justified by, and it must
/// keep reporting the wrong answer for as long as the claim is what stands
/// between a deployment and this outcome.
#[test]
fn two_unclaimed_stacks_strip_an_author_stamp_which_is_why_the_claim_exists() {
    let (w1, w2) = (CoeditDoc::new(), CoeditDoc::new());
    w1.track_undo(ALICE);
    w2.track_undo(ALICE);

    types_through(&w1, &w2, ALICE, 0, "hello");
    assert_eq!(w1.snapshot().1, vec![(1, 10, 5)], "alice owns her own text");

    let client = CoeditDoc::new();
    client.apply_update(&w2.state_update()).expect("catch up");
    client.remove(0, 5);
    w2.apply_update_as(ALICE, &client.state_update())
        .expect("apply");
    w1.apply_relayed(&sync_frame(&w2.state_update()))
        .expect("relay");

    let frame = w1.undo_as(ALICE).expect("undo");
    w2.apply_relayed(&frame).expect("relay");
    let frame = w2.undo_as(ALICE).expect("undo");
    w1.apply_relayed(&frame).expect("relay");

    let (text, spans) = w1.snapshot();
    assert_eq!(text, "hello", "the content came back");
    assert_eq!(
        spans,
        vec![(0, 0, 5)],
        "Two independent stacks no longer strip the author stamp. That is the \
         outcome `claim_undo_stack` exists to make unreachable in a deployment, \
         so if it is now unreachable *here* — at the layer with no claim in it — \
         something else has changed and the claim may no longer be load-bearing. \
         Work out which before deleting anything."
    );
}

/// The single-worker control for the case above: one manager owns both stack
/// items, and authorship survives the identical sequence. This is what makes the
/// limitation specifically a cross-worker one rather than an undo one.
#[test]
fn single_worker_keeps_authorship_across_the_same_interleaving() {
    let w = CoeditDoc::new();
    w.track_undo(ALICE);

    let client = CoeditDoc::new();
    client.insert(ALICE, 0, "hello");
    w.apply_update_as(ALICE, &client.state_update())
        .expect("apply");
    // Past the capture window, so the delete is its own stack item rather than
    // being merged into the insert's.
    std::thread::sleep(std::time::Duration::from_millis(600));

    let client = CoeditDoc::new();
    client.apply_update(&w.state_update()).expect("catch up");
    client.remove(0, 5);
    w.apply_update_as(ALICE, &client.state_update())
        .expect("apply");

    w.undo_as(ALICE).expect("undo"); // the delete
    assert_eq!(w.snapshot(), ("hello".into(), vec![(1, 10, 5)]));
    w.undo_as(ALICE).expect("undo"); // the insert
    assert_eq!(w.text(), "");
    w.redo_as(ALICE).expect("redo");
    assert_eq!(
        w.snapshot(),
        ("hello".into(), vec![(1, 10, 5)]),
        "authorship must survive an undo/redo round trip on one worker"
    );
}
