//! Transaction origins on the co-editing write paths (#146), and the `yrs`
//! behaviour that makes them load-bearing.
//!
//! An [`UndoManager`](yrs::undo::UndoManager) scopes an undo to a set of *tracked
//! origins*. The filter has a default that decides the whole design: an
//! origin-**less** transaction is tracked exactly while no actor origin has been
//! included. So before #146 — when every transaction in `coedit.rs` and
//! `coedit_tree.rs` was origin-less — a manager would have captured every actor's
//! edits *and* every frame arriving over the cross-worker relay, and a Ctrl+Z
//! would have popped an edit made on a machine this process never spoke to.
//!
//! Setting origins on the attributed client paths fixes that **by exclusion**:
//! including one actor origin is what turns the origin-less paths (the relay, the
//! unattributed peer merge, the reconstruction paths) untracked. That is a
//! property of `yrs`, not of this crate, and nothing here would fail if a future
//! `yrs` changed it — the relay would simply start landing on undo stacks again.
//! So it is pinned directly, and the asymmetry is asserted alongside it.

#![cfg(feature = "coedit")]

use yrs::undo::UndoManager;
use yrs::{Doc, GetString, Options, Origin, Text, Transact};

fn bare_doc() -> Doc {
    Doc::with_options(Options {
        offset_kind: yrs::OffsetKind::Bytes,
        ..Default::default()
    })
}

/// The `yrs` default: with no origin included, an origin-less transaction **is**
/// tracked. This is the trap — it is why "attach an `UndoManager`" was never the
/// whole job, and why the relay path staying bare is not sufficient on its own.
#[test]
fn originless_transactions_are_tracked_until_an_origin_is_included() {
    let doc = bare_doc();
    let text = doc.get_or_insert_text("t");
    let mgr = UndoManager::<()>::new(&doc, &text);

    text.insert(&mut doc.transact_mut(), 0, "relayed");

    assert!(
        mgr.can_undo(),
        "yrs no longer tracks origin-less transactions by default. That is the \
         behaviour the co-edit origins work around; re-read `author_origin` and \
         this module's header before adjusting anything."
    );
}

/// …and including one actor origin turns them untracked. This is the mechanism
/// that keeps relayed frames off a local undo stack, so it is asserted rather
/// than assumed.
#[test]
fn including_an_actor_origin_excludes_the_originless_paths() {
    let doc = bare_doc();
    let text = doc.get_or_insert_text("t");
    let mut mgr = UndoManager::<()>::new(&doc, &text);
    mgr.include_origin(Origin::from("7,1"));

    // A relayed frame's shape: no origin.
    text.insert(&mut doc.transact_mut(), 0, "relayed");
    assert!(
        !mgr.can_undo(),
        "an origin-less transaction landed on an actor-scoped undo stack — a \
         Ctrl+Z would pop another worker's edit"
    );

    // Another actor's edit: an origin, but not this one's.
    text.insert(
        &mut doc.transact_mut_with(Origin::from("9,1")),
        0,
        "theirs ",
    );
    assert!(!mgr.can_undo(), "another actor's edit is not mine to undo");

    // This actor's own edit is the only thing on the stack.
    text.insert(&mut doc.transact_mut_with(Origin::from("7,1")), 0, "mine ");
    assert!(mgr.can_undo());

    mgr.undo_blocking();
    assert_eq!(
        text.get_string(&doc.transact()),
        "theirs relayed",
        "undo took more (or less) than the actor's own insert"
    );
}
