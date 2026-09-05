//! Opt-in CRDT co-editing (`docs/DESIGN.md` §4e, roadmap M8): a Yjs-in-Rust
//! (`yrs`) document that humans and agents type into concurrently.
//!
//! Each insert is stamped with its author as a formatting attribute, so the
//! document always knows which `(actor, session)` wrote each character run.
//! [`CoeditDoc::snapshot`] materializes the current text together with that
//! per-span authorship, and [`Fs::checkpoint_coedit`] lands it through
//! [`Fs::write_as_blamed`] — so live, character-level, interleaved co-editing
//! feeds the *same* byte-range blame index as ordinary writes, with no lossy
//! projection. This is the "live co-editing" half of M8; commit-time merge
//! (everything else) is unchanged.
//!
//! Enabled by the `coedit` feature.

use crate::attribution::{BlameRange, WriteCtx};
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::format;
use crate::metadata::MetadataStore;
use parking_lot::Mutex;
use similar::{ChangeTag, TextDiff};
use std::collections::HashMap;
use std::sync::Arc;
use yrs::encoding::read::Cursor;
use yrs::sync::protocol::MSG_AUTH;
use yrs::sync::{Message, MessageReader, SyncMessage};
use yrs::types::Attrs;
use yrs::types::text::YChange;
use yrs::undo::UndoManager;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{
    Any, Doc, GetString, OffsetKind, Options, Origin, Out, ReadTxn, StateVector, Text, TextRef,
    Transact, Update,
};

/// How long a co-editing undo-stack claim stands without renewal (#146).
///
/// The trade is between how long a crashed worker denies an actor undo and how
/// often a live one writes a renewal. A minute matches
/// [`posixlock::LEASE_SECS`](crate::posixlock::LEASE_SECS) and for the same
/// reasons; the failure it bounds is milder here, since waiting one out costs a
/// greyed-out button rather than a stuck byte range.
pub const UNDO_CLAIM_LEASE_SECS: i64 = 60;

/// The formatting-attribute key under which each run's `"actor,session"` is kept.
/// Shared with [`crate::coedit_tree`], which stamps the same key on tree nodes.
pub(crate) const AUTHOR_KEY: &str = "a";

/// The attribute *value* stamped under [`AUTHOR_KEY`]: `"actor,session"`.
pub(crate) fn author_value(ctx: WriteCtx) -> String {
    format!("{},{}", ctx.actor, ctx.session.unwrap_or(0))
}

/// The author formatting attribute for `ctx`, as a `yrs` attribute set.
pub(crate) fn author_attrs(ctx: WriteCtx) -> Attrs {
    Attrs::from([(AUTHOR_KEY.into(), Any::from(author_value(ctx)))])
}

/// Per-**actor** undo stacks over one live CRDT document (#146), shared by both
/// document shapes.
///
/// # What an undo is here
///
/// An **ordinary forward edit**, and that is a ruling rather than an
/// implementation detail. Text an undo removes leaves blame like any other
/// deletion; content it restores comes back carrying the author stamp it had,
/// because `yrs` re-integrates the original items rather than fresh copies — on
/// both shapes, the flat `a` attribute and the tree's `a`/`n` pair alike. So
/// nothing here touches the blame index, and the checkpoints stay honest with no
/// special case.
///
/// The alternative — undo *unwinds* the record, as if the insert never happened —
/// was rejected. This is a filesystem whose premise is that every edit is
/// attributable to the actor that made it, and an undo that erases evidence is a
/// way for an agent to write, be reviewed, and then launder the edit out of the
/// append-only op-log. [`Fs::revert_session`](crate::Fs::revert_session) already
/// made this call the other way, appending a `revert` op rather than popping one.
///
/// # Keyed by actor
///
/// Not by session: a person with the document open in two tabs gets one stack,
/// which is what every editor does and what "undo my own typing" means to the
/// person pressing the key. Each of their sessions contributes its own origin to
/// the same manager.
///
/// # Lock order
///
/// The `Mutex` exists because `yrs` needs `&mut UndoManager` to pop a stack while
/// every method on the documents takes `&self`. **Never take this lock while
/// holding a transaction on the document**: [`pop`](Self::pop) opens its own write
/// transaction through `undo_blocking`, so the order is fixed — this map first,
/// never the reverse.
#[derive(Default)]
pub(crate) struct UndoStacks {
    inner: Mutex<HashMap<i64, UndoManager<()>>>,
}

impl UndoStacks {
    /// Start tracking `ctx`'s edits within `scope`, so its actor can undo them.
    ///
    /// **Must be called before the edits it should cover.** A `yrs`
    /// [`UndoManager`] captures changes by observing transactions as they commit;
    /// one created afterwards sees an empty stack, however recent the edit. So
    /// this belongs at the point a co-editing socket opens, not at the point
    /// somebody presses Ctrl+Z — by then it is too late for everything they typed.
    ///
    /// Idempotent per session: calling it again for an actor adds that session's
    /// origin to the actor's existing stack, so a second tab joins the stack it
    /// already has rather than starting a rival one.
    pub(crate) fn track<T>(&self, doc: &Doc, scope: &T, ctx: WriteCtx)
    where
        T: AsRef<yrs::branch::Branch>,
    {
        let mut stacks = self.inner.lock();
        let mgr = stacks
            .entry(ctx.actor)
            // Scoped to the document's content root: a change outside it is not
            // this document's content and has no business on a content undo stack.
            .or_insert_with(|| UndoManager::new(doc, scope));
        // Including any actor origin is also what makes the origin-*less* paths
        // (the cross-worker relay, an unattributed merge) untracked — see
        // [`author_origin`]. Until the first call here, this manager would capture
        // them, and a Ctrl+Z would pop an edit from another worker.
        mgr.include_origin(author_origin(ctx));
    }

    /// Drop `actor`'s stack — at their last socket's disconnect.
    ///
    /// Undo is an editor affordance, not history: a stack does not outlive the
    /// room, and nothing tries to rebuild one. Dropping the manager unsubscribes
    /// it from the document.
    ///
    /// # A stack is per worker, and that has one sharp edge
    ///
    /// A room is one process's memory, so behind a load balancer one actor with
    /// two tabs can hold two stacks. Mostly benign — they are disjoint, since a
    /// relayed frame carries no origin and is never captured on the receiving
    /// worker, so each pops only what was typed through it and the replicas
    /// converge. But the author stamp is a formatting attribute written in the
    /// same transaction as the insert it describes, so one worker's undo of an
    /// insert removes a stamp the *other* worker's undo of a deletion then
    /// restores content without — and `checkpoint_coedit` credits an
    /// unattributed span to the checkpointer.
    ///
    /// `tests/coedit_undo_multiworker.rs` pins the precondition, with a
    /// single-worker control proving it is specifically a cross-worker effect.
    /// The real fix is one stack per (actor, path) across workers; sticky
    /// routing on actor+path avoids it meanwhile.
    pub(crate) fn untrack(&self, actor: i64) {
        self.inner.lock().remove(&actor);
    }

    /// Whether `actor` has anything to undo (or, with `redo`, to redo).
    pub(crate) fn can(&self, actor: i64, redo: bool) -> bool {
        self.inner
            .lock()
            .get(&actor)
            .is_some_and(|m| if redo { m.can_redo() } else { m.can_undo() })
    }

    /// Pop `ctx`'s actor's most recent action (or, with `redo`, re-apply the one
    /// they last undid), returning the **y-sync frame** to fan out to the room —
    /// empty when there was nothing to pop.
    ///
    /// The bytes are framed exactly as a room's ordinary broadcast is, and for the
    /// same reason: an undo produces an ordinary y-sync `Update`, so it travels
    /// the existing fan-out and nothing new goes on the wire. Only the *request*
    /// needed a channel, and that is the surface's problem. Framing it here rather
    /// than at the surface also keeps `yrs` out of the callers — the SDK depends
    /// on it only as a dev-dependency.
    pub(crate) fn pop(&self, doc: &Doc, ctx: WriteCtx, redo: bool) -> Result<Vec<u8>> {
        let mut stacks = self.inner.lock();
        let Some(mgr) = stacks.get_mut(&ctx.actor) else {
            // Not an error: an actor with no manager has nothing to undo, which is
            // what a client asking too early (or after a reconnect onto a fresh
            // room) should be told. Refusing would make a benign race look like a
            // failure.
            return Ok(Vec::new());
        };

        // Pin the pre-image *and drop the read transaction* before popping:
        // `undo_blocking` opens its own write transaction on the same document, so
        // holding this across the call would deadlock.
        let sv_before = doc.transact().state_vector();

        let changed = if redo {
            mgr.redo_blocking()
        } else {
            mgr.undo_blocking()
        };
        if !changed {
            return Ok(Vec::new());
        }

        // Encode exactly what the pop did, the same way `apply_update_as` encodes
        // exactly what a client's update did, then frame it as the `Update`
        // message a room's fan-out carries.
        let delta = doc.transact().encode_state_as_update_v1(&sv_before);
        let mut frame = EncoderV1::new();
        Message::Sync(SyncMessage::Update(delta)).encode(&mut frame);
        Ok(frame.to_vec())
    }
}

/// The `yrs` **transaction origin** for `ctx`'s edits — the same
/// `"actor,session"` string [`author_value`] stamps on the content itself, so a
/// transaction and the runs it introduces name their author identically.
///
/// # Why every attributed mutation must set one (#146)
///
/// `yrs`'s [`UndoManager`](yrs::undo::UndoManager) scopes an undo to a set of
/// tracked origins, which is what makes "undo *my* typing, not my colleague's
/// paragraph" expressible at all. Its filter has a default that bites: an
/// origin-**less** transaction is tracked exactly while no origin has been
/// included (`should_skip` in `yrs`'s `undo.rs` compares
/// `tracked_origins.len() == 1`, the manager's own). Every transaction in this
/// module used to be origin-less, so a manager attached then would have captured
/// not just every actor's edits but every frame arriving over the cross-worker
/// relay — undoing changes that were never made on this worker at all.
///
/// Setting the origin here is what closes that, and it closes it *by exclusion*:
/// once one actor origin is included, origin-less transactions stop being
/// tracked. So the paths that deliberately carry **no** origin —
/// [`apply_relayed`], [`CoeditDoc::apply_update`], the reconstruction paths — are
/// excluded for free, and that is load-bearing rather than incidental. Do not
/// "tidy up" by giving them origins too: `tests/coedit_origin.rs` fails if the
/// asymmetry is lost, and pins the `yrs` behaviour it rests on.
pub(crate) fn author_origin(ctx: WriteCtx) -> Origin {
    Origin::from(author_value(ctx))
}

/// The **raw** `AUTHOR_KEY` value a run carries, or `None` when it carries none
/// (or a non-string one, which the server never writes).
///
/// Deliberately unparsed: enforcement compares stamps against the server's own
/// string, and a malformed value like `"1,2,junk"` would parse to a plausible
/// `(1, 2)` and compare equal to a legitimate stamp. Comparing raw normalises
/// every malformed variant into "not what the server would have written".
pub(crate) fn raw_author(attrs: Option<&Attrs>) -> Option<Arc<str>> {
    raw_attr(attrs, AUTHOR_KEY)
}

/// The raw string value of formatting attribute `key`, or `None` when absent or
/// not a string. See [`raw_author`] for why the value is never parsed here.
pub(crate) fn raw_attr(attrs: Option<&Attrs>, key: &str) -> Option<Arc<str>> {
    match attrs.and_then(|a| a.get(key)) {
        Some(Any::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The attribute set putting `value` under `key` — or removing it, via
/// `Any::Null`, when the run should carry nothing there.
pub(crate) fn attr_or_null(key: &str, value: &Option<Arc<str>>) -> (Arc<str>, Any) {
    let any = match value {
        Some(v) => Any::String(v.clone()),
        None => Any::Null,
    };
    (Arc::from(key), any)
}

/// The attribute set that puts `value` under [`AUTHOR_KEY`] — or **removes** the
/// key, via `Any::Null`, when the run is meant to carry no author at all.
pub(crate) fn author_attr(value: &Option<Arc<str>>) -> Attrs {
    let any = match value {
        Some(v) => Any::String(v.clone()),
        None => Any::Null,
    };
    Attrs::from([(AUTHOR_KEY.into(), any)])
}

/// Parse an `(actor, session)` pair from an author attribute's string value.
pub(crate) fn parse_author(value: &str) -> (i64, i64) {
    let mut it = value.split(',');
    let actor = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let session = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    (actor, session)
}

/// A live co-edited document: a `yrs` text CRDT whose inserts are attributed.
///
/// Two edit paths land in the same attributed CRDT:
///
/// * **In-process** — [`insert`](Self::insert) / [`remove`](Self::remove), each
///   insert carrying its actor. Used by tests and any Rust caller.
/// * **The Yjs wire protocol** — [`handle_sync`](Self::handle_sync) drives the
///   y-sync handshake so an *unmodified* Yjs editor (PlateJS, `y-websocket`, …)
///   connects and exchanges binary updates. The server recovers authorship for
///   these generic clients in [`apply_update_as`](Self::apply_update_as), never
///   trusting a client-named author.
///
/// Either way changes merge commutatively, so peers converge regardless of order.
/// [`snapshot`](Self::snapshot) yields the current text plus its per-span
/// authorship — the input to [`Fs::checkpoint_coedit`].
///
/// **Document indices here are UTF-8 byte offsets, not UTF-16 code units.** That
/// is `yrs`'s `OffsetKind::Bytes`, which [`new`](Self::new) selects explicitly.
/// Vanilla Yjs indexes UTF-16, so this differs from the JS API — but the choice is
/// local to *this process's* index arguments and never reaches the wire: `yrs`
/// splits blocks with a hardcoded `OffsetKind::Utf16` internally, so block clocks
/// and lengths (the things an update actually encodes) are UTF-16 regardless. A
/// browser client is unaffected.
///
/// Bytes are the right unit here because every consumer of this module speaks
/// bytes: [`snapshot`](Self::snapshot)'s spans, the blame index, and
/// [`Fs::write_as_blamed`] all do. Mixing the two silently misattributes non-ASCII
/// text — `utf16_len` offsets fed to a byte-indexed document credited a 6-byte
/// "héllo" as 5 bytes and orphaned the 6th — so the units are now stated at the
/// boundary and identical throughout.
pub struct CoeditDoc {
    doc: Doc,
    text: TextRef,
    /// Per-actor undo stacks (#146). See [`UndoStacks`].
    undo: UndoStacks,
}

impl Default for CoeditDoc {
    fn default() -> Self {
        Self::new()
    }
}

// A live-editing room shares one `CoeditDoc` across every connected socket's task
// (behind a lock), so this must hold. `yrs::Doc`/`TextRef` assert it via their own
// `unsafe impl`s; pin it here so a future change that breaks it fails at compile
// time in `origofs-core`, not deep in a transport crate.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CoeditDoc>();
};

impl CoeditDoc {
    /// A fresh, empty document.
    ///
    /// `offset_kind` is set explicitly rather than left to `Doc::new`'s default.
    /// It *is* the default today, but every index in this module is a byte offset
    /// and a `yrs` bump that flipped the default would silently misindex every
    /// `format`/`insert`/`remove_range` call rather than failing to compile.
    pub fn new() -> Self {
        let doc = Doc::with_options(Options {
            offset_kind: OffsetKind::Bytes,
            ..Default::default()
        });
        let text = doc.get_or_insert_text("content");
        Self {
            doc,
            text,
            undo: UndoStacks::default(),
        }
    }

    /// Insert `chunk` at character `index`, attributed to `ctx`.
    pub fn insert(&self, ctx: WriteCtx, index: u32, chunk: &str) {
        let mut txn = self.doc.transact_mut_with(author_origin(ctx));
        self.text
            .insert_with_attributes(&mut txn, index, chunk, author_attrs(ctx));
    }

    /// Remove `len` characters starting at `index`.
    ///
    /// Takes no [`WriteCtx`], so the transaction carries no origin and is
    /// invisible to every per-actor undo stack — see [`author_origin`]. That is
    /// the honest answer for a removal with nobody to attribute it to; a caller
    /// that wants its deletion to be undoable has an actor and should drive the
    /// document through [`apply_update_as`](Self::apply_update_as).
    pub fn remove(&self, index: u32, len: u32) {
        let mut txn = self.doc.transact_mut();
        self.text.remove_range(&mut txn, index, len);
    }

    /// The full current text.
    pub fn text(&self) -> String {
        self.text.get_string(&self.doc.transact())
    }

    /// An opaque update carrying this document's whole state, for a peer to
    /// [`apply_update`](Self::apply_update).
    pub fn state_update(&self) -> Vec<u8> {
        self.doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    /// This document's encoded **state vector** — the compact "what I already
    /// have" summary Yjs peers exchange. It is what a CRDT suggestion records as
    /// its base: unlike a content hash it does not say "the file must still be
    /// exactly this", it says "this proposal was computed knowing these ops",
    /// which is all a merge needs.
    pub fn state_vector(&self) -> Vec<u8> {
        self.doc.transact().state_vector().encode_v1()
    }

    /// Merge a peer's update into this document (idempotent and commutative).
    ///
    /// Unattributed, and therefore **deliberately origin-less**: nobody's undo
    /// stack should offer to pop a merge this process did not perform. See
    /// [`author_origin`].
    pub fn apply_update(&self, update: &[u8]) -> Result<()> {
        let update = Update::decode_v1(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;
        self.doc
            .transact_mut()
            .apply_update(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("apply co-edit update: {e}")))?;
        Ok(())
    }

    /// Apply a raw `yrs` update from an *unmodified* Yjs client (PlateJS,
    /// `y-websocket`, …) and attribute exactly the text it inserted to `ctx`'s
    /// actor — server-side, never trusting any author the client may name.
    ///
    /// A generic client speaks the Yjs binary protocol and knows nothing about
    /// our authorship attribute, so we recover it ourselves: capture the text and
    /// its authorship, apply the update, then **re-assert the authorship of the
    /// whole document** — every surviving character keeps the author it had, and
    /// every introduced character takes `ctx`. Runs that disagree are repaired by
    /// CRDT formatting. The repair is itself a CRDT change, so it persists in the
    /// sidecar and rides the returned delta out to every peer and worker.
    ///
    /// **Enforcement is total, not insert-only.** Stamping only what a *text*
    /// diff called inserted meant a formatting-only update — no text change, so
    /// no ranges — left the client's own `a` attribute standing, and it flowed
    /// into the durable blame index with the file bytes and their content hash
    /// unchanged. See the module's enforcement section for the read rule this
    /// establishes and why nothing on the byte axis could have caught it.
    ///
    /// Returns the update to relay to peers — the client's content *plus* our
    /// attribution — or an empty vector if the update changed nothing (already
    /// seen). This, not the raw inbound bytes, is what a room must broadcast, so
    /// authorship always travels with the content, already repaired.
    pub fn apply_update_as(&self, ctx: WriteCtx, update: &[u8]) -> Result<Vec<u8>> {
        let update = Update::decode_v1(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;

        // Apply and repair in one transaction, so no observer and no concurrent
        // reader can ever see the un-normalised intermediate state. (The room
        // lock already serialises this against the checkpoint sweeper; this
        // removes the class rather than the instance.)
        //
        // The transaction carries `ctx`'s origin (#146), which is what lets a
        // per-actor `UndoManager` tell this client's typing from everyone else's.
        // Being one transaction matters twice over here: the authorship repair
        // below lands *inside* it, so an undo pops the content and its author
        // stamp together and can never strand one without the other.
        let mut txn = self.doc.transact_mut_with(author_origin(ctx));

        // Pin the pre-image: its authorship (to carry across survivors) and its
        // state vector (to encode exactly this update's effect for relay).
        let sv_before = txn.state_vector();
        let plain = self.text.get_string(&txn);
        let before = scan_runs(&self.text, &txn, Some(&plain), raw_author);

        txn.apply_update(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("apply co-edit update: {e}")))?;

        let plain = self.text.get_string(&txn);
        let after = scan_runs(&self.text, &txn, Some(&plain), raw_author);
        // `plain` is authoritative here, so this can only fail on a document the
        // server itself put out of shape. Refusing beats repairing off a run map
        // whose indices do not address the content they claim to.
        if !after.indexable {
            return Err(OrigoFSError::InvalidArgument(
                "co-edit update left the document unindexable; refusing to attribute it".into(),
            ));
        }
        let mine: Option<Arc<str>> = Some(Arc::from(author_value(ctx)));
        let want = intended_stamps(
            &before.flat,
            &stamp_tiling(&before.runs),
            &after.flat,
            // Text with no pre-image author stays unattributed rather than being
            // adopted by whoever next touched the document; `checkpoint_coedit`
            // resolves that to the checkpointer.
            &None,
            || mine.clone(),
        );
        for (byte_start, byte_len, stamp) in diverging_runs(&want, &stamp_tiling(&after.runs)) {
            let Some((index, len)) = doc_range(&after.runs, byte_start, byte_len) else {
                continue; // unreachable for a range from this document's own runs
            };
            self.text.format(&mut txn, index, len, author_attr(&stamp));
        }

        // Encode before the transaction commits: `TransactionMut` reads the store
        // it has already written, so this sees the repair.
        if txn.state_vector() == sv_before {
            return Ok(Vec::new()); // nothing new — don't relay a no-op
        }
        Ok(txn.encode_state_as_update_v1(&sv_before))
    }

    /// Merge a y-sync frame relayed from another worker — content another replica
    /// already attributed — *without* re-attribution. This is the cross-worker
    /// relay's apply path; client input must instead go through
    /// [`handle_sync`](Self::handle_sync), which attributes. Idempotent: a frame
    /// already merged (or folded into a checkpoint) is a no-op.
    pub fn apply_relayed(&self, frame: &[u8]) -> Result<()> {
        apply_relayed(&self.doc, frame)
    }

    /// The y-sync frame to greet a freshly-connected client with: a `SyncStep1`
    /// carrying our state vector, so the client sends back (as `SyncStep2`)
    /// whatever we're missing. Pair with [`handle_sync`](Self::handle_sync), which
    /// also answers the client's own `SyncStep1`.
    pub fn sync_start(&self) -> Vec<u8> {
        sync_start(&self.doc)
    }

    /// The y-sync frame carrying this document's whole state, to catch up a client
    /// that missed frames. See [`state_frame`].
    pub fn state_frame(&self) -> Vec<u8> {
        state_frame(&self.doc)
    }

    /// Drive one inbound y-sync payload from a connection authenticated as `ctx`.
    /// A payload may pack several messages; each is handled in order.
    ///
    /// This is the transport-agnostic core of live co-editing: the axum WebSocket
    /// route, the FastAPI route, and the tests all funnel bytes through here.
    /// Content the client contributes is attributed to `ctx` by
    /// [`apply_update_as`](Self::apply_update_as) — the server is the sole
    /// authority on authorship. Awareness (cursor presence) is relayed verbatim;
    /// the server keeps no awareness state, so peers gossip presence through the
    /// room's fan-out.
    ///
    /// # Framing (#162)
    ///
    /// `data` must carry the **y-websocket** envelope: an outer message tag
    /// (`messageSync` = 0) wrapping the y-sync payload, which is what `y-websocket`,
    /// `y-protocols` and every browser client built on them emit. A **bare y-sync**
    /// frame — what you get writing a client against the y-sync protocol directly —
    /// starts with `messageYjsUpdate` = 2, which is `messageAuth` in the outer
    /// envelope: it decodes, it is ignored, and the socket then connects, syncs
    /// nothing and reports nothing. Any such message is counted in
    /// [`SyncReply::unhandled`] and logged at `warn`; check it if a room is not
    /// converging.
    pub fn handle_sync(&self, ctx: WriteCtx, data: &[u8]) -> Result<SyncReply> {
        drive_sync(&self.doc, data, |update| self.apply_update_as(ctx, update))
    }

    // --- undo / redo (#146) ---------------------------------------------
    //
    // The machinery is [`UndoStacks`]; both document shapes share it. See there
    // for the rulings — an undo is an ordinary forward edit, and a stack does not
    // outlive the room.

    /// Start tracking `ctx`'s edits so its actor can undo them.
    ///
    /// **Must be called before the edits it should cover** — see
    /// [`UndoStacks::track`].
    pub fn track_undo(&self, ctx: WriteCtx) {
        self.undo.track(&self.doc, &self.text, ctx);
    }

    /// Drop `actor`'s undo stack — at their last socket's disconnect.
    pub fn untrack_undo(&self, actor: i64) {
        self.undo.untrack(actor);
    }

    /// Whether `actor` has anything to undo.
    pub fn can_undo(&self, actor: i64) -> bool {
        self.undo.can(actor, false)
    }

    /// Whether `actor` has anything to redo.
    pub fn can_redo(&self, actor: i64) -> bool {
        self.undo.can(actor, true)
    }

    /// Undo `ctx`'s actor's most recent action, returning the y-sync frame to fan
    /// out to the room — or an empty vector if there was nothing to undo.
    ///
    /// Scoped to the actor's own origins, so this can only ever pop something they
    /// did. A colleague's paragraph is not reachable from here, which is the
    /// entire reason the origins exist.
    pub fn undo_as(&self, ctx: WriteCtx) -> Result<Vec<u8>> {
        self.undo.pop(&self.doc, ctx, false)
    }

    /// Redo the action `ctx`'s actor most recently undid. Same return shape as
    /// [`undo_as`](Self::undo_as).
    pub fn redo_as(&self, ctx: WriteCtx) -> Result<Vec<u8>> {
        self.undo.pop(&self.doc, ctx, true)
    }

    /// Reconstruct a document from a serialized state produced by
    /// [`state_update`](Self::state_update) — the durable form used to persist and
    /// resume a co-editing session.
    pub fn load(update: &[u8]) -> Result<Self> {
        let this = Self::new();
        this.apply_update(update)?;
        Ok(this)
    }

    /// Rebuild a document from flat `text` and its per-span `(actor, session,
    /// byte_len)` authorship — the structural inverse of [`snapshot`](Self::snapshot).
    /// Used to resurrect a live doc from the durable truth (the file plus its blame)
    /// when the persisted CRDT sidecar has fallen out of sync with the file — after
    /// an accepted suggestion, a branch merge, or any plain write changed the bytes
    /// underneath. A span with actor `0` is inserted unattributed. `spans` must tile
    /// `text` on char boundaries (as blame does); a boundary mid-character errors
    /// rather than panicking.
    pub fn from_blamed(text: &str, spans: &[(i64, i64, u64)]) -> Result<Self> {
        let this = Self::new();
        let mut txn = this.doc.transact_mut();
        // One cursor, not two: the document indexes in bytes (see [`CoeditDoc`]),
        // so the byte offset into `text` *is* the insert index. These were
        // previously tracked separately — a byte offset and a UTF-16 one — which
        // silently diverged on any non-ASCII span.
        let mut byte_off = 0usize;
        for &(actor, session, byte_len) in spans {
            let end = byte_off + byte_len as usize;
            let piece = text.get(byte_off..end).ok_or_else(|| {
                OrigoFSError::InvalidArgument("co-edit rebuild: span not on a char boundary".into())
            })?;
            if actor != 0 {
                let attrs = author_attrs(WriteCtx::session(actor, session));
                this.text
                    .insert_with_attributes(&mut txn, byte_off as u32, piece, attrs);
            } else {
                this.text.insert(&mut txn, byte_off as u32, piece);
            }
            byte_off = end;
        }
        drop(txn);
        Ok(this)
    }

    /// Make this document's text become `text` by applying the *difference* as
    /// attributed CRDT operations — inserts and deletes — rather than by replacing
    /// the document. Inserted runs take their author from `spans`, the
    /// `(actor, session, byte_len)` tiling of `text` (as
    /// [`from_blamed`](Self::from_blamed) uses); a run under actor `0`, or past the
    /// end of `spans`, is inserted unattributed.
    ///
    /// Because the edits are CRDT operations, they merge: a replica that has this
    /// change and a replica that has a concurrent one converge on both. That is
    /// what [`Fs::checkpoint_coedit`] uses to fold an out-of-band write into a live
    /// document instead of racing it.
    ///
    /// Character-level, so it is called on documents of editor size; it is not a
    /// bulk-import path.
    pub fn reconcile_with(&self, text: &str, spans: &[(i64, i64, u64)]) -> Result<()> {
        let before = self.text();
        if before == text {
            return Ok(());
        }
        let diff = TextDiff::from_chars(before.as_str(), text);
        let mut txn = self.doc.transact_mut();
        let mut idx: u32 = 0; // byte offset into the document, as we mutate it
        let mut authors = SpanCursor::new(spans);
        // The open insert run: its author and the text accumulated so far. Runs are
        // batched so a word typed by one author is one CRDT insert, not N.
        let mut pending: Option<((i64, i64), String)> = None;

        // Flush the open insert run at the current index, advancing past it.
        macro_rules! flush {
            () => {
                if let Some((author, piece)) = pending.take() {
                    if author.0 != 0 {
                        let attrs = author_attrs(WriteCtx::session(author.0, author.1));
                        self.text
                            .insert_with_attributes(&mut txn, idx, &piece, attrs);
                    } else {
                        self.text.insert(&mut txn, idx, &piece);
                    }
                    idx += doc_len(&piece);
                }
            };
        }

        for change in diff.iter_all_changes() {
            let value = change.value();
            match change.tag() {
                ChangeTag::Equal => {
                    flush!();
                    idx += doc_len(value);
                    authors.advance(value.len());
                }
                // Present in the document, absent from `text`: delete it. The
                // document shrinks under `idx`, so `idx` does not move.
                ChangeTag::Delete => {
                    flush!();
                    self.text.remove_range(&mut txn, idx, doc_len(value));
                }
                ChangeTag::Insert => {
                    let author = authors.author();
                    match &mut pending {
                        Some((a, piece)) if *a == author => piece.push_str(value),
                        _ => {
                            flush!();
                            pending = Some((author, value.to_string()));
                        }
                    }
                    authors.advance(value.len());
                }
            }
        }
        flush!();
        let _ = idx; // the trailing flush's advance is dead, but keeps `flush!` uniform
        drop(txn);
        Ok(())
    }

    /// The current text together with its per-span `(actor, session, byte_len)`
    /// authorship, both walked from one CRDT diff so they always agree. A run with
    /// no recorded author (which should not occur — every insert is attributed)
    /// reports actor `0`; [`Fs::checkpoint_coedit`] resolves that to the
    /// checkpointer.
    pub fn snapshot(&self) -> (String, Vec<(i64, i64, u64)>) {
        let txn = self.doc.transact();
        // `GetString` is authoritative for the bytes: an embed is not text and
        // must never reach the file or the blame index, whatever `diff` renders
        // it as.
        let plain = self.text.get_string(&txn);
        let Scan { runs, .. } = scan_runs(&self.text, &txn, Some(&plain), raw_author);
        let text = plain;
        // Adjacent runs by the same author are coalesced, so a document's
        // authorship has one canonical spelling. Repairs and Yjs's own block
        // splitting both fragment runs without changing who wrote what, and an
        // un-coalesced tiling would make `from_blamed`'s round trip depend on
        // that incidental fragmentation.
        let mut spans: Vec<(i64, i64, u64)> = Vec::new();
        for run in &runs {
            if run.byte_len == 0 {
                continue; // an embed contributes no bytes to the file
            }
            let (actor, session) = run
                .stamp
                .as_deref()
                .map_or((0, 0), |v: &str| parse_author(v));
            match spans.last_mut() {
                Some(last) if last.0 == actor && last.1 == session => last.2 += run.byte_len,
                _ => spans.push((actor, session, run.byte_len)),
            }
        }
        (text, spans)
    }
}

// ─── server-owned authorship: the enforcement machinery ──────────────────────
//
// Stamping only the ranges a *text* diff calls inserted left every other
// character's author attribute writable by the client: a formatting-only update
// changes no text, so it produced no ranges, and whatever `a` the client wrote
// stood — flowing through `snapshot` into the durable blame index with the file
// bytes and their content hash unchanged. Nothing on the byte axis moves, so no
// downstream check can catch it, and for a co-edited file the CRDT attribute is
// the only record of authorship there is.
//
// So stop asking "what did this update insert?" and instead compute, for the
// whole document, what the authorship *must* be, then repair every run that
// disagrees. Authorship becomes a total function of (pre-image authorship, text
// diff, connection identity) rather than something a diff opts into stamping.
//
// The read rule this buys, stated as an induction: `AUTHOR_KEY` may be believed
// as truth exactly on a document no un-normalised client apply has touched. The
// base cases are all server-written (`new`, `insert`, `from_blamed`, `load`,
// `reconcile_with`, `apply_relayed` — the last carrying a peer's already-repaired
// delta), and `apply_update_as` is the inductive step. Totality is what makes the
// step hold; insert-only stamping did not.
//
// The helpers are generic over the stamp because `coedit_tree` enforces the same
// rule over `(author, node-id)` pairs.

/// One run of a co-edited text as the CRDT currently holds it: the stamp it
/// carries, plus its length in the two units that differ.
///
/// `byte_len` is its contribution to the flat string (0 for an embed, which is
/// not text); `doc_len` is its contribution to the *document index space* (1 for
/// an embed). They coincide for ordinary text — the document indexes UTF-8
/// bytes, see [`doc_len`] — and keeping both is what converts a byte range
/// computed over the string back into the index range `format` wants, even in a
/// document containing embeds.
#[derive(Debug, Clone)]
pub(crate) struct DocRun<S> {
    pub stamp: S,
    pub byte_len: u64,
    pub doc_len: u32,
}

/// The result of walking a text node's runs.
pub(crate) struct Scan<S> {
    /// The node's plain text — embeds contribute nothing.
    pub flat: String,
    /// Its runs, in document order.
    pub runs: Vec<DocRun<S>>,
    /// Whether the run list's index accounting agrees with the document's own
    /// length, i.e. whether a byte range over [`flat`](Self::flat) can be mapped
    /// back to a `format` index range at all.
    ///
    /// False means a **string-valued embed** is present that could not be told
    /// apart from text. `Text::diff` renders `ItemContent::Embed(Any::String(s))`
    /// as `Out::Any(Any::String(s))` — byte-for-byte the shape real text has —
    /// while `yrs` indexes *every* embed as exactly one position. Counting such a
    /// chunk's bytes as its index length inflates every index after it, so a
    /// repair silently lands past its target and a forged stamp survives. A
    /// caller that cannot establish the mapping must refuse the update rather
    /// than attempt the repair: a refusal is recoverable, a silent
    /// misattribution is not.
    pub indexable: bool,
}

/// Walk a text node once, returning its flat string and its runs.
///
/// This is the **single** place a stamp is read off a chunk. `extract` pulls the
/// caller's stamp type out of a chunk's formatting attributes — the *raw*
/// attribute value, never a parsed one: a malformed `a` like `"1,2,junk"` parses
/// to a plausible `(1, 2)` and would then compare equal to a legitimate stamp,
/// whereas comparing raw strings against the server's own value normalises every
/// malformed variant away.
///
/// `plain` is the node's authoritative plain text when the caller can obtain one
/// independently of `diff` — `GetString` for a flat `Y.Text`, which excludes
/// embeds. Given it, a string embed is told apart from text exactly, by matching
/// each chunk against the text still to be consumed. Without it (a `Y.XmlText`,
/// whose `GetString` renders formatting as XML tags and so is not the plain
/// text), a string embed is indistinguishable and [`Scan::indexable`] reports
/// false so the caller refuses.
pub(crate) fn scan_runs<T: ReadTxn, R: Text, S>(
    text: &R,
    txn: &T,
    plain: Option<&str>,
    extract: impl Fn(Option<&Attrs>) -> S,
) -> Scan<S> {
    let mut flat = String::new();
    let mut runs = Vec::new();
    for chunk in text.diff(txn, YChange::identity) {
        let stamp = extract(chunk.attributes.as_deref());
        // A chunk is text iff it continues the authoritative plain text. An embed
        // never does, because `plain` excludes embeds entirely.
        let is_text = match (&chunk.insert, plain) {
            (Out::Any(Any::String(piece)), Some(p)) => p[flat.len()..].starts_with(&**piece),
            (Out::Any(Any::String(_)), None) => true,
            _ => false,
        };
        match &chunk.insert {
            Out::Any(Any::String(piece)) if is_text => {
                flat.push_str(piece);
                runs.push(DocRun {
                    stamp,
                    byte_len: piece.len() as u64,
                    doc_len: piece.len() as u32,
                });
            }
            // An embed (an image, a custom object, or a bare string) is not text:
            // it contributes nothing to the flat string but still occupies one
            // document index, so a `format` range computed over the string would
            // be off by one per preceding embed without this.
            _ => runs.push(DocRun {
                stamp,
                byte_len: 0,
                doc_len: 1,
            }),
        }
    }
    // The invariant that makes `doc_range` sound. It holds by construction when
    // `plain` was supplied, and is the only detector when it was not.
    let claimed: u64 = runs.iter().map(|r| r.doc_len as u64).sum();
    let indexable = claimed == text.len(txn) as u64 && plain.is_none_or(|p| p == flat);
    Scan {
        flat,
        runs,
        indexable,
    }
}

/// The `(stamp, byte_len)` tiling of a run list — what the diff walk and the
/// divergence comparison operate on, with embeds (zero bytes) dropped.
pub(crate) fn stamp_tiling<S: Clone + PartialEq>(runs: &[DocRun<S>]) -> Vec<(S, u64)> {
    let mut out = Vec::new();
    for r in runs {
        push_run(&mut out, r.stamp.clone(), r.byte_len);
    }
    out
}

/// Append `(stamp, len)`, merging into the previous run when the stamp matches,
/// so every tiling this module builds is canonical. Without this the same
/// authorship could be expressed as two different tilings and the divergence
/// walk would report repairs that change nothing.
fn push_run<S: PartialEq>(out: &mut Vec<(S, u64)>, stamp: S, len: u64) {
    if len == 0 {
        return;
    }
    match out.last_mut() {
        Some((s, l)) if *s == stamp => *l += len,
        _ => out.push((stamp, len)),
    }
}

/// A forward-only cursor over a `(stamp, byte_len)` tiling.
struct TilingCursor<'a, S> {
    runs: &'a [(S, u64)],
    at: usize,
    used: u64,
}

impl<'a, S: Clone + PartialEq> TilingCursor<'a, S> {
    fn new(runs: &'a [(S, u64)]) -> Self {
        Self {
            runs,
            at: 0,
            used: 0,
        }
    }

    /// Consume `bytes`, appending the stamps they were tiled by to `out`. Past
    /// the end of the tiling — which a tiling taken from the document never
    /// reaches, but a hand-supplied one can — `fallback` is used.
    fn carry(&mut self, mut bytes: u64, out: &mut Vec<(S, u64)>, fallback: &S) {
        while bytes > 0 {
            let Some((stamp, len)) = self.runs.get(self.at) else {
                push_run(out, fallback.clone(), bytes);
                return;
            };
            let take = bytes.min(len - self.used);
            push_run(out, stamp.clone(), take);
            self.used += take;
            bytes -= take;
            if self.used == *len {
                self.at += 1;
                self.used = 0;
            }
        }
    }

    /// Advance past `bytes` without emitting — text this update deleted.
    fn skip(&mut self, mut bytes: u64) {
        while bytes > 0 {
            let Some((_, len)) = self.runs.get(self.at) else {
                return;
            };
            let take = bytes.min(len - self.used);
            self.used += take;
            bytes -= take;
            if self.used == *len {
                self.at += 1;
                self.used = 0;
            }
        }
    }
}

/// The authorship the document **must** have after this update: every character
/// that survived from the pre-image keeps the stamp it had there, and every
/// character the update introduced takes a fresh stamp from `fresh`.
///
/// `fresh` is called once per maximal inserted run, so such a run gets one node
/// id rather than one per diff hunk (which matters only for the tree shape; the
/// flat shape mints the same value every time).
///
/// The result tiles `after` exactly. Note what totality makes load-bearing: the
/// alignment is `similar`'s *character* diff, not CRDT item identity, so moved
/// text is credited to the mover, and text retyped identically to text already
/// present may align as surviving. That was already true of the insert-only
/// stamping this replaces — it is now the explicit rule rather than an emergent
/// one.
///
/// **Why it is still a text diff.** `yrs` *does* expose the CRDT's own answer:
/// `Text::diff_range(txn, Some(hi), Some(lo), …)` marks every chunk not visible
/// in the `lo` snapshot as [`ChangeKind::Added`](yrs::types::text::ChangeKind),
/// which is exactly "the items this update introduced" — no character alignment
/// involved. Driving the stamp from that closes the gap outright, and it does
/// not even need `skip_gc`, since only the added side is consulted.
///
/// It is unusable on `yrs` 0.23.5: `diff_range` panics (`index out of bounds`,
/// `block_store.rs:51`, from `split_by_snapshot`) as soon as a **second client**
/// has contributed to the document. A single-client document is fine, which is
/// why it looks workable until it is tried — and a co-editing room always has
/// more than one client. This runs behind the shared room lock, so a panic there
/// takes down every socket on the document: strictly worse than the
/// misattribution it would prevent. Reproducible in a dozen lines of pure `yrs`.
///
/// So the gap stays, bounded and tested (`coedit_authorship_carries_by_text_diff
/// _not_crdt_identity`): an attacker cannot *name* an actor, only cause a victim
/// to keep credit for characters that coincide with the victim's own. Revisit
/// when the upstream panic is fixed — the replacement is about ten lines.
pub(crate) fn intended_stamps<S: Clone + PartialEq>(
    before: &str,
    before_tiling: &[(S, u64)],
    after: &str,
    unattributed: &S,
    mut fresh: impl FnMut() -> S,
) -> Vec<(S, u64)> {
    let mut out: Vec<(S, u64)> = Vec::new();
    let mut cursor = TilingCursor::new(before_tiling);
    // The stamp for the run of inserts currently open, minted lazily so `fresh`
    // is called once per maximal run rather than once per diff hunk.
    let mut open_insert: Option<S> = None;

    for change in TextDiff::from_chars(before, after).iter_all_changes() {
        let len = change.value().len() as u64;
        match change.tag() {
            ChangeTag::Equal => {
                open_insert = None;
                cursor.carry(len, &mut out, unattributed);
            }
            ChangeTag::Delete => {
                open_insert = None;
                cursor.skip(len);
            }
            ChangeTag::Insert => {
                let stamp = match &open_insert {
                    Some(s) => s.clone(),
                    None => {
                        let s = fresh();
                        open_insert = Some(s.clone());
                        s
                    }
                };
                push_run(&mut out, stamp, len);
            }
        }
    }
    out
}

/// The maximal `(byte_start, byte_len, intended_stamp)` regions where `observed`
/// disagrees with `intended`. Both must tile the same text.
///
/// An empty result means the update left authorship exactly as the server would
/// have written it — the overwhelmingly common case, and why enforcement writes
/// nothing at all on an honest edit.
pub(crate) fn diverging_runs<S: Clone + PartialEq>(
    intended: &[(S, u64)],
    observed: &[(S, u64)],
) -> Vec<(u64, u64, S)> {
    let mut out: Vec<(u64, u64, S)> = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    let (mut i_used, mut j_used) = (0u64, 0u64);
    let mut pos = 0u64;

    while i < intended.len() && j < observed.len() {
        let (want, wlen) = (&intended[i].0, intended[i].1);
        let (have, hlen) = (&observed[j].0, observed[j].1);
        let step = (wlen - i_used).min(hlen - j_used);
        if step == 0 {
            // A zero-length run on either side: step past it rather than spin.
            if wlen == i_used {
                i += 1;
                i_used = 0;
            }
            if hlen == j_used {
                j += 1;
                j_used = 0;
            }
            continue;
        }
        if want != have {
            match out.last_mut() {
                // Extend the open region when it abuts and wants the same stamp.
                Some((s, l, st)) if *s + *l == pos && *st == *want => *l += step,
                _ => out.push((pos, step, want.clone())),
            }
        }
        pos += step;
        i_used += step;
        j_used += step;
        if i_used == wlen {
            i += 1;
            i_used = 0;
        }
        if j_used == hlen {
            j += 1;
            j_used = 0;
        }
    }
    out
}

/// The document index at byte offset `byte_off`, stepping over embeds (zero
/// bytes, one index). `None` for an offset past the end of the text.
fn doc_index_at<S>(runs: &[DocRun<S>], byte_off: u64) -> Option<u32> {
    let (mut byte_pos, mut idx) = (0u64, 0u32);
    for run in runs {
        if byte_off < byte_pos + run.byte_len {
            return Some(idx + (byte_off - byte_pos) as u32);
        }
        byte_pos += run.byte_len;
        idx += run.doc_len;
    }
    (byte_off == byte_pos).then_some(idx)
}

/// Convert a byte range over the flat string into the `(index, len)` document
/// range [`Text::format`] takes.
///
/// `None` for a range that does not land inside the document — which a range
/// derived from the document's own run list cannot produce, but `format`
/// *panics* on an out-of-range index and runs here behind a shared room lock, so
/// a bad range must never reach it.
pub(crate) fn doc_range<S>(
    runs: &[DocRun<S>],
    byte_start: u64,
    byte_len: u64,
) -> Option<(u32, u32)> {
    let start = doc_index_at(runs, byte_start)?;
    let end = doc_index_at(runs, byte_start + byte_len)?;
    (end >= start).then_some((start, end - start))
}

/// A forward-only cursor over a `(actor, session, byte_len)` span tiling, for
/// looking up the author at a byte offset that only ever moves forward.
struct SpanCursor<'a> {
    spans: &'a [(i64, i64, u64)],
    /// Index of the span the cursor currently sits in.
    at: usize,
    /// Bytes consumed within `spans[at]`.
    used: u64,
}

impl<'a> SpanCursor<'a> {
    fn new(spans: &'a [(i64, i64, u64)]) -> Self {
        Self {
            spans,
            at: 0,
            used: 0,
        }
    }

    /// The author at the current offset, or `(0, 0)` past the end of the tiling.
    fn author(&self) -> (i64, i64) {
        match self.spans.get(self.at) {
            Some(&(actor, session, _)) => (actor, session),
            None => (0, 0),
        }
    }

    /// Move `bytes` forward, stepping into later spans as needed.
    fn advance(&mut self, bytes: usize) {
        let mut left = bytes as u64;
        while left > 0 {
            let Some(&(_, _, len)) = self.spans.get(self.at) else {
                return;
            };
            let take = left.min(len - self.used);
            self.used += take;
            left -= take;
            if self.used == len {
                self.at += 1;
                self.used = 0;
            }
        }
    }
}

/// Merge a y-sync frame relayed from another worker into `doc` — content another
/// replica already attributed — *without* re-attribution. Shared by both document
/// shapes; see [`CoeditDoc::apply_relayed`] for what it is for.
pub(crate) fn apply_relayed(doc: &Doc, frame: &[u8]) -> Result<()> {
    let mut decoder = DecoderV1::new(Cursor::new(frame));
    let reader = MessageReader::new(&mut decoder);
    for msg in reader {
        let msg =
            msg.map_err(|e| OrigoFSError::InvalidArgument(format!("bad relayed frame: {e}")))?;
        // Only content messages carry state; awareness/etc. are ignored on the
        // relay (presence is gossiped between the clients on each worker).
        if let Message::Sync(SyncMessage::Update(u) | SyncMessage::SyncStep2(u)) = msg {
            let update = Update::decode_v1(&u)
                .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;
            // **Deliberately origin-less, and this is not an oversight to fix.**
            //
            // A relayed frame is another worker's clients' work, already
            // attributed there. Giving it an origin would put it on *this*
            // worker's per-actor undo stacks, so a Ctrl+Z here would pop an edit
            // made on a machine this process never spoke to. Leaving it bare is
            // what excludes it, because `author_origin`'s note explains the
            // `yrs` filter treats origin-less transactions as untracked the
            // moment any actor origin is included. See `tests/coedit_origin.rs`.
            doc.transact_mut()
                .apply_update(update)
                .map_err(|e| OrigoFSError::InvalidArgument(format!("apply co-edit update: {e}")))?;
        }
    }
    Ok(())
}

/// The y-sync frame carrying `doc`'s **whole** state as an `Update`, for a client
/// that has to be caught up unconditionally.
///
/// This is the recovery frame for a socket dropped from the fan-out. Unlike
/// [`sync_start`], it needs no round trip and no knowledge of what the client
/// already has: a Yjs update is idempotent, so applying the full state is always
/// safe and always sufficient. That matters because the alternative — a
/// `SyncStep1` — only asks the client for what *we* lack, which is the wrong
/// direction to heal a client that is behind.
pub(crate) fn state_frame(doc: &Doc) -> Vec<u8> {
    let state = doc
        .transact()
        .encode_state_as_update_v1(&StateVector::default());
    let mut encoder = EncoderV1::new();
    Message::Sync(SyncMessage::Update(state)).encode(&mut encoder);
    encoder.to_vec()
}

/// The y-sync `SyncStep1` frame greeting a freshly-connected client with `doc`'s
/// state vector. Shared by both document shapes.
pub(crate) fn sync_start(doc: &Doc) -> Vec<u8> {
    let sv = doc.transact().state_vector();
    let mut encoder = EncoderV1::new();
    Message::Sync(SyncMessage::SyncStep1(sv)).encode(&mut encoder);
    encoder.to_vec()
}

/// Drive one inbound y-sync payload against `doc`, applying any content the client
/// contributed through `apply_as` — the caller's *attributing* apply, which is the
/// only thing that differs between the flat and tree shapes.
///
/// A payload may pack several messages; each is handled in order. This is the
/// transport-agnostic core of live co-editing: the axum WebSocket route, the
/// FastAPI route, and the tests all funnel bytes through here. Awareness (cursor
/// presence) is relayed verbatim; the server keeps no awareness state, so peers
/// gossip presence through the room's fan-out.
pub(crate) fn drive_sync(
    doc: &Doc,
    data: &[u8],
    apply_as: impl Fn(&[u8]) -> Result<Vec<u8>>,
) -> Result<SyncReply> {
    let mut decoder = DecoderV1::new(Cursor::new(data));
    let reader = MessageReader::new(&mut decoder);
    let mut reply = EncoderV1::new();
    let mut broadcast = EncoderV1::new();
    let (mut has_reply, mut has_broadcast) = (false, false);
    let mut content_changed = false;
    // *Distinct* tags, and a separate count. One frame may pack thousands of
    // messages — a 16 MiB one (the socket's cap) can hold ~8 million — and these
    // bytes come from a client this module explicitly does not trust. Pushing one
    // entry per message would let a hostile frame make the server allocate half its
    // size again and, worse, render all of it into a single `warn!` line. The
    // diagnostic is "which tags did I not understand", which is at most 256 values
    // and in practice one.
    let mut unhandled: Vec<u8> = Vec::new();
    let mut unhandled_count: u64 = 0;
    let note_unhandled = |tag: u8, seen: &mut Vec<u8>, n: &mut u64| {
        *n += 1;
        if !seen.contains(&tag) {
            seen.push(tag);
        }
    };

    for msg in reader {
        let msg =
            msg.map_err(|e| OrigoFSError::InvalidArgument(format!("bad y-sync frame: {e}")))?;
        match msg {
            // The client wants what we have: answer with the update it lacks.
            Message::Sync(SyncMessage::SyncStep1(sv)) => {
                let update = doc.transact().encode_state_as_update_v1(&sv);
                Message::Sync(SyncMessage::SyncStep2(update)).encode(&mut reply);
                has_reply = true;
            }
            // The client is handing us content (initial sync or a live edit):
            // apply + attribute, then fan the attributed delta out to peers —
            // and back to the sender.
            Message::Sync(SyncMessage::SyncStep2(update))
            | Message::Sync(SyncMessage::Update(update)) => {
                let delta = apply_as(&update)?;
                if !delta.is_empty() {
                    Message::Sync(SyncMessage::Update(delta.clone())).encode(&mut broadcast);
                    has_broadcast = true;
                    content_changed = true;
                    // Echo it to the sender too. Attributing the edit adds CRDT
                    // items (the author marks) to our doc that the sender's doc
                    // lacks — it never saw them, since the sender doesn't get the
                    // broadcast. Without them the sender diverges structurally,
                    // and a *later* peer edit positioned against those items can't
                    // integrate (it waits, pending, on origins the sender is
                    // missing). Sending the delta back keeps every replica — server
                    // and all clients, author included — structurally identical.
                    // Re-applying the sender's own items is a no-op (updates are
                    // idempotent); only the attribution items are new.
                    Message::Sync(SyncMessage::Update(delta)).encode(&mut reply);
                    has_reply = true;
                }
            }
            // Cursor presence: relay to the room, keep no server-side state.
            Message::Awareness(_) | Message::AwarenessQuery => {
                msg.encode(&mut broadcast);
                has_broadcast = true;
            }
            // Auth is resolved out-of-band (before we ever get here); custom
            // tags are not part of our protocol. Neither carries content, so
            // neither has an effect — but *silently* having no effect is what
            // made a framing mismatch undiagnosable (#162), so both are counted
            // and reported.
            Message::Auth(_) => note_unhandled(MSG_AUTH, &mut unhandled, &mut unhandled_count),
            Message::Custom(tag, _) => note_unhandled(tag, &mut unhandled, &mut unhandled_count),
        }
    }

    if !unhandled.is_empty() {
        // The overwhelmingly likely cause, and the one worth naming: a client
        // written against the **y-sync** protocol directly rather than
        // y-websocket sends a bare `[messageYjsUpdate=2, …]` frame, and 2 is
        // `messageAuth` in the outer y-websocket envelope this speaks. It decodes
        // cleanly, carries no content, and used to be dropped without a word — a
        // socket that connects, handshakes, reports the right peer count, and
        // never converges, with nothing anywhere to attribute it to.
        tracing::warn!(
            tags = ?unhandled,
            "co-edit: ignored {unhandled_count} y-sync message(s) this server has no \
             handler for. Frames must carry the y-websocket envelope (outer \
             messageSync=0, then the y-sync payload); a bare y-sync update arrives as \
             tag 2 (messageAuth) and syncs nothing",
        );
    }

    Ok(SyncReply {
        unhandled,
        reply: if has_reply {
            reply.to_vec()
        } else {
            Vec::new()
        },
        broadcast: if has_broadcast {
            broadcast.to_vec()
        } else {
            Vec::new()
        },
        content_changed,
    })
}

/// The routing for one processed y-sync payload: [`reply`](Self::reply) goes back
/// to the connection it came from (e.g. a `SyncStep2` answering its `SyncStep1`);
/// [`broadcast`](Self::broadcast) fans out to the room's *other* peers (the
/// attributed content delta, or relayed awareness). Either may be empty.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReply {
    /// Frames to send back to the originating connection.
    pub reply: Vec<u8>,
    /// Frames to fan out to every other connection in the room.
    pub broadcast: Vec<u8>,
    /// Whether this payload actually changed the **document** — i.e. it carried a
    /// content delta, not only relayed presence.
    ///
    /// A caller driving durability (the WebSocket routes' checkpoint sweeper) must
    /// gate on this rather than on `broadcast` being non-empty. Awareness — cursor
    /// presence — is broadcast too, and every real Yjs client emits it constantly:
    /// on each selection change and on `y-protocols`' periodic heartbeat, with no
    /// typing involved. Treating that as an edit marks the room dirty, so an open
    /// but idle tab writes an op-log entry and a blame rewrite on every sweeper
    /// tick, forever — churn with nothing to crystallize.
    pub content_changed: bool,
    /// The outer y-websocket message tags in this payload that carried no effect —
    /// `messageAuth` (2), which is resolved out-of-band here, and any custom tag.
    /// Empty for every well-framed payload.
    ///
    /// Reported (#162) because the failure it diagnoses is otherwise invisible: a
    /// client written against the **y-sync** protocol directly, rather than the
    /// y-websocket envelope this speaks, sends a bare `[messageYjsUpdate=2, …]`
    /// frame — and 2 is `messageAuth` in the outer envelope. It decodes, it is
    /// ignored, and the socket then connects, handshakes, reports the right peer
    /// count and never converges. A caller that can surface this to its client
    /// should; the server also logs it at `warn`.
    ///
    /// Not an error, deliberately: `messageAuth` is a real y-protocol message and
    /// refusing the whole payload for one would break a conforming client to
    /// diagnose a non-conforming one.
    pub unhandled: Vec<u8>,
}

/// Document-index length of `s`: its UTF-8 **byte** length.
///
/// This is the unit a `CoeditDoc`/`CoeditTreeDoc` indexes in — see
/// [`CoeditDoc`]'s type docs for why, and `coedit_indices_are_utf8_bytes` in
/// `tests/coedit.rs`, which pins it so a `yrs` change cannot flip it silently.
/// It is named rather than inlined as `s.len()` so every index computation in
/// this module points at that one explanation.
#[inline]
pub(crate) fn doc_len(s: &str) -> u32 {
    s.len() as u32
}

/// Hidden directory holding persisted co-edit CRDT sidecars.
///
/// It is an ordinary directory in the working tree, so a sidecar is an ordinary
/// file: it is walked by `commit` into the commit tree, and marked reachable by
/// `gc` from *both* the live-working-tree root and every commit tree. Nothing
/// pins it specially, and nothing needs to — see
/// `origofs-core/tests/coedit_sidecar_gc.rs`, which proves the property rather
/// than assuming it.
pub const COEDIT_SIDECAR_DIR: &str = "/.origofs/ydoc";

/// The sidecar path for a co-edited `path`, hex-encoded so it needs no nested
/// directories and can't collide with another document's sidecar.
pub fn coedit_sidecar_path(path: &str) -> String {
    format!("{COEDIT_SIDECAR_DIR}/{}", hex::encode(path.as_bytes()))
}

/// Length of the coherence hash a sidecar embeds (BLAKE3).
const SIDECAR_HASH_LEN: usize = 32;

/// The **pre-versioning** framing: `[1][32-byte hash][ydoc state]`, written by
/// every origofs up to 0.0.4. Kept readable forever — it is the one format a
/// bucket in the wild is guaranteed to hold, and re-framing an existing sidecar
/// would need a migration pass over the working tree to buy nothing.
///
/// It is unambiguous against the versioned framing because that one opens with an
/// ASCII tag (`O` = 0x4f), which `1` is not.
const LEGACY_SIDECAR_MAGIC: u8 = 1;

/// Frame a sidecar blob:
/// `ORGY | version | [32-byte BLAKE3 of the flat text it crystallized] | [ydoc state]`.
///
/// The embedded hash is the coherence marker — [`open_coedit`](Fs::open_coedit)
/// resumes the CRDT only if the file still hashes to it, else rebuilds from the
/// file.
fn frame_sidecar(text: &[u8], state: &[u8]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(format::HEADER_LEN + SIDECAR_HASH_LEN + state.len());
    blob.extend_from_slice(&format::COEDIT_SIDECAR.header());
    blob.extend_from_slice(blake3::hash(text).as_bytes());
    blob.extend_from_slice(state);
    blob
}

/// Split a framed sidecar blob into `(flat_hash, ydoc_state)`.
///
/// Three outcomes, and the difference between the last two is the whole point of
/// the version byte:
///
/// - `Ok(Some(..))` — a v1 or legacy sidecar, resumable.
/// - `Ok(None)` — not a sidecar this build recognizes *at all* (truncated, or
///   foreign bytes). The sidecar is a resumable **cache**, so the caller falls
///   back: the flat shape rebuilds from the file, which is always safe.
/// - `Err(UnsupportedVersion)` — a sidecar written by a **newer** origofs. That
///   is emphatically not a cache miss: the bytes are fine and the fix is to
///   upgrade, so folding it into the fallback above would report a document with
///   history as one without, and quietly drop the history on the next checkpoint.
///   See `check_store_format` in `engine.rs`, which names this exact path.
pub(crate) fn parse_sidecar(blob: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
    let body = if format::COEDIT_SIDECAR.tagged(blob) {
        match format::COEDIT_SIDECAR.version_of(blob)? {
            1 => &blob[format::HEADER_LEN..],
            // Unreachable while `max_read_version` matches the arms above;
            // `version_of` has already refused anything higher.
            v => return Err(format::COEDIT_SIDECAR.unsupported(v)),
        }
    } else if blob.first() == Some(&LEGACY_SIDECAR_MAGIC) {
        &blob[1..]
    } else {
        return Ok(None);
    };
    Ok((body.len() >= SIDECAR_HASH_LEN).then(|| body.split_at(SIDECAR_HASH_LEN)))
}

/// Tile `text` into consecutive `(actor, session, byte_len)` spans from its blame
/// `ranges`, attributing any byte no range covers to `fallback` (the actor bringing
/// the file into co-editing). Walks by character so every span lands on a char
/// boundary even if a stale range boundary wouldn't.
fn blame_to_spans(
    text: &str,
    mut ranges: Vec<BlameRange>,
    fallback: (i64, i64),
) -> Vec<(i64, i64, u64)> {
    ranges.sort_by_key(|r| r.byte_start);
    let mut spans: Vec<(i64, i64, u64)> = Vec::new();
    let mut ri = 0usize;
    for (byte_off, ch) in text.char_indices() {
        let b = byte_off as u64;
        // Advance past ranges we've moved beyond (blame tiles left-to-right).
        while ri < ranges.len() && ranges[ri].byte_end <= b {
            ri += 1;
        }
        let (actor, session) = match ranges.get(ri) {
            Some(r) if r.byte_start <= b && b < r.byte_end => (r.actor.id, r.session.unwrap_or(0)),
            _ => fallback, // a gap between/after ranges: un-blamed text
        };
        let clen = ch.len_utf8() as u64;
        match spans.last_mut() {
            Some(last) if last.0 == actor && last.1 == session => last.2 += clen,
            _ => spans.push((actor, session, clen)),
        }
    }
    spans
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Checkpoint a co-edited document into `path`: materialize its text and land
    /// it with per-span authorship via [`write_as_blamed`](Self::write_as_blamed),
    /// so blame shows each collaborator's exact character ranges (sub-line and
    /// interleaved). `ctx` is the actor performing the checkpoint (recorded on the
    /// op-log); any span the CRDT left unattributed falls back to `ctx`.
    /// Claim the undo stack for the document `(path, root)` on behalf of
    /// `holder`, or renew a claim it already has. Returns whether it now owns it.
    ///
    /// `root` is the `XmlFragment` root of a tree-shaped document, empty for the
    /// flat shape. A *document* is `(path, shape)`, not a path: one path may be
    /// open in both at once and they are two documents with two stacks, so a
    /// claim keyed on the path alone lets one shape's release free a claim the
    /// other still has a live stack under.
    ///
    /// **A worker must hold this before tracking an actor's edits.** At most one
    /// worker may keep an actor's stack for a document, because two independent
    /// stacks popping overlapping items can strip an author stamp between them
    /// and leave restored text unattributed — see the V22 migration for the
    /// mechanism, and `tests/coedit_undo_multiworker.rs` for the measurement.
    ///
    /// Nothing changes for a single-worker deployment: two tabs there are the
    /// same holder, so both claims succeed and they share one stack exactly as
    /// before. It bites only when routing splits an actor across workers, and
    /// there the honest answer is that the second tab has no undo — which a
    /// client can say, unlike a stack that quietly corrupts attribution.
    pub async fn claim_undo_stack(
        &self,
        path: &str,
        root: &str,
        actor_id: i64,
        holder: &str,
    ) -> Result<bool> {
        let now = self.now_secs();
        self.meta
            .claim_undo_stack(
                path,
                root,
                actor_id,
                holder,
                now + UNDO_CLAIM_LEASE_SECS,
                now,
            )
            .await
    }

    /// Drop `holder`'s claim on the document `(path, root)` — the actor's last
    /// socket on this worker leaving.
    pub async fn release_undo_stack(
        &self,
        path: &str,
        root: &str,
        actor_id: i64,
        holder: &str,
    ) -> Result<bool> {
        self.meta
            .release_undo_stack(path, root, actor_id, holder)
            .await
    }

    /// Drop every claim `holder` has — a clean shutdown, so the next worker to
    /// see those actors does not wait out a lease that nobody will renew.
    pub async fn release_undo_claims_for_holder(&self, holder: &str) -> Result<u64> {
        self.meta.release_undo_claims_for_holder(holder).await
    }

    /// Push out the lease on every claim `holder` has. A live worker calls this
    /// on a timer at well under [`UNDO_CLAIM_LEASE_SECS`].
    pub async fn renew_undo_claims(&self, holder: &str) -> Result<u64> {
        let expires_at = self.now_secs() + UNDO_CLAIM_LEASE_SECS;
        self.meta.renew_undo_claims(holder, expires_at).await
    }

    /// Undo (or, with `redo`, redo) `ctx`'s actor's most recent action on the
    /// live document at `path`, returning the y-sync update to fan out to the
    /// room — empty when there was nothing to pop.
    ///
    /// # Why the write check
    ///
    /// **An undo is a write**, so it takes `WRITE` at the path exactly as
    /// [`open_coedit`](Self::open_coedit) does. The WebSocket that carries a
    /// room authenticates but does not authorize, and the request arriving on an
    /// already-open socket proves only that the actor could open it — the same
    /// reasoning that made both checkpoints re-check as a backstop.
    ///
    /// Two consequences worth stating rather than discovering. A propose-only
    /// actor has no undo at all, and is refused rather than silently no-op'd:
    /// there is no such thing as a proposed undo, and an editor that showed the
    /// key working while nothing happened would be worse than one that says no.
    /// And an actor whose grant is revoked mid-session loses undo immediately,
    /// which is the right way round — unlike releasing a POSIX lock, leaving an
    /// edit *un*-undone strands nothing.
    ///
    /// The scoping to the actor's own work is not this check's job: it is the
    /// transaction origins, and it holds whatever the ACLs say. This decides
    /// whether the caller may write here at all.
    pub async fn undo_coedit(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
        redo: bool,
    ) -> Result<Vec<u8>> {
        self.ensure_may_undo_at(ctx, path, redo).await?;
        if redo {
            doc.redo_as(ctx)
        } else {
            doc.undo_as(ctx)
        }
    }

    /// [`undo_coedit`](Self::undo_coedit) for a tree-shaped document.
    ///
    /// Separate only because the two document types are, and the checkpoints are
    /// split the same way. Everything the doc comment above says applies here
    /// unchanged, including the `WRITE` check — the tree shape is what a rich-text
    /// editor actually binds to, so if anything it is the one that matters.
    pub async fn undo_coedit_tree(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &crate::coedit_tree::CoeditTreeDoc,
        redo: bool,
    ) -> Result<Vec<u8>> {
        self.ensure_may_undo_at(ctx, path, redo).await?;
        if redo {
            doc.redo_as(ctx)
        } else {
            doc.undo_as(ctx)
        }
    }

    /// The `WRITE` check both shapes' undo takes, with the verb the refusal will
    /// name. One place so the two cannot drift into disagreeing about whether an
    /// undo is a write.
    ///
    /// **Public because a surface must be able to run it before anything else.**
    /// A coordinator has to decide whether a room is even open, and whether this
    /// worker holds the actor's stack, to answer an undo request — and both of
    /// those are facts about the document that an actor with no write right must
    /// not learn. So the check runs first, before any lookup, exactly as the read
    /// and write checks elsewhere do; [`undo_coedit`](Self::undo_coedit) and
    /// [`undo_coedit_tree`](Self::undo_coedit_tree) then re-run it as the backstop
    /// for a caller that reached them directly.
    pub async fn ensure_may_undo_at(&self, ctx: WriteCtx, path: &str, redo: bool) -> Result<()> {
        let verb = if redo {
            "redo an edit to"
        } else {
            "undo an edit to"
        };
        self.ensure_may_write_at(ctx, verb, path).await
    }

    pub async fn checkpoint_coedit(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
    ) -> Result<()> {
        // The backstop to `open_coedit`'s check, for a caller holding a
        // `CoeditDoc` it did not open through this workspace. `write_as_blamed`
        // below is deliberately ungated — it is the coordinator's own write — so
        // this is the only thing standing between a doc and the working tree.
        self.ensure_may_write_at(ctx, "check point a co-edited document to", path)
            .await?;
        self.checkpoint_coedit_unchecked(ctx, path, doc).await
    }

    /// [`checkpoint_coedit`](Self::checkpoint_coedit) with the write check
    /// already made by the caller.
    ///
    /// Exactly one caller: accepting a CRDT suggestion, which lands the bytes as
    /// the **author** while the *approver* is the one that had to be authorized —
    /// and `accept_suggestion` has already checked `WRITE` at the suggestion's own
    /// path for that approver. Re-checking as the author refused every proposal
    /// from a propose-only actor, which is the entire population the review queue
    /// exists for: `suggest_coedit` recorded the proposal happily and
    /// `accept_suggestion` then failed with `Denied` naming the *author*, so a
    /// reviewer with full rights could not accept a proposal it had just read.
    pub(crate) async fn checkpoint_coedit_unchecked(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
    ) -> Result<()> {
        self.reconcile_out_of_band(ctx, path, doc).await?;
        let (text, mut spans) = doc.snapshot();
        for span in &mut spans {
            if span.0 == 0 {
                span.0 = ctx.actor;
                span.1 = ctx.session.unwrap_or(0);
            }
        }
        self.write_as_blamed(ctx, path, text.as_bytes(), &spans)
            .await?;
        // Persist the CRDT itself as a sidecar blob so the session is durable and
        // resumable: a checkpoint of only the flat text would drop the op history
        // needed to keep co-editing. Per #33, the ydoc sidecar rides in the same
        // content store, pinned to the commit like any tree file. It's framed with
        // the flat text's hash so a later open can tell whether the file changed
        // underneath (an accepted suggestion, a merge, a plain write) and rebuild
        // instead of resuming a stale CRDT.
        self.mkdir_p(COEDIT_SIDECAR_DIR).await?;
        let blob = frame_sidecar(text.as_bytes(), &doc.state_update());
        self.write(&coedit_sidecar_path(path), &blob).await?;
        // Refresh the live marker's coherence hash to what we just wrote, so the
        // *next* checkpoint can again tell an out-of-band write from our own. Only
        // a *refresh*: checkpointing does not by itself make a path live — that is
        // `open_coedit`'s job — so a one-off checkpoint by a Rust caller holding a
        // `CoeditDoc` leaves no marker behind for a reader to trip over.
        if self.meta.get_live_doc(path).await?.is_some() {
            // `mark_checkpointed`, not `mark_live`: this is the one call site that
            // has actually crystallized the bytes, so it is the only one entitled
            // to stamp `checkpointed_at` (#97).
            self.mark_checkpointed(ctx, path).await?;
        }
        Ok(())
    }

    /// Fold an **out-of-band** write into the live document before it is
    /// checkpointed (issue #75 §3.4).
    ///
    /// A path can be live in a co-editing room *and* written through an ordinary
    /// `write`/`write_as`/suggestion-accept at the same time. Without this the two
    /// simply race: the next checkpoint crystallizes the CRDT over the file and the
    /// out-of-band change vanishes with no conflict and no trace.
    ///
    /// The detector is the live marker's `content_hash` — the content address this
    /// document last checkpointed. If the file's address has moved, somebody else
    /// wrote it.
    ///
    /// Reconciliation then reuses exactly the machinery that already exists rather
    /// than inventing a merge algorithm: the **sidecar** holds this document's state
    /// as of the last checkpoint, so we load it as a *replica*, replay the
    /// out-of-band change onto that replica as attributed CRDT operations
    /// ([`reconcile_with`](CoeditDoc::reconcile_with), authors recovered from the
    /// file's blame exactly as [`open_coedit`](Self::open_coedit) recovers them for
    /// a stale sidecar), and then merge the replica back into the live document.
    /// The replica diverged from the same checkpoint the live document did, so the
    /// two sets of edits are genuinely concurrent and the CRDT merges them — every
    /// edit survives, each keeping its own author. No three-way text merge, no
    /// conflict markers, no lost write.
    ///
    /// A no-op when the path is not live, the file has not moved, or there is no
    /// coherent sidecar to fork the replica from (nothing to merge *against*, so
    /// the existing whole-file behaviour stands).
    async fn reconcile_out_of_band(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
    ) -> Result<()> {
        let Some(live) = self.meta.get_live_doc(path).await? else {
            return Ok(());
        };
        let current = self.current_content_hex(path).await?;
        if current == live.content_hash {
            return Ok(()); // nobody wrote around us
        }
        // Past this point the file is *not* what this document was last coherent
        // with, so writing the document's text over it would destroy whatever
        // landed. Fold that write in where we can, and refuse where we cannot.
        //
        // Every arm below used to `return Ok(())`: "I could not reconcile" was read
        // by the caller as "there was nothing to reconcile", and it went on to
        // overwrite. A branch checkout makes it concrete — `checkout` rematerializes
        // the file *and* swaps the sidecar away (it lives in the working tree),
        // while the live marker is metadata and survives, so the one input
        // reconciliation needs is missing exactly when it is needed. A room opened
        // on the old branch then wrote its content onto the new one, silently. The
        // tree shape has always refused here (`refuse_out_of_band`); this is the
        // flat shape holding the same line, reconciling first where it can.
        let refuse = |why: &str| -> Result<()> {
            Err(OrigoFSError::ForeignWrite(format!(
                "{path} was written outside the co-editing session since its last \
                 checkpoint, and the two versions cannot be merged ({why}) — re-open \
                 the document to pick up the current file, then checkpoint again"
            )))
        };
        let bytes = match self.read(path).await {
            Ok(b) => b,
            // Resurrecting a file somebody deleted is as much a surprise as
            // clobbering one they wrote.
            Err(OrigoFSError::NotFound(_)) => return refuse("the file was removed"),
            Err(e) => return Err(e),
        };
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return refuse("it is no longer valid UTF-8 and cannot be merged as text");
        };
        // The replica: this document as it stood at the last checkpoint.
        let sidecar = match self.read(&coedit_sidecar_path(path)).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => {
                return refuse("its CRDT sidecar is gone, leaving no common base to merge from");
            }
            Err(e) => return Err(e),
        };
        // `?`, not a `refuse`: a sidecar from a newer origofs is an upgrade
        // problem, not an unreconcilable document.
        let Some((_, ydoc)) = parse_sidecar(&sidecar)? else {
            return refuse("its CRDT sidecar is unreadable");
        };
        let replica = CoeditDoc::load(ydoc)?;
        let ranges = self.blame(path).await.unwrap_or_default();
        let spans = blame_to_spans(text, ranges, (ctx.actor, ctx.session.unwrap_or(0)));
        replica.reconcile_with(text, &spans)?;
        // Merge the out-of-band branch back in. `apply_update` (not
        // `apply_update_as`) on purpose: the replica's inserts already carry the
        // authors blame recorded for them, and re-stamping would credit this
        // checkpoint's actor with someone else's out-of-band edit.
        doc.apply_update(&replica.state_update())
    }

    /// Load the co-edited document for `path` **without** marking it live: the
    /// read-only half of [`open_coedit`](Self::open_coedit), used where a doc is
    /// materialized to inspect it (a suggestion preview, an accept) rather than to
    /// start editing it.
    pub(crate) async fn load_coedit(&self, ctx: WriteCtx, path: &str) -> Result<CoeditDoc> {
        match self.read(&coedit_sidecar_path(path)).await {
            Ok(blob) => {
                if let Some((flat_hash, ydoc)) = parse_sidecar(&blob)? {
                    let current = match self.read(path).await {
                        Ok(b) => b,
                        Err(OrigoFSError::NotFound(_)) => bytes::Bytes::new(),
                        Err(e) => return Err(e),
                    };
                    if blake3::hash(&current).as_bytes().as_slice() == flat_hash {
                        return CoeditDoc::load(ydoc); // coherent: resume the CRDT
                    }
                }
                // Stale or unparseable sidecar — fall through to rebuild.
            }
            Err(OrigoFSError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        self.rebuild_coedit(ctx, path).await
    }

    /// Open a co-edited document for `path`: restore the live CRDT from its
    /// persisted sidecar — but only if the sidecar is still **coherent** with the
    /// file (the file hashes to what the sidecar crystallized). If the file moved on
    /// underneath — an accepted suggestion, a branch merge, a plain write — the
    /// sidecar is stale, so rebuild the live doc from the durable truth (the file's
    /// text + its blame) instead, losing no change and preserving authorship. With
    /// no sidecar at all, likewise promote the file (an empty document if it's
    /// absent or binary).
    ///
    /// Opening also **marks the path live** (`docs/DESIGN.md` §4e; issue #75 §3.4),
    /// so a byte reader can tell that the durable blob may lag this document. Call
    /// [`end_coedit`](Self::end_coedit) when the session finishes to clear it.
    pub async fn open_coedit(&self, ctx: WriteCtx, path: &str) -> Result<CoeditDoc> {
        // A co-editing document is a *write* channel onto `path`, so opening one
        // takes the same path-scoped check every other attributed mutation takes.
        // Without it the whole surface was an ACL bypass: an actor refused by
        // `write_or_propose` opened the same path here and its edits landed
        // through `checkpoint_coedit`'s `write_as_blamed`, which is exempt by
        // construction (it is the CRDT coordinator's own path). Checked here
        // rather than only at checkpoint so the refusal arrives when the socket
        // connects, instead of after a session's worth of typing (#123).
        self.ensure_may_write_at(ctx, "co-edit", path).await?;
        let doc = self.load_coedit(ctx, path).await?;
        self.mark_live(ctx, path).await?;
        Ok(doc)
    }

    /// Load a co-edited document to **propose** against: the same reconstruction
    /// [`open_coedit`](Self::open_coedit) does, but it neither requires write
    /// rights nor marks the path live.
    ///
    /// Proposing is what a propose-only actor is *for*, so it must not take the
    /// write check — and a throwaway replica built to compute a proposal is not a
    /// co-editing session, so claiming the path for it would tell every reader the
    /// durable bytes may lag when nothing is editing them.
    pub async fn load_coedit_as(&self, ctx: WriteCtx, path: &str) -> Result<CoeditDoc> {
        self.ensure_may_propose_at(ctx, "propose changes to", path)
            .await?;
        self.load_coedit(ctx, path).await
    }

    // --- CRDT-shaped suggestions (issue #75 §3.2) -------------------------

    /// Propose a change to a co-edited `path` as a **CRDT merge** instead of a
    /// whole file body: the recorded base is the workspace document's Yjs state
    /// vector and the proposal is `doc`'s opaque `encodeStateAsUpdate` blob.
    /// Accepting it is `applyUpdate` — see
    /// [`accept_suggestion`](Self::accept_suggestion).
    ///
    /// This is the propose-and-review path for a document people are *live editing*.
    /// A byte suggestion over such a document is wrong twice over: its base is a
    /// content hash that goes stale on every keystroke elsewhere in the file, and
    /// accepting it replaces the whole body, discarding concurrent work. A CRDT
    /// proposal has neither problem — it merges.
    ///
    /// Both blobs go into the **content** store; the review row holds only their
    /// addresses, exactly as a byte suggestion does, so the metadata database still
    /// never sees document bytes and ordinary GC still reaches them through the
    /// pending-suggestion root.
    ///
    /// `replaces` retires an earlier pending draft of this actor's as this one is
    /// created — see [`suggest`](Self::suggest). Stacked CRDT proposals are less
    /// dangerous than stacked byte ones (a CRDT proposal never goes stale, and
    /// applying an author's earlier state after their later one merges a subset),
    /// but "the proposal I meant is no longer this one" is the same relation on
    /// either shape, and a review queue with three abandoned drafts in it is still
    /// a queue nobody can read.
    pub async fn suggest_coedit(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
        summary: Option<&str>,
        replaces: Option<i64>,
    ) -> Result<i64> {
        // The base is where the *workspace's* document stood when this was
        // proposed — the reviewer's "you were looking at this much of it". It is
        // deliberately not a gate: a CRDT merge is defined against any later state.
        let base = self.load_coedit(ctx, path).await?.state_vector();
        self.suggest_coedit_update(ctx, path, &base, &doc.state_update(), summary, replaces)
            .await
    }

    /// The primitive behind [`suggest_coedit`](Self::suggest_coedit), for a client
    /// that already holds the two Yjs blobs — a browser editor proposes with
    /// `encodeStateVector(doc)` as `base_sv` and `encodeStateAsUpdate(doc)` (or a
    /// diff against the server's vector) as `update`.
    pub async fn suggest_coedit_update(
        &self,
        ctx: WriteCtx,
        path: &str,
        base_sv: &[u8],
        update: &[u8],
        summary: Option<&str>,
        replaces: Option<i64>,
    ) -> Result<i64> {
        if update.is_empty() {
            return Err(OrigoFSError::InvalidArgument(
                "co-edit suggestion: empty update proposes nothing".into(),
            ));
        }
        // Reject a malformed blob at propose time rather than at review time — a
        // suggestion nobody can apply is worse than a refused proposal.
        Update::decode_v1(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;
        let base_hash = Some(self.put_opaque(base_sv).await?);
        let proposed_hash = Some(self.put_opaque(update).await?);
        self.record_suggestion(
            ctx,
            path,
            base_hash,
            proposed_hash,
            summary,
            crate::suggest::SuggestionKind::Crdt,
            replaces,
        )
        .await
    }

    /// Store an opaque blob in the CAS and return its manifest hash in hex. An
    /// empty blob still gets an explicit empty manifest, so `Some(hash)` never
    /// collapses into the `None` that means "propose a deletion".
    pub(crate) async fn put_opaque(&self, blob: &[u8]) -> Result<String> {
        Ok(match self.store_body(blob).await?.0 {
            Some(h) => h.to_hex(),
            // `store_empty_manifest` puts *and* flushes, so this path keeps the
            // same durability barrier `store_body` gives the other one.
            None => self.store_empty_manifest().await?.to_hex(),
        })
    }

    /// Read a suggestion's proposed Yjs update back out of the CAS.
    pub(crate) async fn coedit_suggestion_update(
        &self,
        s: &crate::suggest::Suggestion,
    ) -> Result<bytes::Bytes> {
        let hex = s.proposed_hash.as_deref().ok_or_else(|| {
            OrigoFSError::InvalidArgument(format!(
                "suggestion #{}: a CRDT suggestion cannot propose a deletion",
                s.id
            ))
        })?;
        let hash = crate::types::Hash::from_hex(hex)
            .ok_or_else(|| OrigoFSError::Metadata("bad proposed hash".into()))?;
        self.content_bytes(&hash).await
    }

    /// Apply an accepted CRDT suggestion: merge its update into the live document
    /// and checkpoint. Never a whole-file `write_as`, so a concurrent disjoint edit
    /// survives the accept instead of being clobbered.
    ///
    /// Attribution is unchanged from the byte path and just as strict:
    /// [`apply_update_as`](CoeditDoc::apply_update_as) stamps the text this update
    /// actually *introduces* with the **original author** — server-side, overriding
    /// any authorship the blob claims — and text already in the document keeps the
    /// author it already had. The checkpoint then lands those exact spans in blame.
    /// The approver is recorded by the caller on the review row and the feed.
    pub(crate) async fn apply_coedit_suggestion(
        &self,
        s: &crate::suggest::Suggestion,
        author: WriteCtx,
    ) -> Result<()> {
        let update = self.coedit_suggestion_update(s).await?;
        let doc = self.load_coedit(author, &s.path).await?;
        doc.apply_update_as(author, &update)?;
        // Unchecked *as the author*: `accept_suggestion` already required the
        // approver to hold `WRITE` at this path, and the author is by definition
        // someone who could only propose.
        self.checkpoint_coedit_unchecked(author, &s.path, &doc)
            .await
    }

    /// The `(before, after)` text of applying a CRDT suggestion, for review. The
    /// merge is computed on a throwaway copy of the current document and never
    /// persisted, so previewing has no effect on the workspace — and because it
    /// re-merges against the document *as it is now*, the preview stays truthful as
    /// the document moves on.
    pub(crate) async fn preview_coedit_suggestion(
        &self,
        s: &crate::suggest::Suggestion,
    ) -> Result<(String, String)> {
        let update = self.coedit_suggestion_update(s).await?;
        let author = WriteCtx {
            actor: s.actor_id,
            session: s.session_id,
            tool_call: None,
        };
        let doc = self.load_coedit(author, &s.path).await?;
        let before = doc.text();
        doc.apply_update(&update)?;
        Ok((before, doc.text()))
    }

    /// Build a live doc from the durable truth — the file's current text plus its
    /// blame — attributing any un-blamed text to `ctx`. The fallback for both a
    /// never-co-edited file and a sidecar gone stale.
    async fn rebuild_coedit(&self, ctx: WriteCtx, path: &str) -> Result<CoeditDoc> {
        let bytes = match self.read(path).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => return Ok(CoeditDoc::new()),
            Err(e) => return Err(e),
        };
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return Ok(CoeditDoc::new()); // binary: nothing to co-edit as text
        };
        if text.is_empty() {
            return Ok(CoeditDoc::new());
        }
        let ranges = self.blame(path).await.unwrap_or_default();
        let spans = blame_to_spans(text, ranges, (ctx.actor, ctx.session.unwrap_or(0)));
        CoeditDoc::from_blamed(text, &spans)
    }
}
