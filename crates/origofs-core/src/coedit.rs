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
use crate::metadata::MetadataStore;
use similar::{ChangeTag, TextDiff};
use yrs::encoding::read::Cursor;
use yrs::sync::{Message, MessageReader, SyncMessage};
use yrs::types::Attrs;
use yrs::types::text::YChange;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Any, Doc, GetString, Out, ReadTxn, StateVector, Text, TextRef, Transact, Update};

/// The formatting-attribute key under which each run's `"actor,session"` is kept.
const AUTHOR_KEY: &str = "a";

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
/// Character indices are `yrs` code-unit offsets (UTF-16), as in Yjs; the
/// authorship spans returned by [`snapshot`](Self::snapshot) are in *bytes*, which
/// is what the blame index stores.
pub struct CoeditDoc {
    doc: Doc,
    text: TextRef,
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
    pub fn new() -> Self {
        let doc = Doc::new();
        let text = doc.get_or_insert_text("content");
        Self { doc, text }
    }

    fn author_attrs(ctx: WriteCtx) -> Attrs {
        Attrs::from([(
            AUTHOR_KEY.into(),
            Any::from(format!("{},{}", ctx.actor, ctx.session.unwrap_or(0))),
        )])
    }

    /// Insert `chunk` at character `index`, attributed to `ctx`.
    pub fn insert(&self, ctx: WriteCtx, index: u32, chunk: &str) {
        let mut txn = self.doc.transact_mut();
        self.text
            .insert_with_attributes(&mut txn, index, chunk, Self::author_attrs(ctx));
    }

    /// Remove `len` characters starting at `index`.
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
    /// our authorship attribute, so we recover it ourselves: capture the text,
    /// apply the update, diff before/after to find the inserted ranges, and stamp
    /// those ranges with the connection's actor via CRDT formatting. The stamp is
    /// itself a CRDT change, so it persists in the sidecar and rides the returned
    /// delta out to every peer and worker.
    ///
    /// Returns the update to relay to peers — the client's content *plus* our
    /// attribution — or an empty vector if the update changed nothing (already
    /// seen). This, not the raw inbound bytes, is what a room must broadcast, so
    /// authorship always travels with the content.
    pub fn apply_update_as(&self, ctx: WriteCtx, update: &[u8]) -> Result<Vec<u8>> {
        let update = Update::decode_v1(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;

        // Pin the pre-image: the text (to diff against) and the state vector (to
        // encode exactly this update's effect — content + our stamp — for relay).
        let before = self.text();
        let sv_before = self.doc.transact().state_vector();

        self.doc
            .transact_mut()
            .apply_update(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("apply co-edit update: {e}")))?;

        // Stamp each newly-inserted range with the real author. Formatting after
        // the apply means our attribute overwrites any the client tried to forge.
        let after = self.text();
        let ranges = inserted_ranges(&before, &after);
        if !ranges.is_empty() {
            let attrs = Self::author_attrs(ctx);
            let mut txn = self.doc.transact_mut();
            for (index, len) in ranges {
                self.text.format(&mut txn, index, len, attrs.clone());
            }
        }

        if self.doc.transact().state_vector() == sv_before {
            return Ok(Vec::new()); // nothing new — don't relay a no-op
        }
        Ok(self.doc.transact().encode_state_as_update_v1(&sv_before))
    }

    /// Merge a y-sync frame relayed from another worker — content another replica
    /// already attributed — *without* re-attribution. This is the cross-worker
    /// relay's apply path; client input must instead go through
    /// [`handle_sync`](Self::handle_sync), which attributes. Idempotent: a frame
    /// already merged (or folded into a checkpoint) is a no-op.
    pub fn apply_relayed(&self, frame: &[u8]) -> Result<()> {
        let mut decoder = DecoderV1::new(Cursor::new(frame));
        let reader = MessageReader::new(&mut decoder);
        for msg in reader {
            let msg =
                msg.map_err(|e| OrigoFSError::InvalidArgument(format!("bad relayed frame: {e}")))?;
            // Only content messages carry state; awareness/etc. are ignored on the
            // relay (presence is gossiped between the clients on each worker).
            if let Message::Sync(SyncMessage::Update(u) | SyncMessage::SyncStep2(u)) = msg {
                self.apply_update(&u)?;
            }
        }
        Ok(())
    }

    /// The y-sync frame to greet a freshly-connected client with: a `SyncStep1`
    /// carrying our state vector, so the client sends back (as `SyncStep2`)
    /// whatever we're missing. Pair with [`handle_sync`](Self::handle_sync), which
    /// also answers the client's own `SyncStep1`.
    pub fn sync_start(&self) -> Vec<u8> {
        let sv = self.doc.transact().state_vector();
        let mut encoder = EncoderV1::new();
        Message::Sync(SyncMessage::SyncStep1(sv)).encode(&mut encoder);
        encoder.to_vec()
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
    pub fn handle_sync(&self, ctx: WriteCtx, data: &[u8]) -> Result<SyncReply> {
        let mut decoder = DecoderV1::new(Cursor::new(data));
        let reader = MessageReader::new(&mut decoder);
        let mut reply = EncoderV1::new();
        let mut broadcast = EncoderV1::new();
        let (mut has_reply, mut has_broadcast) = (false, false);

        for msg in reader {
            let msg =
                msg.map_err(|e| OrigoFSError::InvalidArgument(format!("bad y-sync frame: {e}")))?;
            match msg {
                // The client wants what we have: answer with the update it lacks.
                Message::Sync(SyncMessage::SyncStep1(sv)) => {
                    let update = self.doc.transact().encode_state_as_update_v1(&sv);
                    Message::Sync(SyncMessage::SyncStep2(update)).encode(&mut reply);
                    has_reply = true;
                }
                // The client is handing us content (initial sync or a live edit):
                // apply + attribute, then fan the attributed delta out to peers —
                // and back to the sender.
                Message::Sync(SyncMessage::SyncStep2(update))
                | Message::Sync(SyncMessage::Update(update)) => {
                    let delta = self.apply_update_as(ctx, &update)?;
                    if !delta.is_empty() {
                        Message::Sync(SyncMessage::Update(delta.clone())).encode(&mut broadcast);
                        has_broadcast = true;
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
                // tags are not part of our protocol. Ignore both.
                Message::Auth(_) | Message::Custom(_, _) => {}
            }
        }

        Ok(SyncReply {
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
        })
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
        let mut byte_off = 0usize;
        let mut u16_idx = 0u32;
        for &(actor, session, byte_len) in spans {
            let end = byte_off + byte_len as usize;
            let piece = text.get(byte_off..end).ok_or_else(|| {
                OrigoFSError::InvalidArgument("co-edit rebuild: span not on a char boundary".into())
            })?;
            if actor != 0 {
                let attrs = Self::author_attrs(WriteCtx::session(actor, session));
                this.text
                    .insert_with_attributes(&mut txn, u16_idx, piece, attrs);
            } else {
                this.text.insert(&mut txn, u16_idx, piece);
            }
            byte_off = end;
            u16_idx += utf16_len(piece);
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
        let mut idx: u32 = 0; // UTF-16 offset into the document, as we mutate it
        let mut authors = SpanCursor::new(spans);
        // The open insert run: its author and the text accumulated so far. Runs are
        // batched so a word typed by one author is one CRDT insert, not N.
        let mut pending: Option<((i64, i64), String)> = None;

        // Flush the open insert run at the current index, advancing past it.
        macro_rules! flush {
            () => {
                if let Some((author, piece)) = pending.take() {
                    if author.0 != 0 {
                        let attrs = Self::author_attrs(WriteCtx::session(author.0, author.1));
                        self.text
                            .insert_with_attributes(&mut txn, idx, &piece, attrs);
                    } else {
                        self.text.insert(&mut txn, idx, &piece);
                    }
                    idx += utf16_len(&piece);
                }
            };
        }

        for change in diff.iter_all_changes() {
            let value = change.value();
            match change.tag() {
                ChangeTag::Equal => {
                    flush!();
                    idx += utf16_len(value);
                    authors.advance(value.len());
                }
                // Present in the document, absent from `text`: delete it. The
                // document shrinks under `idx`, so `idx` does not move.
                ChangeTag::Delete => {
                    flush!();
                    self.text.remove_range(&mut txn, idx, utf16_len(value));
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
        let chunks = self.text.diff(&txn, YChange::identity);
        let mut text = String::new();
        let mut spans = Vec::new();
        for chunk in chunks {
            // Co-edit docs are plain text; skip any embedded (non-text) value.
            let Out::Any(Any::String(piece)) = &chunk.insert else {
                continue;
            };
            let (actor, session) = author_of(chunk.attributes.as_deref());
            text.push_str(piece);
            spans.push((actor, session, piece.len() as u64));
        }
        (text, spans)
    }
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

/// Parse an `(actor, session)` pair from a run's author attribute, or `(0, 0)`.
fn author_of(attrs: Option<&Attrs>) -> (i64, i64) {
    let Some(Any::String(s)) = attrs.and_then(|a| a.get(AUTHOR_KEY)) else {
        return (0, 0);
    };
    let mut it = s.split(',');
    let actor = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let session = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    (actor, session)
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
}

/// UTF-16 code-unit length of `s` — the unit `yrs`/Yjs index text in.
fn utf16_len(s: &str) -> u32 {
    s.chars().map(|c| c.len_utf16() as u32).sum()
}

/// The `(index, len)` ranges — in UTF-16 code units, so they feed `yrs`
/// formatting directly — that appear in `after` but not `before`: the text this
/// update inserted. A character-level diff so multi-cursor and batched edits each
/// attribute to their exact range rather than one coarse span.
fn inserted_ranges(before: &str, after: &str) -> Vec<(u32, u32)> {
    let diff = TextDiff::from_chars(before, after);
    let mut ranges = Vec::new();
    let mut idx: u32 = 0; // UTF-16 offset into `after`
    let mut run: Option<(u32, u32)> = None; // (start, len) of the open insert run
    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Equal => {
                if let Some(r) = run.take() {
                    ranges.push(r);
                }
                idx += utf16_len(change.value());
            }
            ChangeTag::Insert => {
                let len = utf16_len(change.value());
                match &mut run {
                    Some((_, l)) => *l += len,
                    None => run = Some((idx, len)),
                }
                idx += len;
            }
            // Deleted characters are absent from `after`, so they neither advance
            // the offset nor extend an insert run.
            ChangeTag::Delete => {
                if let Some(r) = run.take() {
                    ranges.push(r);
                }
            }
        }
    }
    if let Some(r) = run.take() {
        ranges.push(r);
    }
    ranges
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

/// Framing tag for the sidecar blob: `[SIDECAR_MAGIC][32-byte BLAKE3 of the flat
/// text it crystallized][ydoc state update]`. The embedded hash is the coherence
/// marker — [`open_coedit`](Fs::open_coedit) resumes the CRDT only if the file
/// still hashes to it, else rebuilds from the file.
const SIDECAR_MAGIC: u8 = 1;

/// Split a framed sidecar blob into `(flat_hash, ydoc_state)`, or `None` if it
/// isn't in the current format (a truncated or corrupt blob — the caller then
/// rebuilds from the flat file, which is always safe). Unlike the content-store
/// objects in [`crate::format`], the sidecar is a resumable *cache*, so an
/// unreadable one costs a rebuild rather than data: falling back is correct here.
fn parse_sidecar(blob: &[u8]) -> Option<(&[u8], &[u8])> {
    if blob.len() >= 33 && blob[0] == SIDECAR_MAGIC {
        Some((&blob[1..33], &blob[33..]))
    } else {
        None
    }
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
    pub async fn checkpoint_coedit(
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
        let state = doc.state_update();
        let mut blob = Vec::with_capacity(1 + 32 + state.len());
        blob.push(SIDECAR_MAGIC);
        blob.extend_from_slice(blake3::hash(text.as_bytes()).as_bytes());
        blob.extend_from_slice(&state);
        self.write(&coedit_sidecar_path(path), &blob).await?;
        // Refresh the live marker's coherence hash to what we just wrote, so the
        // *next* checkpoint can again tell an out-of-band write from our own. Only
        // a *refresh*: checkpointing does not by itself make a path live — that is
        // `open_coedit`'s job — so a one-off checkpoint by a Rust caller holding a
        // `CoeditDoc` leaves no marker behind for a reader to trip over.
        if self.meta.get_live_doc(path).await?.is_some() {
            self.mark_live(ctx, path).await?;
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
        let bytes = match self.read(path).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => return Ok(()), // removed: nothing to fold in
            Err(e) => return Err(e),
        };
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return Ok(()); // binary now: not reconcilable as text
        };
        // The replica: this document as it stood at the last checkpoint.
        let sidecar = match self.read(&coedit_sidecar_path(path)).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let Some((_, ydoc)) = parse_sidecar(&sidecar) else {
            return Ok(());
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
                if let Some((flat_hash, ydoc)) = parse_sidecar(&blob) {
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
        let doc = self.load_coedit(ctx, path).await?;
        self.mark_live(ctx, path).await?;
        Ok(doc)
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
    pub async fn suggest_coedit(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditDoc,
        summary: Option<&str>,
    ) -> Result<i64> {
        // The base is where the *workspace's* document stood when this was
        // proposed — the reviewer's "you were looking at this much of it". It is
        // deliberately not a gate: a CRDT merge is defined against any later state.
        let base = self.load_coedit(ctx, path).await?.state_vector();
        self.suggest_coedit_update(ctx, path, &base, &doc.state_update(), summary)
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
        )
        .await
    }

    /// Store an opaque blob in the CAS and return its manifest hash in hex. An
    /// empty blob still gets an explicit empty manifest, so `Some(hash)` never
    /// collapses into the `None` that means "propose a deletion".
    async fn put_opaque(&self, blob: &[u8]) -> Result<String> {
        Ok(match self.store_body(blob).await?.0 {
            Some(h) => h.to_hex(),
            None => self
                .content
                .put(&crate::chunk::Manifest::default().encode())
                .await?
                .to_hex(),
        })
    }

    /// Read a suggestion's proposed Yjs update back out of the CAS.
    async fn coedit_suggestion_update(
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
        self.checkpoint_coedit(author, &s.path, &doc).await
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
