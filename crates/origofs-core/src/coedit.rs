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

use crate::attribution::WriteCtx;
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
                // apply + attribute, then fan the attributed delta out to peers.
                Message::Sync(SyncMessage::SyncStep2(update))
                | Message::Sync(SyncMessage::Update(update)) => {
                    let delta = self.apply_update_as(ctx, &update)?;
                    if !delta.is_empty() {
                        Message::Sync(SyncMessage::Update(delta)).encode(&mut broadcast);
                        has_broadcast = true;
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
const COEDIT_SIDECAR_DIR: &str = "/.origofs/ydoc";

/// The sidecar path for a co-edited `path`, hex-encoded so it needs no nested
/// directories and can't collide with another document's sidecar.
fn coedit_sidecar_path(path: &str) -> String {
    format!("{COEDIT_SIDECAR_DIR}/{}", hex::encode(path.as_bytes()))
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
        // content store, pinned to the commit like any tree file.
        self.mkdir_p(COEDIT_SIDECAR_DIR).await?;
        self.write(&coedit_sidecar_path(path), &doc.state_update())
            .await?;
        Ok(())
    }

    /// Open a co-edited document for `path`: restore the live CRDT from its
    /// persisted sidecar if one exists — resuming exactly where co-editing left
    /// off — otherwise promote the file's current text into a fresh document
    /// attributed to `ctx` (an empty document if the file is absent or binary).
    pub async fn open_coedit(&self, ctx: WriteCtx, path: &str) -> Result<CoeditDoc> {
        match self.read(&coedit_sidecar_path(path)).await {
            Ok(bytes) => return CoeditDoc::load(&bytes),
            Err(OrigoFSError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let doc = CoeditDoc::new();
        if let Ok(bytes) = self.read(path).await
            && let Ok(text) = std::str::from_utf8(&bytes)
            && !text.is_empty()
        {
            doc.insert(ctx, 0, text);
        }
        Ok(doc)
    }
}
