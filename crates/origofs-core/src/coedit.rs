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
use yrs::types::Attrs;
use yrs::types::text::YChange;
use yrs::updates::decoder::Decode;
use yrs::{Any, Doc, GetString, Out, ReadTxn, StateVector, Text, TextRef, Transact, Update};

/// The formatting-attribute key under which each run's `"actor,session"` is kept.
const AUTHOR_KEY: &str = "a";

/// A live co-edited document: a `yrs` text CRDT whose inserts are attributed.
///
/// Collaborators edit via [`insert`](Self::insert) / [`remove`](Self::remove)
/// (each insert carries its actor) and exchange changes as opaque update blobs
/// ([`state_update`](Self::state_update) / [`apply_update`](Self::apply_update)),
/// which merge commutatively so peers converge regardless of order.
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
