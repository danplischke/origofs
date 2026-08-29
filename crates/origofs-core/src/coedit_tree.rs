//! Structured co-editing: a **tree-shaped** co-edited document (issue #92).
//!
//! [`crate::coedit`] models a document as one flat `Y.Text`. That is the right
//! shape for source files and for anything a diff tool reads, but every mainstream
//! rich-text CRDT binding — `@platejs/yjs`, `y-prosemirror`, `y-slate`, TipTap —
//! binds to a `Y.XmlFragment` tree instead, so none of them can attach to a flat
//! room. A host wanting to use one has to *mirror*: serialize its editor to text on
//! every change and diff it against the shared `Y.Text`. That converges, but the
//! caret is lost on every remote edit, serializer round-trip noise shows up as
//! authored bytes, and — worst — attribution is only ever as sharp as the host's
//! whole-file text diff, which collapses two concurrent edits in different
//! paragraphs into one replaced span.
//!
//! [`CoeditTreeDoc`] is the structured shape: an `XmlFragment` root a rich-text
//! editor binds to natively, attributed by the same server-side rule as the flat
//! path — the text an update *introduces* is stamped with the connection's actor,
//! never with an author the client names.
//!
//! # origofs does not own the schema
//!
//! A tree has no canonical byte serialization, and picking one (Markdown? HTML?)
//! would make this engine responsible for a document model and a dialect. It
//! doesn't take that on. Instead the host — which already owns the schema, because
//! its editor defines it — hands origofs the serialized bytes **plus a span map**
//! at checkpoint time:
//!
//! ```text
//! body:  b"# Title\n\nhello world\n"
//! spans: [(2, 7, "3f2a.1"), (9, 20, "3f2a.4")]      # (byte_start, byte_end, node)
//! ```
//!
//! Each `node` is an id **origofs itself assigned** when it stamped that run or
//! element ([`NODE_KEY`], visible to the client as an ordinary Yjs formatting
//! attribute / XML attribute, so a host reads it straight off `ytext.toDelta()` or
//! `element.getAttribute`). origofs resolves `node → (actor, session)` from its own
//! stamps and lands the result through
//! [`write_as_blamed`](crate::engine::Fs::write_as_blamed) — the same byte-range
//! blame index every other write feeds. The host never names an author.
//!
//! **What this trades away, stated plainly:** a host that supplies a *wrong* span
//! map gets wrong blame. origofs validates that spans are ordered, non-overlapping,
//! in range, and on character boundaries, and it validates that a node id is one it
//! issued — but it cannot validate that the host mapped the right bytes to the right
//! node, because it cannot read the host's serializer. That is the price of not
//! owning the schema, and it is the whole trade.
//!
//! Say the residue precisely, because it is a different trust relationship from
//! the one [`apply_update_as`](CoeditTreeDoc::apply_update_as) closes: a
//! *client* cannot name an author, but a **malicious host** can cite a
//! legitimately-issued node id for the wrong byte range and origofs will believe
//! it. Closing that would mean parsing the host's serialization, which is exactly
//! what this module declines to do. The host is trusted; the clients editing
//! through it are not.
//!
//! Bytes no span covers (a Markdown `- ` bullet marker, a fence, the blank line
//! between paragraphs — punctuation the *serializer* emitted rather than a person)
//! are attributed to the checkpointing actor, the same fallback
//! [`checkpoint_coedit`](crate::engine::Fs::checkpoint_coedit) uses for an
//! unattributed run. A host that would rather credit those bytes to a node can
//! simply widen the span to cover them.
//!
//! # Durability is split between the two sides
//!
//! Because only the host can serialize, only the host can crystallize the *file*.
//! But the CRDT itself is fully known server-side, so
//! [`persist_coedit_tree`](crate::engine::Fs::persist_coedit_tree) writes the ydoc
//! sidecar alone, with no body — a crashed worker then loses no editing history even
//! if the host has not checkpointed in a while. The two calls answer different
//! questions: *persist* keeps the session recoverable, *checkpoint* updates the file
//! and its blame.
//!
//! Enabled by the `coedit` feature.

use crate::attribution::WriteCtx;
use crate::coedit::{
    AUTHOR_KEY, COEDIT_SIDECAR_DIR, SyncReply, attr_or_null, author_attrs, author_value,
    diverging_runs, doc_range, drive_sync, intended_stamps, parse_author, raw_attr, raw_author,
    scan_runs, stamp_tiling,
};
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use yrs::types::Attrs;
use yrs::types::text::YChange;
use yrs::types::xml::{Xml, XmlElementRef, XmlFragment, XmlFragmentRef, XmlOut, XmlTextRef};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{
    Any, BranchID, Doc, OffsetKind, Options, Out, ReadTxn, StateVector, Text, Transact, Update,
};

/// The attribute key under which origofs keeps each stamped run's / element's
/// **node id** — the token a host puts in its span map at checkpoint time.
///
/// On a text run it is a Yjs *formatting* attribute (so it survives splits and is
/// visible in `ytext.toDelta()`); on an element it is an XML attribute
/// (`element.getAttribute("n")`). Server-assigned, always — and now *actually*
/// always: every apply re-asserts it, not merely the one that created the node,
/// so a client can neither label its own content nor re-point an existing run at
/// another id. An id origofs never issued therefore appears nowhere in
/// [`authors`](CoeditTreeDoc::authors) and resolves to no author at all.
pub const NODE_KEY: &str = "n";

/// The default `XmlFragment` root name, matching the flat path's `"content"`.
/// Editors differ (`y-prosemirror` defaults to `"prosemirror"`), so every entry
/// point takes the name explicitly and this is only the value the surfaces default
/// to.
pub const DEFAULT_TREE_ROOT: &str = "content";

/// One `(byte_start, byte_end, node)` entry of a host's span map: the half-open
/// byte range of the serialized body that came from the co-edit node `node`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeSpan {
    /// First byte of the range, inclusive.
    pub start: u64,
    /// One past the last byte of the range.
    pub end: u64,
    /// The [`NODE_KEY`] id origofs stamped on the run or element these bytes were
    /// serialized from.
    pub node: String,
}

impl TreeSpan {
    /// A span covering `start..end` from node `node`.
    pub fn new(start: u64, end: u64, node: impl Into<String>) -> Self {
        Self {
            start,
            end,
            node: node.into(),
        }
    }
}

/// One attributed text run of a tree document (see [`CoeditTreeDoc::runs`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeRun {
    /// The run's raw text.
    pub text: String,
    /// The [`NODE_KEY`] id origofs stamped on it, or `None` for a run it never
    /// stamped (content a host seeded directly). An unstamped run has no author to
    /// resolve, so citing it in a span map falls back to the checkpointer.
    pub node: Option<String>,
    /// The actor that wrote it, or `0` if unstamped.
    pub actor: i64,
    /// That actor's session, or `0`.
    pub session: i64,
}

/// A live, tree-shaped co-edited document: a `yrs` `XmlFragment` whose inserted
/// text runs and elements are attributed server-side.
///
/// Drive it with the same y-sync protocol as [`CoeditDoc`](crate::coedit::CoeditDoc)
/// — [`sync_start`](Self::sync_start) then [`handle_sync`](Self::handle_sync) — so an
/// unmodified Yjs editor binds to it directly. Land it with
/// [`checkpoint_coedit_tree`](Fs::checkpoint_coedit_tree), which needs the host's
/// serialized bytes because origofs does not own the schema (see the module docs).
pub struct CoeditTreeDoc {
    doc: Doc,
    root: Arc<str>,
    frag: XmlFragmentRef,
    /// Monotonic suffix for this replica's node ids. Paired with the `yrs` client
    /// id — which is fresh-random per `Doc`, per process — so ids never collide
    /// across workers or across a reload, on the same assumption Yjs itself makes
    /// for block ids.
    next_node: AtomicU64,
    resumed: bool,
}

// A live-editing room shares one document across every connected socket's task
// (behind a lock), so this must hold — pinned here for the same reason
// [`CoeditDoc`](crate::coedit::CoeditDoc) pins it.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CoeditTreeDoc>();
};

impl CoeditTreeDoc {
    /// A fresh, empty document rooted at the `XmlFragment` named `root`.
    ///
    /// Byte offsets, explicitly, for the same reason as [`CoeditDoc::new`]: every
    /// index this module computes is a byte offset, and the two shapes must agree
    /// because they share `intended_stamps`/`diverging_runs`.
    pub fn new(root: &str) -> Self {
        let doc = Doc::with_options(Options {
            offset_kind: OffsetKind::Bytes,
            ..Default::default()
        });
        let frag = doc.get_or_insert_xml_fragment(root);
        Self {
            doc,
            root: Arc::from(root),
            frag,
            next_node: AtomicU64::new(0),
            resumed: false,
        }
    }

    /// Reconstruct a document from a serialized state produced by
    /// [`state_update`](Self::state_update).
    pub fn load(root: &str, update: &[u8]) -> Result<Self> {
        let mut this = Self::new(root);
        this.apply_update(update)?;
        this.resumed = true;
        Ok(this)
    }

    /// The `XmlFragment` root name this document is bound to.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Whether this document was resumed from a coherent sidecar rather than
    /// created empty.
    ///
    /// **A host must check this.** origofs cannot rebuild a *tree* from a flat file
    /// the way [`open_coedit`](Fs::open_coedit) can — parsing bytes back into nodes
    /// needs the schema, which is the host's. So a document whose sidecar is missing
    /// or stale (an accepted suggestion, a branch merge, a plain write moved the
    /// file underneath) opens **empty**, and a host that binds an editor to it
    /// without seeding it from [`read`](Fs::read) will checkpoint an empty body over
    /// a file with content.
    pub fn resumed(&self) -> bool {
        self.resumed
    }

    /// Whether the tree has no nodes at all.
    pub fn is_empty(&self) -> bool {
        self.frag.len(&self.doc.transact()) == 0
    }

    /// The document's `XmlFragment` root, for a Rust caller building or inspecting
    /// the tree directly. Edits made through it are *not* attributed — that happens
    /// in [`apply_update_as`](Self::apply_update_as), on the update path a real
    /// client uses.
    pub fn fragment(&self) -> &XmlFragmentRef {
        &self.frag
    }

    /// Append `<tag>text</tag>` to the root, attributed to `ctx`, and return the
    /// node id stamped on the text run — ready to cite in a span map.
    ///
    /// The tree analogue of [`CoeditDoc::insert`](crate::coedit::CoeditDoc::insert),
    /// and deliberately just as narrow: it is the in-process path for an agent
    /// seeding or appending to a document, and for a test client. A real editor
    /// does not use it — it owns the schema and drives arbitrary tree edits over
    /// y-sync, where [`apply_update_as`](Self::apply_update_as) attributes them.
    pub fn append_text(&self, ctx: WriteCtx, tag: &str, text: &str) -> String {
        let node = self.fresh_node_id();
        let mut txn = self.doc.transact_mut();
        let el = self
            .frag
            .push_back(&mut txn, yrs::types::xml::XmlElementPrelim::empty(tag));
        let run = el.push_back(&mut txn, yrs::types::xml::XmlTextPrelim::new(text));
        let mut attrs = author_attrs(ctx);
        attrs.insert(NODE_KEY.into(), Any::from(node.clone()));
        run.format(&mut txn, 0, crate::coedit::doc_len(text), attrs);
        el.insert_attribute(&mut txn, AUTHOR_KEY, author_value(ctx));
        el.insert_attribute(&mut txn, NODE_KEY, self.fresh_node_id());
        node
    }

    /// The underlying `yrs` document, for a Rust caller that needs a transaction.
    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    /// An opaque update carrying this document's whole state, for a peer to
    /// [`apply_update`](Self::apply_update).
    pub fn state_update(&self) -> Vec<u8> {
        self.doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    /// This document's encoded **state vector** — the compact "what I already have"
    /// summary Yjs peers exchange.
    pub fn state_vector(&self) -> Vec<u8> {
        self.doc.transact().state_vector().encode_v1()
    }

    /// Merge a peer's update into this document (idempotent and commutative),
    /// **without** attribution. Client input must go through
    /// [`handle_sync`](Self::handle_sync) instead.
    pub fn apply_update(&self, update: &[u8]) -> Result<()> {
        let update = Update::decode_v1(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;
        self.doc
            .transact_mut()
            .apply_update(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("apply co-edit update: {e}")))?;
        Ok(())
    }

    /// Apply a raw `yrs` update from an *unmodified* Yjs client and attribute
    /// exactly the content it introduced to `ctx`'s actor — server-side, never
    /// trusting an author the client may name.
    ///
    /// The detector is deliberately the same one the flat path uses: capture each
    /// text node's raw string before the update, apply, then diff before/after per
    /// node and stamp the inserted ranges. It has to be a *content* diff rather
    /// than "which runs lack an author attribute", because Yjs makes an insert
    /// inherit the formatting attributes at its position — text typed by B against
    /// the end of A's run arrives already wearing A's stamp, and only a content
    /// diff sees through that. Elements are simpler: an element that did not exist
    /// before the update is new, so it is stamped whole.
    ///
    /// Each stamped range also gets a fresh [`NODE_KEY`] id, which is what a host
    /// later cites in its span map.
    ///
    /// Returns the update to relay to peers — the client's content *plus* our
    /// attribution — or an empty vector if the update changed nothing.
    pub fn apply_update_as(&self, ctx: WriteCtx, update: &[u8]) -> Result<Vec<u8>> {
        let update = Update::decode_v1(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;

        let before = self.scan();
        let sv_before = self.doc.transact().state_vector();

        self.doc
            .transact_mut()
            .apply_update(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("apply co-edit update: {e}")))?;

        self.reconcile(ctx, &before);

        if self.doc.transact().state_vector() == sv_before {
            return Ok(Vec::new()); // nothing new — don't relay a no-op
        }
        Ok(self.doc.transact().encode_state_as_update_v1(&sv_before))
    }

    /// The y-sync frame to greet a freshly-connected client with.
    pub fn sync_start(&self) -> Vec<u8> {
        crate::coedit::sync_start(&self.doc)
    }

    /// The y-sync frame carrying this document's whole state, to catch up a client
    /// that missed frames. See [`crate::coedit::state_frame`].
    pub fn state_frame(&self) -> Vec<u8> {
        crate::coedit::state_frame(&self.doc)
    }

    /// Drive one inbound y-sync payload from a connection authenticated as `ctx`.
    /// Content the client contributes is attributed to `ctx` by
    /// [`apply_update_as`](Self::apply_update_as).
    pub fn handle_sync(&self, ctx: WriteCtx, data: &[u8]) -> Result<SyncReply> {
        drive_sync(&self.doc, data, |update| self.apply_update_as(ctx, update))
    }

    /// Merge a y-sync frame relayed from another worker — content another replica
    /// already attributed — *without* re-attribution.
    pub fn apply_relayed(&self, frame: &[u8]) -> Result<()> {
        crate::coedit::apply_relayed(&self.doc, frame)
    }

    /// Every node id this document has stamped, mapped to the `(actor, session)`
    /// that authored it. This is what a host's span map is resolved against; an id
    /// origofs never issued is simply absent.
    ///
    /// An id carried by two runs with **conflicting** authors is dropped rather
    /// than resolved to whichever came last in document order. Enforcement stops
    /// the server ever minting such a duplicate, but a replica merge or a sidecar
    /// written by an older build still can, and silently crediting one of the two
    /// claimants is the one outcome worse than crediting nobody: `tile_spans`
    /// falls back to the checkpointer for an unresolved id, which is honest.
    /// The same author on two ranges is legitimate — a repair splits a run
    /// without changing who wrote it — and must not poison.
    pub fn authors(&self) -> HashMap<String, (i64, i64)> {
        let txn = self.doc.transact();
        let mut out: HashMap<String, (i64, i64)> = HashMap::new();
        let mut poisoned: HashSet<String> = HashSet::new();
        let mut claim = |id: String, author: (i64, i64)| match out.get(&id) {
            Some(prev) if *prev != author => {
                poisoned.insert(id);
            }
            _ => {
                out.insert(id, author);
            }
        };
        for node in self.frag.successors(&txn) {
            match node {
                XmlOut::Text(text) => {
                    for chunk in text.diff(&txn, YChange::identity) {
                        let Some(attrs) = chunk.attributes.as_deref() else {
                            continue;
                        };
                        let Some(Any::String(id)) = attrs.get(NODE_KEY) else {
                            continue;
                        };
                        let Some(Any::String(author)) = attrs.get(AUTHOR_KEY) else {
                            continue;
                        };
                        claim(id.to_string(), parse_author(author));
                    }
                }
                XmlOut::Element(el) => {
                    if let Some(id) = el.get_attribute(&txn, NODE_KEY)
                        && let Some(author) = el.get_attribute(&txn, AUTHOR_KEY)
                    {
                        claim(id, parse_author(&author));
                    }
                }
                XmlOut::Fragment(_) => {} // nested fragments carry no attributes
            }
        }
        for id in poisoned {
            out.remove(&id);
        }
        out
    }

    /// Every text run in document order, with the node id and author stamped on it
    /// — the server-side reading of what a browser host gets from
    /// `ytext.toDelta()`.
    ///
    /// This is what a caller serializing the document *itself* (a Rust or Python
    /// agent rather than a browser editor) walks to build its span map: emit bytes
    /// for each run, record the range it occupied, cite [`TreeRun::node`].
    pub fn runs(&self) -> Vec<TreeRun> {
        let txn = self.doc.transact();
        let mut out = Vec::new();
        for node in self.frag.successors(&txn) {
            let XmlOut::Text(text) = node else { continue };
            for chunk in text.diff(&txn, YChange::identity) {
                let Out::Any(Any::String(piece)) = &chunk.insert else {
                    continue;
                };
                let attrs = chunk.attributes.as_deref();
                let id = match attrs.and_then(|a| a.get(NODE_KEY)) {
                    Some(Any::String(id)) => Some(id.to_string()),
                    _ => None,
                };
                let (actor, session) = match attrs.and_then(|a| a.get(AUTHOR_KEY)) {
                    Some(Any::String(a)) => parse_author(a),
                    _ => (0, 0),
                };
                out.push(TreeRun {
                    text: piece.to_string(),
                    node: id,
                    actor,
                    session,
                });
            }
        }
        out
    }

    /// The whole tree's text, in document order, with no structure — a cheap
    /// human-readable projection for tests, previews and logs. It is **not** the
    /// durable body: that is the host's serialization (see the module docs).
    pub fn plain_text(&self) -> String {
        let txn = self.doc.transact();
        let mut out = String::new();
        for node in self.frag.successors(&txn) {
            if let XmlOut::Text(text) = node {
                out.push_str(&raw_text(&text, &txn));
            }
        }
        out
    }

    /// A fresh node id for this replica.
    fn fresh_node_id(&self) -> String {
        let n = self.next_node.fetch_add(1, Ordering::Relaxed);
        format!("{:x}.{:x}", self.doc.client_id(), n)
    }

    /// The pre-image an [`apply_update_as`](Self::apply_update_as) reconciles
    /// against: every text node's string *and its stamp tiling*, plus every
    /// element's stamp attributes.
    ///
    /// The stamps, not merely the strings, are what make enforcement total — see
    /// [`reconcile`](Self::reconcile).
    fn scan(&self) -> PreImage {
        let txn = self.doc.transact();
        let mut texts = HashMap::new();
        let mut elements = HashMap::new();
        for node in self.frag.successors(&txn) {
            let id = node.id();
            match &node {
                XmlOut::Text(text) => {
                    let scan = scan_runs(text, &txn, None, tree_stamp);
                    texts.insert(id, (scan.flat, stamp_tiling(&scan.runs)));
                }
                XmlOut::Element(el) => {
                    elements.insert(
                        id,
                        (
                            el.get_attribute(&txn, AUTHOR_KEY),
                            el.get_attribute(&txn, NODE_KEY),
                        ),
                    );
                }
                XmlOut::Fragment(_) => {} // nested fragments carry no attributes
            }
        }
        PreImage { texts, elements }
    }

    /// Re-assert the authorship of **every** node, given the pre-image from
    /// before the update.
    ///
    /// This replaces a pass that stamped only fresh elements and text ranges a
    /// content diff called inserted. That left three knobs a client could turn on
    /// content it did not write — re-stamp `a` on an existing run, re-point `n`
    /// on an existing run (including onto a victim's id, which the `authors()`
    /// map then resolved to the victim), and rewrite either attribute on an
    /// existing element — because an existing node was never looked at again.
    /// The bail-out when nothing looked new was the tree half of the flat
    /// shape's forgery, and it is gone: every node is reconciled on every apply.
    fn reconcile(&self, ctx: WriteCtx, before: &PreImage) {
        // Collect under a read transaction, mutate under a write one: `successors`
        // borrows the transaction for the walk, and formatting needs a mutable one.
        let mut text_fixes: Vec<(XmlTextRef, Vec<TextFix>)> = Vec::new();
        let mut element_fixes: Vec<(XmlElementRef, TreeStamp)> = Vec::new();
        let empty: (String, Vec<(TreeStamp, u64)>) = (String::new(), Vec::new());
        {
            let txn = self.doc.transact();
            for node in self.frag.successors(&txn) {
                let id = node.id();
                match node {
                    XmlOut::Text(text) => {
                        let after = scan_runs(&text, &txn, None, tree_stamp);
                        // A `Y.XmlText` has no plain-text authority to check
                        // against (`GetString` renders formatting as XML tags),
                        // so a string-valued embed cannot be told apart from
                        // text here. Skip the node rather than repair off a run
                        // map whose indices do not address what they claim to —
                        // a missed repair on one node is recoverable, a
                        // misdirected `format` is not.
                        if !after.indexable {
                            tracing::warn!(
                                "coedit-tree: node holds an embed that cannot be \
                                 indexed for attribution; leaving it unreconciled"
                            );
                            continue;
                        }
                        // A text node absent from the pre-image is entirely new,
                        // so it scans as empty and every character is an insert.
                        let (was, was_tiling) = before.texts.get(&id).unwrap_or(&empty);
                        let want =
                            intended_stamps(was, was_tiling, &after.flat, &(None, None), || {
                                (
                                    Some(Arc::from(author_value(ctx))),
                                    Some(Arc::from(self.fresh_node_id())),
                                )
                            });
                        let fixes: Vec<TextFix> = diverging_runs(&want, &stamp_tiling(&after.runs))
                            .into_iter()
                            .filter_map(|(b0, blen, stamp)| {
                                doc_range(&after.runs, b0, blen).map(|(i, l)| (i, l, stamp.clone()))
                            })
                            .collect();
                        if !fixes.is_empty() {
                            text_fixes.push((text, fixes));
                        }
                    }
                    XmlOut::Element(el) => {
                        let want = match before.elements.get(&id) {
                            // Existing: its stamps must still be what they were.
                            Some((a, n)) => {
                                (a.as_deref().map(Arc::from), n.as_deref().map(Arc::from))
                            }
                            // New: stamped with this connection, unconditionally,
                            // so a value the client wrote is overwritten.
                            None => (
                                Some(Arc::from(author_value(ctx))),
                                Some(Arc::from(self.fresh_node_id())),
                            ),
                        };
                        let have: TreeStamp = (
                            el.get_attribute(&txn, AUTHOR_KEY).as_deref().map(Arc::from),
                            el.get_attribute(&txn, NODE_KEY).as_deref().map(Arc::from),
                        );
                        // Compare before writing: `insert_attribute` with an
                        // identical value still creates a CRDT item, so blind
                        // re-assertion would churn the document on every keystroke
                        // and grow the sidecar without bound.
                        if have != want {
                            element_fixes.push((el, want));
                        }
                    }
                    XmlOut::Fragment(_) => {}
                }
            }
        }
        if text_fixes.is_empty() && element_fixes.is_empty() {
            return; // authorship already agrees — the common case, writes nothing
        }
        let mut txn = self.doc.transact_mut();
        for (text, fixes) in text_fixes {
            for (index, len, (author, node)) in fixes {
                let attrs = Attrs::from([
                    attr_or_null(AUTHOR_KEY, &author),
                    attr_or_null(NODE_KEY, &node),
                ]);
                text.format(&mut txn, index, len, attrs);
            }
        }
        for (el, (author, node)) in element_fixes {
            match &author {
                Some(v) => el.insert_attribute(&mut txn, AUTHOR_KEY, v.to_string()),
                None => el.remove_attribute(&mut txn, &AUTHOR_KEY),
            }
            match &node {
                Some(v) => el.insert_attribute(&mut txn, NODE_KEY, v.to_string()),
                None => el.remove_attribute(&mut txn, &NODE_KEY),
            }
        }
    }
}

/// A node's `(author, node-id)` stamp, as the raw attribute values the CRDT
/// holds. Raw rather than parsed for the reason given on [`raw_author`].
type TreeStamp = (Option<Arc<str>>, Option<Arc<str>>);

/// One repair to apply to a text node: the `(index, len)` document range and the
/// stamp it must end up carrying.
type TextFix = (u32, u32, TreeStamp);

/// Pull a text run's `(author, node-id)` stamp off its formatting attributes.
fn tree_stamp(attrs: Option<&Attrs>) -> TreeStamp {
    (raw_author(attrs), raw_attr(attrs, NODE_KEY))
}

/// The state of a document immediately before an update was applied: enough to
/// re-assert what its authorship must be afterwards.
struct PreImage {
    /// Each text node's raw string and its `(stamp, byte_len)` tiling.
    texts: HashMap<BranchID, (String, Vec<(TreeStamp, u64)>)>,
    /// Each element's `(author, node-id)` attributes.
    elements: HashMap<BranchID, (Option<String>, Option<String>)>,
}

/// An `XmlText` node's raw string, with no formatting markup.
///
/// Deliberately not `GetString`, which renders a `Y.XmlText`'s formatting as XML
/// tags — the author stamps would then be part of the string and every stamp would
/// register as a content change in the next diff.
fn raw_text<T: ReadTxn>(text: &XmlTextRef, txn: &T) -> String {
    let mut out = String::new();
    for chunk in text.diff(txn, YChange::identity) {
        if let Out::Any(Any::String(piece)) = &chunk.insert {
            out.push_str(piece);
        }
    }
    out
}

/// Framing tag for a tree sidecar blob:
/// `[TREE_SIDECAR_MAGIC][root len][root][32-byte BLAKE3 of the body it crystallized][ydoc state]`.
///
/// Distinct from the flat sidecar's tag so neither can ever be read as the other,
/// and carrying the root name because a tree opened under a different root would
/// silently be a different (empty) document.
const TREE_SIDECAR_MAGIC: u8 = 2;

/// The sidecar path for a tree-co-edited `path`. The `t` prefix keeps it clear of
/// the flat sidecar's hex-encoded name, so one path can have both without collision.
pub fn coedit_tree_sidecar_path(path: &str) -> String {
    format!("{COEDIT_SIDECAR_DIR}/t{}", hex::encode(path.as_bytes()))
}

/// Frame a tree sidecar blob.
fn frame_tree_sidecar(root: &str, body: &[u8], state: &[u8]) -> Result<Vec<u8>> {
    let root = root.as_bytes();
    let len = u8::try_from(root.len()).map_err(|_| {
        OrigoFSError::InvalidArgument("co-edit tree root name must be at most 255 bytes".into())
    })?;
    let mut blob = Vec::with_capacity(2 + root.len() + 32 + state.len());
    blob.push(TREE_SIDECAR_MAGIC);
    blob.push(len);
    blob.extend_from_slice(root);
    blob.extend_from_slice(blake3::hash(body).as_bytes());
    blob.extend_from_slice(state);
    Ok(blob)
}

/// Split a framed tree sidecar into `(root, body_hash, ydoc_state)`, or `None` if
/// it is not in the current format. As with the flat sidecar, an unreadable one is
/// a cache miss rather than data loss — the caller opens an empty document and the
/// host reseeds.
fn parse_tree_sidecar(blob: &[u8]) -> Option<(&str, &[u8], &[u8])> {
    if blob.len() < 2 || blob[0] != TREE_SIDECAR_MAGIC {
        return None;
    }
    let root_len = blob[1] as usize;
    let rest = blob.get(2..)?;
    let root = std::str::from_utf8(rest.get(..root_len)?).ok()?;
    let rest = rest.get(root_len..)?;
    let hash = rest.get(..32)?;
    Some((root, hash, &rest[32..]))
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Open a tree-shaped co-edited document for `path`, rooted at the
    /// `XmlFragment` named `root`, and mark the path **live**.
    ///
    /// Resumes the CRDT from its sidecar when that sidecar is still coherent with
    /// the file (the file hashes to the body last checkpointed from this document).
    /// Otherwise — no sidecar, a stale one, or one written under a different root —
    /// returns an **empty** document with [`resumed`](CoeditTreeDoc::resumed) false,
    /// because reconstructing a tree from flat bytes would need the host's schema.
    /// Seed it before binding an editor; see
    /// [`resumed`](CoeditTreeDoc::resumed).
    pub async fn open_coedit_tree(
        &self,
        ctx: WriteCtx,
        path: &str,
        root: &str,
    ) -> Result<CoeditTreeDoc> {
        // Same path-scoped check the flat shape takes in `open_coedit`, and for
        // the same reason: this socket is a write channel onto `path` (#123).
        self.ensure_may_write_at(ctx, "co-edit", path).await?;
        let doc = self.load_coedit_tree(path, root).await?;
        self.mark_live(ctx, path).await?;
        Ok(doc)
    }

    /// Resume a tree document to **check point** against, without opening a
    /// session on it: the write check [`open_coedit_tree`](Self::open_coedit_tree)
    /// takes, without the live marker it claims.
    ///
    /// This is what a host's checkpoint route wants when no socket is attached —
    /// the editor closed, or the app is landing bytes from a "Save" button. Using
    /// `open_coedit_tree` there marked the path live and never cleared it, because
    /// the matching `end_coedit` lives on the socket's disconnect path that this
    /// flow never reaches: every socket-less checkpoint leaked a permanent marker
    /// telling readers the durable bytes may lag an editor that is not there.
    pub async fn load_coedit_tree_as(
        &self,
        ctx: WriteCtx,
        path: &str,
        root: &str,
    ) -> Result<CoeditTreeDoc> {
        self.ensure_may_write_at(ctx, "co-edit", path).await?;
        self.load_coedit_tree(path, root).await
    }

    /// The read-only half of [`open_coedit_tree`](Self::open_coedit_tree): resume
    /// the document without marking the path live.
    pub async fn load_coedit_tree(&self, path: &str, root: &str) -> Result<CoeditTreeDoc> {
        let blob = match self.read(&coedit_tree_sidecar_path(path)).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => return Ok(CoeditTreeDoc::new(root)),
            Err(e) => return Err(e),
        };
        let Some((sidecar_root, body_hash, state)) = parse_tree_sidecar(&blob) else {
            return Ok(CoeditTreeDoc::new(root));
        };
        if sidecar_root != root {
            return Ok(CoeditTreeDoc::new(root));
        }
        let current = match self.read(path).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => bytes::Bytes::new(),
            Err(e) => return Err(e),
        };
        if blake3::hash(&current).as_bytes().as_slice() != body_hash {
            return Ok(CoeditTreeDoc::new(root)); // the file moved under us
        }
        CoeditTreeDoc::load(root, state)
    }

    /// Checkpoint a tree-shaped co-edited document into `path`: land the host's
    /// serialized `body` with per-node authorship resolved from `spans`, then
    /// persist the CRDT sidecar so the session stays resumable.
    ///
    /// `spans` must be ordered, non-overlapping, within `body`, and on character
    /// boundaries; a byte no span covers — and a byte whose node id origofs never
    /// issued — is attributed to `ctx`, the actor performing the checkpoint. See
    /// the module docs for why the host supplies the bytes.
    ///
    /// # Concurrency
    ///
    /// Unlike [`checkpoint_coedit`](Self::checkpoint_coedit), an **out-of-band write
    /// is refused rather than reconciled**, with [`OrigoFSError::Conflict`]. The flat
    /// path can fold a foreign write in because it can diff text into CRDT
    /// operations; parsing bytes back into *nodes* would need the schema origofs
    /// deliberately does not have. Silently clobbering is the one outcome worse than
    /// an error, so the host is told: re-read the file, reseed the tree, checkpoint
    /// again.
    pub async fn checkpoint_coedit_tree(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditTreeDoc,
        body: &[u8],
        spans: &[TreeSpan],
    ) -> Result<()> {
        // The backstop to `open_coedit_tree`'s check — and it carries more weight
        // here than on the flat shape, because this call takes the host's `body`
        // and replaces the file with it wholesale.
        self.ensure_may_write_at(ctx, "check point a co-edited document to", path)
            .await?;
        self.checkpoint_coedit_tree_unchecked(ctx, path, doc, body, spans)
            .await
    }

    /// [`checkpoint_coedit_tree`](Self::checkpoint_coedit_tree) with the write
    /// check already made by the caller — see
    /// [`checkpoint_coedit_unchecked`](Fs::checkpoint_coedit_unchecked) for why
    /// accepting a proposal needs it.
    pub(crate) async fn checkpoint_coedit_tree_unchecked(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditTreeDoc,
        body: &[u8],
        spans: &[TreeSpan],
    ) -> Result<()> {
        let text = std::str::from_utf8(body).map_err(|_| {
            OrigoFSError::InvalidArgument("co-edit tree checkpoint requires UTF-8 text".into())
        })?;
        self.refuse_out_of_band(path).await?;
        let blamed = tile_spans(text, spans, &doc.authors(), ctx)?;
        self.write_as_blamed(ctx, path, body, &blamed).await?;
        self.write_tree_sidecar(path, doc, body).await?;
        // Only this call has actually crystallized the bytes, so it is the only one
        // entitled to stamp `checkpointed_at` (#97).
        if self.meta.get_live_doc(path).await?.is_some() {
            self.mark_checkpointed(ctx, path).await?;
        }
        Ok(())
    }

    /// Persist a tree document's CRDT sidecar **without** landing a body — the
    /// server-side half of durability for a shape whose bytes only the host can
    /// produce (see the module docs).
    ///
    /// A room can then be swept on a timer so a worker crash costs no editing
    /// history, even though the file and its blame only move when the host
    /// checkpoints. The sidecar is framed against the file's *current* bytes, so it
    /// stays coherent (and therefore resumable) exactly as long as nobody writes
    /// around the document.
    ///
    /// Deliberately does not stamp `checkpointed_at`: the durable file has not
    /// moved, and saying otherwise would tell a reader its bytes are fresher than
    /// they are.
    pub async fn persist_coedit_tree(&self, path: &str, doc: &CoeditTreeDoc) -> Result<()> {
        let body = match self.read(path).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => bytes::Bytes::new(),
            Err(e) => return Err(e),
        };
        self.write_tree_sidecar(path, doc, &body).await
    }

    /// Write `doc`'s state as `path`'s tree sidecar, framed against `body` as the
    /// coherence marker.
    async fn write_tree_sidecar(&self, path: &str, doc: &CoeditTreeDoc, body: &[u8]) -> Result<()> {
        self.mkdir_p(COEDIT_SIDECAR_DIR).await?;
        let blob = frame_tree_sidecar(doc.root(), body, &doc.state_update())?;
        self.write(&coedit_tree_sidecar_path(path), &blob).await
    }

    // --- tree-shaped suggestions (issues #75 §3.2, #92) -------------------

    /// Resume a tree document to **propose against**, without opening a session
    /// on it: the *propose* check, and no live marker.
    ///
    /// The tree counterpart of [`load_coedit_as`](Fs::load_coedit_as), and note
    /// the deliberate asymmetry with [`load_coedit_tree_as`](Fs::load_coedit_tree_as)
    /// next to it — that one exists for a host's socket-less *checkpoint* and so
    /// takes the write check. Proposing is what a propose-only actor is for, so
    /// gating this on `WRITE` would refuse exactly the callers it exists to
    /// serve; and a throwaway replica built to compute a proposal is not a
    /// session, so claiming the path for it would tell every reader the durable
    /// bytes may lag an editor that is not there.
    pub async fn load_coedit_tree_to_propose(
        &self,
        ctx: WriteCtx,
        path: &str,
        root: &str,
    ) -> Result<CoeditTreeDoc> {
        self.ensure_may_propose_at(ctx, "propose changes to", path)
            .await?;
        self.load_coedit_tree(path, root).await
    }

    /// Propose a change to a tree-shaped co-edited `path` as a **CRDT merge**.
    ///
    /// The tree counterpart of [`suggest_coedit`](Fs::suggest_coedit), and the
    /// reason it had to exist: without it a propose-only actor could propose
    /// against a flat `Y.Text` document and had no way at all to propose against
    /// an `XmlFragment` one — so on the shape a rich-text editor actually uses,
    /// the review queue was unreachable and the only paths open to such an actor
    /// were a byte suggestion (whose base goes stale on every keystroke and whose
    /// acceptance discards concurrent work) or nothing.
    pub async fn suggest_coedit_tree(
        &self,
        ctx: WriteCtx,
        path: &str,
        doc: &CoeditTreeDoc,
        summary: Option<&str>,
    ) -> Result<i64> {
        // Where the *workspace's* document stood when this was proposed — the
        // reviewer's "you were looking at this much of it". Not a gate: a CRDT
        // merge is defined against any later state.
        let base = self
            .load_coedit_tree(path, doc.root())
            .await?
            .state_vector();
        self.suggest_coedit_tree_update(ctx, path, &base, &doc.state_update(), summary)
            .await
    }

    /// The primitive behind [`suggest_coedit_tree`](Fs::suggest_coedit_tree), for
    /// a client that already holds the two Yjs blobs — a browser editor proposes
    /// with `encodeStateVector(doc)` as `base_sv` and `encodeStateAsUpdate(doc)`
    /// as `update`.
    pub async fn suggest_coedit_tree_update(
        &self,
        ctx: WriteCtx,
        path: &str,
        base_sv: &[u8],
        update: &[u8],
        summary: Option<&str>,
    ) -> Result<i64> {
        if update.is_empty() {
            return Err(OrigoFSError::InvalidArgument(
                "co-edit tree suggestion: empty update proposes nothing".into(),
            ));
        }
        // Refuse a malformed blob at propose time rather than at review time: a
        // suggestion nobody can apply is worse than a refused proposal.
        yrs::Update::decode_v1(update)
            .map_err(|e| OrigoFSError::InvalidArgument(format!("bad co-edit update: {e}")))?;
        let base_hash = Some(self.put_opaque(base_sv).await?);
        let proposed_hash = Some(self.put_opaque(update).await?);
        self.record_suggestion(
            ctx,
            path,
            base_hash,
            proposed_hash,
            summary,
            crate::suggest::SuggestionKind::CrdtTree,
        )
        .await
    }

    /// The proposed Yjs update behind a tree suggestion, for a host that wants to
    /// merge it into a document it already holds — the live room, say, rather
    /// than a resumed replica.
    ///
    /// Public, unlike the flat shape's equivalent, because on this shape the host
    /// is *required* to be in the loop: it owns the schema, so it owns the merge
    /// and the serialization that follows.
    pub async fn coedit_tree_suggestion_update(&self, id: i64) -> Result<bytes::Bytes> {
        let s = self
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        if s.kind != crate::suggest::SuggestionKind::CrdtTree {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} is a {} suggestion, not a tree co-edit proposal",
                s.kind.as_str()
            )));
        }
        self.coedit_suggestion_update(&s).await
    }

    /// Merge a tree suggestion into a resumed replica and hand it back, for a
    /// host that would rather origofs did the resume than do it itself.
    ///
    /// Persists nothing. The document that comes back is the workspace's document
    /// **plus** the proposal; serialize it and pass the bytes to
    /// [`accept_coedit_tree_suggestion`](Fs::accept_coedit_tree_suggestion).
    pub async fn merge_coedit_tree_suggestion(&self, id: i64, root: &str) -> Result<CoeditTreeDoc> {
        let update = self.coedit_tree_suggestion_update(id).await?;
        let s = self
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        let doc = self.load_coedit_tree(&s.path, root).await?;
        doc.apply_update(&update)?;
        Ok(doc)
    }

    /// The `(before, after)` plain text of applying a tree suggestion, for review.
    ///
    /// Computed on a throwaway replica and never persisted, so previewing has no
    /// effect on the workspace — and because it re-merges against the document as
    /// it stands now, the preview stays truthful as the document moves on. The
    /// replica resumes under the sidecar's own root, so a reviewer does not have
    /// to know the host's schema to render a diff.
    pub(crate) async fn preview_coedit_tree_suggestion(
        &self,
        s: &crate::suggest::Suggestion,
    ) -> Result<(String, String)> {
        let update = self.coedit_suggestion_update(s).await?;
        let root = self
            .coedit_tree_root(&s.path)
            .await?
            .unwrap_or_else(|| DEFAULT_TREE_ROOT.to_string());
        let doc = self.load_coedit_tree(&s.path, &root).await?;
        let before = doc.plain_text();
        doc.apply_update(&update)?;
        Ok((before, doc.plain_text()))
    }

    /// The `XmlFragment` name a path's tree sidecar was written under, or `None`
    /// when there is no readable sidecar.
    ///
    /// The root is not stored on the suggestion row because the document already
    /// knows it: two proposals against one path must resume under the same root
    /// or they are not proposals against the same document, and the sidecar is
    /// the one place that fact is recorded.
    pub async fn coedit_tree_root(&self, path: &str) -> Result<Option<String>> {
        let blob = match self.read(&coedit_tree_sidecar_path(path)).await {
            Ok(b) => b,
            Err(OrigoFSError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(parse_tree_sidecar(&blob).map(|(root, _, _)| root.to_string()))
    }

    /// Accept a tree suggestion: land the host's serialized `body` **attributed to
    /// the proposal's author**, and resolve the row, in one call.
    ///
    /// Why the host supplies the bytes is the same reason
    /// [`checkpoint_coedit_tree`](Fs::checkpoint_coedit_tree) does: origofs does
    /// not own the document schema, so it cannot turn a tree back into a file.
    /// The host merges (its own room, or
    /// [`merge_coedit_tree_suggestion`](Fs::merge_coedit_tree_suggestion)),
    /// serializes, and calls this.
    ///
    /// The two review rules the flat path has hold here too, and are checked
    /// before anything is written: the approver must not be the author, and the
    /// row must still be pending. The bytes land as the *author*, with `ctx`
    /// recorded as the approver — an acceptance credits the person who wrote the
    /// change, not the one who waved it through.
    pub async fn accept_coedit_tree_suggestion(
        &self,
        ctx: WriteCtx,
        id: i64,
        doc: &CoeditTreeDoc,
        body: &[u8],
        spans: &[TreeSpan],
    ) -> Result<()> {
        let s = self
            .get_suggestion(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("suggestion #{id}")))?;
        if s.kind != crate::suggest::SuggestionKind::CrdtTree {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} is a {} suggestion; accept it with accept_suggestion",
                s.kind.as_str()
            )));
        }
        if s.status != crate::suggest::SuggestionStatus::Pending {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} is already {}",
                s.status.as_str()
            )));
        }
        // The review gate, part one, exactly as `accept_suggestion` applies it:
        // accepting lands a real attributed edit, so a propose-only approver would
        // be a direct write wearing a review as a costume — and two propose-only
        // agents could rubber-stamp each other into full write access (#78).
        // Checked at the suggestion's own path, because that is where the write
        // lands.
        self.ensure_may_write_at(ctx, "accept suggestions for", &s.path)
            .await?;
        if ctx.actor == s.actor_id {
            return Err(OrigoFSError::InvalidArgument(format!(
                "suggestion #{id} cannot be accepted by its author (actor {}); \
                 acceptance requires a different reviewer",
                s.actor_id
            )));
        }
        let author = WriteCtx {
            actor: s.actor_id,
            session: s.session_id,
            tool_call: None,
        };
        // Unchecked *as the author*: the approver's right to land these bytes was
        // established above, and the author is by definition someone who could
        // only propose — checking as them would refuse every proposal the queue
        // exists to carry.
        self.checkpoint_coedit_tree_unchecked(author, &s.path, doc, body, spans)
            .await?;
        // Resolve after the write, like the byte and flat-CRDT paths, and check
        // the compare-and-set: a lost CAS means an accept raced a reject, and the
        // caller must not be told the acceptance worked while the row reads
        // "rejected". The write has already landed by then — inherent to applying
        // before resolving — so this cannot be undone, but it must not be silent.
        if !self
            .meta
            .resolve_suggestion(
                id,
                crate::suggest::SuggestionStatus::Accepted,
                Some(ctx.actor),
                self.now_secs(),
            )
            .await?
        {
            return Err(OrigoFSError::Conflict(format!(
                "suggestion #{id} was resolved by someone else while it was being \
                 accepted; the proposed body has already been written to {}",
                s.path
            )));
        }
        Ok(())
    }

    /// Refuse the checkpoint if somebody wrote `path` around the live document.    /// Refuse the checkpoint if somebody wrote `path` around the live document.
    async fn refuse_out_of_band(&self, path: &str) -> Result<()> {
        let Some(live) = self.meta.get_live_doc(path).await? else {
            return Ok(()); // not live: nothing claims to own these bytes
        };
        if self.current_content_hex(path).await? == live.content_hash {
            return Ok(());
        }
        Err(OrigoFSError::Conflict(format!(
            "{path} was written outside the co-editing session since its last \
             checkpoint; a tree document cannot be reconciled with a foreign write \
             (origofs cannot parse bytes back into nodes) — re-read the file, reseed \
             the document, and checkpoint again"
        )))
    }
}

/// Turn a host's span map into the contiguous `(actor, session, byte_len)` tiling
/// [`write_as_blamed`](Fs::write_as_blamed) takes, resolving each node id through
/// `authors` and filling every uncovered byte with `ctx`.
fn tile_spans(
    text: &str,
    spans: &[TreeSpan],
    authors: &HashMap<String, (i64, i64)>,
    ctx: WriteCtx,
) -> Result<Vec<(i64, i64, u64)>> {
    let len = text.len() as u64;
    let fallback = (ctx.actor, ctx.session.unwrap_or(0));
    let mut out: Vec<(i64, i64, u64)> = Vec::new();
    let mut at = 0u64;

    // Append `count` bytes authored by `author`, merging into the run before it.
    let mut push = |author: (i64, i64), count: u64| {
        if count == 0 {
            return;
        }
        match out.last_mut() {
            Some(last) if (last.0, last.1) == author => last.2 += count,
            _ => out.push((author.0, author.1, count)),
        }
    };

    for span in spans {
        if span.end < span.start {
            return Err(OrigoFSError::InvalidArgument(format!(
                "co-edit tree span {}..{} ends before it starts",
                span.start, span.end
            )));
        }
        if span.start < at {
            return Err(OrigoFSError::InvalidArgument(format!(
                "co-edit tree spans must be ordered and non-overlapping: {}..{} \
                 starts before the previous span ended at {at}",
                span.start, span.end
            )));
        }
        if span.end > len {
            return Err(OrigoFSError::InvalidArgument(format!(
                "co-edit tree span {}..{} runs past the {len}-byte body",
                span.start, span.end
            )));
        }
        // A boundary inside a character would split it across two blame ranges,
        // which no reader can render — and it always means the host's serializer
        // counted in the wrong unit, so it is worth saying so rather than storing.
        for edge in [span.start, span.end] {
            if !text.is_char_boundary(edge as usize) {
                return Err(OrigoFSError::InvalidArgument(format!(
                    "co-edit tree span boundary {edge} is not a character boundary \
                     (byte offsets, not UTF-16 code units)"
                )));
            }
        }
        push(fallback, span.start - at);
        let author = authors.get(&span.node).copied().unwrap_or(fallback);
        push(author, span.end - span.start);
        at = span.end;
    }
    push(fallback, len - at);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(actor: i64) -> WriteCtx {
        WriteCtx::session(actor, 1)
    }

    #[test]
    fn spans_tile_and_fill_gaps_with_the_checkpointer() {
        let authors = HashMap::from([("a".to_string(), (7, 3))]);
        // "# T\n\nhi\n": only "hi" comes from a node; the rest is serializer output.
        let tiled =
            tile_spans("# T\n\nhi\n", &[TreeSpan::new(5, 7, "a")], &authors, ctx(2)).unwrap();
        assert_eq!(tiled, vec![(2, 1, 5), (7, 3, 2), (2, 1, 1)]);
    }

    #[test]
    fn adjacent_spans_by_one_author_merge() {
        let authors = HashMap::from([("a".into(), (7, 3)), ("b".into(), (7, 3))]);
        let tiled = tile_spans(
            "hello",
            &[TreeSpan::new(0, 2, "a"), TreeSpan::new(2, 5, "b")],
            &authors,
            ctx(2),
        )
        .unwrap();
        assert_eq!(tiled, vec![(7, 3, 5)]);
    }

    #[test]
    fn an_unknown_node_id_falls_back_rather_than_inventing_an_author() {
        let tiled = tile_spans(
            "hey",
            &[TreeSpan::new(0, 3, "nope")],
            &HashMap::new(),
            ctx(9),
        )
        .unwrap();
        assert_eq!(tiled, vec![(9, 1, 3)]);
    }

    #[test]
    fn overlapping_out_of_range_and_mid_character_spans_are_refused() {
        let authors = HashMap::new();
        let overlap = [TreeSpan::new(0, 3, "a"), TreeSpan::new(2, 4, "b")];
        assert!(tile_spans("abcd", &overlap, &authors, ctx(1)).is_err());
        let past = [TreeSpan::new(0, 9, "a")];
        assert!(tile_spans("abcd", &past, &authors, ctx(1)).is_err());
        let backwards = [TreeSpan::new(3, 1, "a")];
        assert!(tile_spans("abcd", &backwards, &authors, ctx(1)).is_err());
        // "é" is two bytes; 1 lands inside it.
        let mid_char = [TreeSpan::new(0, 1, "a")];
        assert!(tile_spans("é", &mid_char, &authors, ctx(1)).is_err());
    }

    #[test]
    fn a_sidecar_round_trips_its_root_and_coherence_hash() {
        let framed = frame_tree_sidecar("content", b"body", b"state").unwrap();
        let (root, hash, state) = parse_tree_sidecar(&framed).unwrap();
        assert_eq!(root, "content");
        assert_eq!(hash, blake3::hash(b"body").as_bytes());
        assert_eq!(state, b"state");
        // A flat sidecar must never parse as a tree one.
        assert!(parse_tree_sidecar(&[1, 0, 0, 0]).is_none());
        assert!(parse_tree_sidecar(&[]).is_none());
        assert!(parse_tree_sidecar(&[TREE_SIDECAR_MAGIC, 200, 1, 2]).is_none());
    }
}
