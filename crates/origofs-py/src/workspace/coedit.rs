//! Live CRDT co-editing, both document shapes, plus undo claims and the cross-worker relay.

use super::super::*;

#[pymethods]
impl Workspace {
    /// Load a co-edited document to **propose** against, without opening a session
    /// on it: the same reconstruction `open_coedit` does, but it needs only the
    /// propose right (not write) and does not mark the path live. This is the
    /// document to build a `suggest_coedit` proposal from.
    fn load_coedit_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let doc = ws.load_coedit_as(c, &path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditDoc {
                        inner: Arc::new(tokio::sync::Mutex::new(doc)),
                    },
                )
            })
        })
    }

    /// Resume a tree document to **checkpoint** against without opening a session
    /// on it: the same write check `open_coedit_tree` takes, without the live
    /// marker it claims. This is what a checkpoint route uses when no socket is
    /// attached, so a "Save" with no editor open leaks no live marker.
    #[pyo3(signature = (ctx, path, root=None))]
    fn load_coedit_tree_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        root: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let root = root.unwrap_or_else(|| origofs_core::DEFAULT_TREE_ROOT.to_string());
            let doc = ws
                .load_coedit_tree_as(c, &path, &root)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditTreeDoc {
                        inner: Arc::new(tokio::sync::Mutex::new(doc)),
                    },
                )
            })
        })
    }

    /// Resume a tree document to **propose against**: the *propose* check, and no
    /// live marker.
    ///
    /// Note the asymmetry with `load_coedit_tree_as` above, which serves a
    /// socket-less checkpoint and so takes the write check. Gating this on write
    /// would refuse exactly the propose-only agents it exists for.
    #[pyo3(signature = (ctx, path, root=None))]
    fn load_coedit_tree_to_propose<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        root: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let root = root.unwrap_or_else(|| origofs_core::DEFAULT_TREE_ROOT.to_string());
            let doc = ws
                .load_coedit_tree_to_propose(c, &path, &root)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditTreeDoc {
                        inner: Arc::new(tokio::sync::Mutex::new(doc)),
                    },
                )
            })
        })
    }

    /// Propose a change to a **tree-shaped** co-edited path as a CRDT merge, the
    /// `XmlFragment` counterpart of `suggest_coedit` — and the shape a rich-text
    /// editor actually uses. Without it a propose-only agent had no way to
    /// propose against such a document at all.
    ///
    /// ``replaces`` retires an earlier pending draft of this actor's as this one is
    /// created — see ``suggest``.
    #[pyo3(signature = (ctx, path, doc, summary=None, replaces=None))]
    fn suggest_coedit_tree<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        doc: Py<CoeditTreeDoc>,
        summary: Option<String>,
        replaces: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            ws.suggest_coedit_tree(c, &path, &guard, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)
        })
    }

    /// The primitive behind `suggest_coedit_tree`, for a client that already
    /// holds the two Yjs blobs (a browser editor sends `encodeStateVector` +
    /// `encodeStateAsUpdate`).
    ///
    /// ``replaces`` retires an earlier pending draft of this actor's as this one is
    /// created — see ``suggest``.
    #[pyo3(signature = (ctx, path, base_sv, update, summary=None, replaces=None))]
    // A pyo3 binding mirrors the SDK signature it forwards to, plus `py`. Packing
    // them into a struct would change the *Python* call shape for no gain — the
    // keyword arguments are the API.
    #[allow(clippy::too_many_arguments)]
    fn suggest_coedit_tree_update<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        base_sv: Vec<u8>,
        update: Vec<u8>,
        summary: Option<String>,
        replaces: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.suggest_coedit_tree_update(c, &path, &base_sv, &update, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)
        })
    }

    /// The proposed Yjs update behind a tree suggestion, for merging into a
    /// document you already hold (the live room, rather than a fresh replica).
    fn coedit_tree_suggestion_update<'py>(
        &self,
        py: Python<'py>,
        id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let bytes = ws
                .coedit_tree_suggestion_update(id)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// Merge a tree suggestion into a resumed replica and hand it back. Persists
    /// nothing: serialize the result and pass the bytes to
    /// `accept_coedit_tree_suggestion`.
    #[pyo3(signature = (id, root=None))]
    fn merge_coedit_tree_suggestion<'py>(
        &self,
        py: Python<'py>,
        id: i64,
        root: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let root = root.unwrap_or_else(|| origofs_core::DEFAULT_TREE_ROOT.to_string());
            let doc = ws
                .merge_coedit_tree_suggestion(id, &root)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditTreeDoc {
                        inner: Arc::new(tokio::sync::Mutex::new(doc)),
                    },
                )
            })
        })
    }

    /// The ``XmlFragment`` name a path's tree sidecar was written under, or
    /// ``None`` when there is no readable sidecar. A reviewer has no schema, so
    /// this is how it learns which root to resume under.
    fn coedit_tree_root<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.coedit_tree_root(&path).await.map_err(to_pyerr)
        })
    }

    /// Accept a tree suggestion: land your serialized ``body`` attributed to the
    /// proposal's **author**, and resolve the row, in one call.
    ///
    /// ``accept_suggestion`` refuses a tree proposal, because landing one means
    /// writing the document back out as bytes and only you know the schema for
    /// that — the same reason ``checkpoint_coedit_tree`` takes a body. The
    /// approver must hold write at the path and must differ from the author.
    fn accept_coedit_tree_suggestion<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        id: i64,
        doc: Py<CoeditTreeDoc>,
        body: Vec<u8>,
        spans: Vec<(u64, u64, String)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        let spans: Vec<origofs_sdk::TreeSpan> = spans
            .into_iter()
            .map(|(start, end, node)| origofs_sdk::TreeSpan::new(start, end, node))
            .collect();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            ws.accept_coedit_tree_suggestion(c, id, &guard, &body, &spans)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Open a live co-editing document for `path` (roadmap M8): resume the CRDT
    /// from its persisted sidecar if one exists, else promote the file's current
    /// text into a fresh document attributed to `ctx`. Returns a [`CoeditDoc`] to
    /// drive over the Yjs y-sync protocol and land with `checkpoint_coedit`.
    fn open_coedit<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let doc = ws.open_coedit(c, &path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditDoc {
                        inner: Arc::new(tokio::sync::Mutex::new(doc)),
                    },
                )
            })
        })
    }

    /// Checkpoint a live co-editing `doc` into `path`, landing each collaborator's
    /// exact character spans in the byte-range blame index and persisting the CRDT
    /// sidecar so the session is durable and resumable. `ctx` is the actor
    /// performing the checkpoint (its authorship is not imposed on others' spans).
    fn checkpoint_coedit<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        doc: Py<CoeditDoc>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            ws.checkpoint_coedit(c, &path, &guard)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Undo (or, with ``redo=True``, redo) `ctx`'s actor's most recent action on
    /// the live flat `doc`, returning the y-sync frame to fan out to the room —
    /// empty bytes when there was nothing to undo.
    ///
    /// Scoped to that actor's own edits, so it can never reach a collaborator's
    /// work or anything that arrived over the cross-worker relay. **An undo is a
    /// write**, so it takes ``WRITE`` at `path` exactly as ``open_coedit`` does
    /// and raises ``PermissionError`` otherwise — a propose-only actor is refused
    /// rather than silently no-op'd, because there is no such thing as a proposed
    /// undo.
    ///
    /// The actor must have been tracked (``doc.track_undo(ctx)``) before the edits
    /// this would pop, which in a server means at socket join.
    #[pyo3(signature = (ctx, path, doc, redo=false))]
    fn undo_coedit<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        doc: Py<CoeditDoc>,
        redo: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            let frame = ws
                .undo_coedit(c, &path, &guard, redo)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &frame).unbind()))
        })
    }

    /// Claim the undo stack for the document ``(path, root)`` on behalf of
    /// ``holder`` (this worker), or renew a claim it already has. ``root`` is the
    /// ``XmlFragment`` root of a tree document and defaults to the flat shape's
    /// empty string — a *document* is ``(path, shape)``, not a path, and one path
    /// may be open in both at once. Returns whether it now owns it.
    ///
    /// **A worker must hold this before calling ``doc.track_undo``.** At most one
    /// may keep an actor's stack for a document: two independent stacks can pop
    /// items touching the same content, and because origofs's author stamp is
    /// written in the same undo step as the insert it describes, one worker's
    /// undo can strip a stamp the other's restore needs — leaving text present
    /// but unattributed, which the next checkpoint credits to the checkpointer.
    ///
    /// Single-worker deployments are unaffected: two tabs are the same holder, so
    /// both claims succeed and they share one stack.
    #[pyo3(signature = (path, actor_id, holder, root=None))]
    fn claim_undo_stack<'py>(
        &self,
        py: Python<'py>,
        path: String,
        actor_id: i64,
        holder: String,
        root: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let root = root.unwrap_or_default();
        future_into_py(py, async move {
            ws.claim_undo_stack(&path, &root, actor_id, &holder)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop ``holder``'s claim on the document ``(path, root)`` — the actor's
    /// last socket on this worker leaving, so another worker can serve them
    /// immediately rather than waiting out a lease.
    #[pyo3(signature = (path, actor_id, holder, root=None))]
    fn release_undo_stack<'py>(
        &self,
        py: Python<'py>,
        path: String,
        actor_id: i64,
        holder: String,
        root: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let root = root.unwrap_or_default();
        future_into_py(py, async move {
            ws.release_undo_stack(&path, &root, actor_id, &holder)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop every undo claim ``holder`` has — a clean shutdown.
    fn release_undo_claims_for_holder<'py>(
        &self,
        py: Python<'py>,
        holder: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.release_undo_claims_for_holder(&holder)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Push out the lease on every undo claim ``holder`` has. A live worker calls
    /// this on a timer at well under the lease (60s).
    fn renew_undo_claims<'py>(
        &self,
        py: Python<'py>,
        holder: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.renew_undo_claims(&holder).await.map_err(to_pyerr)
        })
    }

    /// The ``WRITE`` check an undo takes, on its own — for a surface that must
    /// authorize *before* looking up whether a room is open or who holds its undo
    /// stack, since both are facts about the document a refused actor must not
    /// learn. Raises ``PermissionError`` when the actor may not write at `path`.
    #[pyo3(signature = (ctx, path, redo=false))]
    fn ensure_may_undo<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        redo: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.ensure_may_undo(c, &path, redo).await.map_err(to_pyerr)
        })
    }

    /// ``undo_coedit`` for a **tree-shaped** document (issue #92).
    ///
    /// The live document moves immediately; the *file* moves when you next call
    /// ``checkpoint_coedit_tree`` with your own serialized bytes, because origofs
    /// does not own the schema.
    #[pyo3(signature = (ctx, path, doc, redo=false))]
    fn undo_coedit_tree<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        doc: Py<CoeditTreeDoc>,
        redo: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            let frame = ws
                .undo_coedit_tree(c, &path, &guard, redo)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &frame).unbind()))
        })
    }

    /// Open a **tree-shaped** live co-editing document for `path` (issue #92),
    /// rooted at the ``XmlFragment`` named `root` — the shape
    /// `@platejs/yjs`/`@slate-yjs/core`, `y-prosemirror` and TipTap bind to natively.
    ///
    /// Resumes from the sidecar when it is still coherent with the file; otherwise
    /// the document opens **empty** with ``resumed()`` false, because rebuilding a
    /// tree from flat bytes would need your schema. Seed it from ``read(path)``
    /// before binding an editor.
    #[pyo3(signature = (ctx, path, root=None))]
    fn open_coedit_tree<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        root: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let root = root.unwrap_or_else(|| origofs_sdk::DEFAULT_TREE_ROOT.to_string());
        future_into_py(py, async move {
            let doc = ws
                .open_coedit_tree(c, &path, &root)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditTreeDoc {
                        inner: Arc::new(tokio::sync::Mutex::new(doc)),
                    },
                )
            })
        })
    }

    /// Checkpoint a tree-shaped co-editing `doc` into `path`: land **your**
    /// serialized `body` with per-node authorship resolved from `spans`.
    ///
    /// `spans` is a list of ``(byte_start, byte_end, node)`` tuples saying which
    /// bytes of `body` came from which co-edit node — ordered, non-overlapping, on
    /// character boundaries. origofs resolves each node to the author it stamped
    /// itself, so you name ranges and nodes, never an actor. Bytes no span covers
    /// (your serializer's own punctuation) are attributed to `ctx`.
    ///
    /// Raises ``Conflict`` if the file was written outside the session since the
    /// last checkpoint: a tree cannot be reconciled with a foreign write, so the
    /// alternative would be clobbering it silently. Re-read, reseed, retry.
    fn checkpoint_coedit_tree<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        doc: Py<CoeditTreeDoc>,
        body: Vec<u8>,
        spans: Vec<(u64, u64, String)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        let spans: Vec<origofs_sdk::TreeSpan> = spans
            .into_iter()
            .map(|(start, end, node)| origofs_sdk::TreeSpan::new(start, end, node))
            .collect();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            ws.checkpoint_coedit_tree(c, &path, &guard, &body, &spans)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Persist a tree document's CRDT sidecar **without** landing a body — the
    /// server-side half of durability for a shape only you can serialize.
    ///
    /// Call it on a timer for long-lived rooms: a crash then costs no editing
    /// history, while the file and its blame stay where the last real checkpoint
    /// left them (so it deliberately does not stamp "last saved").
    fn persist_coedit_tree<'py>(
        &self,
        py: Python<'py>,
        path: String,
        doc: Py<CoeditTreeDoc>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let inner = doc.borrow(py).inner.clone();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            ws.persist_coedit_tree(&path, &guard)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Propose a change to a co-edited `path` as a **CRDT merge** rather than a
    /// whole file body: the review row records the workspace document's Yjs state
    /// vector as its base and `doc`'s ``encodeStateAsUpdate`` blob as the proposal.
    /// Accepting it merges (``applyUpdate``) instead of overwriting, so a
    /// concurrent disjoint edit is neither clobbered nor false-rejected as stale.
    /// Returns the suggestion id.
    ///
    /// ``replaces`` retires an earlier pending draft of this actor's as this one is
    /// created — see ``suggest``.
    #[pyo3(signature = (ctx, path, doc, summary=None, replaces=None))]
    fn suggest_coedit<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        doc: Py<CoeditDoc>,
        summary: Option<String>,
        replaces: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            let id = ws
                .suggest_coedit(c, &path, &guard, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// The primitive behind `suggest_coedit`, for a client that already holds the
    /// two Yjs blobs — a browser editor proposes with ``encodeStateVector(doc)`` as
    /// `base_sv` and ``encodeStateAsUpdate(doc)`` as `update`.
    ///
    /// ``replaces`` retires an earlier pending draft of this actor's as this one is
    /// created — see ``suggest``.
    #[pyo3(signature = (ctx, path, base_sv, update, summary=None, replaces=None))]
    // A pyo3 binding mirrors the SDK signature it forwards to, plus `py`. Packing
    // them into a struct would change the *Python* call shape for no gain — the
    // keyword arguments are the API.
    #[allow(clippy::too_many_arguments)]
    fn suggest_coedit_update<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        base_sv: Vec<u8>,
        update: Vec<u8>,
        summary: Option<String>,
        replaces: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let id = ws
                .suggest_coedit_update(c, &path, &base_sv, &update, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// End a live co-editing session for `path`: clear its live marker so byte
    /// readers stop being told the durable blob may lag. Checkpoint *first* — this
    /// only drops the flag. Idempotent.
    fn end_coedit<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.end_coedit(&path).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// The live-document marker for `path`, or ``None`` when nothing has it open.
    /// A byte reader consults this to tell "these bytes are the whole truth" from
    /// "these bytes may lag an open ``Y.Doc``".
    fn live_doc<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let live = ws.live_doc(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| match live {
                Some(l) => live_doc_dict(py, &l).map(Some),
                None => Ok(None),
            })
        })
    }

    /// Every path currently open in a live co-editing session.
    fn live_paths<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let list = ws.live_paths().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                list.iter()
                    .map(|l| live_doc_dict(py, l))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Read `path` **and** report whether it is live: ``(bytes, live | None)``. The
    /// bytes are exactly what `read` returns; the second element is the live marker
    /// when an open CRDT document may be ahead of them. Reading never blocks,
    /// fails, or forces a checkpoint on account of a live path — it *surfaces* the
    /// staleness, and a caller that needs the freshest bytes checkpoints the room
    /// first, then reads.
    fn read_live<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let (data, live) = ws.read_live(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let bytes = PyBytes::new(py, &data).into_any().unbind();
                let marker = match live {
                    Some(l) => live_doc_dict(py, &l)?,
                    None => py.None(),
                };
                Ok((bytes, marker))
            })
        })
    }

    /// Ensure the cross-worker relay's backing table exists (idempotent). Call it
    /// before a room accepts edits. Requires the Postgres backend.
    fn coedit_relay_init<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.coedit_relay_init().await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Publish a co-editing update `delta` (a y-sync frame) for `path` to peer
    /// workers, tagged with this worker's `origin` id. Requires the Postgres backend.
    fn coedit_publish<'py>(
        &self,
        py: Python<'py>,
        path: String,
        origin: String,
        delta: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.coedit_publish(&path, &origin, &delta)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Every relayed op currently held for `path` (as `CoeditRelayNote`s), for a
    /// worker that just started hosting it to replay and catch up (idempotent).
    /// Requires the Postgres backend.
    fn coedit_replay<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let notes = ws.coedit_replay(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                notes
                    .into_iter()
                    .map(|n| {
                        Py::new(
                            py,
                            CoeditRelayNote {
                                seq: n.seq,
                                origin: n.origin,
                                path: n.path,
                                delta: n.delta,
                            },
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Subscribe to the cross-worker co-editing relay. Returns a `CoeditRelaySub`
    /// whose `recv()` yields peers' updates in order. Requires the Postgres backend.
    fn coedit_subscribe<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let sub = ws.coedit_subscribe().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditRelaySub {
                        inner: Arc::new(tokio::sync::Mutex::new(sub)),
                    },
                )
            })
        })
    }

    // --- live collaboration -------------------------------------------------
}
