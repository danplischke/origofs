//! The change feed, presence, and the propose-and-review suggestion queue.

use super::super::*;

#[pymethods]
impl Workspace {
    /// Append an arbitrary event to the change feed, returning its sequence
    /// number.
    ///
    /// Every mutating method emits its own event, so this is for the things origofs
    /// cannot see: an agent finished a task, a review was requested, a deploy went
    /// out. Feed consumers (`watch`, `subscribe`) receive it like any other, so a
    /// host's own milestones interleave with file changes in one ordered stream
    /// rather than needing a second channel.
    #[pyo3(signature = (kind, path, actor_id = None, session_id = None, detail = None, branch = None))]
    #[allow(clippy::too_many_arguments)]
    fn record_event<'py>(
        &self,
        py: Python<'py>,
        kind: String,
        path: String,
        actor_id: Option<i64>,
        session_id: Option<i64>,
        detail: Option<String>,
        branch: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.record_event(origofs_sdk::EventInit {
                actor_id,
                session_id,
                kind,
                path,
                detail,
                branch,
            })
            .await
            .map_err(to_pyerr)
        })
    }

    /// Change-feed events strictly after `after_seq` (oldest first).
    #[pyo3(signature = (after_seq=0))]
    fn watch<'py>(&self, py: Python<'py>, after_seq: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let events = ws.watch(after_seq).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                events
                    .iter()
                    .map(|e| event_dict(py, e))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// A **push** subscription to the change feed (Postgres `LISTEN/NOTIFY`):
    /// `await`ing the returned object's `recv()` blocks until the next batch of
    /// events, instead of polling `watch`. Optionally branch-scoped. Raises on
    /// non-Postgres backends (use `watch` there).
    #[pyo3(signature = (after_seq=0, branch=None))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        after_seq: i64,
        branch: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let sub = ws
                .subscribe(after_seq, branch.as_deref())
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    Subscription {
                        inner: Arc::new(tokio::sync::Mutex::new(sub)),
                    },
                )
            })
        })
    }

    /// Sessions active within the last `window_secs` seconds.
    #[pyo3(signature = (window_secs=60))]
    fn presence<'py>(&self, py: Python<'py>, window_secs: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let list = ws.presence(window_secs).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                list.iter()
                    .map(|p| presence_dict(py, p))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Heartbeat a session's presence (and current path).
    #[pyo3(signature = (actor_id, session_id, path=None))]
    fn touch<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        session_id: i64,
        path: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.touch(actor_id, session_id, path.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    // --- agent-suggestion review queue --------------------------------------

    /// Propose an edit to `path` for review (does not touch the working tree).
    ///
    /// ``replaces`` names a pending proposal of your own on this path to retire as
    /// this one is created — how you **revise** a proposal (#164). Without it a
    /// revision is a *sibling*: two pending drafts on one base, which origofs
    /// resolves correctly on accept and incorrectly on reject, where the abandoned
    /// earlier draft stays pending with a current base and still accepts cleanly.
    ///
    /// Opt-in rather than the default for a second proposal on the same path,
    /// because two drafts a reviewer chooses between is a real workflow and origofs
    /// cannot tell it from a revision. A caller that does not know its prior id
    /// finds it with ``list_suggestions("pending", path)``.
    #[pyo3(signature = (ctx, path, data, summary=None, replaces=None))]
    fn suggest<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        data: Vec<u8>,
        summary: Option<String>,
        replaces: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let id = ws
                .suggest(c, &path, &data, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// Propose deleting `path`. ``replaces`` retires an earlier draft — see
    /// ``suggest``.
    #[pyo3(signature = (ctx, path, summary=None, replaces=None))]
    fn suggest_delete<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        summary: Option<String>,
        replaces: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let id = ws
                .suggest_delete(c, &path, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// Withdraw a pending suggestion its author has abandoned, without applying or
    /// rejecting it (#164).
    ///
    /// The standalone form of ``suggest(..., replaces=)``: use it when a draft is
    /// being retired with **nothing taking its place**. Where a replacement is
    /// being proposed in the same breath, prefer ``replaces`` — every propose call
    /// takes it, byte and CRDT alike, and then the two cannot come apart. Distinct
    /// from ``reject_suggestion``, which records that a reviewer looked and
    /// declined.
    ///
    /// The author may always retire their own; anyone else needs ``WRITE`` at its
    /// path, exactly as rejecting somebody else's proposal does.
    #[pyo3(signature = (id, ctx, reason=None))]
    fn supersede_suggestion<'py>(
        &self,
        py: Python<'py>,
        id: i64,
        ctx: WriteCtx,
        reason: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.supersede_suggestion(id, c, reason.as_deref())
                .await
                .map_err(to_pyerr)
        })
    }

    /// Suggestions, optionally filtered by `status` and/or `path`, newest first.
    #[pyo3(signature = (status=None, path=None))]
    fn list_suggestions<'py>(
        &self,
        py: Python<'py>,
        status: Option<String>,
        path: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let st = match status.as_deref() {
                Some(s) => Some(
                    SuggestionStatus::parse(s)
                        .ok_or_else(|| PyValueError::new_err(format!("unknown status {s:?}")))?,
                ),
                None => None,
            };
            let list = ws
                .list_suggestions(st, path.as_deref())
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                list.iter()
                    .map(|s| suggestion_dict(py, s))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// A single suggestion by id, or `None`.
    fn get_suggestion<'py>(&self, py: Python<'py>, id: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let s = ws.get_suggestion(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match s {
                Some(s) => suggestion_dict(py, &s).map(Some),
                None => Ok(None),
            })
        })
    }

    /// Render a suggestion as a unified line diff.
    fn suggestion_diff<'py>(&self, py: Python<'py>, id: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let patch = ws.suggestion_diff(id).await.map_err(to_pyerr)?;
            Ok(patch)
        })
    }

    /// A suggestion's base and proposed **content**, read from the store — so a
    /// reviewer UI can render an inline diff without stashing the proposed bytes
    /// itself. Returns ``{"base": str, "proposed": str | None}`` (``proposed`` is
    /// ``None`` when the suggestion proposes a deletion).
    fn suggestion_content<'py>(&self, py: Python<'py>, id: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let c = ws.suggestion_content(id).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let d = PyDict::new(py);
                d.set_item("base", c.base)?;
                d.set_item("proposed", c.proposed)?;
                Ok(d.into_any().unbind())
            })
        })
    }

    /// Accept a pending suggestion, attributed to `approver`.
    fn accept_suggestion<'py>(
        &self,
        py: Python<'py>,
        id: i64,
        approver: WriteCtx,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = approver.inner;
        future_into_py(py, async move {
            // The address now at the path (`None` for an accepted deletion), so a
            // caller can confirm what landed without re-reading (#163).
            ws.accept_suggestion(id, c).await.map_err(to_pyerr)
        })
    }

    /// Reject a pending suggestion.
    fn reject_suggestion<'py>(
        &self,
        py: Python<'py>,
        id: i64,
        approver: WriteCtx,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = approver.inner;
        future_into_py(py, async move {
            ws.reject_suggestion(id, c).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    // --- schema / migrations ------------------------------------------------
}
