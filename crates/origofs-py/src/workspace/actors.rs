//! Who is writing: actors, sessions, blame, write policy, the op-log, session revert, and attribution completeness.

use super::super::*;

#[pymethods]
impl Workspace {
    /// Set an actor's write policy: `"direct"` (writes land) or `"propose"` (writes
    /// are routed through the suggestion queue for review by a different actor). A
    /// bounded, actor-agnostic trust gate; the default is `"direct"`.
    fn set_write_policy<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        policy: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let p = CoreWritePolicy::parse(&policy).ok_or_else(|| {
                to_pyerr(origofs_sdk::OrigoFSError::InvalidArgument(format!(
                    "unknown write policy {policy:?} (expected `direct` or `propose`)"
                )))
            })?;
            ws.set_write_policy(actor_id, p).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Register a human actor; returns its id.
    #[pyo3(signature = (name, auth_subject=None))]
    fn create_human<'py>(
        &self,
        py: Python<'py>,
        name: String,
        auth_subject: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let id = ws
                .create_human(&name, auth_subject.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// Register an agent actor (optionally controlled by a human); returns id.
    #[pyo3(signature = (name, model, controller=None))]
    fn create_agent<'py>(
        &self,
        py: Python<'py>,
        name: String,
        model: String,
        controller: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let id = ws
                .create_agent(&name, &model, controller)
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// Look up an actor by external identity (`auth_subject`); returns a dict or
    /// `None`. Use this (or `find_or_create_*`) to map your app's user id to an
    /// origofs actor without keeping a side table.
    fn actor_by_subject<'py>(
        &self,
        py: Python<'py>,
        subject: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let found = ws.actor_by_subject(&subject).await.map_err(to_pyerr)?;
            Python::attach(|py| match found {
                Some(a) => Ok(Some(actor_dict(py, &a)?)),
                None => Ok(None),
            })
        })
    }

    /// Look up an actor by its numeric id, or `None`. Resolves the bare
    /// `actor_id` carried by events/suggestions/presence to a full actor dict.
    fn actor<'py>(&self, py: Python<'py>, id: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let found = ws.get_actor(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match found {
                Some(a) => Ok(Some(actor_dict(py, &a)?)),
                None => Ok(None),
            })
        })
    }

    /// Every registered actor (oldest first). Handy to build a client-side
    /// directory that resolves the `actor_id` in events/suggestions to a name.
    fn list_actors<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let actors = ws.list_actors().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                actors
                    .iter()
                    .map(|a| actor_dict(py, a))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Idempotently map your app's user id (`auth_subject`) to a **human** actor:
    /// returns the existing actor for that subject, or creates one. Race-safe.
    fn find_or_create_human<'py>(
        &self,
        py: Python<'py>,
        auth_subject: String,
        display_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.find_or_create_human(&auth_subject, &display_name)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Idempotently map an external identity to an **agent** actor.
    #[pyo3(signature = (auth_subject, display_name, model, controller=None))]
    fn find_or_create_agent<'py>(
        &self,
        py: Python<'py>,
        auth_subject: String,
        display_name: String,
        model: String,
        controller: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.find_or_create_agent(&auth_subject, &display_name, &model, controller)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Open a session for an actor; returns its id.
    #[pyo3(signature = (actor_id, client=None))]
    fn create_session<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        client: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let id = ws
                .create_session(actor_id, client.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// Per-byte-range authorship for a path (each span carries `byte_start`/
    /// `byte_end`, the derived `line_start`/`line_end`, `session`, and `actor`).
    fn blame<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let ranges = ws.blame(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                ranges
                    .iter()
                    .map(|b| blame_dict(py, b))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    //
    // `revert_session` is a headline feature ("undo just the agent's work") that
    // existed *only* in the Rust SDK — no CLI subcommand, no HTTP route, no MCP
    // tool, no binding.

    /// Remove exactly the lines an actor authored in one session, across every
    /// file that session touched, leaving other actors' edits intact. Returns the
    /// list of paths changed.
    ///
    /// `path_prefix` bounds the revert to one subtree, matched on directory
    /// boundaries — `/tenant-a` covers `/tenant-a/notes.txt` and never
    /// `/tenant-abc/notes.txt`. A multi-tenant host needs it: an "undo this
    /// agent's work" button lives in one tenant's UI, and an unscoped revert
    /// would follow the session wherever else it wrote. Filtering here rather
    /// than pre-flighting with `edit_ops` also closes the window where a write
    /// lands between the check and the revert.
    ///
    /// ```python
    /// changed = await ws.revert_session(agent, session, path_prefix="/tenant-a")
    /// ```
    #[pyo3(signature = (actor_id, session_id, path_prefix = None))]
    fn revert_session<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        session_id: i64,
        path_prefix: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.revert_session(actor_id, session_id, path_prefix.as_deref())
                .await
                .map_err(to_pyerr)
        })
    }

    /// [`revert_session`](Self::revert_session), authorized against `ctx`.
    ///
    /// The target actor/session stay parameters — a revert is a review action
    /// performed on someone else's work — while `ctx` is the reviewer performing
    /// it, who must hold write permission over what is being reverted: the named
    /// subtree, or the whole workspace when `path_prefix` is `None`.
    ///
    /// **A surface serving possibly-untrusted callers wants this one.**
    ///
    /// ```python
    /// changed = await ws.revert_session_as(reviewer, agent, session, path_prefix="/tenant-a")
    /// ```
    #[pyo3(signature = (ctx, actor_id, session_id, path_prefix = None))]
    fn revert_session_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        actor_id: i64,
        session_id: i64,
        path_prefix: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.revert_session_as(c, actor_id, session_id, path_prefix.as_deref())
                .await
                .map_err(to_pyerr)
        })
    }

    /// The append-only edit-op log for an actor (optionally one session) — the
    /// ground truth behind blame, as a list of dicts.
    #[pyo3(signature = (actor_id, session_id = None))]
    fn edit_ops<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        session_id: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let ops = ws.edit_ops(actor_id, session_id).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                ops.into_iter()
                    .map(|o| {
                        let d = PyDict::new(py);
                        d.set_item("id", o.id)?;
                        d.set_item("actor_id", o.actor_id)?;
                        d.set_item("session_id", o.session_id)?;
                        d.set_item("path", o.path)?;
                        d.set_item("op", o.op)?;
                        d.set_item("byte_start", o.byte_start)?;
                        d.set_item("byte_len", o.byte_len)?;
                        d.set_item("pre_hash", o.pre_hash)?;
                        d.set_item("post_hash", o.post_hash)?;
                        d.set_item("ts", o.ts)?;
                        Ok(d.unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Whether this workspace requires every surface-initiated mutation to name an
    /// actor. Off by default.
    ///
    /// This is an attribution-**completeness** switch, not a security boundary: it
    /// makes an unattributed mutation an error so a blame trail has no holes in it,
    /// and it says nothing about who may do what. `grant`/`revoke` and the write
    /// policy are the access-control layer.
    fn require_attribution<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.require_attribution().await.map_err(to_pyerr)
        })
    }

    /// Turn the attribution requirement on or off.
    fn set_require_attribution<'py>(
        &self,
        py: Python<'py>,
        required: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.set_require_attribution(required)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Whether this workspace answers POSIX advisory locks itself (issue #119).
    ///
    /// Off by default. A FUSE mount that does not answer `setlk` still has
    /// working advisory locks — the kernel serves them locally, per mount — so
    /// this is not "locking on/off", it is whether locks are coordinated
    /// *between* mounts.
    fn posix_locks_enabled<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.posix_locks_enabled().await.map_err(to_pyerr)
        })
    }

    /// Turn cross-mount advisory locking on or off.
    fn set_posix_locks_enabled<'py>(
        &self,
        py: Python<'py>,
        on: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.set_posix_locks_enabled(on).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// The advisory locks currently held on `path`, live leases only.
    ///
    /// Read-only on purpose. The locks are taken by *mounts*, whose lifetime is
    /// what a lock is scoped to and whose renewal timer is what keeps its lease
    /// alive; a lock taken by a library call would have neither and would quietly
    /// expire under its holder. So Python can see who holds what — which is the
    /// service-side question — and a mount is what takes them.
    fn posix_locks<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let held = ws.posix_locks(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                held.into_iter()
                    .map(|l| {
                        let d = PyDict::new(py);
                        d.set_item("owner", l.owner)?;
                        d.set_item("holder", l.holder)?;
                        d.set_item("pid", l.pid)?;
                        d.set_item("start", l.start)?;
                        d.set_item("end", l.end)?;
                        d.set_item("exclusive", l.exclusive)?;
                        Ok(d.unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Refuse an unattributed mutation when this workspace requires attribution.
    ///
    /// **A surface calls this on the path where no actor was named** — it is what
    /// makes `require_attribution` mean anything. Enforcement is surface-side by
    /// design (the unattributed engine ops exist for internal machinery and are
    /// exempt by construction), so a workspace with the switch on is only actually
    /// enforced on the surfaces that call it. The CLI does; a Python service has to,
    /// and could not before this was bound.
    ///
    /// `op` names the operation in the error. Raises `PermissionError`, or returns
    /// `None` when attribution is not required.
    fn ensure_attributed<'py>(&self, py: Python<'py>, op: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.ensure_attributed(&op).await.map_err(to_pyerr)?;
            Ok(())
        })
    }
}
