//! Path-scoped grants and the workspace switches that govern them.

use super::super::*;

#[pymethods]
impl Workspace {
    //
    // `set_write_policy` is per **actor**, whole workspace, and takes no path — a
    // trust gate, not an access-control system. A grant is
    // `(actor, path_prefix) -> perms` with longest matching prefix winning, which
    // makes "may write /docs, may only propose under /src" representable.
    //
    // Permissions are named, not a bitmask: `"write"`, `"read+write"`, or
    // `["read", "propose"]`. An empty list is an explicit deny for that subtree.

    /// Grant `perms` to an actor under `path_prefix` (absolute; `"/"` is the whole
    /// workspace). `granted_by` names the actor making the change, for the audit
    /// trail — every grant change is recorded in the change feed.
    ///
    /// A relative prefix is refused rather than read as absolute: a grant that
    /// silently applied to a subtree the operator did not mean would fail open.
    #[pyo3(signature = (actor_id, path_prefix, perms, granted_by = None))]
    fn grant<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        path_prefix: String,
        perms: &Bound<'py, PyAny>,
        granted_by: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let perms = parse_perms(perms)?;
        future_into_py(py, async move {
            ws.fs()
                .grant(actor_id, &path_prefix, perms, granted_by)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Remove a grant, reporting whether one was there.
    #[pyo3(signature = (actor_id, path_prefix, revoked_by = None))]
    fn revoke<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        path_prefix: String,
        revoked_by: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.fs()
                .revoke(actor_id, &path_prefix, revoked_by)
                .await
                .map_err(to_pyerr)
        })
    }

    /// `grant`, performed **by** `ctx` and checked: the granter needs `write` at
    /// the prefix, and may not hand on a permission it does not hold there.
    /// `granted_by` is recorded from `ctx`, not supplied by the caller.
    ///
    /// **This is the form a service must use.** Plain `grant` takes no
    /// authorization at all — it exists for provisioning, which has no actor to
    /// check — so an admin endpoint built on it would let any authenticated caller
    /// grant itself `write` at `/`. Raises `PermissionError` when refused.
    fn grant_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        actor_id: i64,
        path_prefix: String,
        perms: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let perms = parse_perms(perms)?;
        future_into_py(py, async move {
            ws.grant_as(c, actor_id, &path_prefix, perms)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// `revoke`, performed **by** `ctx` and checked: `write` at the prefix, the
    /// same administrative gate as `grant_as`.
    fn revoke_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        actor_id: i64,
        path_prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.revoke_as(c, actor_id, &path_prefix)
                .await
                .map_err(to_pyerr)
        })
    }

    /// `set_acl_default_deny`, checked at the root — a workspace switch reaches
    /// every path, so it takes the whole-workspace check.
    fn set_acl_default_deny_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        deny: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.set_acl_default_deny_as(c, deny).await.map_err(to_pyerr)
        })
    }

    /// `set_acl_enforce_reads`, checked at the root. Ungated, an actor denied a
    /// read could switch enforcement off and retry.
    fn set_acl_enforce_reads_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        on: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.set_acl_enforce_reads_as(c, on).await.map_err(to_pyerr)
        })
    }

    /// `set_write_policy`, checked at the root — the policy is the fallback
    /// wherever no grant applies, so setting it reaches every path.
    fn set_write_policy_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        actor_id: i64,
        policy: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let p = CoreWritePolicy::parse(&policy).ok_or_else(|| {
                to_pyerr(origofs_sdk::OrigoFSError::InvalidArgument(format!(
                    "unknown write policy {policy:?} (expected `direct` or `propose`)"
                )))
            })?;
            ws.set_write_policy_as(c, actor_id, p)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Every grant in this workspace, or just one actor's, as a list of
    /// `{actor_id, path_prefix, perms, granted_at, granted_by}`.
    #[pyo3(signature = (actor_id = None))]
    fn list_grants<'py>(
        &self,
        py: Python<'py>,
        actor_id: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let grants = ws.fs().list_grants(actor_id).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                grants
                    .iter()
                    .map(|g| acl_grant_dict(py, g))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// The permissions an actor has at `path`, as a list of names.
    ///
    /// Longest matching prefix wins, matched on directory boundaries. With **no**
    /// matching grant this falls back to the actor's write policy rather than
    /// denying — grants are additive refinement, so a workspace that has never
    /// written one behaves exactly as it did before ACLs existed. Flip that with
    /// `set_acl_default_deny(True)`.
    fn effective_perms<'py>(
        &self,
        py: Python<'py>,
        actor_id: i64,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let perms = ws
                .fs()
                .effective_perms(actor_id, &path)
                .await
                .map_err(to_pyerr)?;
            Ok(perms_list(perms))
        })
    }

    /// Whether an actor with no matching grant is denied rather than falling back
    /// to its write policy.
    fn acl_default_deny<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.fs().acl_default_deny().await.map_err(to_pyerr)
        })
    }

    /// Switch the workspace between fallback (the default) and deny-by-default.
    ///
    /// Deny-by-default is the safer posture and the wrong *default*: turning it on
    /// stops every actor that has no explicit grant, which is all of them until an
    /// operator writes some. Making it a deliberate switch means the grants get
    /// written first.
    fn set_acl_default_deny<'py>(
        &self,
        py: Python<'py>,
        deny: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.fs().set_acl_default_deny(deny).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Refuse an operation at `path` for an actor without `write` there, the
    /// path-bearing counterpart of `ensure_may_write` (issue #123). Raises
    /// `PermissionError`, or returns `None` if allowed.
    ///
    /// The denial deliberately says only that the actor may not perform the op,
    /// never whether the path exists — the check runs before any lookup precisely
    /// so it cannot leak existence.
    fn ensure_may_write_at<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        op: String,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.fs()
                .ensure_may_write_at(ctx.inner, &op, &path)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Whether reads are checked against `read` grants (issue #124). Off by
    /// default; see `set_acl_enforce_reads`.
    fn acl_enforce_reads<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(
            py,
            async move { ws.acl_enforce_reads().await.map_err(to_pyerr) },
        )
    }

    /// Turn read enforcement on or off for this workspace.
    ///
    /// Off by default, and deliberately a switch rather than a default: reads have
    /// never been checked, so an existing workspace holds no read grants and
    /// turning this on without writing them first stops every actor at once — the
    /// same hazard `set_acl_default_deny` carries.
    fn set_acl_enforce_reads<'py>(&self, py: Python<'py>, on: bool) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.set_acl_enforce_reads(on).await.map_err(to_pyerr)
        })
    }

    /// Refuse a read of `path` for an actor without `read` there. Raises
    /// `PermissionError`, or returns `None` if allowed. A no-op unless the
    /// workspace has read enforcement on.
    fn ensure_may_read_at<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        op: String,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.ensure_may_read_at(ctx.inner, &op, &path)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// `read`, checked against `read` at the path.
    fn read_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let bytes = ws.read_as(c, &path).await.map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    /// `read_range`, checked against `read` at the path.
    fn read_range_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        off: u64,
        len: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let bytes = ws
                .read_range_as(c, &path, off, len)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    /// `stat`, checked against `read` at the path.
    fn stat_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let inode = ws.stat_as(c, &path).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// `readlink`, checked against `read` at the path.
    fn readlink_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.readlink_as(c, &path).await.map_err(to_pyerr)
        })
    }

    /// `blame`, checked against `read` at the path — blame reports who wrote which
    /// bytes, so it is a read of the file by another name.
    fn blame_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let ranges = ws.blame_as(c, &path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let out: PyResult<Vec<_>> = ranges.iter().map(|b| blame_dict(py, b)).collect();
                Ok(out?.into_pyobject(py)?.unbind().into_any())
            })
        })
    }

    /// `ls`, checked against `read` at the directory **and at every entry**.
    ///
    /// An entry the actor may not read is absent rather than refused, so the
    /// listing and `stat_as` agree about it — if they disagreed, the difference
    /// between them would be an existence oracle.
    fn ls_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let entries = ws.ls_as(c, &path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let out: PyResult<Vec<_>> = entries.iter().map(|e| dir_entry_dict(py, e)).collect();
                Ok(out?.into_pyobject(py)?.unbind().into_any())
            })
        })
    }

    /// `diff`, with entries at unreadable paths removed.
    fn diff_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        from_: String,
        to: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let changes = ws.diff_as(c, &from_, &to).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                changes
                    .iter()
                    .map(|d| diff_dict(py, d))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// `diff_file`, checked against `read` at the path — a unified diff of a file
    /// is that file's content in another arrangement.
    fn diff_file_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        from_: String,
        to: String,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.diff_file_as(c, &from_, &to, &path)
                .await
                .map_err(to_pyerr)
        })
    }

    /// `presence`, with sessions at unreadable paths removed — and sessions
    /// naming no path removed too, because a row with no path still says a
    /// neighbour is connected.
    fn presence_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        window_secs: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let list = ws.presence_as(c, window_secs).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                list.iter()
                    .map(|p| presence_dict(py, p))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// `list_suggestions`, with proposals against unreadable paths removed.
    #[pyo3(signature = (ctx, status=None, path=None))]
    fn list_suggestions_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        status: Option<String>,
        path: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let st = match status.as_deref() {
                Some(s) => Some(
                    SuggestionStatus::parse(s)
                        .ok_or_else(|| PyValueError::new_err(format!("unknown status {s:?}")))?,
                ),
                None => None,
            };
            let list = ws
                .list_suggestions_as(c, st, path.as_deref())
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                list.iter()
                    .map(|s| suggestion_dict(py, s))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// `get_suggestion`, answering ``None`` for a proposal against a path the
    /// actor may not read.
    ///
    /// Not found rather than denied: a suggestion id is a guessable,
    /// workspace-global handle, so a refusal would confirm one exists at that id
    /// — the existence answer the check is there to withhold.
    fn get_suggestion_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let s = ws.get_suggestion_as(c, id).await.map_err(to_pyerr)?;
            Python::attach(|py| match s {
                Some(s) => suggestion_dict(py, &s).map(Some),
                None => Ok(None),
            })
        })
    }

    /// `suggestion_diff`, raising the ordinary not-found for a proposal against a
    /// path the actor may not read.
    fn suggestion_diff_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.suggestion_diff_as(c, id).await.map_err(to_pyerr)
        })
    }

    /// `live_doc`, answering ``None`` for a path the actor may not read — a
    /// filter, because "is this path live" is an existence question.
    fn live_doc_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let live = ws.live_doc_as(c, &path).await.map_err(to_pyerr)?;
            Python::attach(|py| match live {
                Some(l) => live_doc_dict(py, &l).map(Some),
                None => Ok(None),
            })
        })
    }

    /// `live_paths`, with unreadable paths removed. Unfiltered it is a
    /// workspace-wide list of exactly which files someone is editing right now.
    fn live_paths_as<'py>(&self, py: Python<'py>, ctx: WriteCtx) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let list = ws.live_paths_as(c).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                list.iter()
                    .map(|l| live_doc_dict(py, l))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Refuse an op that reaches **every** path for an actor without `write` at
    /// `/` — the path-less counterpart of `ensure_may_write_at` (issue #123).
    ///
    /// Having no path is not the same as touching none: `commit`, `checkout`,
    /// `create_branch`, an unbounded `revert_session` and a `dump` all reach the
    /// whole workspace, so they are checked at the root rather than skipping the
    /// grant layer. A surface that adds its own workspace-wide route wants this
    /// one; the bound methods already call it themselves.
    ///
    /// Raises `PermissionError`, or returns `None` if allowed.
    fn ensure_may_write_workspace<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        op: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.fs()
                .ensure_may_write_workspace(ctx.inner, &op)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }
}
