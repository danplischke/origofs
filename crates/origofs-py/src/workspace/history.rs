//! Commits, branches, diffs, merges, replication between workspaces, LFS-style path locks, and the schema.

use super::super::*;

#[pymethods]
impl Workspace {
    /// Snapshot the working tree into a commit; returns the commit hash (hex).
    fn commit<'py>(
        &self,
        py: Python<'py>,
        author: String,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let h = ws.commit(&author, &message).await.map_err(to_pyerr)?;
            Ok(h.to_hex())
        })
    }

    /// Commit history (HEAD, first-parent), newest first.
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let log = ws.log().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                log.iter()
                    .map(|c| commit_dict(py, c))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Working-tree changes relative to HEAD.
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let changes = ws.status().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                changes
                    .iter()
                    .map(|d| diff_dict(py, d))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Changed paths between two refs/commits (`from_` -> `to`; see `rename`
    /// for why the parameter is `from_` and not `from`).
    fn diff<'py>(&self, py: Python<'py>, from_: String, to: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let changes = ws.diff(&from_, &to).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                changes
                    .iter()
                    .map(|d| diff_dict(py, d))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// A unified line diff of one path between two refs/commits.
    fn diff_file<'py>(
        &self,
        py: Python<'py>,
        from_: String,
        to: String,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let patch = ws.diff_file(&from_, &to, &path).await.map_err(to_pyerr)?;
            Ok(patch)
        })
    }

    /// Create a branch at the current HEAD commit.
    fn create_branch<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.create_branch(&name).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Switch the working tree to a branch.
    fn checkout<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.checkout(&name).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// All branches as `{name, hash}`.
    fn branches<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let branches = ws.list_branches().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                branches
                    .iter()
                    .map(|(name, hash)| {
                        let d = PyDict::new(py);
                        d.set_item("name", name)?;
                        d.set_item("hash", hash.to_hex())?;
                        Ok(d.into_any().unbind())
                    })
                    .collect::<PyResult<Vec<Py<PyAny>>>>()
            })
        })
    }

    /// The current branch name (or `None` if detached).
    fn current_branch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let b = ws.current_branch().await.map_err(to_pyerr)?;
            Ok(b)
        })
    }

    /// Rebuild refs + the working tree from the content store's object graph, for
    /// disaster recovery after the metadata DB is lost. Open a workspace with a
    /// FRESH metadata DB pointed at the surviving content store (same S3/dir),
    /// then call this: it recovers committed files, directories, symlinks, and
    /// branch names/tips. Returns a report dict. Blame/attribution and
    /// uncommitted edits are NOT recovered (they live only in the DB).
    fn rebuild<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let report = ws.rebuild().await.map_err(to_pyerr)?;
            Python::attach(|py| rebuild_report_dict(py, &report))
        })
    }

    /// Read-only companion to `rebuild`: report what a rebuild would recover
    /// (commits, branches, the branch that would be checked out) without
    /// modifying the workspace.
    fn scan<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let report = ws.scan().await.map_err(to_pyerr)?;
            Python::attach(|py| rebuild_report_dict(py, &report))
        })
    }

    // --- attribution --------------------------------------------------------

    /// The metadata DB's schema state as `{current, latest, up_to_date}`. origofs
    /// migrates forward automatically on open; this lets you introspect it.
    fn schema_version<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let current = ws.schema_version().await.map_err(to_pyerr)?;
            let latest = ws.latest_schema_version();
            Python::attach(|py| {
                let d = PyDict::new(py);
                d.set_item("current", current)?;
                d.set_item("latest", latest)?;
                d.set_item("up_to_date", current >= latest)?;
                Ok(d.into_any().unbind())
            })
        })
    }

    /// Apply any pending metadata migrations (idempotent — a normal open already
    /// does this). Returns `{from, to, migrated}`. Forward-only.
    fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let (from, to) = ws.migrate().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let d = PyDict::new(py);
                d.set_item("from", from)?;
                d.set_item("to", to)?;
                d.set_item("migrated", to > from)?;
                Ok(d.into_any().unbind())
            })
        })
    }

    // --- mounting / serving -------------------------------------------------

    //
    // `create_branch`/`checkout` were bound but `merge` was not, which made
    // branching a one-way door from Python: you could diverge and never reconcile.

    /// Merge `branch` into the current branch. Returns
    /// `{outcome, commit, conflicts}` — `outcome` is one of `"up_to_date"`,
    /// `"fast_forward"`, `"merged"`, or `"conflicts"`.
    #[pyo3(signature = (branch, author, message = None))]
    fn merge_branch<'py>(
        &self,
        py: Python<'py>,
        branch: String,
        author: String,
        message: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let message = message.unwrap_or_else(|| format!("merge {branch}"));
            let outcome = ws
                .merge_branch(&branch, &author, &message)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| merge_outcome_dict(py, &outcome))
        })
    }

    /// Merge the commit `theirs` (a hex hash) into the current branch — the
    /// by-hash counterpart of `merge_branch`, for merging something with no branch
    /// name: a detached head, a commit read out of `log`, or a ref another
    /// workspace advanced.
    fn merge<'py>(
        &self,
        py: Python<'py>,
        theirs: String,
        author: String,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let h = parse_hash(&theirs)?;
            let outcome = ws.merge(h, &author, &message).await.map_err(to_pyerr)?;
            Python::attach(|py| merge_outcome_dict(py, &outcome))
        })
    }

    /// Reconcile `branch` with `remote` in both directions: fetch what the remote
    /// has, push what it lacks, merge if the two diverged, and advance both refs.
    /// Returns a report dict.
    ///
    /// Per-byte-range blame **travels with the content** both ways, with actors
    /// matched on `auth_subject` so the same person resolves to one actor across
    /// resyncs. The op-log, audit log, change feed and pending suggestions do not.
    /// Both working trees must be clean, both workspaces must have versioning
    /// enabled, and `branch` must be the local current branch.
    ///
    /// A conflicted merge leaves the conflicts in *this* workspace's working tree
    /// with `MERGE_HEAD` set, exactly as `merge` does, and does not advance the
    /// remote: resolve, commit, and resync again.
    fn resync<'py>(
        &self,
        py: Python<'py>,
        remote: PyRef<'py, Workspace>,
        branch: String,
        author: String,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let other = remote.inner.clone();
        future_into_py(py, async move {
            let report = ws
                .resync(&other, &branch, &author, &message)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| resync_report_dict(py, &report))
        })
    }

    /// Copy the commit closure reachable from `head` into `remote`'s content
    /// store, stopping at objects it already has. Returns
    /// `{objects, bytes, skipped}`.
    ///
    /// The push half of `resync` on its own: it moves objects only and never
    /// touches a ref, so it is safe to run ahead of time to make a later resync
    /// cheap — which is the point, since the object copy is the slow part.
    fn push_objects<'py>(
        &self,
        py: Python<'py>,
        remote: PyRef<'py, Workspace>,
        head: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let other = remote.inner.clone();
        future_into_py(py, async move {
            let h = parse_hash(&head)?;
            let stats = ws.push_objects(&other, h).await.map_err(to_pyerr)?;
            Python::attach(|py| transfer_stats_dict(py, &stats))
        })
    }

    /// The fetch half: copy the closure of `head` **from** `remote` into this
    /// workspace's content store. Refs are untouched.
    fn fetch_objects<'py>(
        &self,
        py: Python<'py>,
        remote: PyRef<'py, Workspace>,
        head: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let other = remote.inner.clone();
        future_into_py(py, async move {
            let h = parse_hash(&head)?;
            let stats = ws.fetch_objects(&other, h).await.map_err(to_pyerr)?;
            Python::attach(|py| transfer_stats_dict(py, &stats))
        })
    }

    /// Unresolved merge conflicts as a list of `{path, kind}`.
    fn conflicts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let conflicts = ws.conflicts().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                conflicts
                    .into_iter()
                    .map(|(path, kind)| {
                        let d = PyDict::new(py);
                        d.set_item("path", path)?;
                        d.set_item("kind", kind)?;
                        Ok(d.unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// This workspace's versioning mode: `"off"`, `"native"`, or `"git"`.
    fn versioning_mode<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let mode = ws.versioning_mode().await.map_err(to_pyerr)?;
            Ok(mode.as_str().to_string())
        })
    }

    /// Set the versioning mode. `"off"` disables commits entirely.
    fn set_versioning_mode<'py>(
        &self,
        py: Python<'py>,
        mode: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let parsed = origofs_sdk::VersioningMode::parse(&mode).ok_or_else(|| {
                PyValueError::new_err(format!(
                    "unknown versioning mode {mode:?}; expected \"off\", \"native\", or \"git\""
                ))
            })?;
            ws.set_versioning_mode(parsed).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Take an advisory exclusive lock on `path`. Returns `True` if acquired.
    fn lock<'py>(
        &self,
        py: Python<'py>,
        path: String,
        owner: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(
            py,
            async move { ws.lock(&path, &owner).await.map_err(to_pyerr) },
        )
    }

    /// Release a lock held by `owner`. Returns `True` if one was released.
    fn unlock<'py>(
        &self,
        py: Python<'py>,
        path: String,
        owner: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.unlock(&path, &owner).await.map_err(to_pyerr)
        })
    }

    /// Held locks as a list of `{path, owner, acquired_at}`.
    fn locks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let locks = ws.locks().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                locks
                    .into_iter()
                    .map(|(path, owner, at)| {
                        let d = PyDict::new(py);
                        d.set_item("path", path)?;
                        d.set_item("owner", owner)?;
                        d.set_item("acquired_at", at)?;
                        Ok(d.unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }
}
