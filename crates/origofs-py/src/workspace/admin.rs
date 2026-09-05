//! Operating a workspace: other workspaces in the same store, maintenance, the portable dump format, trash, and performance introspection.

use super::super::*;

#[pymethods]
impl Workspace {
    //
    // `workspace`/`workspaces` had no binding at all, so a Python caller got
    // exactly one workspace — the `default` one every `open_*` lands in — and the
    // whole workspace layer of `docs/MULTI_TENANCY.md` was unreachable from the
    // surface most services are built on.

    /// Open (creating on first use) another **workspace** in this same store.
    ///
    /// Workspaces share the store's content and identity (actors, blame, audit)
    /// and are separated by a `workspace_id`; each has its own root, refs, working
    /// tree, suggestion queue, change feed, and presence. The returned handle
    /// shares this one's connection pool and content store, so it is cheap.
    ///
    /// Note there is no actor→workspace mapping in origofs: which actor may reach
    /// which workspace is for the layer that resolves identity to enforce.
    fn workspace<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let scoped = ws.workspace(&name).await.map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: scoped }))
        })
    }

    /// The names of every workspace in this store, oldest first.
    fn workspaces<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move { ws.workspaces().await.map_err(to_pyerr) })
    }

    //
    // None of this was bound, while the *packed* constructors were — so a Python
    // caller could open a store whose space could never be reclaimed, and could
    // not back up the one half of a workspace that `fsck --rebuild` cannot
    // reconstruct.

    /// Reclaim content unreachable from any ref or the live working tree. Returns
    /// `{reachable, deleted, bytes_freed, skipped_young, skipped_undated}`.
    ///
    /// Safe alongside active writers (the sweep is age-gated), though cheapest on
    /// a quiet workspace. A packed content store additionally needs `repack()` to
    /// actually hand the space back.
    fn gc<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let stats = ws.gc().await.map_err(to_pyerr)?;
            Python::attach(|py| gc_stats_dict(py, &stats))
        })
    }

    /// [`gc`] with an explicit grace period in seconds. `0` disables the age gate
    /// and is only safe on a quiesced store; a value between 0 and the
    /// dedup-refresh floor is refused rather than silently honoured.
    fn gc_with_grace<'py>(&self, py: Python<'py>, grace_secs: u64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let stats = ws.gc_with_grace(grace_secs).await.map_err(to_pyerr)?;
            Python::attach(|py| gc_stats_dict(py, &stats))
        })
    }

    /// Seal any buffered content so it is durable. A no-op on most backends; on a
    /// packed store it seals the open pack.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.flush().await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Rewrite packs to drop dead chunks, returning the bytes reclaimed. Only a
    /// packed store has anything to do here — and on one, this is the *only* way
    /// deleted content's space comes back.
    fn repack<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move { ws.repack().await.map_err(to_pyerr) })
    }

    /// Release this workspace's backend resources (issue #154).
    ///
    /// A long-lived host — a FastAPI lifespan, a worker supervisor — opens a
    /// workspace at startup and wants its Postgres pool gone at shutdown. There
    /// was nothing to await: the pool is reclaimed when the Rust handle drops,
    /// and Python cannot make that happen on demand, so a reload or a second
    /// lifespan left the old pool alive holding its connections.
    ///
    /// Flushes first, so a packed content store's buffered chunks are sealed
    /// rather than discarded — a shutdown that loses writes is not one.
    ///
    /// One-way, and there is no reopen: call ``open_pg`` again, which is cheap.
    /// Later calls fail with an "unavailable" backend error rather than hanging
    /// or silently reconnecting, because a call after shutdown is a lifecycle bug
    /// and a store that quietly comes back hides it. Idempotent — a teardown hook
    /// that runs twice is fine.
    ///
    /// ```python
    /// @asynccontextmanager
    /// async def lifespan(app):
    ///     app.state.ws = await origofs.Workspace.open_pg(dsn, cas)
    ///     yield
    ///     await app.state.ws.aclose()
    /// ```
    ///
    /// There is deliberately no synchronous ``close()``. Every I/O method here is
    /// async, so the only way to offer one would be to block on the runtime — and
    /// called from inside a running event loop, which is where a server\'s
    /// shutdown hook lives, that deadlocks instead of closing. A footgun shaped
    /// like a convenience is worse than an absent method.
    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.close().await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Enter an ``async with`` block, yielding this workspace unchanged.
    ///
    /// The workspace is already open by the time it exists — ``open_*`` is the
    /// constructor — so entering does no work. The block exists for the exit.
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    /// Leave an ``async with`` block, closing the workspace.
    ///
    /// Returns ``None`` (never a true value), so an exception raised inside the
    /// block propagates: closing is cleanup, not error handling.
    #[pyo3(signature = (*_args))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _args: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.close().await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Write a consistent snapshot of the **metadata** store to `dest`, returning
    /// a description of what was written.
    ///
    /// This is the half of a workspace nothing can reconstruct: `rebuild()`
    /// recovers committed files, directories, symlinks, and branches from the
    /// content store alone, but blame, the audit log, the actor registry, and
    /// every uncommitted edit live only in the database. SQLite uses the online
    /// backup API, so a live workspace can be snapshotted without stopping
    /// writers; Postgres refuses and points at `pg_dump`/PITR rather than
    /// producing something that merely resembles a backup.
    fn backup_metadata<'py>(&self, py: Python<'py>, dest: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.backup_metadata(&dest).await.map_err(to_pyerr)
        })
    }

    /// Write an engine-independent metadata dump to `path`, authorized as `ctx`.
    /// Returns the number of records written.
    ///
    /// JSON Lines, one record per row. This is the half of a workspace the content
    /// store cannot rebuild — `rebuild()` recovers committed files, dirs, symlinks
    /// and branches from the bucket alone and none of the attribution — and it is
    /// the supported SQLite -> Postgres migration path.
    ///
    /// # Why this takes a `ctx` when the Rust `dump` does not
    ///
    /// A dump is whole-**store**: every workspace, every actor including its
    /// `auth_subject` (the value identity is resolved by, server-side), every ACL
    /// grant, all blame and the audit log. None of it is path-scoped, so no `Scope`
    /// narrows a dump and no subtree grant bounds it — in a workspace-per-tenant
    /// deployment, one tenant's dump reads every other tenant's metadata.
    ///
    /// So the binding is the authorized form only, and the check is `write` at `/`
    /// — the same one `commit` and an unbounded `revert_session` take. Gating a
    /// read on a write permission is deliberate: the engine has no read-side ACL,
    /// and "may write anywhere in this workspace" is the only permission that
    /// already means administrative reach over the whole of it. Where no grant
    /// covers `/`, this falls back to the actor's write policy, so a workspace with
    /// no ACLs behaves as it always did.
    ///
    /// **The content store is not dumped** — a dump references content by hash.
    /// Restoring against a store that does not hold those chunks gives you every
    /// name, actor and blame span and no readable bytes.
    fn dump_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let f = std::fs::File::create(&path).map_err(PyOSError::new_err)?;
            ws.dump_as(c, std::io::BufWriter::new(f))
                .await
                .map_err(to_pyerr)
        })
    }

    /// Restore a dump written by `dump_as` into a **pristine** store, returning
    /// `{tables, skipped_tables, source_schema_version, total_rows}`.
    ///
    /// # This is a restore, not a merge
    ///
    /// It refuses a store holding anything beyond what an open created — content,
    /// branches, registered actors, or ACL grants. Merging would have to reconcile
    /// two independent id spaces (inode, actor and session ids are all local
    /// sequences), and getting that wrong produces blame attributed to the wrong
    /// actor. Use `resync` to combine two live workspaces.
    ///
    /// The actor and grant halves of that check are what stops a load being a
    /// privilege escalation: a load replaces the identity registry and every grant
    /// with the dump's, so restoring over a configured-but-empty store would hand
    /// it the dump author's permissions. A load cannot itself be ACL-gated — the
    /// identities a check would consult are the ones it installs — so refusing a
    /// store that has any is the check, and there is deliberately no `load_as`.
    fn load<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let f = std::fs::File::open(&path).map_err(PyOSError::new_err)?;
            let report = ws
                .load(std::io::BufReader::new(f))
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                let d = PyDict::new(py);
                let tables = PyDict::new(py);
                for (t, n) in &report.tables {
                    tables.set_item(t, n)?;
                }
                d.set_item("tables", tables)?;
                d.set_item("skipped_tables", report.skipped_tables.clone())?;
                d.set_item("source_schema_version", report.source_schema_version)?;
                d.set_item("total_rows", report.total_rows())?;
                Ok(d.unbind())
            })
        })
    }

    /// Drop presence rows for sessions that stopped heartbeating more than
    /// `grace_secs` ago, returning how many were removed. A long-running server
    /// should call this periodically; nothing else does it.
    fn reap_presence<'py>(&self, py: Python<'py>, grace_secs: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.reap_presence(grace_secs).await.map_err(to_pyerr)
        })
    }

    /// Retire pending suggestions for `path` whose base content has already moved
    /// on, returning how many were superseded. Without this they sit in the review
    /// queue looking actionable and fail on accept.
    fn supersede_stale_suggestions<'py>(
        &self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.supersede_stale_suggestions(&path)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Probe both backends: `{ready, metadata, content}` where each store is
    /// `None` when healthy and an error string otherwise. The Python counterpart
    /// of the HTTP `/readyz`.
    fn ready<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let r = ws.ready().await;
            Python::attach(|py| {
                let d = PyDict::new(py);
                d.set_item("ready", r.is_ready())?;
                d.set_item("metadata", r.metadata.clone())?;
                d.set_item("content", r.content.clone())?;
                Ok(d.unbind())
            })
        })
    }

    //
    // A committed file can be read back out of history; an *uncommitted* one could
    // not be recovered at all. That gap matters more here than on an ordinary
    // filesystem because the users are agents, and "you should have committed
    // first" is not an answer when the actor that failed to commit is the same one
    // that deleted the tree.

    /// This workspace's trash retention in seconds, or `None` when trash is off.
    ///
    /// Off is the default: enabling it by default would silently change *when
    /// space is reclaimed* for every existing deployment, and the first anyone
    /// would learn of it is a storage bill.
    fn trash_retention<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(
            py,
            async move { ws.trash_retention().await.map_err(to_pyerr) },
        )
    }

    /// Enable trash with `secs` of retention, or disable it with `None`.
    ///
    /// Disabling does **not** purge what is already there — existing entries stay
    /// restorable until they are purged explicitly.
    #[pyo3(signature = (secs))]
    fn set_trash_retention<'py>(
        &self,
        py: Python<'py>,
        secs: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.set_trash_retention(secs).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Everything currently recoverable, newest deletion first. Each entry carries
    /// the actor and session that deleted it, so a restore is attributable.
    fn list_trash<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let entries = ws.list_trash().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                entries
                    .iter()
                    .map(|t| trash_entry_dict(py, t))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Put a trashed entry back at its original path, attributed to `ctx`.
    /// Returns the path it was restored to.
    fn restore_trash<'py>(
        &self,
        py: Python<'py>,
        id: i64,
        ctx: WriteCtx,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.restore_trash(id, ctx.inner).await.map_err(to_pyerr)
        })
    }

    /// Permanently drop one trash entry, reporting whether one was there.
    fn purge_trash<'py>(&self, py: Python<'py>, id: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(
            py,
            async move { ws.purge_trash(id).await.map_err(to_pyerr) },
        )
    }

    /// Permanently drop every trash entry whatever its age, returning how many.
    fn empty_trash<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move { ws.empty_trash().await.map_err(to_pyerr) })
    }

    /// Remove a path, capturing it into the trash first when retention is on.
    ///
    /// The unattributed counterpart for a surface with no actor context — prefer
    /// `remove_or_propose` wherever an actor is known, so the deletion carries
    /// blame and the trash entry names who made it.
    fn remove_trashing<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.remove_trashing(&path).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// What one file costs to read: chunk count, size distribution, self-dedup,
    /// and — when `probe` is set — whether the store still holds the chunks.
    ///
    /// `probe` is the only part that touches the content backend, at one `has` per
    /// distinct chunk (one HEAD each against object storage), so it is a parameter
    /// and not unconditional; everything else comes from the manifest a read would
    /// have fetched anyway. Errors the way a read would, so `file_layout` and
    /// `read` disagree about a path only when the read path itself is broken.
    #[pyo3(signature = (path, probe = false))]
    fn file_layout<'py>(
        &self,
        py: Python<'py>,
        path: String,
        probe: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let layout = ws.file_layout(&path, probe).await.map_err(to_pyerr)?;
            Python::attach(|py| file_layout_dict(py, &layout))
        })
    }

    /// Write, read, and re-read generated files against **this** workspace's
    /// backends, reporting throughput and latency per phase.
    ///
    /// This is a **mutating** call: it writes and then deletes `bench-NNNN.bin`
    /// under `dir`, and refuses to start in a directory that already holds
    /// anything unless `force` is set. It is the measurement that cannot be
    /// borrowed from someone else's hardware — bucket latency, whether packing is
    /// on, what the concurrency windows are set to.
    ///
    /// Defaults are 8 files of 8 MiB under `/.origofs-bench`, sized to finish in
    /// seconds; raise both for a real measurement. `seed` defaults to a fresh
    /// value per run — pin it to reproduce one.
    #[pyo3(signature = (
        dir = None,
        files = None,
        file_size = None,
        seed = None,
        keep = false,
        force = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn bench<'py>(
        &self,
        py: Python<'py>,
        dir: Option<String>,
        files: Option<usize>,
        file_size: Option<u64>,
        seed: Option<u64>,
        keep: bool,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            // Start from the engine's own defaults rather than restating them
            // here, so the Python surface cannot drift from the Rust one.
            let mut opts = BenchOpts::new();
            if let Some(d) = dir {
                opts.dir = d;
            }
            if let Some(n) = files {
                opts.files = n;
            }
            if let Some(n) = file_size {
                opts.file_size = n;
            }
            if let Some(s) = seed {
                opts.seed = s;
            }
            opts.keep = keep;
            opts.force = force;
            let report = ws.bench(&opts).await.map_err(to_pyerr)?;
            Python::attach(|py| bench_report_dict(py, &report))
        })
    }
}
