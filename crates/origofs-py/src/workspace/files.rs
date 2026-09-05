//! The POSIX-shaped surface — reads, writes, the namespace, symlinks, and the path-addressed metadata ops — in both their unattributed and attributed forms.

use super::super::*;

#[pymethods]
impl Workspace {
    /// Read a file's bytes.
    fn read<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let bytes = ws.read(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    /// Read the byte range `[off, off+len)` of a file, clamped at EOF (so a `len`
    /// past the end returns only what's there, and an `off` at/after the end
    /// returns `b""`). Only the chunks covering the range are fetched from the
    /// content store, not the whole file — the primitive a range-oriented client
    /// (fsspec, columnar/Parquet readers, HTTP range requests) reads through.
    fn read_range<'py>(
        &self,
        py: Python<'py>,
        path: String,
        off: u64,
        len: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let bytes = ws.read_range(&path, off, len).await.map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    /// Write a file (unattributed). Creates parent directories.
    fn write<'py>(
        &self,
        py: Python<'py>,
        path: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.write(&path, &data).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Stream a file from `src_path` into the workspace at `path`, **attributed**
    /// to `ctx`.
    ///
    /// The way to write a file larger than memory. `write`/`write_as` take a
    /// `bytes` object and copy it into Rust, so a write of an N-byte payload holds
    /// roughly 3N transiently (the Python object, the copy, the chunker's
    /// buffers). This opens the file in Rust and streams it: no bytes cross into
    /// Python at all, and resident memory is bounded regardless of file size.
    ///
    /// Subject to the write policy — a propose-only actor gets `PermissionError`.
    /// Blame covers the whole file rather than being diffed against the previous
    /// body: a streamed write is a wholesale replacement, and not holding the
    /// previous body is the entire point. Use `write_as` when the file fits in
    /// memory *and* its line-level provenance matters.
    ///
    /// ```python
    /// await ws.write_path_as(ctx, "/dataset.parquet", "/tmp/dataset.parquet")
    /// ```
    fn write_path_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        src_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            // Opened on the blocking pool: `File::open` hits the filesystem, and
            // this runs on the same runtime that serves the rest of the process.
            let file = tokio::task::spawn_blocking(move || std::fs::File::open(&src_path))
                .await
                .map_err(|e| PyOSError::new_err(format!("opening the source file panicked: {e}")))?
                .map_err(io_err)?;
            ws.write_reader_as(c, &path, file).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Stream a file from `src_path` into the workspace at `path`, unattributed.
    ///
    /// The counterpart of [`write_path_as`] for genuinely actor-less imports.
    /// Records no blame and no edit-op, and is exempt from the write policy —
    /// prefer `write_path_as` wherever an actor is known.
    fn write_path<'py>(
        &self,
        py: Python<'py>,
        path: String,
        src_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let file = tokio::task::spawn_blocking(move || std::fs::File::open(&src_path))
                .await
                .map_err(|e| PyOSError::new_err(format!("opening the source file panicked: {e}")))?
                .map_err(io_err)?;
            ws.write_reader(&path, file).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Stream a workspace file out to `dest_path` on the local filesystem.
    ///
    /// The read counterpart: `read` returns the whole body as a `bytes` object
    /// (two full copies — the reassembly buffer and the Python object), so it is
    /// bounded by memory. This streams chunk by chunk and is not.
    ///
    /// For a partial read, `read_range` already fetches only the covering chunks.
    fn read_to_path<'py>(
        &self,
        py: Python<'py>,
        path: String,
        dest_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            // `read_to_writer` drives an async writer, so this is `tokio::fs`
            // rather than `std::fs` — the write side stays off the runtime thread
            // without a manual `spawn_blocking` per chunk.
            let file = tokio::fs::File::create(&dest_path).await.map_err(io_err)?;
            let written = ws.read_to_writer(&path, file).await.map_err(to_pyerr)?;
            Ok(written)
        })
    }

    /// Write a file attributed to `ctx` (records blame + an edit-op). This is
    /// how you inject the authenticated user/agent behind a request.
    fn write_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.write_as(c, &path, &data).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Attributed write with **explicit** byte-range authorship — the path an
    /// editor integration takes when it already knows who typed what.
    ///
    /// `spans` is a list of `(actor_id, session_id, byte_len)` runs summing to
    /// `len(data)`, so co-edited content lands with each collaborator's spans
    /// attributed exactly — sub-line and interleaved — instead of going through the
    /// line-diff heuristic `write_as` uses. `ctx` is the actor performing the
    /// checkpoint, recorded on the op-log and the feed.
    ///
    /// `checkpoint_coedit` does this for you for a `CoeditDoc`. Reach for this one
    /// when the document lives in *your* editor rather than in origofs's CRDT.
    fn write_as_blamed<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        data: Vec<u8>,
        spans: Vec<(i64, i64, u64)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.write_as_blamed(c, &path, &data, &spans)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Submit an edit governed by the actor's write policy: a direct actor writes
    /// straight to the working tree; a propose-only actor's edit is queued as a
    /// suggestion for review. Returns a `WriteOutcome`. The entry point an untrusted
    /// surface routes writes through so a propose-only actor can't land an
    /// unreviewed edit.
    ///
    /// ``replaces`` retires an earlier pending draft of this actor's as this one is
    /// created — see ``suggest``.
    #[pyo3(signature = (ctx, path, data, summary=None, replaces=None))]
    fn write_or_propose<'py>(
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
            let outcome = ws
                .write_or_propose(c, &path, &data, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)?;
            let (wrote, suggestion_id) = match outcome {
                CoreWriteOutcome::Wrote => (true, None),
                CoreWriteOutcome::Proposed(id) => (false, Some(id)),
            };
            Python::attach(|py| {
                Py::new(
                    py,
                    WriteOutcome {
                        wrote,
                        suggestion_id,
                    },
                )
            })
        })
    }

    /// Create a directory and any missing parents.
    fn mkdir_p<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.mkdir_p(&path).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// List a directory (returns a list of `{name, ino, kind}`).
    fn ls<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let entries = ws.ls(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                entries
                    .iter()
                    .map(|e| dir_entry_dict(py, e))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Inode metadata for a path.
    fn stat<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let inode = ws.stat(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// Remove a file or empty directory.
    fn remove<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.remove(&path).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Move/rename a path.
    ///
    /// The Rust-side parameter is `from_`, not `from`: `from` is a Python
    /// keyword and pyo3 exposes argument names verbatim, so a parameter
    /// literally named `from` could never be passed by keyword from Python
    /// (`from=...` is a `SyntaxError`) — `from_` is the usual Python idiom for
    /// a name that collides with a keyword, and matches the type stub.
    fn rename<'py>(
        &self,
        py: Python<'py>,
        from_: String,
        to: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.rename(&from_, &to).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    // --- versioning ---------------------------------------------------------

    /// Extract retrieval passages from the working tree — the technology-agnostic
    /// half of RAG. Returns a list of dicts `{path, byte_start, byte_end, hash,
    /// text, blame}`; `hash` is the passage's content address (dedup /
    /// incremental-embedding key) and `blame` is its per-span authorship. No
    /// embeddings/vectors — those live in userland (see `origofs.rag`).
    ///
    /// `segmentation` is one of `content_defined` (default; edit-stable, best for
    /// incremental indexing), `fixed`, `lines`, or `whole_file`. `size`/`overlap`
    /// are reused per strategy (bytes for `fixed`, lines for `lines`, the average
    /// passage size for `content_defined`). `exts` filters by file extension.
    #[pyo3(signature = (
        root=None,
        exts=None,
        segmentation=None,
        size=1024,
        overlap=0,
        with_text=true,
        with_blame=true,
        max_file_bytes=0,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn passages<'py>(
        &self,
        py: Python<'py>,
        root: Option<String>,
        exts: Option<Vec<String>>,
        segmentation: Option<String>,
        size: usize,
        overlap: usize,
        with_text: bool,
        with_blame: bool,
        max_file_bytes: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        // Parse synchronously so a bad segmentation name errors on the call itself.
        let seg = parse_segmentation(
            segmentation.as_deref().unwrap_or("content_defined"),
            size,
            overlap,
        )?;
        let opts = PassageOptions {
            root: root.unwrap_or_else(|| "/".to_string()),
            exts,
            segmentation: seg,
            with_text,
            with_blame,
            max_file_bytes,
        };
        future_into_py(py, async move {
            let ps = ws.passages(&opts).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                ps.iter()
                    .map(|p| passage_dict(py, p))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    //
    // Only `write_as`/`write_or_propose` were bound. `remove`/`rename`/`mkdir_p`/
    // `commit`/`checkout`/`create_branch` were available *only* in their
    // unattributed forms, which are exempt from the §6 write policy by
    // construction — so `set_write_policy(actor, "propose")`, which *was* bound,
    // had no effect on any of them. The gate looked enforced and was not, and none
    // of those mutations carried blame or an edit-op.

    /// Remove `path`, attributed to `ctx` and governed by its write policy: a
    /// `Direct` actor removes it; a propose-only actor's removal is queued for
    /// review. Returns a [`WriteOutcome`].
    ///
    /// ``replaces`` retires an earlier pending draft of this actor's as this one is
    /// created — see ``suggest``.
    #[pyo3(signature = (ctx, path, summary = None, replaces = None))]
    fn remove_or_propose<'py>(
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
            let outcome = ws
                .remove_or_propose(c, &path, summary.as_deref(), replaces)
                .await
                .map_err(to_pyerr)?;
            let (wrote, suggestion_id) = match outcome {
                CoreWriteOutcome::Wrote => (true, None),
                CoreWriteOutcome::Proposed(id) => (false, Some(id)),
            };
            Python::attach(|py| {
                Py::new(
                    py,
                    WriteOutcome {
                        wrote,
                        suggestion_id,
                    },
                )
            })
        })
    }

    /// Move/rename a path, attributed to `ctx` and subject to its write policy.
    /// See [`Workspace::rename`] for why the parameter is `from_`.
    fn rename_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        from_: String,
        to: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.rename_as(c, &from_, &to).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Create a directory and any missing parents, attributed to `ctx` and
    /// subject to its write policy.
    fn mkdir_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.mkdir_as(c, &path).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Create a symlink at `linkpath` pointing at `target`, attributed to `ctx`
    /// and subject to its write policy.
    fn symlink_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        target: String,
        linkpath: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.symlink_as(c, &target, &linkpath)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Snapshot the working tree into a commit, attributed to `ctx` and subject to
    /// its write policy. Returns the commit hash as hex.
    fn commit_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        author: String,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let h = ws.commit_as(c, &author, &message).await.map_err(to_pyerr)?;
            Ok(h.to_hex())
        })
    }

    /// Create a branch at the current HEAD, attributed to `ctx` and subject to its
    /// write policy.
    fn create_branch_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.create_branch_as(c, &name).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Switch the working tree to `branch`, attributed to `ctx` and subject to its
    /// write policy.
    ///
    /// This is the destructive one: checkout truncates and rematerializes the
    /// entire working tree, discarding every uncommitted edit. Prefer it over the
    /// unattributed `checkout` whenever an actor is known.
    fn checkout_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        branch: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.checkout_as(c, &branch).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Raise `PermissionError` if `ctx`'s actor is propose-only.
    ///
    /// Every attributed method above applies this itself; it is exposed for the
    /// administrative operations that have no attributed variant (registering an
    /// actor, setting a policy), so a Python surface can gate those the same way.
    fn ensure_may_write<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        op: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.ensure_may_write(c, &op).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Create a symlink at `linkpath` pointing at `target` (unattributed; prefer
    /// `symlink_as`).
    fn symlink<'py>(
        &self,
        py: Python<'py>,
        target: String,
        linkpath: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.symlink(&target, &linkpath).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Read a symlink's target.
    fn readlink<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(
            py,
            async move { ws.readlink(&path).await.map_err(to_pyerr) },
        )
    }

    //
    // Everything from here down reaches the engine through the sdk's public
    // `Workspace::fs()` accessor rather than a `Workspace` method, because the
    // façade does not (yet) wrap these: usage/quota/statfs (issues #116, #119),
    // ownership and chmod/chown (#121, #122), hard links and xattrs (#119), and
    // the path-scoped ACLs (#123) all live on `Fs`. Binding them here is issue
    // #120's whole point — the alternative is a Python surface that is once again
    // a subset of the Rust one, which is the failure mode `test_parity.py` exists
    // to catch. If the façade grows wrappers later, these bodies become one-line
    // forwards and nothing on the Python side changes.
    //
    // The inode-addressed engine ops (`vfs_*`) are exposed **by path**: an ino is
    // an implementation detail of the mounts, and a Python caller has a path.

    /// Usage of the whole workspace: `{inodes, bytes}` (issue #116).
    ///
    /// **Logical** bytes — the sum of `stat` sizes, not the deduplicated on-disk
    /// footprint. That is the number a user can act on and the number a quota is
    /// checked against; the physical figure is a property of the content store,
    /// which the metadata store cannot see. An inode reachable by several names
    /// (a hard link) counts once.
    fn usage<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let u = ws.fs().usage().await.map_err(to_pyerr)?;
            Python::attach(|py| usage_dict(py, &u))
        })
    }

    /// Recursive usage of the subtree at `path` — the `du` primitive (issue #116).
    ///
    /// One recursive query in the store rather than a walk from here, so it costs
    /// one round trip rather than one per directory level. Still proportional to
    /// the size of the subtree: a reporting call, not a hot path.
    fn du<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let u = ws.fs().du(&path).await.map_err(to_pyerr)?;
            Python::attach(|py| usage_dict(py, &u))
        })
    }

    /// The workspace's capacity limits: `{bytes, inodes}`, each `None` for no
    /// limit (the default, and what every existing workspace has).
    fn quota<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let q = ws.fs().quota().await.map_err(to_pyerr)?;
            Python::attach(|py| quota_dict(py, &q))
        })
    }

    /// Set (or clear) the workspace's quota. `None` in either field is "no limit",
    /// so `set_quota()` with no arguments clears both.
    ///
    /// Setting a limit **below** current usage is allowed and is not retroactive:
    /// nothing is deleted and no file becomes unreadable — further growth is
    /// simply refused until usage falls back under the limit. Refusing it instead
    /// would make a quota impossible to introduce on a workspace that already has
    /// data, which is the only interesting case.
    #[pyo3(signature = (bytes = None, inodes = None))]
    fn set_quota<'py>(
        &self,
        py: Python<'py>,
        bytes: Option<u64>,
        inodes: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.fs()
                .set_quota(Quota { bytes, inodes })
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Answer a `statfs(2)`: `{block_size, total_blocks, free_blocks,
    /// total_inodes, free_inodes}` (issue #119).
    ///
    /// With a quota set the totals are the quota, which makes `df` show a real
    /// percentage. With none, a workspace has no capacity to report — its ceiling
    /// is the object store's — so the total is a synthesized nominal figure that
    /// grows with usage: `df` looks and behaves like `df` instead of printing a
    /// 100%-full filesystem.
    fn statfs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let s = ws.fs().statfs().await.map_err(to_pyerr)?;
            Python::attach(|py| fs_stat_dict(py, &s))
        })
    }

    /// Change a path's mode, returning its fresh `stat` (issue #121).
    ///
    /// Really changes it: before #122 both mounts accepted a `chmod` and did
    /// nothing, so `chmod +x build.sh` returned success on a false premise — and
    /// the mode a file happened to be *created* with was the mode it carried into
    /// committed tree objects and out through `git clone origofs://…`.
    ///
    /// Only the permission bits (`& 0o7777`, so setuid/setgid/sticky included)
    /// move: the format bits are the inode's kind, not a caller's to rewrite, so
    /// the returned `mode` still carries them.
    fn chmod<'py>(&self, py: Python<'py>, path: String, mode: u32) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let inode = ws.chmod(&path, mode).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// Change a path's owning uid/gid, returning its fresh `stat` (issue #122).
    ///
    /// Either half may be `None` to leave it alone — `chown(2)`'s `-1` sentinel,
    /// which is how `chgrp` reaches this.
    ///
    /// This is ownership, **not authorization**: it changes what the kernel
    /// evaluates its permission checks against on a mount, and nothing about what
    /// an actor may do. For that, see `grant`/`effective_perms`.
    #[pyo3(signature = (path, uid = None, gid = None))]
    fn chown<'py>(
        &self,
        py: Python<'py>,
        path: String,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let inode = ws.chown(&path, uid, gid).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// Hard-link `new_path` to the inode already at `existing_path`, returning the
    /// shared inode's fresh `stat` (issue #119).
    ///
    /// Both names then refer to one inode with `nlink == 2`: a write through
    /// either is visible through both, and the content survives until the last
    /// name is removed. Directories are refused (`PermissionError`), as POSIX
    /// requires — a directory hard link would let the dentry graph form a cycle
    /// nothing here is written to survive.
    fn link<'py>(
        &self,
        py: Python<'py>,
        existing_path: String,
        new_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let inode = ws.link(&existing_path, &new_path).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// Read one extended attribute, or `None` when it is not set (issue #119).
    fn getxattr<'py>(
        &self,
        py: Python<'py>,
        path: String,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let value = ws.getxattr(&path, &name).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                Ok(match value {
                    Some(v) => PyBytes::new(py, &v).into_any().unbind(),
                    None => py.None(),
                })
            })
        })
    }

    /// Set one extended attribute (issue #119).
    ///
    /// A value larger than the per-value limit is refused rather than stored: an
    /// xattr lives in the **metadata** store, and the rule the whole design rests
    /// on is that the metadata database never holds large bytes. The limit matches
    /// Linux's own, so nothing that works on ext4 is refused here.
    fn setxattr<'py>(
        &self,
        py: Python<'py>,
        path: String,
        name: String,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.setxattr(&path, &name, &value).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Every extended-attribute name on a path, in name order (issue #119).
    fn listxattr<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(
            py,
            async move { ws.listxattr(&path).await.map_err(to_pyerr) },
        )
    }

    /// Remove one extended attribute, reporting whether it was there (issue #119).
    fn removexattr<'py>(
        &self,
        py: Python<'py>,
        path: String,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.removexattr(&path, &name).await.map_err(to_pyerr)
        })
    }

    //
    // The seven above resolved a path to an inode and called the *unchecked*
    // inode primitive, so none of them ran any authorization, and there was no
    // attributed form to reach for instead — `chmod`, `chown`, `link` and
    // `setxattr`/`removexattr` all mutate, and were reachable from Python with no
    // actor and no check. These are the counterparts.

    /// `chmod`, requiring `ctx` to hold `WRITE` at `path`.
    fn chmod_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        mode: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let inode = ws.chmod_as(c, &path, mode).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// `chown`, requiring `ctx` to hold `WRITE` at `path`.
    #[pyo3(signature = (ctx, path, uid = None, gid = None))]
    fn chown_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let inode = ws.chown_as(c, &path, uid, gid).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// `link`, requiring `ctx` to hold `WRITE` at **`new_path`** — the name being
    /// created, not the file being pointed at.
    fn link_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        existing_path: String,
        new_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let inode = ws
                .link_as(c, &existing_path, &new_path)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// `getxattr`, requiring `ctx` to hold `READ` at `path` (inert unless the
    /// workspace has `acl_enforce_reads` on).
    fn getxattr_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let value = ws.getxattr_as(c, &path, &name).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                Ok(match value {
                    Some(v) => PyBytes::new(py, &v).into_any().unbind(),
                    None => py.None(),
                })
            })
        })
    }

    /// `setxattr`, requiring `ctx` to hold `WRITE` at `path`.
    fn setxattr_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        name: String,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.setxattr_as(c, &path, &name, &value)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// `listxattr`, requiring `ctx` to hold `READ` at `path` (inert unless the
    /// workspace has `acl_enforce_reads` on).
    fn listxattr_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.listxattr_as(c, &path).await.map_err(to_pyerr)
        })
    }

    /// `removexattr`, requiring `ctx` to hold `WRITE` at `path`.
    fn removexattr_as<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            ws.removexattr_as(c, &path, &name).await.map_err(to_pyerr)
        })
    }
}
