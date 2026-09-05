//! The FUSE and NFS mounts, which hold a workspace for their lifetime.

use super::super::*;

#[pymethods]
impl Workspace {
    /// Mount this workspace as a FUSE filesystem at `mountpoint`, in the
    /// background. Returns a `Mount` handle; unmount by calling `.unmount()`,
    /// exiting its `with` block, or dropping it. Requires FUSE (`/dev/fuse`).
    /// Unix only.
    ///
    /// Pass `ctx` to bind the mount to an actor, so every operation through it is
    /// checked against that actor's path grants (issue #141). Without it the mount
    /// is anonymous and the ACLs do not apply to it. The identity is the
    /// *mount's*, not the caller's — the kernel does not say which process issued
    /// a request — and it authorizes without attributing: writes through a mount
    /// still record no blame.
    #[cfg(target_os = "linux")]
    #[pyo3(signature = (mountpoint, ctx = None))]
    fn mount(&self, py: Python<'_>, mountpoint: String, ctx: Option<WriteCtx>) -> PyResult<Mount> {
        let ws = self.inner.clone();
        let mp = mountpoint.clone();
        let c = ctx.map(|c| c.inner);
        let session = py
            .detach(move || origofs_sdk::fuse::spawn_as(ws, Path::new(&mp), c))
            .map_err(io_err)?;
        Ok(Mount {
            session: Some(session),
            mountpoint,
        })
    }

    /// FUSE mounting is not available on this platform (Unix/FUSE only). Use the
    /// HTTP API (`origofs.fastapi`) or embed the SDK directly.
    #[cfg(not(target_os = "linux"))]
    #[pyo3(signature = (_mountpoint, _ctx = None))]
    fn mount(&self, _mountpoint: String, _ctx: Option<WriteCtx>) -> PyResult<()> {
        Err(unsupported("FUSE mounting"))
    }

    /// Serve this workspace over NFSv3 at `addr` (e.g. `127.0.0.1:11111`).
    ///
    /// The returned awaitable runs until it is **cancelled**, until the optional
    /// `shutdown` awaitable resolves, or until the server itself fails. In every
    /// case the server is torn down before the call ends: the accept loop stops,
    /// the listener's fd (and with it the port) is released, and every
    /// per-connection task and socket goes with it — nothing outlives the call.
    ///
    /// ```python
    /// # cancel-driven (unchanged from before) -- `ensure_future`, not
    /// # `create_task`, since this returns a future rather than a coroutine:
    /// task = asyncio.ensure_future(ws.serve_nfs("127.0.0.1:11111"))
    /// task.cancel()
    ///
    /// # or graceful and awaited -- `await task` returns once teardown is done:
    /// stop = asyncio.Event()
    /// task = asyncio.ensure_future(ws.serve_nfs(addr, shutdown=stop.wait()))
    /// stop.set()
    /// await task
    /// ```
    ///
    /// `shutdown` is any awaitable (an `asyncio.Event().wait()` coroutine, a
    /// future, another task); its result is ignored — only its completion is a
    /// signal. It is the deterministic one of the two: it tears the server down
    /// *before* the `await` returns, whereas a cancellation is delivered by the
    /// event loop's done-callback and then completes in the background (so a
    /// caller that cancels and immediately blocks the loop delays the teardown
    /// it asked for — ordinary asyncio semantics). Unix only.
    #[cfg(unix)]
    #[pyo3(signature = (addr, shutdown = None, ctx = None))]
    fn serve_nfs<'py>(
        &self,
        py: Python<'py>,
        addr: String,
        shutdown: Option<Bound<'py, PyAny>>,
        ctx: Option<WriteCtx>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.map(|c| c.inner);
        // Converted while we hold the GIL; the resulting future is plain Rust.
        let stopper = shutdown
            .map(pyo3_async_runtimes::tokio::into_future)
            .transpose()?;
        future_into_py(py, async move {
            // Dropping `server` (which is what a cancelled Python task does to
            // this future) is itself a full teardown — see `NfsServer::drop`.
            let mut server = NfsServer::start(ws, addr, c).map_err(io_err)?;
            let Some(stopper) = stopper else {
                // No explicit handle: run until the caller cancels us.
                return server.joined().await.map_err(io_err);
            };
            let served = tokio::select! {
                r = server.joined() => Some(r),
                _ = stopper => None,
            };
            match served {
                Some(r) => r.map_err(io_err),
                // Asked to stop: drain the accept loop and reap the connections
                // before returning, so the port is free once `await` completes.
                None => server.shutdown().await.map_err(io_err),
            }
        })
    }

    /// NFS serving is not available on this platform (Unix only). Use the HTTP
    /// API (`origofs.fastapi`) or embed the SDK directly.
    #[cfg(not(unix))]
    #[pyo3(signature = (_addr, _shutdown = None, _ctx = None))]
    fn serve_nfs(
        &self,
        _addr: String,
        _shutdown: Option<Bound<'_, PyAny>>,
        _ctx: Option<WriteCtx>,
    ) -> PyResult<()> {
        Err(unsupported("NFS serving"))
    }
}
