//! Python bindings for origofs — an async-native module for driving a workspace
//! from Python (FastAPI, scripts, orchestration).
//!
//! Every I/O method returns a Python awaitable (via `pyo3-async-runtimes`), so
//! it drops straight into `async def` endpoints:
//!
//! ```python
//! import origofs
//! ws = await origofs.Workspace.open_local("meta.db", "cas")
//! # attribute a write to the authenticated user / agent you resolved yourself:
//! ctx = origofs.WriteCtx.session(actor_id, session_id)
//! await ws.write_as(ctx, "/notes.txt", b"hello")
//! ```
//!
//! Structured results come back as plain dicts/lists so they are directly
//! JSON-serializable in an API response. Mounting (FUSE) and NFS serving are
//! exposed so orchestration can live in Python too.

use origofs_core::LocalCasStore;
use origofs_sdk::{
    Actor, BlameRange, CommitInfo, DiffEntry, DiffStatus, DirEntry, Event, EventSubscription,
    GcsConfig as CoreGcsConfig, Inode, LiveDoc, Passage, PassageOptions, Presence, RebuildReport,
    S3Config as CoreS3Config, Segmentation, Suggestion, SuggestionStatus,
    Workspace as CoreWorkspace, WriteCtx as CoreWriteCtx, WriteOutcome as CoreWriteOutcome,
    WritePolicy as CoreWritePolicy,
};
use pyo3::create_exception;
use pyo3::exceptions::{
    PyFileExistsError, PyFileNotFoundError, PyIsADirectoryError, PyNotADirectoryError, PyOSError,
    PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
#[cfg(unix)]
use std::path::Path;
use std::sync::Arc;

create_exception!(origofs, OrigoFSError, pyo3::exceptions::PyException);
create_exception!(origofs, ConflictError, OrigoFSError);

/// Map an origofs error onto the closest Python exception.
fn to_pyerr(e: origofs_sdk::OrigoFSError) -> PyErr {
    use origofs_sdk::OrigoFSError::*;
    let msg = e.to_string();
    match e {
        NotFound(_) | ContentMissing(_) => PyFileNotFoundError::new_err(msg),
        AlreadyExists(_) => PyFileExistsError::new_err(msg),
        NotADirectory(_) => PyNotADirectoryError::new_err(msg),
        IsADirectory(_) => PyIsADirectoryError::new_err(msg),
        DirectoryNotEmpty(_) => PyOSError::new_err(msg),
        InvalidArgument(_) | InvalidPath(_) => PyValueError::new_err(msg),
        Conflict(_) => ConflictError::new_err(msg),
        _ => OrigoFSError::new_err(msg),
    }
}

#[cfg(unix)]
fn io_err(e: std::io::Error) -> PyErr {
    PyOSError::new_err(e.to_string())
}

/// Error for a mount/serve operation that isn't available off Unix (no FUSE/NFS).
#[cfg(not(unix))]
fn unsupported(what: &str) -> PyErr {
    PyOSError::new_err(format!(
        "{what} is not available on this platform (Unix/FUSE only); use the HTTP API (origofs.fastapi) or embed the SDK"
    ))
}

// --- dict builders (kept JSON-serializable) ---------------------------------

fn diff_status_str(s: DiffStatus) -> &'static str {
    match s {
        DiffStatus::Added => "added",
        DiffStatus::Modified => "modified",
        DiffStatus::Deleted => "deleted",
    }
}

fn hash_opt(h: Option<&origofs_sdk::Hash>) -> Option<String> {
    h.map(|h| h.to_hex())
}

fn inode_dict(py: Python<'_>, i: &Inode) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("ino", i.ino)?;
    d.set_item("kind", i.kind.as_str())?;
    d.set_item("mode", i.mode)?;
    d.set_item("nlink", i.nlink)?;
    d.set_item("size", i.size)?;
    d.set_item("content", hash_opt(i.content.as_ref()))?;
    d.set_item("mtime", i.mtime)?;
    d.set_item("ctime", i.ctime)?;
    Ok(d.into_any().unbind())
}

fn dir_entry_dict(py: Python<'_>, e: &DirEntry) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("name", &e.name)?;
    d.set_item("ino", e.ino)?;
    d.set_item("kind", e.kind.as_str())?;
    Ok(d.into_any().unbind())
}

fn commit_dict(py: Python<'_>, c: &CommitInfo) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("hash", c.hash.to_hex())?;
    d.set_item("author", &c.commit.author)?;
    d.set_item("message", &c.commit.message)?;
    d.set_item("timestamp", c.commit.timestamp)?;
    d.set_item(
        "parents",
        c.commit
            .parents
            .iter()
            .map(|h| h.to_hex())
            .collect::<Vec<_>>(),
    )?;
    Ok(d.into_any().unbind())
}

fn diff_dict(py: Python<'_>, e: &DiffEntry) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("path", &e.path)?;
    d.set_item("status", diff_status_str(e.status))?;
    Ok(d.into_any().unbind())
}

fn actor_dict(py: Python<'_>, a: &Actor) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("id", a.id)?;
    d.set_item("kind", a.kind.as_str())?;
    d.set_item("display_name", &a.display_name)?;
    d.set_item("auth_subject", a.auth_subject.clone())?;
    d.set_item("agent_model", a.agent_model.clone())?;
    d.set_item("agent_vendor", a.agent_vendor.clone())?;
    d.set_item("controller_actor_id", a.controller_actor_id)?;
    d.set_item("created_at", a.created_at)?;
    Ok(d.into_any().unbind())
}

fn blame_dict(py: Python<'_>, b: &BlameRange) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    // Exact byte range is the ground truth the design blames by; the line range is
    // derived, for line-oriented views. Both are exposed so a client can render
    // sub-line, character-level authorship (co-editing lands it — M8).
    d.set_item("byte_start", b.byte_start)?;
    d.set_item("byte_end", b.byte_end)?;
    d.set_item("line_start", b.line_start)?;
    d.set_item("line_end", b.line_end)?;
    d.set_item("session", b.session)?;
    d.set_item("actor", actor_dict(py, &b.actor)?)?;
    Ok(d.into_any().unbind())
}

fn passage_dict(py: Python<'_>, p: &Passage) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("path", &p.path)?;
    d.set_item("byte_start", p.byte_start)?;
    d.set_item("byte_end", p.byte_end)?;
    // Content address of the passage bytes — dedup / incremental-embedding key.
    d.set_item("hash", p.hash.to_hex())?;
    // Text is decoded as UTF-8 (lossily); `None` when text wasn't requested.
    match &p.text {
        Some(b) => d.set_item("text", String::from_utf8_lossy(b.as_ref()).into_owned())?,
        None => d.set_item("text", py.None())?,
    }
    d.set_item(
        "blame",
        p.blame
            .iter()
            .map(|b| blame_dict(py, b))
            .collect::<PyResult<Vec<_>>>()?,
    )?;
    Ok(d.into_any().unbind())
}

/// Build a core `Segmentation` from the Python-facing `(kind, size, overlap)`.
/// `size`/`overlap` are reused per strategy (bytes for `fixed`, lines for
/// `lines`, the content-defined average for `content_defined`).
fn parse_segmentation(kind: &str, size: usize, overlap: usize) -> PyResult<Segmentation> {
    Ok(match kind {
        "whole_file" | "whole" => Segmentation::WholeFile,
        "fixed" | "fixed_bytes" => Segmentation::FixedBytes { size, overlap },
        "lines" => Segmentation::Lines {
            max_lines: size,
            overlap,
        },
        "content_defined" | "cdc" => Segmentation::ContentDefined {
            min: size / 4,
            avg: size,
            max: size * 4,
        },
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown segmentation {other:?} (expected whole_file | fixed | lines | content_defined)"
            )));
        }
    })
}

fn event_dict(py: Python<'_>, e: &Event) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("seq", e.seq)?;
    d.set_item("actor_id", e.actor_id)?;
    d.set_item("session_id", e.session_id)?;
    d.set_item("kind", &e.kind)?;
    d.set_item("path", &e.path)?;
    d.set_item("detail", e.detail.clone())?;
    d.set_item("ts", e.ts)?;
    d.set_item("branch", e.branch.clone())?;
    Ok(d.into_any().unbind())
}

fn presence_dict(py: Python<'_>, p: &Presence) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("session_id", p.session_id)?;
    d.set_item("actor_id", p.actor_id)?;
    d.set_item("display_name", &p.display_name)?;
    d.set_item("kind", p.kind.as_str())?;
    d.set_item("path", p.path.clone())?;
    d.set_item("last_seen", p.last_seen)?;
    Ok(d.into_any().unbind())
}

/// A live-document marker: `path` has an open CRDT document, so its durable bytes
/// are a *checkpoint* that may lag what people are typing.
fn live_doc_dict(py: Python<'_>, l: &LiveDoc) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("path", &l.path)?;
    d.set_item("session_id", l.session_id)?;
    d.set_item("actor_id", l.actor_id)?;
    // The file's content address as of the last checkpoint — an out-of-band write
    // is exactly "the file's current address differs from this".
    d.set_item("content_hash", l.content_hash.clone())?;
    d.set_item("since", l.since)?;
    Ok(d.into_any().unbind())
}

fn suggestion_dict(py: Python<'_>, s: &Suggestion) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("id", s.id)?;
    d.set_item("actor_id", s.actor_id)?;
    d.set_item("session_id", s.session_id)?;
    d.set_item("branch", s.branch.clone())?;
    d.set_item("path", &s.path)?;
    d.set_item("base_hash", s.base_hash.clone())?;
    d.set_item("proposed_hash", s.proposed_hash.clone())?;
    d.set_item("summary", s.summary.clone())?;
    // `bytes` (a whole file body) or `crdt` (a Yjs update to merge). It decides
    // what `base_hash`/`proposed_hash` address and how `accept` applies them, so a
    // reviewer UI needs it to know what it is looking at.
    d.set_item("kind", s.kind.as_str())?;
    d.set_item("status", s.status.as_str())?;
    d.set_item("created_ts", s.created_ts)?;
    d.set_item("resolved_ts", s.resolved_ts)?;
    d.set_item("resolved_by", s.resolved_by)?;
    Ok(d.into_any().unbind())
}

fn rebuild_report_dict(py: Python<'_>, r: &RebuildReport) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("objects_scanned", r.objects_scanned)?;
    d.set_item("corrupt", r.corrupt)?;
    d.set_item("commits_found", r.commits_found)?;
    d.set_item("used_mirror", r.used_mirror)?;
    // (name, commit_hex) pairs, one per recovered branch.
    d.set_item("branches", r.branches.clone())?;
    d.set_item("checked_out", r.checked_out.clone())?;
    d.set_item("dirs", r.dirs)?;
    d.set_item("files", r.files)?;
    d.set_item("symlinks", r.symlinks)?;
    Ok(d.into_any().unbind())
}

// --- WriteCtx ---------------------------------------------------------------

/// The actor context to attribute a write to — construct it from whatever
/// user/agent you resolved in your endpoint. Passed by value to `write_as`,
/// `suggest`, `accept_suggestion`, … so it opts into `FromPyObject`.
#[pyclass(frozen, from_py_object)]
#[derive(Clone, Copy)]
struct WriteCtx {
    inner: CoreWriteCtx,
}

#[pymethods]
impl WriteCtx {
    /// Attribute to an actor (no session).
    #[staticmethod]
    fn actor(actor: i64) -> Self {
        Self {
            inner: CoreWriteCtx::actor(actor),
        }
    }

    /// Attribute to an actor acting within a session.
    #[staticmethod]
    fn session(actor: i64, session: i64) -> Self {
        Self {
            inner: CoreWriteCtx::session(actor, session),
        }
    }

    #[getter]
    fn actor_id(&self) -> i64 {
        self.inner.actor
    }

    #[getter]
    fn session_id(&self) -> Option<i64> {
        self.inner.session
    }

    fn __repr__(&self) -> String {
        format!(
            "WriteCtx(actor={}, session={:?})",
            self.inner.actor, self.inner.session
        )
    }
}

/// The outcome of a policy-governed write (see `Workspace.write_or_propose`).
#[pyclass(frozen)]
struct WriteOutcome {
    /// True if the actor writes directly and the edit landed in the working tree.
    #[pyo3(get)]
    wrote: bool,
    /// The suggestion id if the actor is propose-only and the edit was queued for
    /// review; `None` when it was written directly.
    #[pyo3(get)]
    suggestion_id: Option<i64>,
}

#[pymethods]
impl WriteOutcome {
    fn __repr__(&self) -> String {
        match self.suggestion_id {
            Some(id) => format!("WriteOutcome(proposed suggestion #{id})"),
            None => "WriteOutcome(wrote)".to_string(),
        }
    }
}

// --- change-feed push subscription ------------------------------------------

/// A live push subscription to the change feed (Postgres `LISTEN/NOTIFY`).
/// `await sub.recv()` blocks until the next batch of events arrives — a real
/// push, not a poll. Returned by `Workspace.subscribe`.
#[pyclass]
struct Subscription {
    inner: Arc<tokio::sync::Mutex<EventSubscription>>,
}

#[pymethods]
impl Subscription {
    /// Block until the next batch of events, returned oldest-first as dicts.
    /// Returns `[]` once the feed's connection has closed.
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = self.inner.clone();
        future_into_py(py, async move {
            let events = {
                let mut guard = sub.lock().await;
                guard.recv().await.map_err(to_pyerr)?
            };
            Python::attach(|py| {
                events
                    .iter()
                    .map(|e| event_dict(py, e))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }
}

// --- S3 config --------------------------------------------------------------

/// Connection settings for an S3-compatible object store (AWS S3, Cloudflare R2,
/// MinIO, or GCS via its S3-interop XML API). Pass to `Workspace.open_s3` /
/// `open_pg_s3` (and their `_packed` forms).
///
/// When `access_key_id` / `secret_access_key` are omitted, credentials fall back
/// to the **AWS** default chain (`AWS_*` env vars, EC2/ECS instance role) — that
/// is AWS-only and does nothing on GCP. To use GCS over this S3 path, set
/// `endpoint="https://storage.googleapis.com"` and supply GCS **HMAC** interop
/// keys. For native GCS auth (service account / ADC / workload identity) use
/// `GcsConfig` + `Workspace.open_gcs` instead.
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
struct S3Config {
    inner: CoreS3Config,
}

#[pymethods]
impl S3Config {
    #[new]
    #[pyo3(signature = (
        bucket,
        region,
        endpoint = None,
        allow_http = false,
        access_key_id = None,
        secret_access_key = None,
        prefix = None,
    ))]
    fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        allow_http: bool,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        prefix: Option<String>,
    ) -> Self {
        Self {
            inner: CoreS3Config {
                bucket,
                region,
                endpoint,
                allow_http,
                access_key_id,
                secret_access_key,
                prefix,
            },
        }
    }

    // Deliberately omits credentials.
    fn __repr__(&self) -> String {
        format!(
            "S3Config(bucket={:?}, region={:?}, endpoint={:?}, prefix={:?})",
            self.inner.bucket, self.inner.region, self.inner.endpoint, self.inner.prefix
        )
    }
}

// --- GCS config -------------------------------------------------------------

/// Connection settings for a **native** Google Cloud Storage object store (GCS
/// JSON API + OAuth2). Pass to `Workspace.open_gcs` / `open_pg_gcs` (and their
/// `_packed` forms).
///
/// Credentials resolve in order: an explicit `service_account_key` (inline JSON)
/// or `service_account_path` (file); then Application Default Credentials
/// (`application_credentials`, else `GOOGLE_APPLICATION_CREDENTIALS` / `gcloud`);
/// then the GCE/GKE metadata server (workload identity). Unlike `S3Config`, this
/// needs no HMAC keys and no endpoint override.
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
struct GcsConfig {
    inner: CoreGcsConfig,
}

#[pymethods]
impl GcsConfig {
    #[new]
    #[pyo3(signature = (
        bucket,
        service_account_path = None,
        service_account_key = None,
        application_credentials = None,
        prefix = None,
    ))]
    fn new(
        bucket: String,
        service_account_path: Option<String>,
        service_account_key: Option<String>,
        application_credentials: Option<String>,
        prefix: Option<String>,
    ) -> Self {
        Self {
            inner: CoreGcsConfig {
                bucket,
                service_account_path,
                service_account_key,
                application_credentials,
                prefix,
            },
        }
    }

    // Deliberately omits credentials.
    fn __repr__(&self) -> String {
        format!(
            "GcsConfig(bucket={:?}, prefix={:?})",
            self.inner.bucket, self.inner.prefix
        )
    }
}

// --- FUSE mount handle ------------------------------------------------------

/// A live FUSE mount. Unmounts when `unmount()` is called or the object is
/// dropped. Usable as a context manager. Unix only (FUSE).
#[cfg(unix)]
#[pyclass]
struct Mount {
    session: Option<fuser::BackgroundSession>,
    mountpoint: String,
}

#[cfg(unix)]
#[pymethods]
impl Mount {
    /// Unmount now (idempotent).
    fn unmount(&mut self) {
        self.session.take(); // dropping the BackgroundSession unmounts
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, PyAny>) {
        self.unmount();
    }

    fn __repr__(&self) -> String {
        let state = if self.session.is_some() {
            "mounted"
        } else {
            "unmounted"
        };
        format!("Mount(mountpoint={:?}, {state})", self.mountpoint)
    }
}

// --- NFS server teardown ----------------------------------------------------

/// A running NFSv3 server together with everything needed to shut it down —
/// the guard behind [`Workspace.serve_nfs`].
///
/// Two levers, because a dropped future cannot await:
///
/// - a `watch` flag + a [`tokio::task::JoinSet`] holding the accept loop, so a
///   *graceful* shutdown asks the loop to stop and then awaits it;
/// - a private runtime, because `nfsserve`'s accept loop `tokio::spawn`s a
///   **detached** task per connection (which spawns another for its read half).
///   A `JoinSet` here can only reach the accept loop itself: aborting that frees
///   the listener fd, but every live connection's task and socket would survive
///   it. Owning the runtime makes teardown total — shutting it down drops every
///   task spawned on it, closing their sockets with them.
#[cfg(unix)]
struct NfsServer {
    /// Set to `true` to ask the accept loop to stop accepting.
    stop: tokio::sync::watch::Sender<bool>,
    /// The accept-loop task, so shutdown can await (or abort) it.
    tasks: tokio::task::JoinSet<std::io::Result<()>>,
    /// `None` once the runtime has been handed off for shutdown.
    rt: Option<tokio::runtime::Runtime>,
}

#[cfg(unix)]
impl NfsServer {
    /// Start serving `ws` at `addr` on a private runtime. Binding happens inside
    /// the accept-loop task, so a bind failure surfaces from [`Self::joined`].
    fn start(ws: CoreWorkspace, addr: String) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("origofs-nfs")
            .build()?;
        let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn_on(
            async move {
                tokio::select! {
                    r = origofs_sdk::nfs::serve(ws, &addr) => r,
                    // Returning here drops the accept loop, and with it the
                    // listener — its fd (and port) is released right now.
                    _ = stop_rx.changed() => Ok(()),
                }
            },
            rt.handle(),
        );
        Ok(Self {
            stop,
            tasks,
            rt: Some(rt),
        })
    }

    /// Wait for the accept loop to finish — which it normally never does, so this
    /// is the "run forever" arm; it returns early on a bind failure or a panic.
    /// Cancel-safe (`JoinSet::join_next` is), so it can be raced in a `select!`.
    async fn joined(&mut self) -> std::io::Result<()> {
        match self.tasks.join_next().await {
            Some(Ok(r)) => r,
            Some(Err(e)) if e.is_cancelled() => Ok(()),
            Some(Err(e)) => Err(std::io::Error::other(format!("NFS server panicked: {e}"))),
            None => Ok(()),
        }
    }

    /// Graceful, awaited teardown: stop accepting, drain the accept loop, then
    /// shut the runtime down so every per-connection task and socket goes with
    /// it. On return the port is free.
    async fn shutdown(&mut self) -> std::io::Result<()> {
        let _ = self.stop.send(true);
        let outcome = self.joined().await;
        if let Some(rt) = self.rt.take() {
            // `shutdown_timeout` blocks, so keep it off an async worker thread.
            let _ = tokio::task::spawn_blocking(move || {
                rt.shutdown_timeout(std::time::Duration::from_secs(5))
            })
            .await;
        }
        outcome
    }
}

#[cfg(unix)]
impl Drop for NfsServer {
    /// The cancellation path: the awaiting Python task was cancelled, so this
    /// future is being dropped and has no chance to await. Tear down without
    /// blocking — nothing may outlive the call.
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        self.tasks.abort_all();
        if let Some(rt) = self.rt.take() {
            // Never blocks (so it is safe even though we are being dropped from
            // inside a runtime) and still drops every task the runtime owns,
            // closing their sockets.
            rt.shutdown_background();
        }
    }
}

// --- live co-editing (M8) ---------------------------------------------------

/// The routing for one processed y-sync payload (see [`CoeditDoc.handle_sync`]):
/// `reply` goes back to the connection it came from; `broadcast` fans out to the
/// room's other connections. Either may be empty.
#[pyclass(frozen)]
struct CoeditSyncReply {
    reply: Vec<u8>,
    broadcast: Vec<u8>,
}

#[pymethods]
impl CoeditSyncReply {
    /// Frames to send back to the originating connection (`b""` if none).
    #[getter]
    fn reply<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.reply)
    }

    /// Frames to fan out to every other connection in the room (`b""` if none).
    #[getter]
    fn broadcast<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.broadcast)
    }

    fn __repr__(&self) -> String {
        format!(
            "CoeditSyncReply(reply={} bytes, broadcast={} bytes)",
            self.reply.len(),
            self.broadcast.len()
        )
    }
}

/// A live co-edited document (roadmap M8): a Yjs-compatible CRDT whose inserts are
/// attributed per byte range. Obtain one from [`Workspace.open_coedit`], drive it
/// with the Yjs **y-sync** wire protocol so an unmodified editor (PlateJS,
/// `y-websocket`) collaborates directly, and land it with
/// [`Workspace.checkpoint_coedit`].
///
/// The document is internally synchronized, so it is safe to share one instance
/// across many concurrent WebSocket handlers — exactly how a FastAPI room mounts
/// it. Every method is a coroutine (`await` it), matching the rest of the module.
#[pyclass]
struct CoeditDoc {
    // A shared, async-locked handle: the doc outlives any one request and is
    // serialized against the (async) checkpoint, which the GIL alone can't do
    // because it's released across `.await`.
    inner: Arc<tokio::sync::Mutex<origofs_sdk::CoeditDoc>>,
}

#[pymethods]
impl CoeditDoc {
    /// A fresh, empty document — for a Python-side agent that drives edits
    /// directly, or a test client. Server rooms come from `Workspace.open_coedit`.
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(origofs_sdk::CoeditDoc::new())),
        }
    }

    /// Insert `chunk` at character `index` (UTF-16 offset, as in Yjs), attributed
    /// to `ctx`.
    fn insert<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        index: u32,
        chunk: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            inner.lock().await.insert(c, index, &chunk);
            Ok(())
        })
    }

    /// Remove `length` characters starting at `index` (UTF-16 offsets).
    #[pyo3(signature = (index, length))]
    fn remove<'py>(&self, py: Python<'py>, index: u32, length: u32) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.remove(index, length);
            Ok(())
        })
    }

    /// The y-sync frame to greet a freshly-connected client with (a `SyncStep1`
    /// carrying our state vector). Send it as the first message on a new socket.
    fn sync_start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let bytes = inner.lock().await.sync_start();
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// This document's Yjs state vector (`encodeStateVector`) — "how much of the
    /// document I already have". The base half of a CRDT suggestion.
    fn state_vector<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let bytes = inner.lock().await.state_vector();
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// This document's full state as a Yjs update (`encodeStateAsUpdate`) — the
    /// opaque, always-mergeable blob a CRDT suggestion proposes.
    fn state_update<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let bytes = inner.lock().await.state_update();
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// Handle one inbound y-sync payload from a connection authenticated as `ctx`.
    /// Content the client contributes is attributed to `ctx` server-side, never
    /// trusted from the bytes. Returns a [`CoeditSyncReply`] routing the response.
    fn handle_sync<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let out = inner.lock().await.handle_sync(c, &data).map_err(to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    CoeditSyncReply {
                        reply: out.reply,
                        broadcast: out.broadcast,
                    },
                )
            })
        })
    }

    /// Merge a y-sync frame relayed from another worker (already attributed by its
    /// origin) *without* re-attribution — the cross-worker relay's apply path.
    /// Idempotent. Client input must instead go through `handle_sync`.
    fn apply_relayed<'py>(&self, py: Python<'py>, frame: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.apply_relayed(&frame).map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// The full current text (handy for inspection and tests).
    fn text<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let text = inner.lock().await.text();
            Ok(text)
        })
    }

    fn __repr__(&self) -> String {
        "CoeditDoc()".to_string()
    }
}

/// One relayed co-editing update from another worker: the attributed `delta`
/// (a y-sync frame) `origin` produced for `path`, ordered by `seq`.
#[pyclass(frozen)]
struct CoeditRelayNote {
    #[pyo3(get)]
    seq: i64,
    #[pyo3(get)]
    origin: String,
    #[pyo3(get)]
    path: String,
    delta: Vec<u8>,
}

#[pymethods]
impl CoeditRelayNote {
    /// The update payload (a y-sync frame) to feed `CoeditDoc.apply_relayed`.
    #[getter]
    fn delta<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.delta)
    }

    fn __repr__(&self) -> String {
        format!(
            "CoeditRelayNote(seq={}, origin={:?}, path={:?}, delta={} bytes)",
            self.seq,
            self.origin,
            self.path,
            self.delta.len()
        )
    }
}

/// A live subscription to the cross-worker co-editing relay (Postgres
/// `LISTEN/NOTIFY`). `await sub.recv()` blocks until peers publish, then returns
/// their updates in order. Returned by `Workspace.coedit_subscribe`.
#[pyclass]
struct CoeditRelaySub {
    inner: Arc<tokio::sync::Mutex<origofs_sdk::CoeditRelaySub>>,
}

#[pymethods]
impl CoeditRelaySub {
    /// Block until at least one relayed op arrives, then return the batch (in
    /// `seq` order). Returns `[]` once the relay connection has closed. The caller
    /// skips its own `origin` and any path it isn't hosting.
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = self.inner.clone();
        future_into_py(py, async move {
            let notes = {
                let mut guard = sub.lock().await;
                guard.recv().await.map_err(to_pyerr)?
            };
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
}

// --- Workspace --------------------------------------------------------------

/// An origofs workspace. Open one with a classmethod, then drive it with async
/// methods. Cheap to hold; clones share the same backend.
#[pyclass]
struct Workspace {
    inner: CoreWorkspace,
}

#[pymethods]
impl Workspace {
    /// Open (creating if needed) a local workspace: SQLite metadata at
    /// `db_path`, content-addressed chunks under `cas_dir`.
    #[staticmethod]
    fn open_local<'py>(
        py: Python<'py>,
        db_path: String,
        cas_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_local(&db_path, &cas_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a local workspace whose chunks are batched into pack objects
    /// (`data_dir`), with the pack index under `index_dir`.
    #[staticmethod]
    fn open_local_packed<'py>(
        py: Python<'py>,
        db_path: String,
        data_dir: String,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_local_packed(&db_path, &data_dir, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a workspace with Postgres metadata (multi-writer) over a local CAS.
    /// `dsn` is a libpq URL/DSN, e.g. `postgres://user:pass@host/db`.
    #[staticmethod]
    fn open_pg<'py>(py: Python<'py>, dsn: String, cas_dir: String) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let content = Arc::new(LocalCasStore::open(&cas_dir).await.map_err(to_pyerr)?);
            // Via the SDK constructor so the workspace retains its Postgres handle
            // (needed for the `subscribe` LISTEN/NOTIFY push feed).
            let ws = CoreWorkspace::open_pg(&dsn, content)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + an S3-compatible object store for content. Reads are
    /// integrity-verified (a bit-rotted object errors instead of being served).
    #[staticmethod]
    fn open_s3<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_s3(&db_path, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + a **packed** S3 object store (few large PUTs instead of
    /// many tiny ones) with the per-chunk index under `index_dir`. Call
    /// `commit`/`flush` to seal the open pack and `repack` to reclaim space.
    #[staticmethod]
    fn open_s3_packed<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_s3_packed(&db_path, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata (multi-writer) + an S3-compatible object store — the
    /// production pairing for a shared human+agent workspace: many writers on one
    /// database, one shared content store.
    #[staticmethod]
    fn open_pg_s3<'py>(py: Python<'py>, dsn: String, cfg: S3Config) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_s3(&dsn, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata + a **packed** S3 object store with the per-chunk index
    /// under `index_dir`. The recommended object-storage layout for a team.
    #[staticmethod]
    fn open_pg_s3_packed<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: S3Config,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_s3_packed(&dsn, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + a **native** GCS object store (GCS JSON API + OAuth2;
    /// service-account / ADC / workload-identity credentials — see `GcsConfig`).
    /// Reads are integrity-verified (a bit-rotted object errors instead of being
    /// served).
    #[staticmethod]
    fn open_gcs<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_gcs(&db_path, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + a **packed** native GCS object store with the per-chunk
    /// index under `index_dir`. Call `commit`/`flush` to seal the open pack and
    /// `repack` to reclaim space.
    #[staticmethod]
    fn open_gcs_packed<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_gcs_packed(&db_path, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata (multi-writer) + a **native** GCS object store — the
    /// production pairing for a shared human+agent workspace on Google Cloud.
    #[staticmethod]
    fn open_pg_gcs<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_gcs(&dsn, cfg.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Postgres metadata + a **packed** native GCS object store with the per-chunk
    /// index under `index_dir`. The recommended object-storage layout for a team on
    /// Google Cloud.
    #[staticmethod]
    fn open_pg_gcs_packed<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
        index_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_gcs_packed(&dsn, cfg.inner, &index_dir)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// SQLite metadata + an **in-memory** object store — the same object-store
    /// adapter as `open_s3` minus the network, for local dev and tests without a
    /// live bucket. Content is not durable.
    #[staticmethod]
    fn open_object_memory<'py>(py: Python<'py>, db_path: String) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_object_memory(&db_path)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    // --- files --------------------------------------------------------------

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

    /// Submit an edit governed by the actor's write policy: a direct actor writes
    /// straight to the working tree; a propose-only actor's edit is queued as a
    /// suggestion for review. Returns a `WriteOutcome`. The entry point an untrusted
    /// surface routes writes through so a propose-only actor can't land an
    /// unreviewed edit.
    #[pyo3(signature = (ctx, path, data, summary=None))]
    fn write_or_propose<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        data: Vec<u8>,
        summary: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let outcome = ws
                .write_or_propose(c, &path, &data, summary.as_deref())
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

    /// Propose a change to a co-edited `path` as a **CRDT merge** rather than a
    /// whole file body: the review row records the workspace document's Yjs state
    /// vector as its base and `doc`'s ``encodeStateAsUpdate`` blob as the proposal.
    /// Accepting it merges (``applyUpdate``) instead of overwriting, so a
    /// concurrent disjoint edit is neither clobbered nor false-rejected as stale.
    /// Returns the suggestion id.
    #[pyo3(signature = (ctx, path, doc, summary=None))]
    fn suggest_coedit<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        doc: Py<CoeditDoc>,
        summary: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        let inner = doc.borrow(py).inner.clone();
        future_into_py(py, async move {
            let guard = inner.lock().await;
            let id = ws
                .suggest_coedit(c, &path, &guard, summary.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// The primitive behind `suggest_coedit`, for a client that already holds the
    /// two Yjs blobs — a browser editor proposes with ``encodeStateVector(doc)`` as
    /// `base_sv` and ``encodeStateAsUpdate(doc)`` as `update`.
    #[pyo3(signature = (ctx, path, base_sv, update, summary=None))]
    fn suggest_coedit_update<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        base_sv: Vec<u8>,
        update: Vec<u8>,
        summary: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let id = ws
                .suggest_coedit_update(c, &path, &base_sv, &update, summary.as_deref())
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

    /// Whether this workspace is Postgres-backed (multi-writer). The cross-worker
    /// co-editing relay is available exactly when this is true; on SQLite a single
    /// worker holds every room, so no relay is needed.
    fn is_postgres(&self) -> bool {
        self.inner.is_postgres()
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
    #[pyo3(signature = (ctx, path, data, summary=None))]
    fn suggest<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        data: Vec<u8>,
        summary: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let id = ws
                .suggest(c, &path, &data, summary.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(id)
        })
    }

    /// Propose deleting `path`.
    #[pyo3(signature = (ctx, path, summary=None))]
    fn suggest_delete<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        summary: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let id = ws
                .suggest_delete(c, &path, summary.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(id)
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
            ws.accept_suggestion(id, c).await.map_err(to_pyerr)?;
            Ok(())
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

    /// Mount this workspace as a FUSE filesystem at `mountpoint`, in the
    /// background. Returns a `Mount` handle; unmount by calling `.unmount()`,
    /// exiting its `with` block, or dropping it. Requires FUSE (`/dev/fuse`).
    /// Unix only.
    #[cfg(unix)]
    fn mount(&self, py: Python<'_>, mountpoint: String) -> PyResult<Mount> {
        let ws = self.inner.clone();
        let mp = mountpoint.clone();
        let session = py
            .detach(move || origofs_sdk::fuse::spawn(ws, Path::new(&mp)))
            .map_err(io_err)?;
        Ok(Mount {
            session: Some(session),
            mountpoint,
        })
    }

    /// FUSE mounting is not available on this platform (Unix/FUSE only). Use the
    /// HTTP API (`origofs.fastapi`) or embed the SDK directly.
    #[cfg(not(unix))]
    fn mount(&self, _mountpoint: String) -> PyResult<()> {
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
    #[pyo3(signature = (addr, shutdown = None))]
    fn serve_nfs<'py>(
        &self,
        py: Python<'py>,
        addr: String,
        shutdown: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        // Converted while we hold the GIL; the resulting future is plain Rust.
        let stopper = shutdown
            .map(pyo3_async_runtimes::tokio::into_future)
            .transpose()?;
        future_into_py(py, async move {
            // Dropping `server` (which is what a cancelled Python task does to
            // this future) is itself a full teardown — see `NfsServer::drop`.
            let mut server = NfsServer::start(ws, addr).map_err(io_err)?;
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
    #[pyo3(signature = (_addr, _shutdown = None))]
    fn serve_nfs(&self, _addr: String, _shutdown: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        Err(unsupported("NFS serving"))
    }
}

/// Whether a FUSE mount is possible here (`/dev/fuse` present and usable).
/// Always `false` off Unix (no FUSE).
#[cfg(unix)]
#[pyfunction]
fn fuse_mountable() -> bool {
    origofs_sdk::fuse::mountable()
}

#[cfg(not(unix))]
#[pyfunction]
fn fuse_mountable() -> bool {
    false
}

/// The origofs content address (BLAKE3, hex) of `data` — the same hash a passage
/// carries. Lets a Python pipeline key *derived* content (e.g. Markdown converted
/// from a PDF) by the same scheme origofs uses, so dedup / incremental-embedding
/// keys stay consistent across native and converted passages.
#[pyfunction]
fn content_hash(data: Vec<u8>) -> String {
    origofs_sdk::Hash::of(&data).to_hex()
}

/// The compiled extension is imported as `origofs._origofs`; the pure-Python package
/// `origofs` (see `python/origofs/__init__.py`) re-exports everything from it and adds
/// optional integrations like `origofs.fastapi`.
#[pymodule]
fn _origofs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Workspace>()?;
    m.add_class::<WriteCtx>()?;
    m.add_class::<WriteOutcome>()?;
    m.add_class::<S3Config>()?;
    m.add_class::<GcsConfig>()?;
    m.add_class::<Subscription>()?;
    m.add_class::<CoeditDoc>()?;
    m.add_class::<CoeditSyncReply>()?;
    m.add_class::<CoeditRelayNote>()?;
    m.add_class::<CoeditRelaySub>()?;
    #[cfg(unix)]
    m.add_class::<Mount>()?;
    m.add_function(wrap_pyfunction!(fuse_mountable, m)?)?;
    m.add_function(wrap_pyfunction!(content_hash, m)?)?;
    m.add("OrigoFSError", m.py().get_type::<OrigoFSError>())?;
    m.add("ConflictError", m.py().get_type::<ConflictError>())?;
    Ok(())
}
