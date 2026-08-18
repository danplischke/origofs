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
    PyPermissionError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
#[cfg(target_os = "linux")]
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
        // The actor's write policy forbids it — the closest built-in is
        // `PermissionError`, which is what a caller would `except` on.
        Denied(_) => PyPermissionError::new_err(msg),
        _ => OrigoFSError::new_err(msg),
    }
}

/// Map a host I/O error onto the closest Python exception.
///
/// No longer `#[cfg(unix)]`: it used to serve only the FUSE/NFS mount helpers, but
/// the streaming bindings (`write_path`, `read_to_path`) touch the local
/// filesystem on every platform, and gating it would have broken the Windows
/// build the moment they were added.
///
/// The kind is mapped rather than flattened to a bare `OSError`, because
/// `FileNotFoundError`/`PermissionError` are what a caller actually writes
/// `except` for — and both are `OSError` subclasses, so nothing that caught the
/// old shape stops working.
fn io_err(e: std::io::Error) -> PyErr {
    use std::io::ErrorKind;
    let msg = e.to_string();
    match e.kind() {
        ErrorKind::NotFound => PyFileNotFoundError::new_err(msg),
        ErrorKind::PermissionDenied => PyPermissionError::new_err(msg),
        ErrorKind::AlreadyExists => PyFileExistsError::new_err(msg),
        ErrorKind::IsADirectory => PyIsADirectoryError::new_err(msg),
        ErrorKind::NotADirectory => PyNotADirectoryError::new_err(msg),
        _ => PyOSError::new_err(msg),
    }
}

/// Error for a mount/serve operation this platform doesn't have.
///
/// The two surfaces have different reaches, so this is deliberately vague about
/// *which* one is missing and lets the caller name it:
///
/// * **FUSE mounting is Linux-only.** `fuser` links `libfuse`, which on macOS
///   means the macFUSE kernel extension — a system dependency a `pip install`
///   cannot provide, and one the published wheels therefore do not assume.
///   `docs/DESIGN.md` already puts macOS on NFSv3 rather than FUSE.
/// * **NFS serving is Unix-wide**, so macOS keeps `serve_nfs` and the mount
///   story it was always meant to have there.
///
/// Needed on every target except Linux, which has both.
#[cfg(not(target_os = "linux"))]
fn unsupported(what: &str) -> PyErr {
    PyOSError::new_err(format!(
        "{what} is not available on this platform; use serve_nfs (Unix), the HTTP API (origofs.fastapi), or embed the SDK"
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
    d.set_item("uid", i.uid)?;
    d.set_item("gid", i.gid)?;
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
    // When the durable bytes were last crystallized, as distinct from `since`
    // (when the path first went live, which never moves). `None` for a path that
    // is live but has never been checkpointed -- so a UI can say "last saved 3
    // minutes ago" instead of only "this may be stale" (#97).
    d.set_item("checkpointed_at", l.checkpointed_at)?;
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
    // Objects written by a newer origofs than this build can decode. `scan` only
    // reports them; `rebuild` raises instead of restoring a truncated history.
    d.set_item("unsupported", r.unsupported)?;
    d.set_item("unsupported_kinds", r.unsupported_kinds.clone())?;
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
/// is AWS-only and does nothing on GCP. Set `session_token` alongside the key pair
/// for temporary credentials (AWS SSO / SAML federation). To use GCS over this S3
/// path, set `endpoint="https://storage.googleapis.com"` and supply GCS **HMAC**
/// interop keys. For native GCS auth (service account / ADC / workload identity)
/// use `GcsConfig` + `Workspace.open_gcs` instead.
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
        session_token = None,
        prefix = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        allow_http: bool,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
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
                session_token,
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
        allow_http = false,
    ))]
    fn new(
        bucket: String,
        service_account_path: Option<String>,
        service_account_key: Option<String>,
        application_credentials: Option<String>,
        prefix: Option<String>,
        // Only for a plaintext emulator; real GCS is always https.
        allow_http: bool,
    ) -> Self {
        Self {
            inner: CoreGcsConfig {
                bucket,
                service_account_path,
                service_account_key,
                application_credentials,
                prefix,
                allow_http,
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
#[cfg(target_os = "linux")]
#[pyclass]
struct Mount {
    session: Option<fuser::BackgroundSession>,
    mountpoint: String,
}

#[cfg(target_os = "linux")]
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

/// A live **tree-shaped** co-edited document (issue #92): a `Y.XmlFragment` a
/// rich-text editor (`@platejs/yjs`, `y-prosemirror`, `y-slate`, TipTap) binds to
/// natively, instead of `CoeditDoc`'s flat `Y.Text`.
///
/// Obtain one from [`Workspace.open_coedit_tree`], drive it with the same y-sync
/// wire protocol, and land it with [`Workspace.checkpoint_coedit_tree`] — which
/// takes *your* serialized bytes plus a span map, because origofs does not own the
/// document schema. `authors()` is the map those spans resolve against.
///
/// Internally synchronized, so one instance is safe to share across many concurrent
/// WebSocket handlers — exactly how a FastAPI room mounts it.
#[pyclass]
struct CoeditTreeDoc {
    inner: Arc<tokio::sync::Mutex<origofs_sdk::CoeditTreeDoc>>,
}

#[pymethods]
impl CoeditTreeDoc {
    /// A fresh, empty document rooted at the ``XmlFragment`` named `root`. Server
    /// rooms come from `Workspace.open_coedit_tree`.
    #[new]
    #[pyo3(signature = (root=None))]
    fn new(root: Option<String>) -> Self {
        let root = root.unwrap_or_else(|| origofs_sdk::DEFAULT_TREE_ROOT.to_string());
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(origofs_sdk::CoeditTreeDoc::new(
                &root,
            ))),
        }
    }

    /// Append ``<tag>text</tag>`` to the root, attributed to `ctx`, and return the
    /// node id stamped on the text run — ready to cite in a span map.
    ///
    /// The tree analogue of ``CoeditDoc.insert``, and deliberately just as narrow:
    /// the in-process path for a Python-side agent seeding or appending to a
    /// document, and for a test client. A real editor does not use it — it owns the
    /// schema and drives arbitrary tree edits over y-sync, where ``handle_sync``
    /// attributes them.
    #[pyo3(signature = (ctx, tag, text))]
    fn append_text<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        tag: String,
        text: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            Ok(inner.lock().await.append_text(c, &tag, &text))
        })
    }

    /// The y-sync frame to greet a freshly-connected client with.
    fn sync_start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let bytes = inner.lock().await.sync_start();
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// This document's Yjs state vector (``encodeStateVector``).
    fn state_vector<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let bytes = inner.lock().await.state_vector();
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// This document's full state as a Yjs update (``encodeStateAsUpdate``).
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

    /// Merge a y-sync frame that was already attributed elsewhere — a peer
    /// worker's relayed delta, or (for a client-side doc) the server's own reply —
    /// *without* re-attribution. Idempotent.
    fn apply_relayed<'py>(&self, py: Python<'py>, frame: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.apply_relayed(&frame).map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Every node id origofs has stamped, as ``{node: (actor_id, session_id)}``.
    ///
    /// This is what a span map resolves against — useful for inspection, and for a
    /// host that wants to show "who wrote this node" without waiting for a
    /// checkpoint. A node id absent from this map resolves to no author.
    fn authors<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let authors = inner.lock().await.authors();
            Python::attach(|py| {
                let out = pyo3::types::PyDict::new(py);
                for (node, (actor, session)) in authors {
                    out.set_item(node, (actor, session))?;
                }
                Ok(out.unbind())
            })
        })
    }

    /// Whether this document was resumed from a coherent sidecar rather than
    /// created empty.
    ///
    /// **Check this before binding an editor.** origofs cannot rebuild a *tree*
    /// from a flat file — that needs your schema — so a document whose sidecar is
    /// missing or stale opens empty, and checkpointing it would write an empty body
    /// over a file with content. Seed it from ``await ws.read(path)`` first.
    fn resumed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.resumed()) })
    }

    /// Whether the tree has no nodes at all.
    fn is_empty<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.is_empty()) })
    }

    /// Every text run in document order, as
    /// ``{"text", "node", "actor", "session"}`` dicts — the server-side reading of
    /// what a browser host gets from ``ytext.toDelta()``.
    ///
    /// This is what a caller serializing the document *itself* (a Python agent
    /// rather than a browser editor) walks to build its span map: emit bytes for
    /// each run, record the byte range it occupied, cite its ``node``. A run
    /// origofs never stamped has ``node`` ``None`` and actor ``0``.
    fn runs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let runs = inner.lock().await.runs();
            Python::attach(|py| {
                let out = pyo3::types::PyList::empty(py);
                for run in runs {
                    let item = pyo3::types::PyDict::new(py);
                    item.set_item("text", run.text)?;
                    item.set_item("node", run.node)?;
                    item.set_item("actor", run.actor)?;
                    item.set_item("session", run.session)?;
                    out.append(item)?;
                }
                Ok(out.unbind())
            })
        })
    }

    /// The whole tree's text in document order, with no structure — a cheap
    /// projection for inspection and tests. **Not** the durable body: that is your
    /// serialization, which only you can produce.
    fn plain_text<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.plain_text()) })
    }

    fn __repr__(&self) -> String {
        "CoeditTreeDoc()".to_string()
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

    /// Open a local workspace **encrypted at rest**: SQLite metadata at `db_path`,
    /// content under `cas_dir` sealed with XChaCha20-Poly1305 under a key derived
    /// from `passphrase` (Argon2id) over a per-store random salt.
    ///
    /// Encryption was reachable only from Rust — no binding existed at all — so a
    /// Python deployment could not have encryption at rest, whatever the docs
    /// implied.
    ///
    /// The same passphrase must be given on every open; a wrong one fails loudly
    /// rather than returning garbage. The salt is created on first open, is not
    /// secret, and lives beside the content store so it survives losing the
    /// metadata database.
    ///
    /// Two things to know. Key derivation is Argon2id and deliberately slow, and it
    /// runs on the calling thread — call this at startup, not per request. And
    /// addresses stay the *plaintext* hash (convergent encryption) so dedup still
    /// works, which makes a shared encrypted store an existence oracle: use
    /// per-tenant keys if that matters.
    #[staticmethod]
    fn open_local_encrypted<'py>(
        py: Python<'py>,
        db_path: String,
        cas_dir: String,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_local_encrypted(&db_path, &cas_dir, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open an S3-backed workspace **encrypted at rest**. See
    /// [`open_local_encrypted`] for the key-derivation and dedup caveats.
    #[staticmethod]
    fn open_s3_encrypted<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> =
                Arc::new(origofs_sdk::SqliteMetadataStore::open(&db_path).map_err(to_pyerr)?);
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::s3(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a Postgres + S3 workspace **encrypted at rest** — the production
    /// pairing with encryption on. See [`open_local_encrypted`] for the caveats.
    #[staticmethod]
    fn open_pg_s3_encrypted<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: S3Config,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> = Arc::new(
                origofs_sdk::PostgresMetadataStore::connect(&dsn)
                    .await
                    .map_err(to_pyerr)?,
            );
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::s3(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a GCS-backed workspace **encrypted at rest**. See
    /// [`open_local_encrypted`] for the key-derivation and dedup caveats.
    #[staticmethod]
    fn open_gcs_encrypted<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> =
                Arc::new(origofs_sdk::SqliteMetadataStore::open(&db_path).map_err(to_pyerr)?);
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::gcs(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// Open a Postgres + native-GCS workspace **encrypted at rest** — the
    /// production pairing on Google Cloud with encryption on. See
    /// [`open_local_encrypted`] for the caveats.
    #[staticmethod]
    fn open_pg_gcs_encrypted<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
        passphrase: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let meta: Arc<dyn origofs_sdk::MetadataStore> = Arc::new(
                origofs_sdk::PostgresMetadataStore::connect(&dsn)
                    .await
                    .map_err(to_pyerr)?,
            );
            let backend: Arc<dyn origofs_sdk::ContentStore> =
                Arc::new(origofs_sdk::ObjectContentStore::gcs(cfg.inner).map_err(to_pyerr)?);
            let ws = CoreWorkspace::open_encrypted(meta, backend, &passphrase)
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

    /// Grant `actor` access at and below `prefix` (``docs/PERMISSIONS.md`` §3b).
    ///
    /// `perms` is comma-separated: ``"read"``, ``"write"``, ``"propose"``, or
    /// ``"none"``. Longest matching prefix wins, and an actor with no covering
    /// grant falls back to its write policy — so grants are purely additive.
    ///
    /// Grants restrict this SDK, the HTTP API, MCP and the CLI. They are **not**
    /// enforceable through a FUSE/NFS mount, which has no actor context.
    ///
    /// ```python
    /// await ws.grant(agent, "/", "read")              # read-only everywhere...
    /// await ws.grant(agent, "/src/parser", "read,write")  # ...except here
    /// ```
    fn grant<'py>(
        &self,
        py: Python<'py>,
        actor: i64,
        prefix: String,
        perms: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let p = origofs_sdk::Perms::parse(&perms).map_err(to_pyerr)?;
            ws.grant(actor, &prefix, p).await.map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Remove `actor`'s grant at exactly `prefix`. Returns whether one existed, so
    /// a revoke against a typo'd prefix does not look like it closed access.
    fn revoke<'py>(
        &self,
        py: Python<'py>,
        actor: i64,
        prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            ws.revoke(actor, &prefix).await.map_err(to_pyerr)
        })
    }

    /// An actor's grants, longest prefix first: dicts of `prefix` and `perms`.
    fn grants<'py>(&self, py: Python<'py>, actor: i64) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let grants = ws.grants(actor).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let out = PyList::empty(py);
                for g in &grants {
                    let d = PyDict::new(py);
                    d.set_item("prefix", &g.path_prefix)?;
                    d.set_item("perms", g.perms.as_str())?;
                    out.append(d)?;
                }
                Ok(out.into_any().unbind())
            })
        })
    }

    /// What `actor` may do at `path`, after grant resolution: a string like
    /// ``"read,write"`` or ``"none"``.
    fn effective_perms<'py>(
        &self,
        py: Python<'py>,
        actor: i64,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let p = ws.effective_perms(actor, &path).await.map_err(to_pyerr)?;
            Ok(p.as_str())
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

    /// Change a path's permission bits (``chmod``); returns the updated inode dict.
    ///
    /// Only the low 12 bits are honoured — the file-type bits are preserved.
    ///
    /// This is **not** an access check: nothing in origofs consults ``mode`` to
    /// allow or deny an operation. On a FUSE mount the kernel does, because the
    /// mount asks it to. See ``docs/PERMISSIONS.md``.
    ///
    /// ```python
    /// await ws.chmod("/build.sh", 0o755)
    /// ```
    fn chmod<'py>(&self, py: Python<'py>, path: String, mode: u32) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let inode = ws.chmod(&path, mode).await.map_err(to_pyerr)?;
            Python::attach(|py| inode_dict(py, &inode))
        })
    }

    /// Change a path's owning user and/or group (``chown``); returns the updated
    /// inode dict. Passing ``None`` for either leaves that half unchanged, matching
    /// ``chown(2)``'s ``-1``.
    ///
    /// Ownership exists so the mounts can report a real owner; origofs's own
    /// principals are **actors**, not uids (``docs/PERMISSIONS.md`` §2).
    ///
    /// ```python
    /// await ws.chown("/data", uid=1000, gid=1000)
    /// await ws.chown("/data", gid=100)          # uid untouched
    /// ```
    #[pyo3(signature = (path, uid=None, gid=None))]
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

    /// ``chmod`` attributed to ``ctx`` (records an edit-op). Prefer this wherever an
    /// actor is known: a propose-only actor is **refused**, because there is no
    /// propose-shaped equivalent of a ``chmod`` and ``chmod 000`` on a file an agent
    /// may not write is exactly the unreviewed damage the write policy prevents.
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

    /// ``chown`` attributed to ``ctx``. See :meth:`chmod_as`.
    #[pyo3(signature = (ctx, path, uid=None, gid=None))]
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

    /// Open a **tree-shaped** live co-editing document for `path` (issue #92),
    /// rooted at the ``XmlFragment`` named `root` — the shape `@platejs/yjs`,
    /// `y-prosemirror` and `y-slate` bind to natively.
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
    #[cfg(target_os = "linux")]
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
    #[cfg(not(target_os = "linux"))]
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

    // ── multi-workspace ──────────────────────────────────────────────────────
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

    // ── attributed mutations ─────────────────────────────────────────────────
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
    #[pyo3(signature = (ctx, path, summary = None))]
    fn remove_or_propose<'py>(
        &self,
        py: Python<'py>,
        ctx: WriteCtx,
        path: String,
        summary: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            let outcome = ws
                .remove_or_propose(c, &path, summary.as_deref())
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

    // ── symlinks ─────────────────────────────────────────────────────────────

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

    // ── maintenance ──────────────────────────────────────────────────────────
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

    // ── versioning: merge and mode ───────────────────────────────────────────
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

    // ── locks ────────────────────────────────────────────────────────────────

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

    // ── attribution: the op-log and session revert ───────────────────────────
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
}

/// `GcStats` as a plain dict, so it is directly JSON-serializable in a response.
fn gc_stats_dict(py: Python<'_>, s: &origofs_sdk::GcStats) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("reachable", s.reachable)?;
    d.set_item("deleted", s.deleted)?;
    d.set_item("bytes_freed", s.bytes_freed)?;
    d.set_item("skipped_young", s.skipped_young)?;
    d.set_item("skipped_undated", s.skipped_undated)?;
    Ok(d.unbind())
}

/// `MergeOutcome` flattened into `{outcome, commit, conflicts}`.
///
/// A tagged dict rather than four separate result types: the caller almost always
/// wants to branch on the tag and read one field, and this stays JSON-serializable
/// straight into an API response.
fn merge_outcome_dict(py: Python<'_>, outcome: &origofs_sdk::MergeOutcome) -> PyResult<Py<PyDict>> {
    use origofs_sdk::MergeOutcome::*;
    let d = PyDict::new(py);
    match outcome {
        AlreadyUpToDate => {
            d.set_item("outcome", "already_up_to_date")?;
            d.set_item("commit", py.None())?;
            d.set_item("conflicts", Vec::<String>::new())?;
        }
        FastForward(h) => {
            d.set_item("outcome", "fast_forward")?;
            d.set_item("commit", h.to_hex())?;
            d.set_item("conflicts", Vec::<String>::new())?;
        }
        Merged(h) => {
            d.set_item("outcome", "merged")?;
            d.set_item("commit", h.to_hex())?;
            d.set_item("conflicts", Vec::<String>::new())?;
        }
        Conflicts(cs) => {
            d.set_item("outcome", "conflicts")?;
            d.set_item("commit", py.None())?;
            let list: Vec<Py<PyDict>> = cs
                .iter()
                .map(|c| {
                    let e = PyDict::new(py);
                    e.set_item("path", c.path.clone())?;
                    e.set_item("kind", c.kind.clone())?;
                    Ok(e.unbind())
                })
                .collect::<PyResult<_>>()?;
            d.set_item("conflicts", list)?;
        }
    }
    Ok(d.unbind())
}

/// Whether a FUSE mount is possible here (`/dev/fuse` present and usable).
/// Always `false` off Unix (no FUSE).
#[cfg(target_os = "linux")]
#[pyfunction]
fn fuse_mountable() -> bool {
    origofs_sdk::fuse::mountable()
}

#[cfg(not(target_os = "linux"))]
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
    m.add_class::<CoeditTreeDoc>()?;
    m.add_class::<CoeditSyncReply>()?;
    m.add_class::<CoeditRelayNote>()?;
    m.add_class::<CoeditRelaySub>()?;
    #[cfg(target_os = "linux")]
    m.add_class::<Mount>()?;
    m.add_function(wrap_pyfunction!(fuse_mountable, m)?)?;
    m.add_function(wrap_pyfunction!(content_hash, m)?)?;
    m.add("OrigoFSError", m.py().get_type::<OrigoFSError>())?;
    m.add("ConflictError", m.py().get_type::<ConflictError>())?;
    Ok(())
}
