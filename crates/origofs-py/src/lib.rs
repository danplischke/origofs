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

// `Perms`/`AclGrant` (issue #123) and the usage/quota/statfs records (issues
// #116, #119) are the one group of types the sdk does not re-export, because the
// `Workspace` façade has no wrapper for the engine calls that produce them — see
// the "engine surface with no `Workspace` wrapper" note further down. They come
// from `origofs-core` directly, which this crate already depends on.
use origofs_core::{AclGrant, FsStat, LocalCasStore, Perms, Quota, Usage};
use origofs_sdk::{
    Actor, BenchOpts, BenchReport, BenchStage, BlameRange, CommitInfo, DiffEntry, DiffStatus,
    DirEntry, Event, EventSubscription, FileLayout, GcsConfig as CoreGcsConfig, Inode, LiveDoc,
    Passage, PassageOptions, Presence, RebuildReport, Residency, S3Config as CoreS3Config,
    Segmentation, Suggestion, SuggestionStatus, TrashEntry, Tunable, Workspace as CoreWorkspace,
    WriteCtx as CoreWriteCtx, WriteOutcome as CoreWriteOutcome, WritePolicy as CoreWritePolicy,
};
use pyo3::create_exception;
use pyo3::exceptions::{
    PyFileExistsError, PyFileNotFoundError, PyIsADirectoryError, PyNotADirectoryError, PyOSError,
    PyPermissionError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
#[cfg(target_os = "linux")]
use std::path::Path;
use std::sync::Arc;

create_exception!(origofs, OrigoFSError, pyo3::exceptions::PyException);
create_exception!(origofs, ConflictError, OrigoFSError);
// #159: `ConflictError` covered at least two conditions that demand *opposite*
// recoveries — re-diff-and-re-suggest vs. reseed-and-checkpoint — and the only way
// to tell them apart was a substring match on the message, which then breaks on
// any rewording. Both subclass `ConflictError`, so `except ConflictError` and the
// FastAPI 409 mapping keep working unchanged.
create_exception!(origofs, StaleBaseError, ConflictError);
create_exception!(origofs, ForeignWriteError, ConflictError);
// #164: "suggestion #N is already accepted" was a `ValueError` — saying the
// *request* was malformed when it was well-formed and merely out of date. It is
// the third outcome a reviewing caller has to handle beside the two above, and
// unlike either it is terminal: read the row's status, there is nothing to retry.
create_exception!(origofs, AlreadyResolvedError, ConflictError);

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
        StaleBase(_) => StaleBaseError::new_err(msg),
        ForeignWrite(_) => ForeignWriteError::new_err(msg),
        AlreadyResolved(_) => AlreadyResolvedError::new_err(msg),
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
    d.set_item("nlink", i.nlink)?;
    d.set_item("size", i.size)?;
    d.set_item("content", hash_opt(i.content.as_ref()))?;
    d.set_item("mtime", i.mtime)?;
    d.set_item("ctime", i.ctime)?;
    // Ownership (issue #122). `0`/`0` for anything created before the ownership
    // migration and for anything created off a mount — see `chown`. Exposed here
    // because `stat` is the only place a caller can read back what `chown` set,
    // and a chmod/chown surface whose result you cannot observe is the no-op #122
    // was about.
    d.set_item("uid", i.uid)?;
    d.set_item("gid", i.gid)?;
    Ok(d.into_any().unbind())
}

// --- trash (issue #115) ------------------------------------------------------

/// One recoverable deletion. `content` is the manifest address (hex) or `None`
/// for a directory, an empty file, or a symlink; `uid`/`gid` are the ownership the
/// entry is restored with. `actor_id`/`session_id` name who deleted it — `None`
/// for an unattributed delete (internal machinery, or a mount, which has no actor
/// context).
fn trash_entry_dict(py: Python<'_>, t: &TrashEntry) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("id", t.id)?;
    d.set_item("path", &t.path)?;
    d.set_item("kind", t.kind.as_str())?;
    d.set_item("mode", t.mode)?;
    d.set_item("size", t.size)?;
    d.set_item("content", hash_opt(t.content.as_ref()))?;
    d.set_item("symlink_target", t.symlink_target.clone())?;
    d.set_item("uid", t.owner.uid)?;
    d.set_item("gid", t.owner.gid)?;
    d.set_item("actor_id", t.actor_id)?;
    d.set_item("session_id", t.session_id)?;
    d.set_item("deleted_at", t.deleted_at)?;
    Ok(d.into_any().unbind())
}

// --- usage, quota, statfs (issues #116, #119) --------------------------------

/// `{inodes, bytes}`. Both figures are **logical** — the sum of `stat` sizes, not
/// the deduplicated on-disk footprint. That is what `du`/`df` should say and what
/// a quota is checked against.
fn usage_dict(py: Python<'_>, u: &Usage) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("inodes", u.inodes)?;
    d.set_item("bytes", u.bytes)?;
    Ok(d.unbind())
}

/// `{bytes, inodes}`, each `None` for "no limit" (the default).
fn quota_dict(py: Python<'_>, q: &Quota) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("bytes", q.bytes)?;
    d.set_item("inodes", q.inodes)?;
    Ok(d.unbind())
}

/// A `statfs(2)` answer, denominated in `block_size`-byte blocks.
fn fs_stat_dict(py: Python<'_>, s: &FsStat) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("block_size", s.block_size)?;
    d.set_item("total_blocks", s.total_blocks)?;
    d.set_item("free_blocks", s.free_blocks)?;
    d.set_item("total_inodes", s.total_inodes)?;
    d.set_item("free_inodes", s.free_inodes)?;
    Ok(d.unbind())
}

// --- path-scoped ACLs (issue #123) -------------------------------------------

/// A [`Perms`] bitset as the list of names it contains, in `read`/`write`/
/// `propose` order — JSON-serializable, and the same vocabulary `grant` accepts.
/// An empty list is an explicit deny (`Perms::NONE`), which is a grant, not the
/// absence of one.
fn perms_list(p: Perms) -> Vec<&'static str> {
    let mut out = Vec::new();
    for (bit, name) in [
        (Perms::READ, "read"),
        (Perms::WRITE, "write"),
        (Perms::PROPOSE, "propose"),
    ] {
        if p.contains(bit) {
            out.push(name);
        }
    }
    out
}

/// Parse the Python-facing permission spelling into a [`Perms`] bitset.
///
/// Accepts a string (`"write"`, `"read+write"`, `"read,propose"`, `"none"`, `""`)
/// or any iterable of strings (`["read", "write"]`). A string is *not* left to
/// pyo3's sequence extraction: a `str` is an iterable of one-character strings, so
/// `"read"` would silently become four unknown permissions rather than one known
/// one.
fn parse_perms(obj: &Bound<'_, PyAny>) -> PyResult<Perms> {
    let names: Vec<String> = match obj.extract::<String>() {
        Ok(s) => s
            .split(['+', ',', ' '])
            .filter(|p| !p.trim().is_empty())
            .map(|p| p.trim().to_string())
            .collect(),
        Err(_) => obj
            .try_iter()?
            .map(|item| item?.extract::<String>())
            .collect::<PyResult<Vec<_>>>()?,
    };
    let mut perms = Perms::NONE;
    for n in names {
        perms = perms
            | match n.to_ascii_lowercase().as_str() {
                "read" => Perms::READ,
                "write" => Perms::WRITE,
                "propose" => Perms::PROPOSE,
                "none" => Perms::NONE,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown permission {other:?} (expected \"read\", \"write\", \
                         \"propose\", or \"none\")"
                    )));
                }
            };
    }
    Ok(perms)
}

/// One prefix grant. `path_prefix` is `""` for a grant over the whole workspace —
/// the root prefix, which every more specific grant outranks.
fn acl_grant_dict(py: Python<'_>, g: &AclGrant) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("actor_id", g.actor_id)?;
    d.set_item("path_prefix", &g.path_prefix)?;
    d.set_item("perms", perms_list(g.perms))?;
    d.set_item("granted_at", g.granted_at)?;
    d.set_item("granted_by", g.granted_by)?;
    Ok(d.into_any().unbind())
}

// --- performance introspection (issue #118) ----------------------------------

/// Which of a file's chunks the store still holds. **Presence, not cache
/// residency** — a tiered store answers from either tier and nothing on the
/// object-safe trait tells them apart.
fn residency_dict(py: Python<'_>, r: &Residency) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("present", r.present)?;
    d.set_item("present_bytes", r.present_bytes)?;
    d.set_item("missing", r.missing)?;
    d.set_item(
        "missing_sample",
        r.missing_sample
            .iter()
            .map(|h| h.to_hex())
            .collect::<Vec<_>>(),
    )?;
    Ok(d.unbind())
}

/// What one file costs to read. `chunks` is the read-amplification number: a
/// whole-file read fetches exactly that many objects.
fn file_layout_dict(py: Python<'_>, l: &FileLayout) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("size", l.size)?;
    d.set_item("manifest", hash_opt(l.manifest.as_ref()))?;
    d.set_item("chunks", l.chunks)?;
    d.set_item("distinct_chunks", l.distinct_chunks)?;
    d.set_item("distinct_bytes", l.distinct_bytes)?;
    d.set_item("smallest", l.smallest)?;
    d.set_item("largest", l.largest)?;
    d.set_item("median", l.median)?;
    // Derived, but derived from a formula a caller should not have to re-guess.
    d.set_item("mean", l.mean())?;
    // Repetition *within this file* only — what it shares with other files is not
    // measured, so this is a lower bound on the real saving.
    d.set_item("self_dedup", l.self_dedup())?;
    d.set_item("histogram", l.histogram.clone())?;
    match &l.residency {
        Some(r) => d.set_item("residency", residency_dict(py, r)?)?,
        None => d.set_item("residency", py.None())?,
    }
    d.set_item("chunker", l.chunker)?;
    Ok(d.unbind())
}

/// One measured phase of a benchmark. Durations are seconds (floats), so the
/// record stays JSON-serializable and unit-free at the call site.
fn bench_stage_dict(py: Python<'_>, s: &BenchStage) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("ops", s.ops)?;
    d.set_item("bytes", s.bytes)?;
    // Time *inside* the engine call, not wall time across the phase — body
    // generation between writes is this process's cost, not the store's.
    d.set_item("elapsed_secs", s.elapsed.as_secs_f64())?;
    d.set_item("bytes_per_sec", s.bytes_per_sec())?;
    d.set_item("mean_secs", s.mean().as_secs_f64())?;
    // Nearest-rank quantiles: at the default 8 ops, interpolating invents a
    // precision the sample does not have. Read any of these next to `ops`.
    d.set_item("p50_secs", s.quantile(0.5).as_secs_f64())?;
    d.set_item("p95_secs", s.quantile(0.95).as_secs_f64())?;
    d.set_item("max_secs", s.quantile(1.0).as_secs_f64())?;
    Ok(d.unbind())
}

/// A concurrency knob as configured: `{var, value}`, `value` `None` when the
/// environment variable is unset and the engine default applies.
fn tunable_dict(py: Python<'_>, t: &Tunable) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("var", t.var)?;
    d.set_item("value", t.value)?;
    Ok(d.unbind())
}

/// A benchmark run, echoing the options it ran under so the report is
/// self-describing.
fn bench_report_dict(py: Python<'_>, r: &BenchReport) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    let opts = PyDict::new(py);
    opts.set_item("dir", &r.opts.dir)?;
    opts.set_item("files", r.opts.files)?;
    opts.set_item("file_size", r.opts.file_size)?;
    opts.set_item("seed", r.opts.seed)?;
    opts.set_item("keep", r.opts.keep)?;
    opts.set_item("force", r.opts.force)?;
    d.set_item("opts", opts)?;
    d.set_item("total_bytes", r.total_bytes)?;
    d.set_item("chunker", r.chunker)?;
    d.set_item(
        "upload_concurrency",
        tunable_dict(py, &r.upload_concurrency)?,
    )?;
    d.set_item("fetch_concurrency", tunable_dict(py, &r.fetch_concurrency)?)?;
    d.set_item("chunks", r.chunks)?;
    // Below `chunks` means the run deduplicated against itself and the write
    // figure is overstated — which is why it is reported rather than assumed.
    d.set_item("distinct_chunks", r.distinct_chunks)?;
    d.set_item("write", bench_stage_dict(py, &r.write)?)?;
    d.set_item("read", bench_stage_dict(py, &r.read)?)?;
    // Not "warm" against a "cold" first pass: nothing here evicts a cache, so the
    // honest claim is the narrow one — this pass ran second.
    d.set_item("reread", bench_stage_dict(py, &r.reread)?)?;
    d.set_item("kept", r.kept)?;
    Ok(d.unbind())
}

/// Split an absolute path into `(parent_dir, name)` for the inode-addressed
/// engine ops (`link`). The name itself is left to the engine's
/// `validate_component`, which is the one place that rule lives.
fn split_parent(path: &str) -> PyResult<(String, String)> {
    match path.trim_end_matches('/').rsplit_once('/') {
        Some((dir, name)) if !name.is_empty() => Ok((
            if dir.is_empty() {
                "/".to_string()
            } else {
                dir.to_string()
            },
            name.to_string(),
        )),
        _ => Err(PyValueError::new_err(format!(
            "{path:?} is not an absolute path to a name"
        ))),
    }
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

// --- path scoping (issue #125) ----------------------------------------------

/// A surface's view of a workspace: everything, or one subtree.
///
/// Scoping is **not** authorization. A `Scope` restricts *what a surface can
/// address*; an ACL (`Workspace.grant`) restricts *what an actor may do*. A
/// deployment wants both, and they sit in different places on purpose — a scope
/// belongs to the router or connection and applies before any engine call, so an
/// individual handler cannot forget it.
///
/// This is the engine's own rule, not a second implementation of it. The four
/// properties it encodes are each load-bearing, and three are things a hand-rolled
/// version gets wrong:
///
/// 1. **Directory-boundary matching, not `startswith`** — `/tenant-a` does not
///    cover `/tenant-abc`, precisely the neighbour a scope exists to exclude.
/// 2. **Prepend, don't compare** — `resolve` puts the caller's path *inside* the
///    root, so another tenant's data is not addressable at all rather than
///    addressable and rejected.
/// 3. **A `None` path is outside every scope but the whole one** — a record naming
///    no path (an idle presence row) still tells a scoped reader a neighbour
///    exists.
/// 4. **Out of scope is "not found", never "forbidden"** — so `require` raises
///    `FileNotFoundError`, deliberately not `PermissionError`: a 403 confirms
///    something exists at a path the caller may not see, which is the inference a
///    scope exists to prevent.
///
/// ```python
/// scope = origofs.Scope.at("/tenant-a")
/// await ws.read(scope.resolve("notes.txt"))   # -> /tenant-a/notes.txt
/// scope.require(suggestion["path"])           # FileNotFoundError if not theirs
/// ```
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
struct Scope {
    inner: origofs_sdk::Scope,
}

#[pymethods]
impl Scope {
    /// The whole workspace — no scoping. Every path resolves to itself and every
    /// record is in scope.
    #[staticmethod]
    fn whole() -> Self {
        Self {
            inner: origofs_sdk::Scope::whole(),
        }
    }

    /// A scope rooted at `root`, which **must be absolute**. A trailing slash is
    /// ignored, so `"/t"` and `"/t/"` are the same scope and `"/"` is `whole()`.
    ///
    /// A *relative* root raises rather than being quietly read as absolute: the
    /// root decides what a surface can reach at all, and guessing at an ambiguous
    /// one risks scoping to a subtree the caller did not mean — a scope that is
    /// wrong in that direction fails open.
    #[staticmethod]
    fn at(root: &str) -> PyResult<Self> {
        Ok(Self {
            inner: origofs_sdk::Scope::at(root).map_err(to_pyerr)?,
        })
    }

    /// The normalized root, or `""` for the whole workspace.
    #[getter]
    fn root(&self) -> &str {
        self.inner.root()
    }

    /// Whether this scope covers the whole workspace.
    #[getter]
    fn is_whole(&self) -> bool {
        self.inner.is_whole()
    }

    /// Whether `path` is the root itself or sits beneath it. `None` is contained
    /// only by the whole scope.
    #[pyo3(signature = (path))]
    fn contains(&self, path: Option<&str>) -> bool {
        self.inner.contains(path)
    }

    /// Resolve a caller-supplied path *inside* this scope. A `..` component raises
    /// `ValueError` — refused before any lookup, so it reveals nothing about what
    /// exists.
    fn resolve(&self, path: &str) -> PyResult<String> {
        self.inner.resolve(path).map_err(scope_err)
    }

    /// Refuse a record outside this scope, for anything addressed by something
    /// other than a path (a suggestion id, a lock) where a caller could otherwise
    /// probe for a neighbour's records by guessing ids.
    #[pyo3(signature = (path))]
    fn require(&self, path: Option<&str>) -> PyResult<()> {
        self.inner.require(path).map_err(scope_err)
    }

    fn __repr__(&self) -> String {
        if self.inner.is_whole() {
            "Scope(whole)".to_string()
        } else {
            format!("Scope(root={:?})", self.inner.root())
        }
    }
}

/// Map a scope refusal onto the exception the property it protects requires.
///
/// A traversal is a caller error refused before any lookup, so `ValueError`. Out
/// of scope is **`FileNotFoundError`** rather than `PermissionError` on purpose:
/// a scoped caller must not be able to tell "this exists but is not yours" from
/// "this does not exist", and the exception type is the first place that
/// distinction leaks.
fn scope_err(e: origofs_sdk::ScopeError) -> PyErr {
    match e {
        origofs_sdk::ScopeError::Traversal => {
            PyValueError::new_err("path may not contain '..' (refused before any lookup)")
        }
        origofs_sdk::ScopeError::OutOfScope => {
            PyFileNotFoundError::new_err("no such path in this scope")
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

// --- read-cache config ------------------------------------------------------

/// Bounds for the local read cache in front of an object store (issue #114).
/// Pass to `Workspace.open_s3_cached` / `open_gcs_cached` and their `open_pg_*`
/// forms.
///
/// The tier keeps recently-read chunks on local disk under `dir` and evicts to
/// stay inside **both** bounds: at most `max_bytes` of cache, and never taking the
/// filesystem below `min_free_bytes` free. The second is the one that matters in
/// practice — a cache that fills the disk takes the workspace down with it.
///
/// The defaults are the Rust ones: 8 GiB of cache, yielding under 2 GiB free.
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
struct CacheConfig {
    inner: origofs_sdk::CacheConfig,
}

#[pymethods]
impl CacheConfig {
    #[new]
    #[pyo3(signature = (dir, max_bytes = None, min_free_bytes = None))]
    fn new(dir: String, max_bytes: Option<u64>, min_free_bytes: Option<u64>) -> Self {
        let mut inner = origofs_sdk::CacheConfig::new(dir);
        if let Some(n) = max_bytes {
            inner = inner.max_bytes(n);
        }
        if let Some(n) = min_free_bytes {
            inner = inner.min_free_bytes(n);
        }
        Self { inner }
    }

    fn __repr__(&self) -> String {
        format!(
            "CacheConfig(dir={:?}, max_bytes={}, min_free_bytes={})",
            self.inner.dir, self.inner.max_bytes, self.inner.min_free_bytes
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
    /// The SDK's mount guard, which stops the change-feed watcher **before** the
    /// unmount (issue #75). Holding the guard rather than a bare
    /// `BackgroundSession` is what carries that ordering into Python: dropping
    /// this object from Python has the same teardown discipline as dropping it
    /// from Rust.
    session: Option<origofs_sdk::fuse::Mount>,
    mountpoint: String,
}

#[cfg(target_os = "linux")]
#[pymethods]
impl Mount {
    /// Unmount now (idempotent).
    fn unmount(&mut self) {
        // Dropping the guard stops the watcher, then unmounts, in that order.
        self.session.take();
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
    fn start(
        ws: CoreWorkspace,
        addr: String,
        ctx: Option<origofs_sdk::WriteCtx>,
    ) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("origofs-nfs")
            .build()?;
        let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn_on(
            async move {
                tokio::select! {
                    r = origofs_sdk::nfs::serve_as(ws, &addr, ctx) => r,
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
    content_changed: bool,
    unhandled: Vec<u8>,
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

    /// Whether this payload changed the **document**, rather than only relaying
    /// presence. Gate periodic checkpointing on this: awareness (cursor presence)
    /// is broadcast too, and every real Yjs client emits it constantly without
    /// anyone typing.
    #[getter]
    fn content_changed(&self) -> bool {
        self.content_changed
    }

    /// Outer y-websocket message tags in this payload that carried no effect —
    /// empty for every well-framed payload (#162).
    ///
    /// A non-empty value almost always means the client is sending **bare y-sync**
    /// frames instead of the y-websocket envelope this server speaks: a bare
    /// ``messageYjsUpdate`` is tag 2, which is ``messageAuth`` in the envelope, so
    /// it decodes cleanly and is dropped. The socket then connects, handshakes,
    /// reports the right peer count -- and never converges. Surface this to the
    /// client or log it; the server also logs it at ``warn``.
    #[getter]
    fn unhandled<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.unhandled)
    }

    fn __repr__(&self) -> String {
        format!(
            "CoeditSyncReply(reply={} bytes, broadcast={} bytes, content_changed={}, unhandled={:?})",
            self.reply.len(),
            self.broadcast.len(),
            self.content_changed,
            self.unhandled
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

    /// Insert `chunk` at `index`, a UTF-8 **byte** offset (not UTF-16 as in Yjs:
    /// the document indexes bytes, matching what blame stores), attributed
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

    /// Remove `length` bytes starting at `index` (UTF-8 byte offsets).
    #[pyo3(signature = (index, length))]
    fn remove<'py>(&self, py: Python<'py>, index: u32, length: u32) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.remove(index, length);
            Ok(())
        })
    }

    /// Start tracking `ctx`'s edits so its actor can undo them, and **call this
    /// when a socket joins, not when somebody presses Ctrl+Z**.
    ///
    /// A `yrs` undo manager captures changes by observing transactions as they
    /// commit, so one created after an edit sees an empty stack however recent
    /// that edit was. Tracking lazily on the first undo request would silently
    /// mean there was never anything to undo.
    ///
    /// Idempotent per session: calling it again for the same actor adds that
    /// session's origin to the stack the actor already has, so a second browser
    /// tab shares one stack rather than starting a rival.
    fn track_undo<'py>(&self, py: Python<'py>, ctx: WriteCtx) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            inner.lock().await.track_undo(c);
            Ok(())
        })
    }

    /// Drop `actor`'s undo stack, at their last socket's disconnect. Undo is an
    /// editor affordance, not history: a stack does not outlive the room.
    fn untrack_undo<'py>(&self, py: Python<'py>, actor: i64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.untrack_undo(actor);
            Ok(())
        })
    }

    /// Whether `actor` has anything to undo — for greying out the affordance.
    fn can_undo<'py>(&self, py: Python<'py>, actor: i64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.can_undo(actor)) })
    }

    /// Whether `actor` has anything to redo.
    fn can_redo<'py>(&self, py: Python<'py>, actor: i64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.can_redo(actor)) })
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
                        content_changed: out.content_changed,
                        unhandled: out.unhandled,
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
/// rich-text editor (`@platejs/yjs`/`@slate-yjs/core`, `y-prosemirror`, TipTap) binds to
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

    /// Start tracking `ctx`'s edits for undo — at socket join, for the reason
    /// spelled out on `CoeditDoc.track_undo`.
    fn track_undo<'py>(&self, py: Python<'py>, ctx: WriteCtx) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let c = ctx.inner;
        future_into_py(py, async move {
            inner.lock().await.track_undo(c);
            Ok(())
        })
    }

    /// Drop `actor`'s undo stack, at their last socket's disconnect.
    fn untrack_undo<'py>(&self, py: Python<'py>, actor: i64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.untrack_undo(actor);
            Ok(())
        })
    }

    /// Whether `actor` has anything to undo.
    fn can_undo<'py>(&self, py: Python<'py>, actor: i64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.can_undo(actor)) })
    }

    /// Whether `actor` has anything to redo.
    fn can_redo<'py>(&self, py: Python<'py>, actor: i64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.can_redo(actor)) })
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
                        content_changed: out.content_changed,
                        unhandled: out.unhandled,
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
    /// over a file with content. Seed it from ``await ws.read(path)`` first, then
    /// declare that with ``seeded_from``.
    ///
    /// Since #158 forgetting to is an error rather than data loss: a checkpoint from
    /// an unseeded document over a non-empty file raises ``ForeignWriteError``.
    fn resumed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.resumed()) })
    }

    /// Declare that this document now represents ``body`` — the seeding handshake
    /// (#158).
    ///
    /// origofs cannot parse bytes back into tree nodes (that needs your schema), so
    /// a document that could not resume opens **empty** and checkpointing it would
    /// replace the file's content with nothing. That is refused until you say the
    /// document accounts for those bytes: when ``resumed()`` is false, read the
    /// file, parse it into the tree with your own parser, and pass the same bytes
    /// here.
    ///
    /// Seeding from the file's current bytes *without* parsing them is the
    /// deliberate-overwrite escape hatch: it says "I have looked at what is there
    /// and I am replacing it", which is a thing a host may legitimately mean — and
    /// which now has to be written down rather than being the default.
    fn seeded_from<'py>(&self, py: Python<'py>, body: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.lock().await.seeded_from(&body);
            Ok(())
        })
    }

    /// Hex BLAKE3 of the body this document is **coherent with** — what it resumed
    /// from, was seeded from, or last crystallized — or ``None`` when it has no
    /// established relationship to any file.
    ///
    /// This is what ``checkpoint_coedit_tree`` compares the file against. Note it
    /// is a BLAKE3 of the bytes, *not* the chunk-manifest address ``stat()`` returns
    /// — the two are different hashes of the same content and never compare equal.
    fn base_hash<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            Ok(inner.lock().await.base_hash().map(|h| {
                h.iter().fold(String::with_capacity(64), |mut s, b| {
                    use std::fmt::Write as _;
                    let _ = write!(s, "{b:02x}");
                    s
                })
            }))
        })
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

    /// `open_s3` with a **bounded local read cache** (issue #114).
    ///
    /// Every read of an uncached chunk otherwise costs a network round trip, which
    /// is what makes a mount or a repeated ranged read over an object store slow.
    /// The tier keeps recently-read chunks on local disk inside `cache`'s bounds.
    ///
    /// The packed and encrypted variants were bound and this one was not, so a
    /// Python caller could compose every remote stack except the one that makes a
    /// remote stack fast.
    #[staticmethod]
    fn open_s3_cached<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: S3Config,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_s3_cached(&db_path, cfg.inner, cache.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// `open_gcs` with a bounded local read cache. See `open_s3_cached`.
    #[staticmethod]
    fn open_gcs_cached<'py>(
        py: Python<'py>,
        db_path: String,
        cfg: GcsConfig,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_gcs_cached(&db_path, cfg.inner, cache.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// `open_pg_s3` with a bounded local read cache — the shape a multi-writer
    /// deployment actually wants: one shared database, one shared bucket, and each
    /// host keeping its own hot chunks locally. See `open_s3_cached`.
    #[staticmethod]
    fn open_pg_s3_cached<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: S3Config,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_s3_cached(&dsn, cfg.inner, cache.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, Workspace { inner: ws }))
        })
    }

    /// `open_pg_gcs` with a bounded local read cache. See `open_pg_s3_cached`.
    #[staticmethod]
    fn open_pg_gcs_cached<'py>(
        py: Python<'py>,
        dsn: String,
        cfg: GcsConfig,
        cache: CacheConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let ws = CoreWorkspace::open_pg_gcs_cached(&dsn, cfg.inner, cache.inner)
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

    // ── portable dump / load (issue #117) ────────────────────────────────────

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
    ///
    /// **"Stale" means the base moved on, and only that** (#164) — not "everything
    /// obsolete". Two pending proposals from one actor on one unchanged base are
    /// siblings, both current by this measure, and this returns ``0`` for them.
    /// Retiring a draft its own author abandoned is a different relation: use
    /// ``supersede_suggestion(id, ctx)``, or ``replaces`` on ``suggest`` to do it
    /// as the replacement is proposed.
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

    // ── replication between workspaces ───────────────────────────────────────

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

    // ── trash: a recoverable delete for uncommitted work (issue #115) ────────
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

    // ── the engine surface with no `Workspace` wrapper ───────────────────────
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
            let ino = ws.stat(&path).await.map_err(to_pyerr)?.ino;
            let inode = ws.fs().vfs_chmod(ino, mode).await.map_err(to_pyerr)?;
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
            let ino = ws.stat(&path).await.map_err(to_pyerr)?.ino;
            let inode = ws.fs().vfs_chown(ino, uid, gid).await.map_err(to_pyerr)?;
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
            let ino = ws.stat(&existing_path).await.map_err(to_pyerr)?.ino;
            let (dir, name) = split_parent(&new_path)?;
            let parent = ws.stat(&dir).await.map_err(to_pyerr)?.ino;
            let inode = ws
                .fs()
                .vfs_link(ino, parent, &name)
                .await
                .map_err(to_pyerr)?;
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
            let ino = ws.stat(&path).await.map_err(to_pyerr)?.ino;
            let value = ws.fs().vfs_getxattr(ino, &name).await.map_err(to_pyerr)?;
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
            let ino = ws.stat(&path).await.map_err(to_pyerr)?.ino;
            ws.fs()
                .vfs_setxattr(ino, &name, &value)
                .await
                .map_err(to_pyerr)?;
            Ok(())
        })
    }

    /// Every extended-attribute name on a path, in name order (issue #119).
    fn listxattr<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.inner.clone();
        future_into_py(py, async move {
            let ino = ws.stat(&path).await.map_err(to_pyerr)?.ino;
            ws.fs().vfs_listxattr(ino).await.map_err(to_pyerr)
        })
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
            let ino = ws.stat(&path).await.map_err(to_pyerr)?.ino;
            ws.fs().vfs_removexattr(ino, &name).await.map_err(to_pyerr)
        })
    }

    // ── path-scoped write ACLs (issue #123) ──────────────────────────────────
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

    // ── attribution completeness (issue #128) ────────────────────────────────

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

    // ── performance introspection (issue #118) ───────────────────────────────

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
/// Parse a hex commit hash, rejecting a malformed one with `ValueError` rather
/// than letting it read as "no such object" further down.
fn parse_hash(s: &str) -> PyResult<origofs_sdk::Hash> {
    origofs_sdk::Hash::from_hex(s)
        .ok_or_else(|| PyValueError::new_err(format!("not a hash: {s:?}")))
}

fn transfer_stats_dict(py: Python<'_>, s: &origofs_sdk::TransferStats) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("objects", s.objects)?;
    d.set_item("bytes", s.bytes)?;
    d.set_item("skipped", s.skipped)?;
    Ok(d.unbind())
}

fn resync_report_dict(py: Python<'_>, r: &origofs_sdk::ResyncReport) -> PyResult<Py<PyDict>> {
    use origofs_sdk::ResyncOutcome::*;
    let d = PyDict::new(py);
    d.set_item("branch", &r.branch)?;
    d.set_item("outcome", r.outcome.as_str())?;
    // The head the outcome carries, flattened out so a caller reads one key rather
    // than matching on the tag first.
    d.set_item(
        "head",
        match &r.outcome {
            Pushed(h) | FastForwarded(h) | Merged(h) => Some(h.to_hex()),
            UpToDate | Conflicted => None,
        },
    )?;
    d.set_item("fetched", transfer_stats_dict(py, &r.fetched)?)?;
    d.set_item("pushed", transfer_stats_dict(py, &r.pushed)?)?;
    d.set_item("blame_fetched", r.blame_fetched)?;
    d.set_item("blame_pushed", r.blame_pushed)?;
    let conflicts: Vec<Py<PyDict>> = r
        .conflicts
        .iter()
        .map(|c| {
            let e = PyDict::new(py);
            e.set_item("path", &c.path)?;
            e.set_item("kind", c.kind.as_str())?;
            Ok(e.unbind())
        })
        .collect::<PyResult<_>>()?;
    d.set_item("conflicts", conflicts)?;
    d.set_item("stale_live_paths", r.stale_live_paths.clone())?;
    d.set_item("cas_retries", r.cas_retries)?;
    d.set_item("remote_tree_updated", r.remote_tree_updated)?;
    Ok(d.unbind())
}

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
    m.add_class::<Scope>()?;
    m.add_class::<S3Config>()?;
    m.add_class::<GcsConfig>()?;
    m.add_class::<CacheConfig>()?;
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
    m.add("StaleBaseError", m.py().get_type::<StaleBaseError>())?;
    m.add("ForeignWriteError", m.py().get_type::<ForeignWriteError>())?;
    m.add(
        "AlreadyResolvedError",
        m.py().get_type::<AlreadyResolvedError>(),
    )?;
    // `origofs.__version__`, single-sourced from `[workspace.package].version`:
    // `CARGO_PKG_VERSION` is the same value maturin stamps the wheel with, so the
    // string a caller reads and the version `pip` resolved cannot disagree.
    // Compiled in rather than read back through `importlib.metadata`, which finds
    // no distribution for an extension imported out of a build tree.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
