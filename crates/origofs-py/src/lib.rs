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
/// One revision of a single path: the commit that made it, plus how.
fn path_revision_dict(py: Python<'_>, r: &origofs_sdk::PathRevision) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("commit", r.commit.hash.to_hex())?;
    d.set_item("author", &r.commit.commit.author)?;
    d.set_item("message", &r.commit.commit.message)?;
    d.set_item("timestamp", r.commit.commit.timestamp)?;
    d.set_item("status", r.status.sigil().to_string())?;
    d.set_item("hash", r.hash.map(|h| h.to_hex()))?;
    Ok(d.into_any().unbind())
}

/// One `edit_op` row. Shared by the actor-keyed and path-keyed bindings so the
/// two cannot disagree about the shape of a record the stub declares once.
fn edit_op_dict(py: Python<'_>, o: origofs_sdk::EditOp) -> PyResult<Py<PyAny>> {
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
    Ok(d.into_any().unbind())
}

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

mod workspace;

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
