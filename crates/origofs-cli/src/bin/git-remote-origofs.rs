//! `git-remote-origofs` — a git remote helper that lets the real `git` clone, fetch,
//! and push an origofs workspace over `origofs://` URLs (`docs/DESIGN.md` §4c interop
//! item 2; roadmap M5, `git-remote-origofs`).
//!
//! git speaks the [remote-helper protocol] to a program named
//! `git-remote-<transport>` on `PATH`. We implement the **`connect`**
//! capability: for each operation git asks us to `connect git-upload-pack`
//! (fetch/clone) or `connect git-receive-pack` (push). We materialize the origofs
//! history into a throwaway real git repository with the M5 object codec
//! ([`origofs_sdk::git::export_git`]), hand our stdin/stdout to a genuine
//! `git upload-pack` / `git receive-pack` pointed at that repo — so real git
//! does all the pack-protocol work — and, on push, import whatever it wrote back
//! into origofs ([`origofs_sdk::git::import_git`]).
//!
//! A URL is `origofs://<workspace-path>`, where the workspace is an origofs directory
//! (holding `meta.db` + `cas/`), the same one the `origofs` CLI's `--workspace`
//! points at.
//!
//! Scope: flat branch names under `refs/heads/`; SHA-1 objects (what git clients
//! default to). The temp repo is synced fresh per operation.
//!
//! [remote-helper protocol]: https://git-scm.com/docs/gitremote-helpers

use anyhow::{Context, Result, bail};
use origofs_sdk::Workspace;
use origofs_sdk::git::{ExportOptions, ObjectFormat, export_git, import_git};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn main() {
    if let Err(e) = run() {
        eprintln!("git-remote-origofs: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    // git invokes us as `git-remote-origofs <remote> <url>`; the URL is last.
    let url = std::env::args().next_back().unwrap_or_default();
    let ws_path = url
        .strip_prefix("origofs://")
        .unwrap_or(&url)
        .trim_end_matches('/')
        .to_string();
    if ws_path.is_empty() {
        bail!("usage: git-remote-origofs <remote> origofs://<workspace>");
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // The helper command stream: one command per line. We read stdin *unbuffered*
    // so that when we hand fd 0 to the pack service we never swallow its bytes.
    while let Some(cmd) = read_command() {
        let cmd = cmd.trim_end();
        if cmd.is_empty() {
            continue;
        }
        if cmd == "capabilities" {
            let mut out = std::io::stdout();
            out.write_all(b"connect\n\n")?;
            out.flush()?;
        } else if let Some(service) = cmd.strip_prefix("connect ") {
            let code = handle_connect(&rt, &ws_path, service.trim())?;
            std::process::exit(code);
        } else {
            bail!("unsupported command: {cmd}");
        }
    }
    Ok(())
}

/// Read one newline-terminated command from fd 0, one byte at a time (no
/// buffering, so the pack service inherits an untouched fd afterwards).
fn read_command() -> Option<String> {
    let mut line = Vec::new();
    loop {
        let mut b = [0u8; 1];
        // SAFETY: `b` is a live, uniquely-borrowed 1-byte buffer and the count
        // passed is exactly its length, so `read(2)` cannot write out of bounds.
        // fd 0 is always open for this helper (git wires it up), and a closed or
        // invalid fd is reported as a negative return rather than being undefined.
        let n = unsafe { libc::read(0, b.as_mut_ptr() as *mut libc::c_void, 1) };
        if n <= 0 {
            return (!line.is_empty()).then(|| String::from_utf8_lossy(&line).into_owned());
        }
        if b[0] == b'\n' {
            return Some(String::from_utf8_lossy(&line).into_owned());
        }
        line.push(b[0]);
    }
}

fn handle_connect(rt: &tokio::runtime::Runtime, ws_path: &str, service: &str) -> Result<i32> {
    let ws = rt
        .block_on(Workspace::open_local(
            Path::new(ws_path).join("meta.db"),
            Path::new(ws_path).join("cas"),
        ))
        .with_context(|| format!("opening origofs workspace at {ws_path}"))?;

    let dir = std::env::temp_dir().join(format!(
        "origofs-remote-{}-{}",
        std::process::id(),
        service.replace('/', "_")
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let dir_s = dir.to_str().context("non-utf8 temp path")?;

    // Materialize the current origofs history into a real git repo (SHA-1, what
    // clients default to). An empty workspace becomes an empty repo so a first
    // push has somewhere to land.
    let branches = rt.block_on(ws.list_branches())?;
    if branches.is_empty() {
        run_quiet(&["git", "init", "-q", "-b", "main", dir_s])?;
    } else {
        for (name, _) in &branches {
            rt.block_on(export_git(
                &ws,
                &dir,
                &ExportOptions {
                    format: ObjectFormat::Sha1,
                    branch: Some(name.clone()),
                    lfs_threshold: None,
                },
            ))
            .with_context(|| format!("exporting branch {name}"))?;
        }
        // Prefer `main` as the advertised HEAD when present.
        if branches.iter().any(|(n, _)| n == "main") {
            std::fs::write(dir.join(".git/HEAD"), "ref: refs/heads/main\n")?;
        }
    }

    // Ack: connection established; the service's protocol now flows over our fds.
    {
        let mut out = std::io::stdout();
        out.write_all(b"\n")?;
        out.flush()?;
    }

    let code = match service {
        "git-upload-pack" => run_inherit(&["git", "upload-pack", dir_s])?,
        "git-receive-pack" => {
            let before = read_heads(&dir);
            // Unpack everything to loose objects (our importer reads loose), and
            // allow updating the checked-out branch (we read refs, not the tree).
            let code = run_inherit(&[
                "git",
                "-c",
                "receive.unpackLimit=2147483647",
                "-c",
                "receive.denyCurrentBranch=ignore",
                "receive-pack",
                dir_s,
            ])?;
            // Import every branch the push created or moved back into origofs.
            let after = read_heads(&dir);
            for (branch, oid) in &after {
                if before.get(branch) != Some(oid) {
                    rt.block_on(import_git(&ws, &dir, branch))
                        .with_context(|| format!("importing pushed branch {branch}"))?;
                }
            }
            code
        }
        other => bail!("unsupported service: {other}"),
    };

    let _ = std::fs::remove_dir_all(&dir);
    Ok(code)
}

/// Read `refs/heads/*` (loose + packed) of the repo at `dir` as `branch -> oid`.
fn read_heads(dir: &Path) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Ok(entries) = std::fs::read_dir(dir.join(".git/refs/heads")) {
        for e in entries.flatten() {
            if let Ok(oid) = std::fs::read_to_string(e.path()) {
                out.insert(
                    e.file_name().to_string_lossy().into_owned(),
                    oid.trim().to_string(),
                );
            }
        }
    }
    if let Ok(packed) = std::fs::read_to_string(dir.join(".git/packed-refs")) {
        for line in packed.lines() {
            if line.starts_with('#') || line.starts_with('^') {
                continue;
            }
            if let Some((oid, name)) = line.split_once(' ')
                && let Some(b) = name.trim().strip_prefix("refs/heads/")
            {
                out.entry(b.to_string())
                    .or_insert_with(|| oid.trim().to_string());
            }
        }
    }
    out
}

/// Run a git service with our stdin/stdout/stderr, so it speaks the pack
/// protocol directly to the git process that invoked us.
fn run_inherit(args: &[&str]) -> Result<i32> {
    let status = Command::new(args[0])
        .args(&args[1..])
        .status()
        .with_context(|| format!("running {}", args.join(" ")))?;
    Ok(status.code().unwrap_or(1))
}

/// Run a helper git command with stdout muted — its output must never reach the
/// protocol channel.
fn run_quiet(args: &[&str]) -> Result<()> {
    let status = Command::new(args[0])
        .args(&args[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("running {}", args.join(" ")))?;
    if !status.success() {
        bail!("{} failed", args.join(" "));
    }
    Ok(())
}
