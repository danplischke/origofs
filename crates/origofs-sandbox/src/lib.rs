//! origofs-sandbox — run an unmodified process against an **isolated copy-on-write
//! view** of an origofs workspace, then import what it changed back as an attributed
//! commit (`docs/DESIGN.md` §4e; the agentfs `run` use case for overlay).
//!
//! Flow:
//! 1. **Materialize** the workspace's working tree to a real `lower/` directory.
//! 2. Mount an **unprivileged overlayfs** (`lower` + a scratch `upper`/`work`) in a
//!    user+mount namespace and `exec` the command with cwd in the merged view.
//! 3. On exit, the overlay `upper/` holds exactly the delta (created/modified
//!    files, plus whiteouts for deletions).
//! 4. **Import** that delta back into origofs via attributed writes (blame + edit-op),
//!    or `--discard` it.
//!
//! The kernel overlay is the disposable *scratch*; origofs's own object graph is the
//! durable, versioned, attributed layer the delta lands in.
//!
//! # Two isolation levels
//!
//! **Default (`isolate: false`) — NOT a security boundary.** A plain
//! `unshare -U -r -m` overlay: fast, and fine for code you already trust, but the
//! command runs with your privileges with no `pivot_root`/`chroot` (the whole
//! host filesystem stays reachable by absolute path, including this workspace's
//! `meta.db`/`cas`), no network namespace, and no seccomp. We only strip origofs's
//! own `ORIGOFS_ENCRYPTION_KEY` from the child; everything else your process can
//! reach, the command can too.
//!
//! **Isolated (`isolate: true`, `origofs sandbox --isolate`) — a real filesystem
//! boundary.** Runs the command under [bubblewrap](https://github.com/containers/bubblewrap)
//! ([`bwrap_available`]): a fresh namespace whose root is a tmpfs with only the
//! host toolchain bind-mounted **read-only** and the copy-on-write overlay as the
//! working dir. The rest of the host filesystem — `meta.db`/`cas`, the home dir,
//! cloud/DB credentials — is simply absent, so untrusted code can't read or
//! tamper with any of it. The delta is still captured in `upper/` and imported
//! exactly as before. This is a *filesystem* boundary; the network namespace is
//! left shared on purpose because agents typically need egress, so it does not by
//! itself contain network-reachable resources.

use anyhow::{bail, Context, Result};
use origofs_sdk::{FileKind, Workspace, WriteCtx};
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Options for a sandbox run.
pub struct RunOpts {
    /// Attribute imported changes to this actor (records blame + edit-ops).
    pub actor: Option<i64>,
    /// Throw the delta away instead of importing it.
    pub discard: bool,
    /// Working root for `lower/upper/work/merged` (a temp dir).
    pub work_root: PathBuf,
    /// Run under bubblewrap so the host filesystem (this workspace's `meta.db`/
    /// `cas`, the home dir, credentials) is hidden from the command — a real
    /// filesystem boundary for untrusted code. Requires `bwrap` (see
    /// [`bwrap_available`]). When `false`, the plain copy-on-write overlay is used
    /// (fast, but NOT a security boundary — see the module docs).
    pub isolate: bool,
}

/// The result of a sandbox run.
#[derive(Debug)]
pub struct Outcome {
    pub exit_code: i32,
    pub imported: bool,
    pub files_changed: usize,
}

/// Whether unprivileged overlayfs-in-a-user-namespace works here (probes once).
pub fn overlay_supported() -> bool {
    // A unique probe dir per call: concurrent probes (e.g. parallel tests in one
    // process) share a PID, so a PID-only name would let one probe's cleanup rip
    // out another's mountpoint mid-mount and report a spurious failure.
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir().join(format!(
        "origofs-ovl-probe-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let (low, up, work, merged) = (
        base.join("l"),
        base.join("u"),
        base.join("w"),
        base.join("m"),
    );
    for d in [&low, &up, &work, &merged] {
        let _ = std::fs::create_dir_all(d);
    }
    let script =
        "mount -t overlay overlay -o lowerdir=\"$1\",upperdir=\"$2\",workdir=\"$3\" \"$4\"";
    let ok = std::process::Command::new("unshare")
        .args(["-U", "-r", "-m", "/bin/sh", "-c", script, "probe"])
        .arg(&low)
        .arg(&up)
        .arg(&work)
        .arg(&merged)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    let _ = std::fs::remove_dir_all(&base);
    ok
}

/// Run `cmd` in a sandbox over `ws`'s working tree.
pub async fn run(ws: &Workspace, opts: RunOpts, cmd: &[String]) -> Result<Outcome> {
    if cmd.is_empty() {
        bail!("no command given to sandbox");
    }
    let root = &opts.work_root;
    let lower = root.join("lower");
    let upper = root.join("upper");
    let work = root.join("work");
    let merged = root.join("merged");
    for d in [&lower, &upper, &work, &merged] {
        tokio::fs::create_dir_all(d).await?;
    }

    // 1. materialize the working tree into `lower/`
    export_tree(ws, "/", &lower)
        .await
        .context("materializing workspace into the sandbox lower layer")?;

    // 2. run the command over the overlay — bubblewrap-isolated (a real
    //    filesystem boundary) or a plain copy-on-write overlay, per `opts.isolate`.
    let status = sandbox_command(opts.isolate, &lower, &upper, &work, &merged, cmd)?
        .status()
        .await
        .context("spawning the sandbox")?;
    let exit_code = status.code().unwrap_or(-1);

    // 3. import the captured delta (unless discarding)
    let (imported, files_changed) = if opts.discard {
        (false, 0)
    } else {
        let session = match opts.actor {
            Some(a) => Some(ws.create_session(a, Some("sandbox")).await?),
            None => None,
        };
        let n = import_delta(ws, &upper, &upper, opts.actor, session).await?;
        (true, n)
    };

    Ok(Outcome {
        exit_code,
        imported,
        files_changed,
    })
}

/// Options for a live overlay run.
pub struct LiveOpts {
    /// Attribute the agent's changes to this actor (records blame + edit-ops).
    pub actor: Option<i64>,
    /// Working root for `lower/upper/work/merged` (a temp dir).
    pub work_root: PathBuf,
    /// How often to sync the agent's changes into origofs while it runs.
    pub sync_interval: Duration,
    /// Run the agent under bubblewrap so the host filesystem is hidden — a real
    /// filesystem boundary. Requires `bwrap` ([`bwrap_available`]). See [`RunOpts::isolate`].
    pub isolate: bool,
}

/// Run `cmd` in a native overlay over `ws`'s working tree, streaming the agent's
/// changes into origofs (attributed) **live** — every `sync_interval` while it runs,
/// and once more at exit — so the change feed reflects the agent's edits as they
/// happen instead of only when it finishes. This is the persistent-mount
/// counterpart to [`run`], which imports only on exit.
///
/// The agent works in the merged overlay (native kernel speed, unprivileged);
/// its writes land in `upper/`, which a [`LiveSync`] on the host imports into origofs
/// on the timer. `files_changed` is the number of imports performed over the run.
pub async fn run_live(ws: &Workspace, opts: LiveOpts, cmd: &[String]) -> Result<Outcome> {
    if cmd.is_empty() {
        bail!("no command given to the overlay");
    }
    let root = &opts.work_root;
    let lower = root.join("lower");
    let upper = root.join("upper");
    let work = root.join("work");
    let merged = root.join("merged");
    for d in [&lower, &upper, &work, &merged] {
        tokio::fs::create_dir_all(d).await?;
    }
    export_tree(ws, "/", &lower)
        .await
        .context("materializing workspace into the overlay lower layer")?;

    let session = match opts.actor {
        Some(a) => Some(ws.create_session(a, Some("overlay")).await?),
        None => None,
    };
    let mut sync = LiveSync::new(opts.actor, session);

    let mut child = sandbox_command(opts.isolate, &lower, &upper, &work, &merged, cmd)?
        .spawn()
        .context("spawning the overlay agent")?;

    // Sync the agent's changes into origofs on the timer until it exits. A missed
    // tick (a sync that ran long) just delays the next one rather than bursting.
    let mut ticker = tokio::time::interval(opts.sync_interval.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the interval's first tick fires immediately; skip it.
    let mut changed = 0usize;
    let exit_code = loop {
        tokio::select! {
            status = child.wait() => break status?.code().unwrap_or(-1),
            _ = ticker.tick() => {
                // Best-effort mid-run sync; a transient error is retried next tick.
                changed += sync.sync(ws, &upper).await.unwrap_or(0);
            }
        }
    };
    // Final sync: catch anything written between the last tick and exit.
    changed += sync.sync(ws, &upper).await?;

    Ok(Outcome {
        exit_code,
        imported: true,
        files_changed: changed,
    })
}

/// Build the unprivileged-overlay command: mount `lower`+`upper`/`work` at
/// `merged` inside a fresh user+mount namespace, then `exec` `cmd` with cwd there.
fn overlay_command(
    lower: &Path,
    upper: &Path,
    work: &Path,
    merged: &Path,
    cmd: &[String],
) -> tokio::process::Command {
    // $1=lower $2=upper $3=work $4=merged, then the user command.
    const SCRIPT: &str = "mount -t overlay overlay -o lowerdir=\"$1\",upperdir=\"$2\",workdir=\"$3\" \"$4\" || exit 91\n\
                          cd \"$4\" || exit 92\n\
                          shift 4\n\
                          exec \"$@\"";
    let mut command = tokio::process::Command::new("unshare");
    command
        // Don't hand origofs's own at-rest encryption key to the child: the process
        // inherits our environment, and the overlay is not a trust boundary, so
        // leaking the key that protects the content store would be gratuitous.
        // (Broader environment hygiene is the caller's job — see the module docs.)
        .env_remove("ORIGOFS_ENCRYPTION_KEY")
        .args(["-U", "-r", "-m", "/bin/sh", "-c", SCRIPT, "origofs-sandbox"])
        .arg(lower)
        .arg(upper)
        .arg(work)
        .arg(merged);
    for arg in cmd {
        command.arg(arg);
    }
    command
}

/// Pick the command that runs `cmd` over the overlay: bubblewrap-isolated (a real
/// filesystem boundary) when `isolate`, else the plain copy-on-write overlay.
/// Errors if isolation is requested but `bwrap` isn't available.
fn sandbox_command(
    isolate: bool,
    lower: &Path,
    upper: &Path,
    work: &Path,
    merged: &Path,
    cmd: &[String],
) -> Result<tokio::process::Command> {
    if isolate {
        if !bwrap_available() {
            bail!(
                "isolated run requested but bubblewrap (`bwrap`) is not available on PATH; \
                 install bubblewrap >= 0.8.0 (for overlay support), or run without isolation"
            );
        }
        Ok(bwrap_command(lower, upper, work, cmd))
    } else {
        Ok(overlay_command(lower, upper, work, merged, cmd))
    }
}

/// Whether bubblewrap (`bwrap`) is on PATH for an isolated run. Overlay support
/// additionally needs bwrap >= 0.8.0; an older bwrap fails the run loudly rather
/// than silently dropping the boundary.
pub fn bwrap_available() -> bool {
    std::process::Command::new("bwrap")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Destination mountpoint for the overlay inside the bubblewrap sandbox.
const BWRAP_WORKDIR: &str = "/origofs-work";

/// Build a bubblewrap command that runs `cmd` over an overlay of `lower` (read)
/// plus `upper`/`work` (the writable delta we import afterward), in a real
/// filesystem sandbox: a fresh namespace whose root is a tmpfs with only the host
/// toolchain bind-mounted **read-only** and the overlay as the working dir. The
/// rest of the host filesystem — this workspace's `meta.db`/`cas`, the home dir,
/// cloud/DB credentials — is simply not present, so the command can't read or
/// tamper with any of it.
///
/// **Scope.** This is a *filesystem* boundary. The network namespace is left
/// shared on purpose (agents typically need egress, e.g. to call an API), so
/// network-reachable resources are not isolated by this; drop `--unshare-net`-
/// style isolation to a caller that knows the agent needs no network.
fn bwrap_command(
    lower: &Path,
    upper: &Path,
    work: &Path,
    cmd: &[String],
) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("bwrap");
    command
        .env_remove("ORIGOFS_ENCRYPTION_KEY")
        .args([
            "--unshare-user",
            "--unshare-pid",
            "--unshare-ipc",
            "--unshare-uts",
            "--unshare-cgroup",
            "--die-with-parent",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
        ])
        // Host toolchain, read-only. `-try` tolerates paths that are symlinks or
        // absent on a given distro (e.g. /bin, /lib64, /sbin merged into /usr).
        .args(["--ro-bind", "/usr", "/usr"])
        .args(["--ro-bind-try", "/bin", "/bin"])
        .args(["--ro-bind-try", "/sbin", "/sbin"])
        .args(["--ro-bind-try", "/lib", "/lib"])
        .args(["--ro-bind-try", "/lib64", "/lib64"])
        // /etc read-only for DNS/TLS/user lookups the agent's tools need.
        .args(["--ro-bind-try", "/etc", "/etc"])
        // The copy-on-write overlay: `lower` is the read layer, `upper` captures
        // the delta (imported after exit), mounted at BWRAP_WORKDIR.
        .arg("--overlay-src")
        .arg(lower)
        .arg("--overlay")
        .arg(upper)
        .arg(work)
        .arg(BWRAP_WORKDIR)
        .args(["--chdir", BWRAP_WORKDIR])
        .arg("--");
    for arg in cmd {
        command.arg(arg);
    }
    command
}

fn join_origofs(dir: &str, name: &str) -> String {
    if dir == "/" {
        format!("/{name}")
    } else {
        format!("{dir}/{name}")
    }
}

/// Recursively write the origofs tree rooted at `origofs_dir` into the host `host_dir`.
async fn export_tree(ws: &Workspace, origofs_dir: &str, host_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(host_dir).await?;
    for e in ws.ls(origofs_dir).await? {
        // Defense-in-depth: origofs now rejects these names at the metadata boundary,
        // but refuse to materialize a traversal/separator component here too, so a
        // name planted by a direct object-store writer can't make `host_dir.join`
        // escape the export root and write outside `lower/`.
        if e.name.is_empty() || e.name == "." || e.name == ".." || e.name.contains('/') {
            bail!("refusing to export unsafe path component {:?}", e.name);
        }
        let child_origofs = join_origofs(origofs_dir, &e.name);
        let child_host = host_dir.join(&e.name);
        match e.kind {
            FileKind::Dir => {
                Box::pin(export_tree(ws, &child_origofs, &child_host)).await?;
            }
            FileKind::File => {
                let bytes = ws.read(&child_origofs).await?;
                tokio::fs::write(&child_host, &bytes).await?;
            }
            FileKind::Symlink => {
                let target = ws.readlink(&child_origofs).await?;
                std::os::unix::fs::symlink(&target, &child_host)?;
            }
        }
    }
    Ok(())
}

/// Import the overlay `upper` delta under `dir` back into `ws`.
async fn import_delta(
    ws: &Workspace,
    root: &Path,
    dir: &Path,
    actor: Option<i64>,
    session: Option<i64>,
) -> Result<usize> {
    let mut count = 0;
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let host = entry.path();
        let rel = host.strip_prefix(root).unwrap_or(&host);
        let origofs_path = format!("/{}", rel.to_string_lossy());
        let md = tokio::fs::symlink_metadata(&host).await?;
        let ft = md.file_type();

        if ft.is_char_device() && md.rdev() == 0 {
            // overlayfs whiteout => the path was deleted in the sandbox
            let _ = origofs_rm_rf(ws, &origofs_path).await;
            count += 1;
        } else if ft.is_dir() {
            ws.mkdir_p(&origofs_path).await?;
            count += Box::pin(import_delta(ws, root, &host, actor, session)).await?;
        } else if ft.is_symlink() {
            let target = tokio::fs::read_link(&host).await?;
            let _ = ws.remove(&origofs_path).await;
            ws.symlink(&target.to_string_lossy(), &origofs_path).await?;
            count += 1;
        } else if ft.is_file() {
            let bytes = tokio::fs::read(&host).await?;
            match (actor, session) {
                (Some(a), Some(s)) => {
                    ws.write_as(WriteCtx::session(a, s), &origofs_path, &bytes)
                        .await?
                }
                _ => ws.write(&origofs_path, &bytes).await?,
            }
            count += 1;
        }
    }
    Ok(count)
}

/// A stateful, incremental sync of an overlay `upper/` delta into origofs.
///
/// Unlike the one-shot [`import_delta`], a `LiveSync` remembers what it has
/// already pushed, so repeated calls import only the files the agent has changed
/// since the last tick — the basis of a *live* overlay mount that streams the
/// agent's edits into origofs (attributed, on the change feed) as it works, instead
/// of only when the run ends. Drive [`sync`](Self::sync) on a timer (and once
/// more at teardown) against the same `upper/` directory.
///
/// Change detection is `(mtime, size)` per path (rsync-style): cheap and correct
/// for normal edits. A same-size overwrite within one mtime tick could be missed;
/// the teardown sync and, if needed, a content-hash mode are the backstops.
pub struct LiveSync {
    /// `origofs_path -> (mtime_ns, size)` last imported.
    seen: HashMap<String, (i64, u64)>,
    /// Paths a whiteout deletion has already been applied for (apply once).
    deleted: HashSet<String>,
    actor: Option<i64>,
    session: Option<i64>,
}

impl LiveSync {
    /// A fresh sync that attributes imported writes to `(actor, session)` when
    /// both are present (records blame + edit-ops), else writes unattributed.
    pub fn new(actor: Option<i64>, session: Option<i64>) -> Self {
        Self {
            seen: HashMap::new(),
            deleted: HashSet::new(),
            actor,
            session,
        }
    }

    /// Import everything changed under `upper` since the last call. Returns the
    /// number of origofs paths mutated this round (0 when the agent is idle).
    pub async fn sync(&mut self, ws: &Workspace, upper: &Path) -> Result<usize> {
        Box::pin(self.sync_dir(ws, upper, upper)).await
    }

    async fn sync_dir(&mut self, ws: &Workspace, root: &Path, dir: &Path) -> Result<usize> {
        let mut count = 0;
        let mut rd = match tokio::fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            let host = entry.path();
            let rel = host.strip_prefix(root).unwrap_or(&host);
            let origofs_path = format!("/{}", rel.to_string_lossy());
            let md = tokio::fs::symlink_metadata(&host).await?;
            let ft = md.file_type();

            if ft.is_char_device() && md.rdev() == 0 {
                // overlayfs whiteout => the path was deleted in the overlay.
                if self.deleted.insert(origofs_path.clone()) {
                    let _ = origofs_rm_rf(ws, &origofs_path).await;
                    self.seen.remove(&origofs_path);
                    count += 1;
                }
            } else if ft.is_dir() {
                ws.mkdir_p(&origofs_path).await?;
                count += Box::pin(self.sync_dir(ws, root, &host)).await?;
            } else if ft.is_symlink() {
                let key = (mtime_ns(&md), md.len());
                if self.seen.get(&origofs_path) != Some(&key) {
                    let target = tokio::fs::read_link(&host).await?;
                    let _ = ws.remove(&origofs_path).await;
                    ws.symlink(&target.to_string_lossy(), &origofs_path).await?;
                    self.seen.insert(origofs_path.clone(), key);
                    self.deleted.remove(&origofs_path);
                    count += 1;
                }
            } else if ft.is_file() {
                let key = (mtime_ns(&md), md.len());
                if self.seen.get(&origofs_path) != Some(&key) {
                    let bytes = tokio::fs::read(&host).await?;
                    match (self.actor, self.session) {
                        (Some(a), Some(s)) => {
                            ws.write_as(WriteCtx::session(a, s), &origofs_path, &bytes)
                                .await?
                        }
                        _ => ws.write(&origofs_path, &bytes).await?,
                    }
                    self.seen.insert(origofs_path.clone(), key);
                    self.deleted.remove(&origofs_path);
                    count += 1;
                }
            }
        }
        Ok(count)
    }
}

/// The file's modification time in whole nanoseconds since the epoch.
fn mtime_ns(md: &std::fs::Metadata) -> i64 {
    md.mtime() * 1_000_000_000 + md.mtime_nsec()
}

/// Recursively remove an origofs path (file or directory).
async fn origofs_rm_rf(ws: &Workspace, path: &str) -> Result<()> {
    match ws.stat(path).await {
        Ok(inode) if inode.kind == FileKind::Dir => {
            for e in ws.ls(path).await? {
                Box::pin(origofs_rm_rf(ws, &join_origofs(path, &e.name))).await?;
            }
            ws.remove(path).await?;
        }
        Ok(_) => {
            ws.remove(path).await?;
        }
        Err(_) => {} // already gone
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    // The isolated runner's argv must set up a real filesystem boundary: a COW
    // overlay with delta capture, a read-only host toolchain, fresh namespaces,
    // and the user command after `--`. (bwrap can't be executed in CI, so we pin
    // the command construction instead.)
    #[test]
    fn bwrap_command_builds_an_isolated_overlay_argv() {
        let cmd = vec!["echo".to_string(), "hi".to_string()];
        let c = bwrap_command(
            Path::new("/w/lower"),
            Path::new("/w/upper"),
            Path::new("/w/work"),
            &cmd,
        );
        let std = c.as_std();
        assert_eq!(std.get_program(), OsStr::new("bwrap"));
        let args: Vec<String> = std
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();

        // COW overlay with the writable delta layer we import afterward.
        assert!(
            args.windows(2)
                .any(|w| w[0] == "--overlay-src" && w[1] == "/w/lower"),
            "read layer: {args:?}"
        );
        assert!(
            args.windows(4).any(|w| w[0] == "--overlay"
                && w[1] == "/w/upper"
                && w[2] == "/w/work"
                && w[3] == BWRAP_WORKDIR),
            "writable overlay: {args:?}"
        );
        // Host toolchain is bind-mounted read-only (never read-write).
        assert!(args
            .windows(3)
            .any(|w| w[0] == "--ro-bind" && w[1] == "/usr" && w[2] == "/usr"));
        assert!(
            !args.iter().any(|a| a == "--bind"),
            "nothing host is writable: {args:?}"
        );
        // Real namespaces + working dir inside the overlay.
        assert!(args.contains(&"--unshare-pid".to_string()));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--chdir" && w[1] == BWRAP_WORKDIR));
        // The user command comes last, after the `--` separator, unmodified.
        let sep = args.iter().position(|a| a == "--").expect("`--` separator");
        assert_eq!(&args[sep + 1..], &["echo".to_string(), "hi".to_string()]);
    }
}
