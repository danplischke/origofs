//! Sandbox surface (`sandbox` feature) — run an unmodified process against an
//! **isolated copy-on-write view** of an origofs workspace, then import what it
//! changed back as an attributed commit (`docs/DESIGN.md` §4e — the overlay-backed
//! "run an agent over a copy of the tree" use case).
//!
//! Flow:
//! 1. **Materialize** the workspace's working tree to a real `lower/` directory.
//! 2. Mount an **unprivileged overlayfs** (`lower` + a scratch `upper`/`work`) in a
//!    user+mount namespace and `exec` the command with cwd in the merged view.
//! 3. On exit, the overlay `upper/` holds exactly the delta (created/modified
//!    files, plus whiteouts for deletions — either a character-device whiteout
//!    per removed name, or an *opaque* directory marker; see [`is_opaque_dir`]).
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
//! `meta.db`/`cas`), no network namespace, and no seccomp. The child inherits your
//! environment except origofs's own `ORIGOFS_ENCRYPTION_KEY`; everything else your
//! process can reach, the command can too.
//!
//! **Isolated (`isolate: true`, `origofs sandbox --isolate`) — a real filesystem
//! boundary.** Runs the command under [bubblewrap](https://github.com/containers/bubblewrap)
//! ([`bwrap_available`], with the reason in [`bwrap_gap`]; needs a non-setuid
//! bwrap >= 0.11.0, where the `--overlay` options were added — note that Ubuntu
//! 24.04 and Debian 12 both ship older ones): a fresh namespace whose root is a tmpfs with only the
//! host toolchain bind-mounted **read-only** and the copy-on-write overlay as the
//! working dir. The rest of the host filesystem — `meta.db`/`cas`, the home dir,
//! credential files — is simply absent, so untrusted code can't read or tamper
//! with any of it. The **environment is cleared** too, down to `PATH`/`HOME`/
//! `TMPDIR`: otherwise `AWS_SECRET_ACCESS_KEY`, `DATABASE_URL`, and every API
//! token in the parent's `environ` would be inherited verbatim, which — with
//! egress deliberately left open — hands untrusted code both the secrets and the
//! means to exfiltrate them. `--new-session` detaches the controlling terminal, so
//! the child cannot `TIOCSTI`-inject keystrokes into the launching shell. The delta
//! is still captured in `upper/` and imported exactly as before.
//!
//! This is a *filesystem* boundary; the network namespace is left shared on purpose
//! because agents typically need egress, so it does not by itself contain
//! network-reachable resources. A caller passing secrets to a sandboxed command
//! must do so explicitly.

use crate::{FileKind, Workspace, WriteCtx};
use anyhow::{Context, Result, bail};
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
        let n = import_upper(ws, &upper, opts.actor, session).await?;
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
        // Report *which* of the three ways bubblewrap can be unusable applies, so
        // the operator gets something actionable instead of a blanket "not
        // available on PATH" that is wrong in two of the three cases.
        if let Some(gap) = bwrap_gap() {
            bail!("isolated run requested but {gap}");
        }
        Ok(bwrap_command(lower, upper, work, cmd))
    } else {
        Ok(overlay_command(lower, upper, work, merged, cmd))
    }
}

/// Why bubblewrap can't give us an isolated run here. Returned by [`bwrap_gap`]
/// so the failure can be reported as something the operator can act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BwrapGap {
    /// `bwrap` isn't on PATH, wouldn't execute, or printed an unparseable version.
    Missing,
    /// Present but older than [`MIN_BWRAP_VERSION`], so it has no `--overlay`.
    TooOld { found: (u32, u32, u32) },
    /// New enough by version, but this build has no `--overlay` family. Upstream
    /// disables it for setuid installs, and distributions patch it out.
    NoOverlay { found: (u32, u32, u32) },
}

impl std::fmt::Display for BwrapGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (maj, min, patch) = MIN_BWRAP_VERSION;
        match self {
            Self::Missing => write!(
                f,
                "bubblewrap (`bwrap`) is not available on PATH; install bubblewrap >= \
                 {maj}.{min}.{patch} (for overlay support), or run without isolation"
            ),
            Self::TooOld { found: (a, b, c) } => write!(
                f,
                "bubblewrap {a}.{b}.{c} is older than {maj}.{min}.{patch}, which is where the \
                 `--overlay` options this needs were added; without them there is no \
                 copy-on-write layer to capture a delta from. Upgrade bubblewrap, or run \
                 without isolation"
            ),
            Self::NoOverlay { found: (a, b, c) } => write!(
                f,
                "bubblewrap {a}.{b}.{c} on this system was built without the `--overlay` \
                 options (they are absent from its `--help`), so there is no copy-on-write \
                 layer to capture a delta from. Upstream disables overlays for setuid \
                 installs; install a non-setuid bubblewrap >= {maj}.{min}.{patch}, or run \
                 without isolation"
            ),
        }
    }
}

/// Whether bubblewrap (`bwrap`) can actually provide an isolated run here.
/// See [`bwrap_gap`] for *why* it can't, when it can't.
pub fn bwrap_available() -> bool {
    bwrap_gap().is_none()
}

/// What stops an isolated run on this system, or `None` if nothing does.
///
/// **Capability is probed, not inferred from the version.** The version alone was
/// never sufficient and the floor it checked was wrong besides:
///
/// * The `--overlay` family landed in bubblewrap **0.11.0**, not 0.8.0 as this
///   previously claimed. Ubuntu 24.04 LTS ships 0.9.0 and Debian 12 ships 0.8.0,
///   so on both — i.e. on most machines and on `ubuntu-latest` — the old check
///   passed and the run then died on `bwrap: Unknown option --overlay-src`. It
///   failed closed, but told the operator nothing, which is exactly what checking
///   the version was supposed to prevent.
/// * Even at 0.11.0+ the options are absent from a **setuid** install, so no
///   version floor can imply they are present.
///
/// So the version is checked to give a precise message for a genuinely old
/// bubblewrap, and then the option itself is looked for in `--help`.
pub fn bwrap_gap() -> Option<BwrapGap> {
    let Ok(out) = std::process::Command::new("bwrap")
        .arg("--version")
        .output()
    else {
        return Some(BwrapGap::Missing);
    };
    if !out.status.success() {
        return Some(BwrapGap::Missing);
    }
    // Unparseable output from something calling itself bwrap: treat as unusable
    // rather than assume it is new enough. (`?` would be wrong here — `None` from
    // this function means "nothing stops an isolated run".)
    let Some(found) = parse_bwrap_version(&String::from_utf8_lossy(&out.stdout)) else {
        return Some(BwrapGap::Missing);
    };
    if found < MIN_BWRAP_VERSION {
        return Some(BwrapGap::TooOld { found });
    }
    let help = std::process::Command::new("bwrap").arg("--help").output();
    let has_overlay = help.is_ok_and(|h| {
        let text =
            String::from_utf8_lossy(&h.stdout).into_owned() + &String::from_utf8_lossy(&h.stderr);
        text.contains(BWRAP_OVERLAY_SRC)
    });
    (!has_overlay).then_some(BwrapGap::NoOverlay { found })
}

/// The oldest bubblewrap that has the `--overlay` options this uses. They were
/// added in 0.11.0; without them there is no copy-on-write layer to capture a
/// delta from.
const MIN_BWRAP_VERSION: (u32, u32, u32) = (0, 11, 0);

/// The option whose presence in `--help` is what [`bwrap_gap`] treats as proof
/// that this build actually has overlay support.
const BWRAP_OVERLAY_SRC: &str = "--overlay-src";

/// Parse `bwrap --version` output ("bubblewrap 0.11.0") into a comparable triple.
/// A missing patch component reads as 0, so "bubblewrap 0.8" is 0.8.0.
fn parse_bwrap_version(out: &str) -> Option<(u32, u32, u32)> {
    let token = out.split_whitespace().find(|t| {
        t.split('.')
            .next()
            .is_some_and(|h| !h.is_empty() && h.chars().all(|c| c.is_ascii_digit()))
    })?;
    let mut parts = token
        .split('.')
        .map(|p| p.trim_end_matches(|c: char| !c.is_ascii_digit()));
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let patch = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    Some((major, minor, patch))
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
        // **Clear the environment, don't just remove one key.** The module doc
        // above promises that "cloud/DB credentials" are absent, and that is true
        // of the *filesystem* — `~/.aws/credentials` really is gone. It was not
        // true of the environment: `AWS_SECRET_ACCESS_KEY`, `DATABASE_URL`, and
        // every API token in the parent's `environ` were inherited verbatim, and
        // the network namespace is deliberately shared, so the child had both the
        // secrets and the egress to use them. Removing `ORIGOFS_ENCRYPTION_KEY`
        // alone protected origofs's own key and nothing else.
        //
        // What is put back is the minimum a command needs to run at all. Add to
        // this list deliberately; anything else the sandboxed process needs should
        // be passed explicitly by the caller.
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("HOME", BWRAP_WORKDIR)
        .env("TMPDIR", "/tmp")
        .args([
            "--unshare-user",
            "--unshare-pid",
            "--unshare-ipc",
            "--unshare-uts",
            "--unshare-cgroup",
            "--die-with-parent",
            // A new session detaches the child from the controlling terminal.
            // Without it the child shares our tty and can `TIOCSTI`-inject
            // keystrokes into the shell that launched it — bubblewrap's own
            // documentation calls this out as a sandbox escape, and it would let
            // untrusted code run commands as the user outside the sandbox.
            "--new-session",
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

/// The origofs path an `upper/`-relative host path imports to. The root of the
/// upper layer itself maps to `/`.
fn origofs_path_for(root: &Path, host: &Path) -> String {
    let rel = host.strip_prefix(root).unwrap_or(host);
    if rel.as_os_str().is_empty() {
        "/".to_string()
    } else {
        format!("/{}", rel.to_string_lossy())
    }
}

/// The xattr names overlayfs uses to mark a directory **opaque** — "this upper
/// directory *replaces* the lower one; ignore every lower entry under it". The
/// kernel writes `trusted.overlay.*`; rootless/unprivileged setups (which can't
/// set `trusted.`) use the `user.overlay.*` alias, so both are honored.
const OPAQUE_XATTRS: [&str; 2] = ["trusted.overlay.opaque", "user.overlay.opaque"];
/// The redirect marker: this upper directory stands in for a *differently named*
/// lower directory (a rename). We don't follow it — we only warn, so the import
/// isn't silently wrong. See [`import_delta`].
const REDIRECT_XATTRS: [&str; 2] = ["trusted.overlay.redirect", "user.overlay.redirect"];

/// Read an extended attribute without following symlinks; `None` when it is
/// absent, or when the platform/filesystem has no xattrs at all.
///
/// Declared here rather than pulled from `libc` because `libc` is an optional,
/// `fuse`-gated dependency of this crate and the sandbox surface must not need it.
#[cfg(target_os = "linux")]
fn lgetxattr_value(path: &Path, name: &str) -> Option<Vec<u8>> {
    use std::ffi::{CString, c_char, c_void};
    use std::os::unix::ffi::OsStrExt;

    unsafe extern "C" {
        fn lgetxattr(
            path: *const c_char,
            name: *const c_char,
            value: *mut c_void,
            size: usize,
        ) -> isize;
    }

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let c_name = CString::new(name).ok()?;
    // A zero-size probe returns the value's length (or -1), so an arbitrarily
    // long value (a `redirect` path) never trips ERANGE.
    // SAFETY: both pointers are NUL-terminated and live for the call; a null
    // value buffer with size 0 is the documented "how big is it?" form.
    let len = unsafe { lgetxattr(c_path.as_ptr(), c_name.as_ptr(), std::ptr::null_mut(), 0) };
    if len < 0 {
        return None;
    }
    let mut buf = vec![0u8; len as usize];
    if buf.is_empty() {
        return Some(buf);
    }
    // SAFETY: `buf` is a live allocation of exactly `buf.len()` writable bytes.
    let n = unsafe {
        lgetxattr(
            c_path.as_ptr(),
            c_name.as_ptr(),
            buf.as_mut_ptr().cast::<c_void>(),
            buf.len(),
        )
    };
    if n < 0 {
        return None;
    }
    buf.truncate(n as usize);
    Some(buf)
}

/// Non-Linux hosts have no overlayfs markers to read: always "absent".
#[cfg(not(target_os = "linux"))]
fn lgetxattr_value(_path: &Path, _name: &str) -> Option<Vec<u8>> {
    None
}

/// Whether `path` is an overlayfs **opaque directory** — an upper-layer directory
/// carrying `trusted.overlay.opaque` (or the unprivileged `user.overlay.opaque`)
/// with the value `"y"`.
///
/// Opaque means the upper directory *replaces* the lower one instead of merging
/// with it: every lower-layer entry under that path is invisible in the merged
/// view. It is the second way overlayfs records deletions (the first being a
/// character-device whiteout), and the one the kernel uses after e.g.
/// `rm -rf dir && mkdir dir`. Importing such a directory as a plain merge would
/// let the deleted children silently reappear, so both the one-shot
/// [`import_upper`] and the incremental [`LiveSync`] prune them.
///
/// Always `false` off Linux, where there is no overlayfs to mark anything.
pub fn is_opaque_dir(path: &Path) -> bool {
    OPAQUE_XATTRS
        .iter()
        .any(|name| lgetxattr_value(path, name).as_deref() == Some(b"y"))
}

/// The overlayfs `redirect` marker on `path`, if any (a lower-layer path this
/// upper directory stands in for after a rename). We don't act on it; callers
/// warn so a redirected rename isn't imported as if it were nothing.
fn redirect_marker(path: &Path) -> Option<String> {
    REDIRECT_XATTRS
        .iter()
        .find_map(|name| lgetxattr_value(path, name))
        .map(|v| String::from_utf8_lossy(&v).into_owned())
}

/// Apply an opaque directory to the workspace: every workspace child of
/// `origofs_dir` that the upper layer does **not** list is removed, recursively,
/// so the upper directory replaces the lower one instead of merging with it.
/// `present` holds the upper directory's entry names (whiteouts included — those
/// are deletions the normal import path handles). Returns the origofs paths removed.
async fn apply_opaque(
    ws: &Workspace,
    origofs_dir: &str,
    present: &HashSet<String>,
    ctx: Option<WriteCtx>,
) -> Result<Vec<String>> {
    let Ok(existing) = ws.ls(origofs_dir).await else {
        return Ok(Vec::new()); // nothing on the origofs side to replace
    };
    let mut removed = Vec::new();
    for e in existing {
        if present.contains(&e.name) {
            continue;
        }
        // Defense-in-depth, same rule as `export_tree`: never build a path out of
        // a component that could traverse, even one that came back from origofs.
        if e.name.is_empty() || e.name == "." || e.name == ".." || e.name.contains('/') {
            bail!("refusing to delete unsafe path component {:?}", e.name);
        }
        let victim = join_origofs(origofs_dir, &e.name);
        origofs_rm_rf(ws, &victim, ctx).await?;
        removed.push(victim);
    }
    Ok(removed)
}

/// Snapshot a host directory's entries as `(names, paths)`. Read up front because
/// an opaque marker has to be applied against the *whole* name set before any
/// entry is imported.
async fn read_dir_snapshot(dir: &Path) -> std::io::Result<(HashSet<String>, Vec<PathBuf>)> {
    let mut names = HashSet::new();
    let mut paths = Vec::new();
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        // Lossy, exactly like the origofs path built from it below, so the name a
        // whiteout/opaque decision is keyed on is the name that gets imported.
        names.insert(entry.file_name().to_string_lossy().into_owned());
        paths.push(entry.path());
    }
    Ok((names, paths))
}

/// Import an overlay `upper/` delta into `ws` in one shot: the write layer of a
/// finished [`run`], turned into attributed writes and deletions against the
/// workspace. Returns the number of origofs paths mutated.
///
/// Exposed so a caller that drives its own overlay (or a test with a hand-built
/// `upper/` tree) can reuse exactly the import the sandbox uses — including
/// whiteout and opaque-directory handling. Pass both `actor` and `session` for
/// attributed writes (blame + edit-ops); otherwise the writes are unattributed.
pub async fn import_upper(
    ws: &Workspace,
    upper: &Path,
    actor: Option<i64>,
    session: Option<i64>,
) -> Result<usize> {
    Box::pin(import_delta(ws, upper, upper, actor, session)).await
}

/// Import the overlay `upper` delta under `dir` back into `ws`.
///
/// Deletions arrive two ways and both are honored: a character-device whiteout
/// (rdev 0) removes that one path, and an *opaque* directory ([`is_opaque_dir`])
/// removes every workspace child the upper directory doesn't list. A `redirect`
/// marker (a renamed directory) is not followed, but is logged rather than
/// silently mis-imported.
async fn import_delta(
    ws: &Workspace,
    root: &Path,
    dir: &Path,
    actor: Option<i64>,
    session: Option<i64>,
) -> Result<usize> {
    // One context for every mutation this import performs, so a deletion, a
    // directory, and a symlink are attributed exactly like the file writes
    // alongside them.
    let ctx = write_ctx(actor, session);
    let mut count = 0;
    let (names, hosts) = read_dir_snapshot(dir).await?;
    if is_opaque_dir(dir) {
        count += apply_opaque(ws, &origofs_path_for(root, dir), &names, ctx)
            .await?
            .len();
    }
    if let Some(target) = redirect_marker(dir) {
        tracing::warn!(
            dir = %dir.display(),
            redirect = %target,
            "overlay redirect marker on an imported directory is not followed; \
             the renamed directory's lower contents are left where they were"
        );
    }
    for host in hosts {
        let origofs_path = origofs_path_for(root, &host);
        let md = tokio::fs::symlink_metadata(&host).await?;
        let ft = md.file_type();

        if ft.is_char_device() && md.rdev() == 0 {
            // overlayfs whiteout => the path was deleted in the sandbox
            let _ = origofs_rm_rf(ws, &origofs_path, ctx).await;
            count += 1;
        } else if ft.is_dir() {
            mkdir_attributed(ws, &origofs_path, ctx).await?;
            count += Box::pin(import_delta(ws, root, &host, actor, session)).await?;
        } else if ft.is_symlink() {
            let target = tokio::fs::read_link(&host).await?;
            symlink_attributed(ws, &target.to_string_lossy(), &origofs_path, ctx).await?;
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
/// Unlike the one-shot [`import_upper`], a `LiveSync` remembers what it has
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
    /// Directories an opaque marker has already been applied for (apply once, so
    /// a later tick doesn't re-prune children a *different* writer legitimately
    /// added to the workspace after the replacement).
    opaque: HashSet<String>,
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
            opaque: HashSet::new(),
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
        let ctx = write_ctx(self.actor, self.session);
        let mut count = 0;
        let (names, hosts) = match read_dir_snapshot(dir).await {
            Ok(snapshot) => snapshot,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        // An opaque directory replaces the lower one: prune the workspace children
        // the agent's write layer no longer lists (once — see `opaque`).
        let dir_path = origofs_path_for(root, dir);
        if is_opaque_dir(dir) && self.opaque.insert(dir_path.clone()) {
            for victim in apply_opaque(ws, &dir_path, &names, ctx).await? {
                let sub = format!("{victim}/");
                self.seen
                    .retain(|p, _| p != &victim && !p.starts_with(&sub));
                count += 1;
            }
        }
        for host in hosts {
            let origofs_path = origofs_path_for(root, &host);
            let md = tokio::fs::symlink_metadata(&host).await?;
            let ft = md.file_type();

            if ft.is_char_device() && md.rdev() == 0 {
                // overlayfs whiteout => the path was deleted in the overlay.
                if self.deleted.insert(origofs_path.clone()) {
                    let _ = origofs_rm_rf(ws, &origofs_path, ctx).await;
                    self.seen.remove(&origofs_path);
                    count += 1;
                }
            } else if ft.is_dir() {
                mkdir_attributed(ws, &origofs_path, ctx).await?;
                count += Box::pin(self.sync_dir(ws, root, &host)).await?;
            } else if ft.is_symlink() {
                let key = (mtime_ns(&md), md.len());
                if self.seen.get(&origofs_path) != Some(&key) {
                    let target = tokio::fs::read_link(&host).await?;
                    symlink_attributed(ws, &target.to_string_lossy(), &origofs_path, ctx).await?;
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

/// The [`WriteCtx`] an import attributes through, when it has both halves.
///
/// An import needs an actor *and* a session to be attributable; with either
/// missing there is nothing meaningful to record, and the unattributed engine
/// methods are the honest fallback rather than a fabricated identity.
fn write_ctx(actor: Option<i64>, session: Option<i64>) -> Option<WriteCtx> {
    match (actor, session) {
        (Some(a), Some(s)) => Some(WriteCtx::session(a, s)),
        _ => None,
    }
}

/// Recursively remove an origofs path (file or directory), attributed to the
/// importing actor when there is one.
///
/// Deletions used to import through the unattributed `remove`, so
/// `origofs sandbox --actor 7 -- rm -rf src/` recorded *nothing* about who
/// removed the tree: no blame, no `edit_op`, no audit row. In a system whose
/// premise is that every change is attributable, an unattributed delete is the
/// worst gap to have, because a deletion is the change you most want to trace.
async fn origofs_rm_rf(ws: &Workspace, path: &str, ctx: Option<WriteCtx>) -> Result<()> {
    match ws.stat(path).await {
        Ok(inode) if inode.kind == FileKind::Dir => {
            for e in ws.ls(path).await? {
                Box::pin(origofs_rm_rf(ws, &join_origofs(path, &e.name), ctx)).await?;
            }
            remove_attributed(ws, path, ctx).await?;
        }
        Ok(_) => {
            remove_attributed(ws, path, ctx).await?;
        }
        Err(_) => {} // already gone
    }
    Ok(())
}

/// `remove`, attributed when the import has an actor.
///
/// Uses `remove_or_propose` rather than `remove_as`: it is the variant that
/// honours the §6 write policy, so a propose-only actor's deletion is queued for
/// review instead of applied. An import is the actor's own work arriving by a
/// different route, not privileged machinery, so it gets the same gate the
/// front door has.
async fn remove_attributed(ws: &Workspace, path: &str, ctx: Option<WriteCtx>) -> Result<()> {
    match ctx {
        Some(ctx) => {
            ws.remove_or_propose(ctx, path, Some("removed in sandbox"))
                .await?;
        }
        None => ws.remove(path).await?,
    }
    Ok(())
}

/// `mkdir_p`, attributed when the import has an actor.
async fn mkdir_attributed(ws: &Workspace, path: &str, ctx: Option<WriteCtx>) -> Result<()> {
    match ctx {
        Some(ctx) => ws.mkdir_as(ctx, path).await?,
        None => ws.mkdir_p(path).await?,
    }
    Ok(())
}

/// `symlink`, attributed when the import has an actor. The existing entry is
/// removed first because `symlink` refuses to overwrite.
async fn symlink_attributed(
    ws: &Workspace,
    target: &str,
    path: &str,
    ctx: Option<WriteCtx>,
) -> Result<()> {
    let _ = remove_attributed(ws, path, ctx).await;
    match ctx {
        Some(ctx) => ws.symlink_as(ctx, target, path).await?,
        None => ws.symlink(target, path).await?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    // The isolated runner's argv must set up a real filesystem boundary: a COW
    // overlay with delta capture, a read-only host toolchain, fresh namespaces,
    // and the user command after `--`. This pins what we *ask* bubblewrap for;
    // that the boundary actually holds is asserted end-to-end in
    // `tests/sandbox.rs` (`isolated_*`), which really does execute bwrap.
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
        assert!(
            args.windows(3)
                .any(|w| w[0] == "--ro-bind" && w[1] == "/usr" && w[2] == "/usr")
        );
        assert!(
            !args.iter().any(|a| a == "--bind"),
            "nothing host is writable: {args:?}"
        );
        // Real namespaces + working dir inside the overlay.
        assert!(args.contains(&"--unshare-pid".to_string()));
        assert!(
            args.windows(2)
                .any(|w| w[0] == "--chdir" && w[1] == BWRAP_WORKDIR)
        );
        // The user command comes last, after the `--` separator, unmodified.
        let sep = args.iter().position(|a| a == "--").expect("`--` separator");
        assert_eq!(&args[sep + 1..], &["echo".to_string(), "hi".to_string()]);
    }

    /// The flag [`bwrap_gap`] looks for in `--help` must be one the runner
    /// actually passes. If `bwrap_command` were changed to a different overlay
    /// spelling, the probe would keep answering about an option we no longer use
    /// and the gate would be meaningless.
    #[test]
    fn the_probed_option_is_the_one_the_runner_passes() {
        let c = bwrap_command(
            Path::new("/w/lower"),
            Path::new("/w/upper"),
            Path::new("/w/work"),
            &["true".to_string()],
        );
        assert!(
            c.as_std()
                .get_args()
                .any(|a| a == OsStr::new(BWRAP_OVERLAY_SRC)),
            "capability probe checks for {BWRAP_OVERLAY_SRC}, which the runner must pass"
        );
    }

    /// Pin the floor at the release that actually introduced `--overlay`.
    ///
    /// This was 0.8.0 and wrong: the options landed in **0.11.0**. The practical
    /// effect of the old value is what makes it worth a test — the two versions
    /// below are what current LTS distributions ship, and both sailed through the
    /// old gate only to die on `bwrap: Unknown option --overlay-src`.
    #[test]
    fn the_version_floor_is_where_overlay_support_actually_landed() {
        assert_eq!(MIN_BWRAP_VERSION, (0, 11, 0));
        for (distro, v) in [
            ("Debian 12", (0, 8, 0)),
            ("Ubuntu 24.04 LTS", (0, 9, 0)),
            ("Ubuntu 25.04", (0, 10, 0)),
        ] {
            assert!(
                v < MIN_BWRAP_VERSION,
                "{distro} ships bwrap {v:?}, which has no --overlay and must not pass the gate"
            );
        }
        assert!((0, 11, 0) >= MIN_BWRAP_VERSION);
    }

    /// A version new enough is *not* sufficient — a setuid install of 0.11+ has no
    /// `--overlay` — so the gap check must not stop at the version comparison.
    /// Whichever way this machine's bubblewrap falls, the verdict has to agree
    /// with whether `--help` really offers the option.
    #[test]
    fn capability_is_probed_rather_than_inferred_from_the_version() {
        let Ok(help) = std::process::Command::new("bwrap").arg("--help").output() else {
            return; // no bwrap here; `Missing` is covered by the type's Display
        };
        let text = String::from_utf8_lossy(&help.stdout).into_owned()
            + &String::from_utf8_lossy(&help.stderr);
        let offers_overlay = text.contains(BWRAP_OVERLAY_SRC);
        match bwrap_gap() {
            None => assert!(
                offers_overlay,
                "reported usable, but --help has no {BWRAP_OVERLAY_SRC}"
            ),
            Some(BwrapGap::NoOverlay { .. }) => assert!(
                !offers_overlay,
                "reported as lacking overlays, but --help offers {BWRAP_OVERLAY_SRC}"
            ),
            // Missing/TooOld are about the binary itself, not the option set.
            Some(_) => {}
        }
    }
}
