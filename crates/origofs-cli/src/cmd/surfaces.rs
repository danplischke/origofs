//! The long-running surfaces and the sandbox: everything that serves or mounts rather than returning.

use crate::*;

pub async fn sandbox(
    ws: &Workspace,
    workspace_dir: &Path,
    actor: Option<i64>,
    discard: bool,
    isolate: bool,
    cmd: Vec<String>,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (actor, discard, isolate, cmd);
        return Err(unix_only("sandbox", "an unprivileged overlayfs mount"));
    }
    #[cfg(unix)]
    {
        if isolate {
            // Surface the specific reason (absent / too old / built without
            // overlays), not a blanket "needs bwrap on PATH".
            if let Some(gap) = origofs_sdk::sandbox::bwrap_gap() {
                anyhow::bail!("--isolate is unavailable: {gap}");
            }
        } else if !origofs_sdk::sandbox::overlay_supported() {
            anyhow::bail!(
                "unprivileged overlayfs is unavailable here (needs user-namespace overlay support)"
            );
        } else {
            // Say it at the moment it matters. Without `--isolate` the child
            // runs with the invoker's privileges over a plain copy-on-write
            // overlay: the whole host filesystem stays reachable, including
            // this workspace's meta.db and cas, with no network namespace and
            // no seccomp. That caveat lived only in `--help` and doc comments,
            // while strictly less dangerous things (a non-loopback NFS or
            // metrics bind) both warned at runtime.
            eprintln!(
                "warning: running without --isolate: this captures edits but is NOT a \
                 security boundary. The command runs with your privileges and can read \
                 and modify anything you can, including this workspace's meta.db and \
                 cas. Run only code you trust, or pass --isolate for a real filesystem \
                 boundary (needs a non-setuid bwrap >= 0.11.0, for --overlay support)."
            );
        }
        let tmp = workspace_dir.join(format!("sandbox-{}", std::process::id()));
        let opts = origofs_sdk::sandbox::RunOpts {
            actor,
            discard,
            work_root: tmp.clone(),
            isolate,
        };
        let outcome = origofs_sdk::sandbox::run(ws, opts, &cmd).await?;
        let _ = std::fs::remove_dir_all(&tmp);
        if outcome.imported {
            println!(
                "command exited {}; imported {} change(s)",
                outcome.exit_code, outcome.files_changed
            );
        } else {
            println!("command exited {}; delta discarded", outcome.exit_code);
        }
        std::process::exit(outcome.exit_code);
    }
}

pub async fn overlay(
    ws: &Workspace,
    workspace_dir: &Path,
    actor: Option<i64>,
    sync_ms: u64,
    isolate: bool,
    cmd: Vec<String>,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (actor, sync_ms, isolate, cmd);
        return Err(unix_only("overlay", "an unprivileged overlayfs mount"));
    }
    #[cfg(unix)]
    {
        if isolate {
            // Surface the specific reason (absent / too old / built without
            // overlays), not a blanket "needs bwrap on PATH".
            if let Some(gap) = origofs_sdk::sandbox::bwrap_gap() {
                anyhow::bail!("--isolate is unavailable: {gap}");
            }
        } else if !origofs_sdk::sandbox::overlay_supported() {
            anyhow::bail!(
                "unprivileged overlayfs is unavailable here (needs user-namespace overlay support)"
            );
        } else {
            // Say it at the moment it matters. Without `--isolate` the child
            // runs with the invoker's privileges over a plain copy-on-write
            // overlay: the whole host filesystem stays reachable, including
            // this workspace's meta.db and cas, with no network namespace and
            // no seccomp. That caveat lived only in `--help` and doc comments,
            // while strictly less dangerous things (a non-loopback NFS or
            // metrics bind) both warned at runtime.
            eprintln!(
                "warning: running without --isolate: this captures edits but is NOT a \
                 security boundary. The command runs with your privileges and can read \
                 and modify anything you can, including this workspace's meta.db and \
                 cas. Run only code you trust, or pass --isolate for a real filesystem \
                 boundary (needs a non-setuid bwrap >= 0.11.0, for --overlay support)."
            );
        }
        let tmp = workspace_dir.join(format!("overlay-{}", std::process::id()));
        let opts = origofs_sdk::sandbox::LiveOpts {
            actor,
            work_root: tmp.clone(),
            sync_interval: std::time::Duration::from_millis(sync_ms),
            isolate,
        };
        let outcome = origofs_sdk::sandbox::run_live(ws, opts, &cmd).await?;
        let _ = std::fs::remove_dir_all(&tmp);
        println!(
            "agent exited {}; streamed {} change(s) into origofs",
            outcome.exit_code, outcome.files_changed
        );
        std::process::exit(outcome.exit_code);
    }
}

pub async fn mount(ws: Workspace, mountpoint: PathBuf, actor: Option<i64>) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (mountpoint, actor);
        return Err(unix_only("mount", "FUSE (`/dev/fuse`)"));
    }
    #[cfg(unix)]
    {
        if !origofs_sdk::fuse::mountable() {
            anyhow::bail!("FUSE mount unavailable here (needs root + /dev/fuse)");
        }
        std::fs::create_dir_all(&mountpoint)?;
        println!(
            "mounting origofs at {} (unmount with `umount` to stop)",
            mountpoint.display()
        );
        // The mount drives its own runtime, so run it off the async main thread.
        let ctx = read_ctx(actor)?;
        if ctx.is_none() {
            println!("  (anonymous mount: path ACLs do not apply — pass --actor to bind one)");
        }
        let handle = std::thread::spawn(move || origofs_sdk::fuse::mount_as(ws, &mountpoint, ctx));
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("mount thread panicked"))??;
    }

    Ok(())
}

pub async fn mcp(ws: Workspace, agent_name: String, model: String) -> Result<()> {
    let server = origofs_sdk::mcp::McpServer::create(ws, &agent_name, &model).await?;
    server.serve_stdio().await?;

    Ok(())
}

pub async fn watch(ws: &Workspace, since: i64, follow: bool) -> Result<()> {
    let mut cursor = since;
    loop {
        for e in ws.watch(cursor).await? {
            let who = e
                .actor_id
                .map(|a| format!("actor:{a}"))
                .unwrap_or_else(|| "-".to_string());
            let detail = e.detail.map(|d| format!("  ({d})")).unwrap_or_default();
            println!("{}\t{}\t{who}\t{}{detail}", e.seq, e.kind, e.path);
            cursor = e.seq;
        }
        if !follow {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

pub async fn presence(ws: &Workspace, window: i64, actor: Option<i64>) -> Result<()> {
    let rows = match read_ctx(actor)? {
        Some(ctx) => ws.presence_as(ctx, window).await?,
        None => ws.presence(window).await?,
    };
    for p in rows {
        let path = p.path.unwrap_or_else(|| "-".to_string());
        println!(
            "{}\t{}\t{path}\t(seen {})",
            p.kind.as_str(),
            p.display_name,
            p.last_seen
        );
    }

    Ok(())
}

/// Everything `origofs serve` takes beyond the workspace.
///
/// A struct rather than ten positional parameters. The flags travelled together
/// as one match arm's bindings while this lived inside `main`, so nothing named
/// them as a group; pulling the body out made the arity visible and it is worth
/// fixing rather than silencing — at ten arguments, two `Option<usize>` and two
/// `Vec<String>` next to each other are a call waiting to be mis-ordered.
pub struct ServeArgs {
    pub addr: std::net::SocketAddr,
    pub auth_tokens: Vec<String>,
    pub gate_reads: bool,
    pub root: Option<String>,
    pub cors_origins: Vec<String>,
    pub max_body_bytes: Option<usize>,
    pub request_timeout: Option<u64>,
    pub max_concurrent_requests: Option<usize>,
    pub metrics: bool,
}

pub async fn serve(ws: Workspace, args: ServeArgs) -> Result<()> {
    let ServeArgs {
        addr,
        auth_tokens,
        gate_reads,
        root,
        cors_origins,
        max_body_bytes,
        request_timeout,
        max_concurrent_requests,
        metrics,
    } = args;
    // Validated here rather than left to `router_with`, which *panics* on a
    // malformed root — correct for a library whose caller is code, wrong
    // for a value a user typed.
    if let Some(r) = &root {
        origofs_sdk::Scope::at(r).with_context(|| format!("--root {r:?}"))?;
    }
    let auth = build_api_auth(&ws, &addr, &auth_tokens_with_env(auth_tokens)).await?;
    let defaults = origofs_sdk::api::ApiOptions::default();
    // `ApiOptions` has a feature-gated field (`checkpoint`, under
    // `coedit`), so a literal naming every other field is *exhaustive*
    // under one feature set and not another: `..defaults` is load-bearing
    // with `coedit` on and `needless_update` with it off. Clippy only ever
    // sees the set it was compiled with, so one of the two readings is
    // always wrong — hence the allow rather than a different shape.
    // Dropping the update would break the `coedit` build; rebuilding this
    // by field assignment trades it for `field_reassign_with_default`.
    #[allow(clippy::needless_update)]
    let options = origofs_sdk::api::ApiOptions {
        gate_reads,
        root,
        cors_origins,
        max_body_bytes: max_body_bytes.unwrap_or(defaults.max_body_bytes),
        request_timeout: match request_timeout {
            Some(0) => None,
            Some(s) => Some(std::time::Duration::from_secs(s)),
            None => defaults.request_timeout,
        },
        max_concurrent_requests: match max_concurrent_requests {
            Some(0) => None,
            Some(n) => Some(n),
            None => defaults.max_concurrent_requests,
        },
        ..defaults
    };
    // `build_api_auth` refuses to serve unauthenticated *writes* off
    // loopback. Reads are a separate decision and default to open, so say
    // so rather than letting a public bind quietly publish every file's
    // bytes, its blame map, and the change feed.
    if !gate_reads && !addr.ip().is_loopback() {
        eprintln!(
            "warning: reads are unauthenticated on {addr} (non-loopback bind); anyone who can reach it can read every file, its blame, the audit log and the review queue. Pass --gate-reads, or gate reads at your proxy."
        );
    }
    if metrics || env_flag("ORIGOFS_METRICS") {
        init_metrics()?;
        // `/metrics` gets the same auth treatment as `/readyz`: open. Its
        // labels are closed sets (error code/class, matched route
        // template), so it exposes no path, actor, or content — but say so
        // rather than letting a public bind surprise anyone.
        if !addr.ip().is_loopback() {
            eprintln!(
                "warning: exposing unauthenticated Prometheus metrics at http://{addr}/metrics (non-loopback bind, same posture as /readyz); restrict it at your proxy if scrapes shouldn't be public"
            );
        }
        println!("exposing Prometheus metrics at http://{addr}/metrics");
    }
    tracing::info!(%addr, "starting origofs HTTP API");
    println!("serving origofs at http://{addr} (SIGTERM/Ctrl-C to stop; in-flight requests drain)");
    let ws = std::sync::Arc::new(ws);
    // Housekeeping. `reap_presence` and `supersede_stale_suggestions`
    // existed with no caller anywhere, so a long-running `origofs serve`
    // grew its presence table forever and left suggestions pending against
    // bases that had already moved. A server is exactly the process that
    // should be running them; nothing else is long-lived enough to.
    let janitor = tokio::spawn(spawn_janitor(ws.clone()));
    let result = origofs_sdk::api::serve_with(ws, addr, auth, options).await;
    janitor.abort();
    result?;

    Ok(())
}

pub async fn nfs(ws: Workspace, addr: String, actor: Option<i64>) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (addr, actor);
        return Err(unix_only("nfs", "the NFSv3 server surface"));
    }
    #[cfg(unix)]
    {
        // NFSv3 is unauthenticated; warn loudly if this isn't a loopback bind.
        if addr
            .parse::<std::net::SocketAddr>()
            .map(|s| !s.ip().is_loopback())
            .unwrap_or(false)
        {
            eprintln!(
                "warning: binding NFS to a non-loopback address ({addr}); NFSv3 has no authentication — anyone who can reach it gets full, unattributed access. Prefer a loopback bind reached over a tunnel/VPN."
            );
        }
        println!(
            "serving origofs over NFSv3 at {addr} (SIGTERM/Ctrl-C to stop)\n  mount with: mount -t nfs -o vers=3,tcp,port=<port>,mountport=<port>,nolock <host>:/ /mnt"
        );
        origofs_sdk::nfs::serve_as(ws, &addr, read_ctx(actor)?).await?;
    }

    Ok(())
}
