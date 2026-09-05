//! Commits, branches, diffs, merges and the real-git bridge.

use crate::*;

pub async fn commit(
    ws: &Workspace,
    message: String,
    author: String,
    actor: Option<i64>,
) -> Result<()> {
    let hash = match resolve_actor(actor)? {
        Some(actor) => {
            let ctx = cli_ctx(ws, actor).await?;
            ws.commit_as(ctx, &author, &message).await?
        }
        None => {
            ws.ensure_attributed("commit").await?;
            ws.commit(&author, &message).await?
        }
    };
    let branch = ws.current_branch().await?.unwrap_or_else(|| "?".into());
    println!("[{branch} {}] {message}", &hash.to_hex()[..12]);

    Ok(())
}

pub async fn log(ws: &Workspace) -> Result<()> {
    for ci in ws.log().await? {
        println!(
            "{} {}  {}",
            &ci.hash.to_hex()[..12],
            ci.commit.author,
            ci.commit.message
        );
    }

    Ok(())
}

pub async fn status(ws: &Workspace) -> Result<()> {
    let changes = ws.status().await?;
    if changes.is_empty() {
        println!("clean (working tree matches HEAD)");
    }
    for d in changes {
        println!("{} {}", d.status.sigil(), d.path);
    }

    Ok(())
}

pub async fn diff(
    ws: &Workspace,
    from: String,
    to: String,
    path: Option<String>,
    actor: Option<i64>,
) -> Result<()> {
    match path {
        Some(p) => {
            let patch = match read_ctx(actor)? {
                Some(ctx) => ws.diff_file_as(ctx, &from, &to, &p).await?,
                None => ws.diff_file(&from, &to, &p).await?,
            };
            if patch.is_empty() {
                println!("{p}: unchanged between {from} and {to}");
            } else {
                print!("{patch}");
            }
        }
        None => {
            let changes = match read_ctx(actor)? {
                Some(ctx) => ws.diff_as(ctx, &from, &to).await?,
                None => ws.diff(&from, &to).await?,
            };
            if changes.is_empty() {
                println!("no differences between {from} and {to}");
            }
            for d in changes {
                println!("{} {}", d.status.sigil(), d.path);
            }
        }
    };
    Ok(())
}

pub async fn branch(ws: &Workspace, name: Option<String>) -> Result<()> {
    match name {
        Some(name) => {
            ws.create_branch(&name).await?;
            println!("created branch {name}");
        }
        None => {
            let current = ws.current_branch().await?;
            for (name, hash) in ws.list_branches().await? {
                let marker = if current.as_deref() == Some(&name) {
                    "* "
                } else {
                    "  "
                };
                println!("{marker}{name}\t{}", &hash.to_hex()[..12]);
            }
        }
    };
    Ok(())
}

pub async fn checkout(ws: &Workspace, branch: String) -> Result<()> {
    ws.checkout(&branch).await?;
    println!("switched to branch {branch}");

    Ok(())
}

pub async fn merge(
    ws: &Workspace,
    branch: String,
    author: String,
    message: Option<String>,
) -> Result<()> {
    let msg = message.unwrap_or_else(|| format!("merge {branch}"));
    match ws.merge_branch(&branch, &author, &msg).await? {
        MergeOutcome::AlreadyUpToDate => println!("already up to date"),
        MergeOutcome::FastForward(h) => {
            println!("fast-forward to {}", &h.to_hex()[..12])
        }
        MergeOutcome::Merged(h) => println!("merged as {}", &h.to_hex()[..12]),
        MergeOutcome::Conflicts(cs) => {
            println!(
                "merge stopped with {} conflict(s); resolve then commit:",
                cs.len()
            );
            for c in cs {
                println!("  {} {}", c.kind, c.path);
            }
        }
    }

    Ok(())
}

pub async fn resync(
    ws: &Workspace,
    remote: Option<PathBuf>,
    remote_config: Option<PathBuf>,
    branch: Option<String>,
    author: String,
    message: Option<String>,
) -> Result<()> {
    if remote.is_none() && remote_config.is_none() {
        anyhow::bail!("resync needs a remote: pass --remote <DIR> and/or --remote-config <FILE>");
    }
    // `--remote` alone means a plain local SQLite + local-CAS workspace at
    // that directory; `--remote-config` selects the backends, rooting any
    // defaulted path at `--remote` (or the config file's own directory).
    let remote_root = remote.clone().unwrap_or_else(|| {
        remote_config
            .as_ref()
            .and_then(|p| p.parent().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("."))
    });
    let remote_cfg = match &remote_config {
        Some(path) => config::Config::load(path)?,
        None => config::Config::default(),
    };
    std::fs::create_dir_all(&remote_root)?;
    let remote_ws = remote_cfg.open(&remote_root).await?;

    let branch = match branch {
        Some(b) => b,
        None => ws.current_branch().await?.ok_or_else(|| {
            anyhow::anyhow!("HEAD is detached; pass --branch to name the branch to resync")
        })?,
    };
    let msg = message.unwrap_or_else(|| format!("resync {branch}"));
    let report = ws.resync(&remote_ws, &branch, &author, &msg).await?;

    match &report.outcome {
        origofs_sdk::ResyncOutcome::UpToDate => {
            println!("{}: already up to date", report.branch)
        }
        origofs_sdk::ResyncOutcome::Pushed(h) => println!(
            "{}: pushed {} to the remote",
            report.branch,
            &h.to_hex()[..12]
        ),
        origofs_sdk::ResyncOutcome::FastForwarded(h) => {
            println!("{}: fast-forwarded to {}", report.branch, &h.to_hex()[..12])
        }
        origofs_sdk::ResyncOutcome::Merged(h) => println!(
            "{}: merged as {} and pushed",
            report.branch,
            &h.to_hex()[..12]
        ),
        origofs_sdk::ResyncOutcome::Conflicted => println!(
            "{}: merge stopped with {} conflict(s); the remote was not advanced — \
                 resolve, commit, then resync again:",
            report.branch,
            report.conflicts.len()
        ),
    }
    for c in &report.conflicts {
        println!("  {} {}", c.kind, c.path);
    }
    println!(
        "  fetched {} object(s), {} B ({} already present)",
        report.fetched.objects, report.fetched.bytes, report.fetched.skipped
    );
    println!(
        "  pushed  {} object(s), {} B ({} already present)",
        report.pushed.objects, report.pushed.bytes, report.pushed.skipped
    );
    println!(
        "  blame carried: {} in, {} out",
        report.blame_fetched, report.blame_pushed
    );
    if report.cas_retries > 0 {
        println!(
            "  retried {} time(s) after a concurrent remote push",
            report.cas_retries
        );
    }
    if report.remote_tree_updated {
        println!("  the remote working tree was rematerialized at the new head");
    }
    for p in &report.stale_live_paths {
        println!("  warning: {p} has an open live document; its merged bytes may lag it");
    }

    Ok(())
}

pub async fn conflicts(ws: &Workspace) -> Result<()> {
    for (path, kind) in ws.conflicts().await? {
        println!("{kind}\t{path}");
    }

    Ok(())
}

pub async fn lock(ws: &Workspace, path: String, owner: String) -> Result<()> {
    if ws.lock(&path, &owner).await? {
        println!("locked {path}");
    } else {
        println!("already locked: {path}");
    }

    Ok(())
}

pub async fn unlock(ws: &Workspace, path: String, owner: String) -> Result<()> {
    if ws.unlock(&path, &owner).await? {
        println!("unlocked {path}");
    } else {
        println!("not your lock: {path}");
    }

    Ok(())
}

pub async fn locks(ws: &Workspace) -> Result<()> {
    for (path, owner, _at) in ws.locks().await? {
        println!("{owner}\t{path}");
    }

    Ok(())
}

pub async fn git(ws: &Workspace, cmd: GitCmd) -> Result<()> {
    match cmd {
        GitCmd::Export {
            dir,
            branch,
            format,
            lfs_threshold,
        } => {
            let format = origofs_sdk::git::ObjectFormat::parse(&format)
                .ok_or_else(|| anyhow::anyhow!("format must be `sha1` or `sha256`"))?;
            let opts = origofs_sdk::git::ExportOptions {
                format,
                branch,
                lfs_threshold,
            };
            let out = origofs_sdk::git::export_git(ws, &dir, &opts).await?;
            println!(
                "exported branch {} ({} commit(s), {} lfs object(s)) to {}",
                out.branch,
                out.commits,
                out.lfs_objects,
                dir.display()
            );
            println!("head {} {}", format.as_str(), out.head);
        }
        GitCmd::Import { dir, branch } => {
            let head = origofs_sdk::git::import_git(ws, &dir, &branch).await?;
            println!(
                "imported branch {branch} at {} from {}",
                &head.to_hex()[..12],
                dir.display()
            );
        }
    };
    Ok(())
}
