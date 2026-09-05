//! Reading and shaping the working tree: the POSIX-flavoured commands.

use crate::*;

pub async fn init(workspace_dir: &Path) -> Result<()> {
    println!(
        "initialized origofs workspace at {}",
        workspace_dir.display()
    );

    Ok(())
}

pub async fn mkdir(ws: &Workspace, path: String, actor: Option<i64>) -> Result<()> {
    match resolve_actor(actor)? {
        Some(actor) => {
            let ctx = cli_ctx(ws, actor).await?;
            ws.mkdir_as(ctx, &path).await?;
        }
        None => {
            ws.ensure_attributed("mkdir").await?;
            ws.mkdir_p(&path).await?;
        }
    };
    Ok(())
}

pub async fn write(
    ws: &Workspace,
    path: String,
    from: Option<PathBuf>,
    actor: Option<i64>,
) -> Result<()> {
    // Convenience: ensure the parent directory exists before writing.
    if let Some(parent) = path
        .rsplit_once('/')
        .map(|(p, _)| p)
        .filter(|p| !p.is_empty())
    {
        ws.mkdir_p(parent).await?;
    }
    // `write` resolves its actor the same way the other mutating commands
    // do since #128, so `ORIGOFS_ACTOR` attributes it too.
    let actor = resolve_actor(actor)?;
    match (from, actor) {
        // Unattributed streaming from a file (large files stay off-heap).
        (Some(p), None) => {
            ws.ensure_attributed("write").await?;
            let file = std::fs::File::open(p)?;
            ws.write_reader(&path, file).await?;
        }
        // Attributed write. `--actor` used to force `std::fs::read` of the
        // whole file — streaming and attribution were mutually exclusive
        // until `write_reader_as` — so a large file could be written only
        // by giving up the attribution that is the point of this system.
        (from, Some(actor)) => {
            let session = ws.create_session(actor, Some("cli")).await?;
            let ctx = WriteCtx::session(actor, session);

            // A propose-only actor's edit is queued for review, and a
            // suggestion needs the bytes — so that path buffers, whatever
            // the source. That is fine by construction: nobody reviews a
            // multi-gigabyte diff. Deciding here rather than letting
            // `write_reader_as` refuse keeps `origofs policy <actor>
            // propose` behaving identically with and without `--from`.
            let may_write_directly = ws.ensure_may_write(ctx, "write a file").await.is_ok();

            match (from, may_write_directly) {
                // The good case: stream straight from the file.
                (Some(p), true) => {
                    let file = std::fs::File::open(p)?;
                    ws.write_reader_as(ctx, &path, file).await?;
                }
                // Buffered: stdin has no length to stream against here, and
                // a propose-only write has to hold the proposed bytes.
                (from, _) => {
                    let data = match from {
                        Some(p) => std::fs::read(p)?,
                        None => {
                            let mut buf = Vec::new();
                            std::io::stdin().read_to_end(&mut buf)?;
                            buf
                        }
                    };
                    // `write_or_propose`, not `write_as`: the raw attributed
                    // write is exempt from the §6 policy by construction, so
                    // `origofs policy <actor> propose` had no effect on
                    // `origofs write` — the CLI ignored the gate its own
                    // subcommand sets.
                    match ws.write_or_propose(ctx, &path, &data, None, None).await? {
                        origofs_sdk::WriteOutcome::Wrote => {}
                        origofs_sdk::WriteOutcome::Proposed(suggestion_id) => {
                            println!(
                                "actor {actor} is propose-only: queued suggestion #{suggestion_id} for {path} (pending review)"
                            );
                        }
                    }
                }
            }
        }
        (None, None) => {
            ws.ensure_attributed("write").await?;
            let mut buf = Vec::new();
            std::io::stdin().read_to_end(&mut buf)?;
            ws.write(&path, &buf).await?;
        }
    }

    Ok(())
}

pub async fn read(ws: &Workspace, path: String, actor: Option<i64>) -> Result<()> {
    let bytes = match read_ctx(actor)? {
        Some(ctx) => ws.read_as(ctx, &path).await?,
        None => ws.read(&path).await?,
    };
    std::io::stdout().write_all(&bytes)?;

    Ok(())
}

pub async fn ls(ws: &Workspace, path: String, actor: Option<i64>) -> Result<()> {
    let entries = match read_ctx(actor)? {
        Some(ctx) => ws.ls_as(ctx, &path).await?,
        None => ws.ls(&path).await?,
    };
    for e in entries {
        println!("{}\t{}", e.kind.as_str(), e.name);
    }

    Ok(())
}

pub async fn stat(ws: &Workspace, path: String, actor: Option<i64>) -> Result<()> {
    let i = match read_ctx(actor)? {
        Some(ctx) => ws.stat_as(ctx, &path).await?,
        None => ws.stat(&path).await?,
    };
    println!(
        "ino={} kind={} mode={:o} nlink={} size={}",
        i.ino,
        i.kind.as_str(),
        i.mode,
        i.nlink,
        i.size
    );

    Ok(())
}

pub async fn info(ws: &Workspace, path: String, no_probe: bool) -> Result<()> {
    let info = ws.file_layout(&path, !no_probe).await?;
    print_info(&path, &info);

    Ok(())
}

pub async fn rm(ws: &Workspace, path: String, actor: Option<i64>) -> Result<()> {
    match resolve_actor(actor)? {
        Some(actor) => {
            let ctx = cli_ctx(ws, actor).await?;
            // `remove_or_propose`, not `remove`: a propose-only actor's delete
            // is queued for review rather than refused, which is how `write`
            // already behaves. Refusing would make the two inconsistent in the
            // opposite direction.
            match ws.remove_or_propose(ctx, &path, None, None).await? {
                origofs_sdk::WriteOutcome::Wrote => {}
                origofs_sdk::WriteOutcome::Proposed(id) => {
                    println!(
                        "actor {actor} is propose-only: queued suggestion #{id} to delete {path} (pending review)"
                    );
                }
            }
        }
        None => {
            ws.ensure_attributed("rm").await?;
            ws.remove(&path).await?;
        }
    };
    Ok(())
}

pub async fn mv(ws: &Workspace, from: String, to: String, actor: Option<i64>) -> Result<()> {
    match resolve_actor(actor)? {
        Some(actor) => {
            let ctx = cli_ctx(ws, actor).await?;
            ws.rename_as(ctx, &from, &to).await?;
        }
        None => {
            ws.ensure_attributed("mv").await?;
            ws.rename(&from, &to).await?;
        }
    };
    Ok(())
}

pub async fn du(ws: &Workspace, path: String, actor: Option<i64>) -> Result<()> {
    // Through `stat_as` first, so a subtree the actor may not read is
    // refused rather than measured — a byte count is a fact about a
    // subtree, and `du` would otherwise report on one `ls` hides.
    if let Some(ctx) = read_ctx(actor)? {
        ws.ensure_may_read_at(ctx, "measure", &path).await?;
    }
    let u = if path == "/" {
        ws.usage().await?
    } else {
        ws.du(&path).await?
    };
    println!("{}\t{} inodes\t{} bytes", path, u.inodes, u.bytes);

    Ok(())
}

pub async fn quota(ws: &Workspace, bytes: Option<String>, inodes: Option<String>) -> Result<()> {
    let current = ws.quota().await?;
    if bytes.is_none() && inodes.is_none() {
        let u = ws.usage().await?;
        println!(
            "bytes:  {} / {}",
            u.bytes,
            current
                .bytes
                .map(|b| b.to_string())
                .unwrap_or_else(|| "unlimited".into())
        );
        println!(
            "inodes: {} / {}",
            u.inodes,
            current
                .inodes
                .map(|i| i.to_string())
                .unwrap_or_else(|| "unlimited".into())
        );
    } else {
        let next = origofs_sdk::Quota {
            bytes: match bytes.as_deref() {
                None => current.bytes,
                Some(v) => parse_limit(v)?,
            },
            inodes: match inodes.as_deref() {
                None => current.inodes,
                Some(v) => parse_limit(v)?,
            },
        };
        ws.set_quota(next).await?;
        println!(
            "quota set: bytes={} inodes={}",
            next.bytes
                .map(|b| b.to_string())
                .unwrap_or_else(|| "unlimited".into()),
            next.inodes
                .map(|i| i.to_string())
                .unwrap_or_else(|| "unlimited".into())
        );
    }

    Ok(())
}
