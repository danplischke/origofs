//! Who wrote what, and the propose-and-review queue that governs who may.

use crate::*;

pub async fn suggest(
    ws: &Workspace,
    path: String,
    actor: i64,
    session: Option<i64>,
    summary: Option<String>,
    from: Option<PathBuf>,
    delete: bool,
) -> Result<()> {
    let ctx = match session {
        Some(s) => WriteCtx::session(actor, s),
        None => WriteCtx::actor(actor),
    };
    let id = if delete {
        ws.suggest_delete(ctx, &path, summary.as_deref()).await?
    } else {
        let data = match from {
            Some(p) => std::fs::read(p)?,
            None => {
                let mut buf = Vec::new();
                std::io::stdin().read_to_end(&mut buf)?;
                buf
            }
        };
        ws.suggest(ctx, &path, &data, summary.as_deref()).await?
    };
    println!("suggestion #{id} created (pending review)");

    Ok(())
}

pub async fn suggestions(
    ws: &Workspace,
    status: Option<String>,
    path: Option<String>,
    actor: Option<i64>,
) -> Result<()> {
    let st = match status.as_deref() {
        Some(s) => Some(
            SuggestionStatus::parse(s).ok_or_else(|| anyhow::anyhow!("unknown status {s:?}"))?,
        ),
        None => None,
    };
    let list = match read_ctx(actor)? {
        Some(ctx) => ws.list_suggestions_as(ctx, st, path.as_deref()).await?,
        None => ws.list_suggestions(st, path.as_deref()).await?,
    };
    if list.is_empty() {
        println!("no suggestions");
    }
    for s in list {
        // The kind matters to a reviewer: a `crdt` proposal merges into a
        // live document (and is never stale), a `bytes` one replaces the
        // file and can be superseded when the base moves.
        println!(
            "#{:<4} {:<10} {:<5} actor={} {}{}",
            s.id,
            s.status.as_str(),
            s.kind.as_str(),
            s.actor_id,
            s.path,
            s.summary.map(|m| format!("  — {m}")).unwrap_or_default(),
        );
    }

    Ok(())
}

pub async fn suggestion_diff(ws: &Workspace, id: i64, actor: Option<i64>) -> Result<()> {
    let patch = match read_ctx(actor)? {
        Some(ctx) => ws.suggestion_diff_as(ctx, id).await?,
        None => ws.suggestion_diff(id).await?,
    };
    if patch.is_empty() {
        println!("(no change)");
    } else {
        print!("{patch}");
    }

    Ok(())
}

pub async fn accept(ws: &Workspace, id: i64, actor: i64, session: Option<i64>) -> Result<()> {
    let ctx = match session {
        Some(s) => WriteCtx::session(actor, s),
        None => WriteCtx::actor(actor),
    };
    ws.accept_suggestion(id, ctx).await?;
    println!("accepted suggestion #{id}");

    Ok(())
}

pub async fn reject(ws: &Workspace, id: i64, actor: i64, session: Option<i64>) -> Result<()> {
    let ctx = match session {
        Some(s) => WriteCtx::session(actor, s),
        None => WriteCtx::actor(actor),
    };
    ws.reject_suggestion(id, ctx).await?;
    println!("rejected suggestion #{id}");

    Ok(())
}

pub async fn actor(
    ws: &Workspace,
    name: String,
    agent: bool,
    model: String,
    controller: Option<i64>,
) -> Result<()> {
    let id = if agent {
        ws.create_agent(&name, &model, controller).await?
    } else {
        ws.create_human(&name, None).await?
    };
    println!("{id}");

    Ok(())
}

pub async fn write_policy(ws: &Workspace, actor: i64, policy: String) -> Result<()> {
    let p = origofs_sdk::WritePolicy::parse(&policy).ok_or_else(|| {
        origofs_sdk::OrigoFSError::InvalidArgument(format!(
            "unknown write policy {policy:?} (expected `direct` or `propose`)"
        ))
    })?;
    ws.set_write_policy(actor, p).await?;
    println!("actor #{actor} write policy set to {}", p.as_str());

    Ok(())
}

pub async fn revert_session(
    ws: &Workspace,
    actor: i64,
    session: i64,
    by: Option<i64>,
    path_prefix: Option<String>,
) -> Result<()> {
    // A revert is performed *on* someone else's work, so the target comes
    // from `--actor` while `--by` is the reviewer doing it. When `--by` is
    // given, the reviewer must hold write permission over what it is
    // reverting — the named subtree, or the whole workspace when no prefix
    // bounds it — so a propose-only or ACL-restricted actor cannot revert
    // anyone.
    let changed = match by {
        Some(by) => {
            let s = ws.create_session(by, Some("cli")).await?;
            ws.revert_session_as(
                WriteCtx::session(by, s),
                actor,
                session,
                path_prefix.as_deref(),
            )
            .await?
        }
        None => {
            ws.revert_session(actor, session, path_prefix.as_deref())
                .await?
        }
    };
    println!(
        "reverted actor {actor} session {session}: {} file(s) changed",
        changed.len()
    );
    for p in &changed {
        println!("  {p}");
    }

    Ok(())
}

pub async fn blame(ws: &Workspace, path: String, actor: Option<i64>) -> Result<()> {
    let ranges = match read_ctx(actor)? {
        Some(ctx) => ws.blame_as(ctx, &path).await?,
        None => ws.blame(&path).await?,
    };
    for r in ranges {
        let who = format!("{}:{}", r.actor.kind.as_str(), r.actor.display_name);
        if r.line_start == r.line_end {
            println!("{:>4}       {who}", r.line_start);
        } else {
            println!("{:>4}-{:<4}  {who}", r.line_start, r.line_end);
        }
    }

    Ok(())
}

pub async fn acl(ws: &Workspace, cmd: AclCmd) -> Result<()> {
    run_acl(ws, cmd).await?;
    Ok(())
}

pub async fn require_attribution(ws: &Workspace, setting: Option<String>) -> Result<()> {
    match setting.as_deref() {
        None => {
            let on = ws.require_attribution().await?;
            println!("require-attribution is {}", if on { "on" } else { "off" });
        }
        Some(v) => {
            let on = match v {
                "on" | "true" | "1" => true,
                "off" | "false" | "0" => false,
                other => {
                    return Err(origofs_sdk::OrigoFSError::InvalidArgument(format!(
                        "unknown setting {other:?} (expected `on` or `off`)"
                    ))
                    .into());
                }
            };
            ws.set_require_attribution(on).await?;
            println!(
                "require-attribution set to {}",
                if on { "on" } else { "off" }
            );
        }
    };
    Ok(())
}
