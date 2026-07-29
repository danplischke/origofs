//! origofs — a minimal CLI over an origofs workspace (M0).
//!
//! Enough to exercise the engine from a shell:
//!
//! ```text
//! origofs --workspace ./ws init
//! echo hello | origofs --workspace ./ws write /notes/a.txt
//! origofs --workspace ./ws ls /notes
//! origofs --workspace ./ws read /notes/a.txt
//! ```

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use origofs_sdk::{MergeOutcome, SuggestionStatus, Workspace, WriteCtx};
use std::io::{Read, Write};
use std::path::PathBuf;

mod config;

#[derive(Parser)]
#[command(
    name = "origofs",
    version,
    about = "agent-and-human filesystem (M0 skeleton)"
)]
struct Cli {
    /// Workspace directory; holds `meta.db` and `cas/`.
    #[arg(long, default_value = ".origofs")]
    workspace: PathBuf,

    /// Format for the library's tracing output (written to stderr). The level
    /// filter comes from `ORIGOFS_LOG` (or `RUST_LOG`), defaulting to `info`.
    #[arg(long, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,

    /// Backend configuration file (TOML). Without it, a local SQLite + local-CAS
    /// workspace under `--workspace`; with it, the metadata/content backends it
    /// names (Postgres, S3, GCS). See `deploy/config.example.toml`.
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

/// How the CLI renders the library's tracing output.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum LogFormat {
    /// Human-readable single-line records.
    #[default]
    Text,
    /// Structured JSON records (one object per line), for log pipelines.
    Json,
}

#[derive(Subcommand)]
enum Cmd {
    /// Initialize the workspace.
    Init,
    /// Create a directory and any missing parents.
    Mkdir { path: String },
    /// Write a file's contents from `--from <file>` or stdin.
    Write {
        path: String,
        /// Read data from this file instead of stdin.
        #[arg(long)]
        from: Option<PathBuf>,
        /// Attribute the write to this actor id (records blame + an edit-op).
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Print a file's contents to stdout.
    Read { path: String },
    /// List a directory.
    Ls {
        #[arg(default_value = "/")]
        path: String,
    },
    /// Show inode metadata for a path.
    Stat { path: String },
    /// Remove a file or empty directory.
    Rm { path: String },
    /// Move/rename a path.
    Mv { from: String, to: String },
    /// Snapshot the working tree into a commit.
    Commit {
        #[arg(short, long)]
        message: String,
        #[arg(long, default_value = "origofs")]
        author: String,
    },
    /// Show commit history (HEAD, first-parent).
    Log,
    /// Show working-tree changes relative to HEAD.
    Status,
    /// Compare two refs/commits: changed paths, or one file's line diff with
    /// `--path`. E.g. `origofs diff main feature` or `origofs diff main feature --path /x`.
    Diff {
        from: String,
        to: String,
        /// Show a unified line diff of just this path.
        #[arg(long)]
        path: Option<String>,
    },
    /// Propose an edit to a path for review (bytes from `--from`/stdin),
    /// attributed to `--actor`. `--delete` proposes removing the path instead.
    Suggest {
        path: String,
        #[arg(long)]
        actor: i64,
        #[arg(long)]
        session: Option<i64>,
        #[arg(long)]
        summary: Option<String>,
        #[arg(long)]
        from: Option<PathBuf>,
        #[arg(long)]
        delete: bool,
    },
    /// List suggestions (filter with `--status` and/or `--path`).
    Suggestions {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        path: Option<String>,
    },
    /// Show a suggestion's unified diff (base → proposed).
    SuggestionDiff { id: i64 },
    /// Accept a pending suggestion, attributed to `--actor` as the approver.
    Accept {
        id: i64,
        #[arg(long)]
        actor: i64,
        #[arg(long)]
        session: Option<i64>,
    },
    /// Reject a pending suggestion.
    Reject {
        id: i64,
        #[arg(long)]
        actor: i64,
        #[arg(long)]
        session: Option<i64>,
    },
    /// Create a branch at HEAD, or list branches when no name is given.
    Branch { name: Option<String> },
    /// Switch the working tree to a branch.
    Checkout { branch: String },
    /// Merge a branch into the current branch.
    Merge {
        branch: String,
        #[arg(long, default_value = "origofs")]
        author: String,
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Reconcile this (offline/solo) workspace with a remote one over a branch,
    /// merging any divergence with the ordinary three-way merge engine. Objects
    /// move both ways as needed and per-line blame travels with them; the remote
    /// branch only advances by compare-and-swap. Both working trees must be clean.
    /// E.g. `origofs resync --remote-config team.toml` or `origofs resync --remote ../shared`.
    Resync {
        /// The remote workspace directory (holds `meta.db` and `cas/`, like
        /// `--workspace`). Also roots any path defaulted by `--remote-config`.
        #[arg(long, value_name = "DIR")]
        remote: Option<PathBuf>,
        /// Backend configuration file (TOML) for the remote workspace — the same
        /// format as `--config`, for a Postgres/S3/GCS remote.
        #[arg(long, value_name = "FILE")]
        remote_config: Option<PathBuf>,
        /// Branch to reconcile (defaults to the local current branch).
        #[arg(long)]
        branch: Option<String>,
        #[arg(long, default_value = "origofs")]
        author: String,
        #[arg(short, long)]
        message: Option<String>,
    },
    /// List unresolved merge conflicts.
    Conflicts,
    /// Acquire an exclusive lock on a path.
    Lock {
        path: String,
        #[arg(long, default_value = "cli")]
        owner: String,
    },
    /// Release a lock on a path.
    Unlock {
        path: String,
        #[arg(long, default_value = "cli")]
        owner: String,
    },
    /// List held locks.
    Locks,
    /// Register an actor (human by default; `--agent` for an agent).
    Actor {
        name: String,
        #[arg(long)]
        agent: bool,
        #[arg(long, default_value = "unknown")]
        model: String,
        /// The human actor id that launched this agent.
        #[arg(long)]
        controller: Option<i64>,
    },
    /// Set an actor's write policy: `direct` (writes land immediately) or `propose`
    /// (writes are routed through the suggestion queue for review). Actor-agnostic.
    WritePolicy {
        /// The actor id to configure.
        actor: i64,
        /// `direct` or `propose`.
        policy: String,
    },
    /// Show per-line authorship (blame) for a file.
    Blame { path: String },
    /// Run a command over a copy-on-write view of the workspace, then import what
    /// it changed as an attributed commit (or `--discard`). By default this is an
    /// edit-capture view, not a security sandbox — the command runs with your
    /// privileges and can reach the host; run only code you trust, or pass
    /// `--isolate` to hide the host filesystem behind bubblewrap (a real boundary
    /// for untrusted code). Usage: `origofs sandbox --actor 1 -- <cmd> [args...]`
    Sandbox {
        /// Attribute imported changes to this actor id.
        #[arg(long)]
        actor: Option<i64>,
        /// Discard the sandbox's changes instead of importing them.
        #[arg(long)]
        discard: bool,
        /// Isolate the command under bubblewrap so the host filesystem (incl. this
        /// workspace's meta.db/cas and your credentials) is hidden — a real
        /// boundary for untrusted code. Requires `bwrap` on PATH.
        #[arg(long)]
        isolate: bool,
        /// The command to run (after `--`).
        #[arg(last = true, required = true)]
        cmd: Vec<String>,
    },
    /// Run an agent in a live native overlay mount over the workspace: it works
    /// in a fast unprivileged kernel overlay while its changes stream into origofs
    /// (attributed) as it goes — not just on exit. By default an edit-capture
    /// view, not a security sandbox — the agent runs with your privileges and can
    /// reach the host; run only agents you trust, or pass `--isolate` for a real
    /// bubblewrap boundary. Usage: `origofs overlay --actor 1 -- <cmd>`
    Overlay {
        /// Attribute the agent's changes to this actor id.
        #[arg(long)]
        actor: Option<i64>,
        /// How often (ms) to sync the agent's changes into origofs while it runs.
        #[arg(long, default_value_t = 500)]
        sync_ms: u64,
        /// Isolate the agent under bubblewrap so the host filesystem is hidden — a
        /// real boundary for untrusted code. Requires `bwrap` on PATH.
        #[arg(long)]
        isolate: bool,
        /// The agent command to run (after `--`).
        #[arg(last = true, required = true)]
        cmd: Vec<String>,
    },
    /// Mount the workspace as a POSIX filesystem via FUSE (blocks until
    /// unmounted; needs root + /dev/fuse).
    Mount { mountpoint: PathBuf },
    /// Serve the workspace to agents over MCP (JSON-RPC on stdio). Every write
    /// is attributed to the given agent.
    Mcp {
        #[arg(long, default_value = "mcp-agent")]
        agent_name: String,
        #[arg(long, default_value = "unknown")]
        model: String,
    },
    /// Interoperate with the real `git` (export/import genuine git objects).
    Git {
        #[command(subcommand)]
        cmd: GitCmd,
    },
    /// Reclaim content unreachable from any branch or the working tree.
    Gc,
    /// Recover a workspace from the content store after a metadata-DB loss.
    /// Scans the object graph (commits, trees, chunks, the ref mirror) and, with
    /// `--rebuild`, restores refs + the working tree onto a fresh DB. Read-only
    /// without `--rebuild`. Does not recover blame/attribution (DB-only).
    Fsck {
        /// Rebuild the metadata DB (refs + working tree) from content, instead of
        /// only reporting what would be recovered.
        #[arg(long)]
        rebuild: bool,
    },
    /// Tail the change feed (who changed what). `--follow` polls for new events.
    Watch {
        /// Only show events after this seq cursor.
        #[arg(long, default_value_t = 0)]
        since: i64,
        /// Keep polling for new events instead of exiting.
        #[arg(long)]
        follow: bool,
    },
    /// Show the sessions currently active in the workspace.
    Presence {
        /// Consider sessions seen within this many seconds active.
        #[arg(long, default_value_t = 60)]
        window: i64,
    },
    /// Serve the workspace over HTTP/JSON (blocks until stopped).
    Serve {
        /// Address to bind, e.g. `127.0.0.1:8080`.
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: std::net::SocketAddr,
        /// Bearer-token → actor mapping `TOKEN=ACTOR_ID[:SESSION_ID]` (repeatable).
        /// Required to bind a non-loopback address; on loopback with none given,
        /// all writes are attributed to an auto-created local actor (dev only).
        #[arg(long = "auth-token", value_name = "TOKEN=ACTOR[:SESSION]")]
        auth_tokens: Vec<String>,
        /// Install the Prometheus recorder and expose `GET /metrics` (also
        /// `ORIGOFS_METRICS=1`). Off by default: without it nothing is exported and
        /// that route answers `503 metrics not enabled`. Like `/readyz`, the
        /// endpoint is unauthenticated.
        #[arg(long)]
        metrics: bool,
    },
    /// Serve the workspace over NFSv3 (blocks; mount with `-o vers=3,tcp,port=…`).
    Nfs {
        /// Address to bind, e.g. `127.0.0.1:11111`.
        #[arg(long, default_value = "127.0.0.1:11111")]
        addr: String,
    },
}

#[derive(Subcommand)]
enum GitCmd {
    /// Export a branch as a real git repository the `git` CLI can read.
    Export {
        /// Directory to write the git repository into.
        dir: PathBuf,
        /// Branch to export (defaults to the current branch).
        #[arg(long)]
        branch: Option<String>,
        /// Object id format: `sha1` (GitHub-compatible) or `sha256`.
        #[arg(long, default_value = "sha1")]
        format: String,
        /// Write files at least this many bytes as git-LFS pointers.
        #[arg(long)]
        lfs_threshold: Option<u64>,
    },
    /// Import a real git repository's history into the workspace.
    Import {
        /// Directory of the git repository to import.
        dir: PathBuf,
        #[arg(long, default_value = "main")]
        branch: String,
    },
}

/// Install the tracing subscriber for the CLI. The level filter is read from
/// `ORIGOFS_LOG` (then `RUST_LOG`, then `info`); records go to **stderr** so they
/// never corrupt a data channel on stdout — notably the `origofs mcp` JSON-RPC
/// transport. origofs-core/-sdk only *emit* spans and events; this is the one
/// place they are shown (a Rust embedder installs its own subscriber instead).
fn init_tracing(format: LogFormat) {
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{EnvFilter, fmt};
    // Default to `info`, but quiet the Postgres driver — it forwards benign server
    // NOTICEs (e.g. "relation already exists" from idempotent migrations) at info.
    let filter = EnvFilter::try_from_env("ORIGOFS_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info,tokio_postgres=warn"));
    // Log each instrumented operation once when its span closes, with the elapsed
    // time — so the spans double as per-operation latency records.
    let builder = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_span_events(FmtSpan::CLOSE);
    match format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }
}

/// Latency buckets for the `_seconds` histograms, in seconds: sub-millisecond
/// metadata operations through multi-second object-store round trips. Picking
/// buckets is the *binary's* call — the library only records raw observations.
const METRICS_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Install the Prometheus recorder — the metrics counterpart to [`init_tracing`],
/// and for the same reason: origofs-core/-sdk only *record* measurements, they
/// install no exporter and start no server, so a Rust embedder pays nothing and
/// wires up their own. This is the one place the `origofs` binary opts in.
///
/// It installs a recorder, registers the `# HELP`/`# TYPE` metadata, and hands the
/// exposition renderer to the HTTP surface, which serves it at **`GET /metrics`**
/// on the same address as the API (no second listener, so the endpoint inherits
/// the API's bind address and middleware). Until this runs, `/metrics` answers
/// `503 metrics not enabled`.
///
/// Off unless `origofs serve --metrics` (or `ORIGOFS_METRICS=1`) asks for it, so a
/// plain `origofs read`/`write` allocates no metrics registry.
fn init_metrics() -> Result<()> {
    use metrics_exporter_prometheus::PrometheusBuilder;
    let handle = PrometheusBuilder::new()
        .set_buckets(METRICS_BUCKETS)
        .map_err(|e| anyhow::anyhow!("configuring metrics histogram buckets: {e}"))?
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("installing the Prometheus recorder: {e}"))?;
    origofs_sdk::api::describe_metrics();
    if !origofs_sdk::api::set_metrics_renderer(move || handle.render()) {
        anyhow::bail!("a metrics renderer is already installed in this process");
    }
    Ok(())
}

/// Whether an environment variable is set to a truthy value. Used for
/// `ORIGOFS_METRICS`, the env-var twin of `serve --metrics`.
fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Open the workspace the CLI operates on. With `--config` this selects the
/// Postgres/S3/GCS backends the file names; without it, the default local
/// SQLite/local-CAS workspace under `--workspace` (honoring `ORIGOFS_ENCRYPTION_KEY`
/// for encryption at rest), exactly as before.
async fn open_workspace(cli: &Cli) -> Result<Workspace> {
    let cfg = match &cli.config {
        Some(path) => config::Config::load(path)?,
        None => config::Config::default(),
    };
    cfg.open(&cli.workspace).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log_format);
    std::fs::create_dir_all(&cli.workspace)?;
    let ws = open_workspace(&cli).await?;

    match cli.cmd {
        Cmd::Init => {
            println!(
                "initialized origofs workspace at {}",
                cli.workspace.display()
            );
        }
        Cmd::Mkdir { path } => {
            ws.mkdir_p(&path).await?;
        }
        Cmd::Write { path, from, actor } => {
            // Convenience: ensure the parent directory exists before writing.
            if let Some(parent) = path
                .rsplit_once('/')
                .map(|(p, _)| p)
                .filter(|p| !p.is_empty())
            {
                ws.mkdir_p(parent).await?;
            }
            match (from, actor) {
                // Unattributed streaming from a file (large files stay off-heap).
                (Some(p), None) => {
                    let file = std::fs::File::open(p)?;
                    ws.write_reader(&path, file).await?;
                }
                // Attributed write: read into memory, record blame + an edit-op.
                (from, Some(actor)) => {
                    let data = match from {
                        Some(p) => std::fs::read(p)?,
                        None => {
                            let mut buf = Vec::new();
                            std::io::stdin().read_to_end(&mut buf)?;
                            buf
                        }
                    };
                    let session = ws.create_session(actor, Some("cli")).await?;
                    ws.write_as(WriteCtx::session(actor, session), &path, &data)
                        .await?;
                }
                (None, None) => {
                    let mut buf = Vec::new();
                    std::io::stdin().read_to_end(&mut buf)?;
                    ws.write(&path, &buf).await?;
                }
            }
        }
        Cmd::Read { path } => {
            let bytes = ws.read(&path).await?;
            std::io::stdout().write_all(&bytes)?;
        }
        Cmd::Ls { path } => {
            for e in ws.ls(&path).await? {
                println!("{}\t{}", e.kind.as_str(), e.name);
            }
        }
        Cmd::Stat { path } => {
            let i = ws.stat(&path).await?;
            println!(
                "ino={} kind={} mode={:o} nlink={} size={}",
                i.ino,
                i.kind.as_str(),
                i.mode,
                i.nlink,
                i.size
            );
        }
        Cmd::Rm { path } => {
            ws.remove(&path).await?;
        }
        Cmd::Mv { from, to } => {
            ws.rename(&from, &to).await?;
        }
        Cmd::Commit { message, author } => {
            let hash = ws.commit(&author, &message).await?;
            let branch = ws.current_branch().await?.unwrap_or_else(|| "?".into());
            println!("[{branch} {}] {message}", &hash.to_hex()[..12]);
        }
        Cmd::Log => {
            for ci in ws.log().await? {
                println!(
                    "{} {}  {}",
                    &ci.hash.to_hex()[..12],
                    ci.commit.author,
                    ci.commit.message
                );
            }
        }
        Cmd::Status => {
            let changes = ws.status().await?;
            if changes.is_empty() {
                println!("clean (working tree matches HEAD)");
            }
            for d in changes {
                println!("{} {}", d.status.sigil(), d.path);
            }
        }
        Cmd::Diff { from, to, path } => match path {
            Some(p) => {
                let patch = ws.diff_file(&from, &to, &p).await?;
                if patch.is_empty() {
                    println!("{p}: unchanged between {from} and {to}");
                } else {
                    print!("{patch}");
                }
            }
            None => {
                let changes = ws.diff(&from, &to).await?;
                if changes.is_empty() {
                    println!("no differences between {from} and {to}");
                }
                for d in changes {
                    println!("{} {}", d.status.sigil(), d.path);
                }
            }
        },
        Cmd::Suggest {
            path,
            actor,
            session,
            summary,
            from,
            delete,
        } => {
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
        }
        Cmd::Suggestions { status, path } => {
            let st = match status.as_deref() {
                Some(s) => Some(
                    SuggestionStatus::parse(s)
                        .ok_or_else(|| anyhow::anyhow!("unknown status {s:?}"))?,
                ),
                None => None,
            };
            let list = ws.list_suggestions(st, path.as_deref()).await?;
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
        }
        Cmd::SuggestionDiff { id } => {
            let patch = ws.suggestion_diff(id).await?;
            if patch.is_empty() {
                println!("(no change)");
            } else {
                print!("{patch}");
            }
        }
        Cmd::Accept { id, actor, session } => {
            let ctx = match session {
                Some(s) => WriteCtx::session(actor, s),
                None => WriteCtx::actor(actor),
            };
            ws.accept_suggestion(id, ctx).await?;
            println!("accepted suggestion #{id}");
        }
        Cmd::Reject { id, actor, session } => {
            let ctx = match session {
                Some(s) => WriteCtx::session(actor, s),
                None => WriteCtx::actor(actor),
            };
            ws.reject_suggestion(id, ctx).await?;
            println!("rejected suggestion #{id}");
        }
        Cmd::Branch { name } => match name {
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
        },
        Cmd::Checkout { branch } => {
            ws.checkout(&branch).await?;
            println!("switched to branch {branch}");
        }
        Cmd::Merge {
            branch,
            author,
            message,
        } => {
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
        }
        Cmd::Resync {
            remote,
            remote_config,
            branch,
            author,
            message,
        } => {
            if remote.is_none() && remote_config.is_none() {
                anyhow::bail!(
                    "resync needs a remote: pass --remote <DIR> and/or --remote-config <FILE>"
                );
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
        }
        Cmd::Conflicts => {
            for (path, kind) in ws.conflicts().await? {
                println!("{kind}\t{path}");
            }
        }
        Cmd::Lock { path, owner } => {
            if ws.lock(&path, &owner).await? {
                println!("locked {path}");
            } else {
                println!("already locked: {path}");
            }
        }
        Cmd::Unlock { path, owner } => {
            if ws.unlock(&path, &owner).await? {
                println!("unlocked {path}");
            } else {
                println!("not your lock: {path}");
            }
        }
        Cmd::Locks => {
            for (path, owner, _at) in ws.locks().await? {
                println!("{owner}\t{path}");
            }
        }
        Cmd::Actor {
            name,
            agent,
            model,
            controller,
        } => {
            let id = if agent {
                ws.create_agent(&name, &model, controller).await?
            } else {
                ws.create_human(&name, None).await?
            };
            println!("{id}");
        }
        Cmd::WritePolicy { actor, policy } => {
            let p = origofs_sdk::WritePolicy::parse(&policy).ok_or_else(|| {
                origofs_sdk::OrigoFSError::InvalidArgument(format!(
                    "unknown write policy {policy:?} (expected `direct` or `propose`)"
                ))
            })?;
            ws.set_write_policy(actor, p).await?;
            println!("actor #{actor} write policy set to {}", p.as_str());
        }
        Cmd::Blame { path } => {
            for r in ws.blame(&path).await? {
                let who = format!("{}:{}", r.actor.kind.as_str(), r.actor.display_name);
                if r.line_start == r.line_end {
                    println!("{:>4}       {who}", r.line_start);
                } else {
                    println!("{:>4}-{:<4}  {who}", r.line_start, r.line_end);
                }
            }
        }
        Cmd::Sandbox {
            actor,
            discard,
            isolate,
            cmd,
        } => {
            if isolate {
                if !origofs_sdk::sandbox::bwrap_available() {
                    anyhow::bail!(
                        "--isolate needs bubblewrap (`bwrap`) on PATH (>= 0.8.0, for overlay support)"
                    );
                }
            } else if !origofs_sdk::sandbox::overlay_supported() {
                anyhow::bail!(
                    "unprivileged overlayfs is unavailable here (needs user-namespace overlay support)"
                );
            }
            let tmp = cli
                .workspace
                .join(format!("sandbox-{}", std::process::id()));
            let opts = origofs_sdk::sandbox::RunOpts {
                actor,
                discard,
                work_root: tmp.clone(),
                isolate,
            };
            let outcome = origofs_sdk::sandbox::run(&ws, opts, &cmd).await?;
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
        Cmd::Overlay {
            actor,
            sync_ms,
            isolate,
            cmd,
        } => {
            if isolate {
                if !origofs_sdk::sandbox::bwrap_available() {
                    anyhow::bail!(
                        "--isolate needs bubblewrap (`bwrap`) on PATH (>= 0.8.0, for overlay support)"
                    );
                }
            } else if !origofs_sdk::sandbox::overlay_supported() {
                anyhow::bail!(
                    "unprivileged overlayfs is unavailable here (needs user-namespace overlay support)"
                );
            }
            let tmp = cli
                .workspace
                .join(format!("overlay-{}", std::process::id()));
            let opts = origofs_sdk::sandbox::LiveOpts {
                actor,
                work_root: tmp.clone(),
                sync_interval: std::time::Duration::from_millis(sync_ms),
                isolate,
            };
            let outcome = origofs_sdk::sandbox::run_live(&ws, opts, &cmd).await?;
            let _ = std::fs::remove_dir_all(&tmp);
            println!(
                "agent exited {}; streamed {} change(s) into origofs",
                outcome.exit_code, outcome.files_changed
            );
            std::process::exit(outcome.exit_code);
        }
        Cmd::Mount { mountpoint } => {
            if !origofs_sdk::fuse::mountable() {
                anyhow::bail!("FUSE mount unavailable here (needs root + /dev/fuse)");
            }
            std::fs::create_dir_all(&mountpoint)?;
            println!(
                "mounting origofs at {} (unmount with `umount` to stop)",
                mountpoint.display()
            );
            // The mount drives its own runtime, so run it off the async main thread.
            let handle = std::thread::spawn(move || origofs_sdk::fuse::mount(ws, &mountpoint));
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("mount thread panicked"))??;
        }
        Cmd::Mcp { agent_name, model } => {
            let server = origofs_sdk::mcp::McpServer::create(ws, &agent_name, &model).await?;
            server.serve_stdio().await?;
        }
        Cmd::Git { cmd } => match cmd {
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
                let out = origofs_sdk::git::export_git(&ws, &dir, &opts).await?;
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
                let head = origofs_sdk::git::import_git(&ws, &dir, &branch).await?;
                println!(
                    "imported branch {branch} at {} from {}",
                    &head.to_hex()[..12],
                    dir.display()
                );
            }
        },
        Cmd::Gc => {
            let stats = ws.gc().await?;
            println!(
                "gc: kept {} object(s), deleted {} ({} bytes freed)",
                stats.reachable, stats.deleted, stats.bytes_freed
            );
        }
        Cmd::Fsck { rebuild } => {
            let report = if rebuild {
                ws.rebuild().await?
            } else {
                ws.scan().await?
            };
            let corrupt = if report.corrupt > 0 {
                format!(", {} corrupt", report.corrupt)
            } else {
                String::new()
            };
            println!(
                "fsck: scanned {} object(s){corrupt}, found {} commit(s)",
                report.objects_scanned, report.commits_found
            );
            if report.branches.is_empty() {
                println!("  no commits to recover (empty or non-versioned workspace)");
            } else {
                let src = if report.used_mirror {
                    "ref mirror"
                } else {
                    "inferred heads"
                };
                println!("  {} branch(es) via {src}:", report.branches.len());
                for (name, hex) in &report.branches {
                    let tip = &hex[..hex.len().min(12)];
                    let head = if report.checked_out.as_deref() == Some(name) {
                        "  (HEAD)"
                    } else {
                        ""
                    };
                    println!("    {name}\t{tip}{head}");
                }
            }
            if rebuild {
                if let Some(branch) = &report.checked_out {
                    println!(
                        "  rebuilt working tree @ {branch}: {} dir(s), {} file(s), {} symlink(s)",
                        report.dirs, report.files, report.symlinks
                    );
                }
                println!("  note: blame/attribution is not recoverable (DB-only)");
            } else {
                println!("  (dry run — pass --rebuild to restore the metadata DB)");
            }
        }
        Cmd::Watch { since, follow } => {
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
        }
        Cmd::Presence { window } => {
            for p in ws.presence(window).await? {
                let path = p.path.unwrap_or_else(|| "-".to_string());
                println!(
                    "{}\t{}\t{path}\t(seen {})",
                    p.kind.as_str(),
                    p.display_name,
                    p.last_seen
                );
            }
        }
        Cmd::Serve {
            addr,
            auth_tokens,
            metrics,
        } => {
            let auth = build_api_auth(&ws, &addr, &auth_tokens).await?;
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
            println!("serving origofs at http://{addr} (Ctrl-C to stop)");
            origofs_sdk::api::serve(std::sync::Arc::new(ws), addr, auth).await?;
        }
        Cmd::Nfs { addr } => {
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
                "serving origofs over NFSv3 at {addr}\n  mount with: mount -t nfs -o vers=3,tcp,port=<port>,mountport=<port>,nolock <host>:/ /mnt"
            );
            origofs_sdk::nfs::serve(ws, &addr).await?;
        }
    }
    Ok(())
}

/// Build the HTTP API authenticator from `--auth-token` specs. origofs never trusts a
/// client-named actor, so the server must resolve identity itself. With no specs:
/// refuse to expose a non-loopback address, and on loopback attribute all writes
/// to an auto-created local actor (dev convenience only).
async fn build_api_auth(
    ws: &Workspace,
    addr: &std::net::SocketAddr,
    specs: &[String],
) -> Result<std::sync::Arc<dyn origofs_sdk::api::Authenticator>> {
    if specs.is_empty() {
        if !addr.ip().is_loopback() {
            anyhow::bail!(
                "refusing to expose an unauthenticated API on {addr}: pass --auth-token TOKEN=ACTOR_ID (repeatable), or bind a loopback address for local dev"
            );
        }
        let actor = ws.find_or_create_human("local", "local").await?;
        eprintln!(
            "warning: no --auth-token given; attributing all writes to local actor {actor} (dev only, loopback bind)"
        );
        return Ok(std::sync::Arc::new(origofs_sdk::api::LocalDevAuth(
            origofs_sdk::api::Principal {
                actor,
                session: None,
            },
        )));
    }
    let mut bearer = origofs_sdk::api::BearerAuth::new();
    for spec in specs {
        let (token, who) = spec.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("bad --auth-token {spec:?}; expected TOKEN=ACTOR_ID[:SESSION_ID]")
        })?;
        let (actor, session) = match who.split_once(':') {
            Some((a, s)) => (a.parse()?, Some(s.parse()?)),
            None => (who.parse()?, None),
        };
        bearer = bearer.with_token(token.to_string(), actor, session);
    }
    Ok(std::sync::Arc::new(bearer))
}
