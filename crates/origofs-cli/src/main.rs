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

use anyhow::{Context, Result};
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
    Mkdir {
        path: String,
        /// Attribute the mkdir to this actor id, and check its write policy.
        /// Falls back to `ORIGOFS_ACTOR`; see `origofs require-attribution`.
        #[arg(long)]
        actor: Option<i64>,
    },
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
    Read {
        path: String,
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// List a directory.
    Ls {
        #[arg(default_value = "/")]
        path: String,
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Show inode metadata for a path.
    Stat {
        path: String,
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Explain what a file costs to read: chunk count, chunk-size distribution,
    /// self-dedup, and whether the content store still holds the chunks.
    Info {
        path: String,
        /// Skip the store-presence probe, which costs one `has` (one HEAD against
        /// object storage) per distinct chunk. Everything else comes from the
        /// manifest, which a read would fetch anyway.
        #[arg(long)]
        no_probe: bool,
    },
    /// Measure this workspace's own backends end to end: write N files, read them
    /// back twice, report throughput and latency. The number Criterion cannot give
    /// you, because it depends on your bucket, your latency, and your settings.
    ///
    /// Writes and then deletes `bench-NNNN.bin` under `--dir`, and refuses to
    /// start if that directory already holds anything (see `--force`).
    Bench {
        /// Workspace directory to run in. Created if absent, removed afterwards.
        #[arg(long, default_value = "/.origofs-bench")]
        dir: String,
        /// How many files to write and read back.
        #[arg(long, default_value_t = 8)]
        files: usize,
        /// Bytes per file; accepts `K`/`M`/`G` suffixes (`64M`, `1G`).
        #[arg(long, default_value = "8M", value_parser = parse_size)]
        size: u64,
        /// Pin the body seed to reproduce a run. Defaults to a fresh value each
        /// run, so that a second run writes genuinely new bytes instead of
        /// deduplicating against the first and reporting that as write throughput.
        #[arg(long)]
        seed: Option<u64>,
        /// Leave the sample files in place instead of deleting them.
        #[arg(long)]
        keep: bool,
        /// Run in `--dir` even though it already holds entries. The benchmark
        /// still only writes and deletes its own `bench-NNNN.bin` names.
        #[arg(long)]
        force: bool,
    },
    /// Remove a file or empty directory.
    Rm {
        path: String,
        /// Attribute the delete to this actor id, and check its write policy — a
        /// propose-only actor's delete is queued for review rather than executed.
        /// Falls back to `ORIGOFS_ACTOR`; see `origofs require-attribution`.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Move/rename a path.
    Mv {
        from: String,
        to: String,
        /// Attribute the rename to this actor id, and check its write policy.
        /// Falls back to `ORIGOFS_ACTOR`; see `origofs require-attribution`.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Snapshot the working tree into a commit.
    Commit {
        #[arg(short, long)]
        message: String,
        /// The name recorded *in* the commit object. Free-form, and not an
        /// identity — use `--actor` for that.
        #[arg(long, default_value = "origofs")]
        author: String,
        /// Attribute the commit to this actor id, and check its write policy.
        /// Falls back to `ORIGOFS_ACTOR`; see `origofs require-attribution`.
        #[arg(long)]
        actor: Option<i64>,
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
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
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
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Show a suggestion's unified diff (base → proposed).
    SuggestionDiff {
        id: i64,
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
    },
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
    /// Recursive usage of a subtree (`du`), or of the whole workspace.
    ///
    /// Counts an inode with several names once, and sums *logical* size — what
    /// the files say they are, not what the deduplicated, chunked content store
    /// actually holds. A quota expressed in physical bytes would move under a
    /// user who changed nothing, because someone else's write can dedup against
    /// theirs; see `stats.rs`.
    Du {
        /// The subtree to measure. Defaults to the whole workspace.
        #[arg(default_value = "/")]
        path: String,
        /// Report as this actor, so `acl_enforce_reads` applies. Falls back to
        /// `ORIGOFS_ACTOR`.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Show the workspace's capacity limits and what it is using, or set them.
    ///
    /// No limit is the default and what every existing workspace has. `off`
    /// clears a limit.
    Quota {
        /// Byte limit: a size (`10G`, `500M`), `off`, or omit to leave unchanged.
        #[arg(long)]
        bytes: Option<String>,
        /// Inode limit: a count, `off`, or omit to leave unchanged.
        #[arg(long)]
        inodes: Option<String>,
    },
    /// Inspect, restore from, and configure the trash (issue #115).
    ///
    /// A committed file can be read back out of history; an **uncommitted** one
    /// could not be recovered at all, which matters more here than it would for
    /// an ordinary filesystem because the users are agents and `rm -rf` on a bad
    /// path is a routine failure mode. Retention is off by default — turning it
    /// on silently would change when space is reclaimed for every existing
    /// deployment — so `origofs trash retention 7d` is the first call.
    ///
    /// The engine and the SDK have had all of this since #115. Nothing exposed
    /// it: no subcommand, no route, no tool. A recovery path nobody can reach
    /// does not recover anything.
    Trash {
        #[command(subcommand)]
        cmd: TrashCmd,
    },
    /// Inspect and change the workspace's path ACLs (issues #123, #124).
    ///
    /// The ACLs are the one part of the engine no surface exposed: no HTTP route,
    /// no MCP tool, and until now no subcommand. `CLAUDE.md` calls that out —
    /// safety by absence of a route is not safety, and it also meant a workspace
    /// could not be *configured* without writing Rust or Python.
    ///
    /// Every mutating form takes `--actor` and goes through the gated `_as`
    /// variant, so granting is itself authorized: you need `WRITE` at the prefix,
    /// and you cannot hand out a bit you do not hold there. Omitting `--actor`
    /// uses the ungated provisioning form, which is correct for exactly one case
    /// — the first grant in a fresh workspace, which by construction precedes
    /// anyone holding rights in it — and says so when it does.
    Acl {
        #[command(subcommand)]
        cmd: AclCmd,
    },
    /// Require every mutating CLI command to name an actor (`on`), or allow
    /// unattributed ones (`off`); with no argument, print the current setting.
    ///
    /// This is an attribution-completeness switch, not access control: an actor id
    /// on a command line is self-asserted, and a local process holding the
    /// workspace directory can bypass the CLI entirely. It catches the script that
    /// forgot, which is the failure that actually happens.
    RequireAttribution {
        /// `on` or `off`. Omit to print the current setting.
        setting: Option<String>,
    },
    /// Turn cross-mount POSIX advisory locking on (`on`) or off (`off`); with no
    /// argument, print the current setting.
    ///
    /// **Off by default, and worth understanding before turning on.** A FUSE mount
    /// that does not answer `fcntl` locks still has working advisory locks — the
    /// kernel serves them locally, per mount — so this does not add locking to a
    /// workspace that had none. What it adds is coordination *between* mounts: two
    /// processes on two machines against one workspace. It also takes locking over
    /// from the kernel for that mount, which is why it is a deliberate switch.
    ///
    /// Mounts read it once, at mount time; remount to pick up a change.
    PosixLocks {
        /// `on` or `off`. Omit to print the current setting.
        setting: Option<String>,
        /// Instead of the setting, list the locks held on this path.
        #[arg(long)]
        path: Option<String>,
    },
    /// Show per-line authorship (blame) for a file.
    Blame {
        path: String,
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Undo exactly the lines one actor authored in one session, across every file
    /// that session touched, leaving other actors' edits intact. `--by` is the
    /// actor performing the revert (must be permitted to write).
    RevertSession {
        /// The actor whose work is being undone.
        #[arg(long)]
        actor: i64,
        /// The session to undo.
        #[arg(long)]
        session: i64,
        /// The actor performing the revert; checked against the write policy.
        #[arg(long)]
        by: Option<i64>,
        /// Bound the revert to this subtree, matched on directory boundaries
        /// (`/tenant-a` covers `/tenant-a/notes.txt`, never `/tenant-abc/...`).
        /// Omit to revert everywhere the session wrote.
        #[arg(long)]
        path_prefix: Option<String>,
    },
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
    Mount {
        mountpoint: PathBuf,
        /// Bind the mount to this actor, so every operation through it is checked
        /// against that actor's path grants (issue #141).
        ///
        /// Falls back to `ORIGOFS_ACTOR`. Unset, the mount is anonymous and the
        /// ACLs do not apply to it — the historical behaviour.
        ///
        /// This is the *mount's* identity, not the caller's: the kernel does not
        /// tell origofs which process issued a request, so one actor covers
        /// everything that goes through this mountpoint. It bounds what the mount
        /// can reach; it does not authenticate anyone.
        #[arg(long)]
        actor: Option<i64>,
    },
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
    /// Write an engine-independent dump of the whole metadata store to a file
    /// (or stdout with `-`), as JSON Lines.
    ///
    /// The metadata DB is the half the content store cannot rebuild: `fsck
    /// --rebuild` recovers committed files, dirs, symlinks and branches from the
    /// bucket alone, and none of the attribution. This is how that half moves —
    /// as a backup, or as the SQLite → Postgres migration path.
    ///
    /// **Metadata only.** File bytes stay in the content store, which this does
    /// not touch: a dump references content by hash. Restoring it against a
    /// different, empty content store gives you the names and the blame with
    /// nothing to read — point the restored workspace at the same store, or copy
    /// the store across too.
    Dump {
        /// Where to write. `-` writes to stdout.
        #[arg(default_value = "-")]
        out: String,
    },
    /// Restore a dump written by `origofs dump` into a **pristine** workspace
    /// (or stdin with `-`).
    ///
    /// Refuses to merge into a workspace that already holds data: inode numbers,
    /// actor ids and session ids are all local sequences, and reconciling two id
    /// spaces silently wrong would produce blame attributed to the wrong actor.
    ///
    /// **Metadata only** — see `dump`. The restored workspace needs the same
    /// content store, or reads fail with `content missing for hash ...`.
    Load {
        /// Where to read from. `-` reads stdin.
        #[arg(default_value = "-")]
        input: String,
    },
    /// Reclaim content unreachable from any branch or the working tree.
    Gc,
    /// Compact the content store, reclaiming space held by deleted objects.
    ///
    /// Meaningful for a **packed** store (the recommended object-storage layout,
    /// and what `deploy/config.example.toml` sets): deleting a chunk only clears
    /// its index entry, so the bytes stay in their pack until a repack rewrites
    /// the survivors. Run it after `gc`.
    Repack,
    /// Seal any buffered writes to durable storage.
    ///
    /// A no-op for stores that write through; a packed store seals its open pack.
    Flush,
    /// Apply any pending schema migrations and report the versions.
    ///
    /// Opening a workspace already migrates, so this is for running the upgrade
    /// deliberately — as a deploy step, before starting the new binaries.
    Migrate,
    /// Show the workspace's schema version and the newest this binary knows.
    SchemaVersion,
    /// Back up the **metadata** store — the half that cannot be rebuilt.
    ///
    /// `fsck --rebuild` recovers committed files, directories, symlinks, and
    /// branches from the content store alone, but blame, the audit log, the actor
    /// registry, and every uncommitted edit exist only in the database. SQLite is
    /// snapshotted with the online backup API, so writers need not stop.
    Backup {
        /// Where to write the snapshot. Must not already exist.
        dest: PathBuf,
    },
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
        /// Read as this actor, so `acl_enforce_reads` is applied to the answer.
        /// Falls back to `ORIGOFS_ACTOR`. Optional: unset, the read is
        /// unattributed and open, which is what an unenforced workspace does
        /// anyway.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Serve the workspace over HTTP/JSON (blocks until stopped).
    Serve {
        /// Address to bind, e.g. `127.0.0.1:8080`.
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: std::net::SocketAddr,
        /// Bearer-token → actor mapping `TOKEN=ACTOR_ID[:SESSION_ID]` (repeatable).
        /// Required to bind a non-loopback address; on loopback with none given,
        /// all writes are attributed to an auto-created local actor (dev only).
        ///
        /// Prefer `ORIGOFS_AUTH_TOKENS` (same syntax, newline- or
        /// comma-separated) for anything real: an argument is visible to every
        /// process on the host via `ps` and lands in shell history, which is the
        /// reason `ORIGOFS_ENCRYPTION_KEY` is env-only. Both may be given; they
        /// are merged.
        #[arg(long = "auth-token", value_name = "TOKEN=ACTOR[:SESSION]")]
        auth_tokens: Vec<String>,
        /// Require a credential for **reads** as well as writes.
        ///
        /// Off by default, which means file bytes, blame, the change feed, the
        /// audit log and the review queue are served to anyone who can reach the
        /// port. Writes are gated either way. Turn this on for any bind that is
        /// not loopback, or gate reads at your proxy.
        #[arg(long)]
        gate_reads: bool,
        /// Serve only this subtree of the workspace, e.g. `/tenant-a`.
        ///
        /// Every path a caller supplies is resolved *inside* it, so nothing
        /// outside is addressable at all, and workspace-wide listings are
        /// filtered to it. Scoping, not authorization — see `docs/MULTI_TENANCY.md`.
        #[arg(long, value_name = "PATH")]
        root: Option<String>,
        /// Allow cross-origin browser requests from this origin (repeatable).
        /// With none given, same-origin only.
        #[arg(long = "cors-origin", value_name = "ORIGIN")]
        cors_origins: Vec<String>,
        /// Maximum upload body size in bytes (default 64 MiB). `PUT` buffers the
        /// whole body, so this bounds per-request allocation.
        #[arg(long, value_name = "BYTES")]
        max_body_bytes: Option<usize>,
        /// Per-request timeout in seconds (default 60). `0` disables it.
        #[arg(long, value_name = "SECS")]
        request_timeout: Option<u64>,
        /// Maximum concurrent in-flight requests (default 512). `0` disables the
        /// cap.
        #[arg(long, value_name = "N")]
        max_concurrent_requests: Option<usize>,
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
        /// Bind the export to this actor, so every operation through it is checked
        /// against that actor's path grants (issue #141).
        ///
        /// Falls back to `ORIGOFS_ACTOR`. Read the warning above first: NFSv3
        /// authenticates nobody, so this bounds what the export can reach, not who
        /// reached it. It is the difference between "anyone on this port gets the
        /// whole workspace" and "…gets what this actor may touch" — worth having,
        /// and not a substitute for the network boundary.
        #[arg(long)]
        actor: Option<i64>,
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
/// The actor a mutating command should act as: `--actor` if given, else
#[derive(Subcommand)]
enum TrashCmd {
    /// List everything currently recoverable, newest deletion first.
    List,
    /// Put a trashed entry back at the path it was deleted from.
    Restore {
        /// The entry id, from `origofs trash list`.
        id: i64,
        /// Attribute the restore to this actor; falls back to `ORIGOFS_ACTOR`.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// Permanently drop one entry, or every entry with `--all`.
    Purge {
        /// The entry id. Omit with `--all`.
        id: Option<i64>,
        /// Drop everything instead of one entry.
        #[arg(long)]
        all: bool,
    },
    /// Show the retention window, or set it. `off` disables trash; a duration
    /// (`7d`, `48h`, `3600s`, or bare seconds) enables it.
    ///
    /// Disabling does not purge what is already retained — use `trash purge
    /// --all` for that, so "stop collecting" and "throw away what I have" stay
    /// separate decisions.
    Retention {
        /// `off`, or a duration. Omit to print the current setting.
        setting: Option<String>,
    },
}

#[derive(Subcommand)]
enum AclCmd {
    /// Show the grants in the workspace, or one actor's, plus the two switches.
    Show {
        /// Only this actor's grants.
        #[arg(long)]
        actor: Option<i64>,
    },
    /// What an actor may actually do at a path, after longest-prefix matching and
    /// the write-policy fallback. The question an ACL bug is usually asking.
    Check {
        /// The actor to resolve for.
        actor: i64,
        /// The path to resolve at.
        path: String,
    },
    /// Grant permissions to an actor under a path prefix.
    Grant {
        /// The actor receiving the grant.
        actor: i64,
        /// The path prefix it applies to, matched on directory boundaries.
        prefix: String,
        /// `read`, `write`, `propose`, `none`, or a combination: `read+write`.
        perms: String,
        /// Grant *as* this actor, which requires `WRITE` at the prefix and
        /// refuses to hand out a bit the granter does not hold there. Falls back
        /// to `ORIGOFS_ACTOR`. Omit only to provision a fresh workspace.
        #[arg(long)]
        by: Option<i64>,
    },
    /// Remove an actor's grant at exactly this prefix.
    Revoke {
        actor: i64,
        prefix: String,
        /// Revoke *as* this actor; same check as `grant`. Falls back to
        /// `ORIGOFS_ACTOR`.
        #[arg(long)]
        by: Option<i64>,
    },
    /// Deny an actor with no matching grant (`on`), or fall back to its write
    /// policy (`off`, the default). With no argument, print the current setting.
    DefaultDeny {
        setting: Option<String>,
        #[arg(long)]
        by: Option<i64>,
    },
    /// Check `READ` on the attributed reads (`on`), or leave reads open (`off`,
    /// the default). With no argument, print the current setting.
    ///
    /// Off by default because reads went unchecked for long enough that no
    /// existing workspace holds read grants; enforcing on upgrade would stop
    /// every actor at once. Turn it on once the grants are in place — `acl check`
    /// is how you confirm that before you do.
    EnforceReads {
        setting: Option<String>,
        #[arg(long)]
        by: Option<i64>,
    },
}

/// `ORIGOFS_ACTOR` (issue #128).
///
/// # Why an environment fallback rather than a required flag
///
/// #128 framed the choice as required-and-breaking versus optional-and-useless,
/// and steered at "a configured identity, as `serve` does for the API". This is
/// that: a shell session, a CI job, or an agent harness exports `ORIGOFS_ACTOR`
/// once and every subsequent command is attributed, without every existing script
/// breaking on the next release.
///
/// **It is not an identity check.** Whoever writes the command line also writes
/// the environment, so this asserts identity, it does not verify it. Verification
/// needs a server that resolves the caller — which is what `build_api_auth` does
/// for the HTTP surface, and why that one refuses to expose an unauthenticated
/// API off-loopback. A shell has nobody to do the resolving. What this buys is
/// that the attribution *gets recorded* — see `origofs require-attribution` for
/// making that mandatory rather than merely available.
fn resolve_actor(flag: Option<i64>) -> anyhow::Result<Option<i64>> {
    if let Some(a) = flag {
        return Ok(Some(a));
    }
    match std::env::var("ORIGOFS_ACTOR") {
        Ok(v) if !v.trim().is_empty() => v.trim().parse::<i64>().map(Some).map_err(|_| {
            anyhow::anyhow!("ORIGOFS_ACTOR is set to {v:?}, which is not an actor id")
        }),
        _ => Ok(None),
    }
}

/// Parse a quota limit: `off`/`none` for no limit, or a count with an optional
/// binary suffix (`10G`, `500M`, `2T`). Bare numbers are the unit the field
/// counts — bytes for `--bytes`, inodes for `--inodes`.
fn parse_limit(v: &str) -> anyhow::Result<Option<u64>> {
    let v = v.trim();
    if matches!(
        v.to_ascii_lowercase().as_str(),
        "off" | "none" | "unlimited"
    ) {
        return Ok(None);
    }
    let (digits, mult) = match v
        .chars()
        .last()
        .map(|c| c.to_ascii_uppercase())
        .filter(|c| "KMGT".contains(*c))
    {
        Some(c) => (
            &v[..v.len() - 1],
            match c {
                'K' => 1u64 << 10,
                'M' => 1 << 20,
                'G' => 1 << 30,
                _ => 1u64 << 40,
            },
        ),
        None => (v, 1),
    };
    let n: u64 = digits.trim().parse().map_err(|_| {
        anyhow::anyhow!("unknown limit {v:?} (expected `off`, a count, or a size like `10G`)")
    })?;
    Ok(Some(n * mult))
}

/// Parse a retention window: `off`, or `7d`/`48h`/`30m`/`3600s`/`3600`.
///
/// Bare seconds are accepted because that is what the API takes, but a retention
/// window is a human-scale quantity and `604800` is not a number anyone should
/// have to recognise.
fn parse_retention(v: &str) -> anyhow::Result<Option<i64>> {
    let v = v.trim();
    if matches!(v, "off" | "none" | "0" | "disabled") {
        return Ok(None);
    }
    let (digits, mult) = match v.strip_suffix(['d', 'h', 'm', 's']) {
        Some(d) => (
            d,
            match v.chars().last() {
                Some('d') => 86_400,
                Some('h') => 3_600,
                Some('m') => 60,
                _ => 1,
            },
        ),
        None => (v, 1),
    };
    let n: i64 = digits.trim().parse().map_err(|_| {
        anyhow::anyhow!("unknown retention {v:?} (expected `off`, or a duration like `7d`)")
    })?;
    if n <= 0 {
        anyhow::bail!("retention must be positive (use `off` to disable)");
    }
    Ok(Some(n * mult))
}

/// Render a retention window back the way it would be typed.
fn format_retention(secs: i64) -> String {
    for (unit, n) in [("d", 86_400), ("h", 3_600), ("m", 60)] {
        if secs % n == 0 {
            return format!("{}{unit}", secs / n);
        }
    }
    format!("{secs}s")
}

/// Run one `origofs trash …` subcommand.
async fn run_trash(ws: &Workspace, cmd: TrashCmd) -> anyhow::Result<()> {
    match cmd {
        TrashCmd::List => {
            let entries = ws.list_trash().await?;
            if entries.is_empty() {
                // Distinguish "nothing deleted" from "not collecting", because
                // the second is a configuration answer and the first is not.
                match ws.trash_retention().await? {
                    Some(_) => println!("trash is empty"),
                    None => println!(
                        "trash is disabled (nothing is being retained); enable it with \
                         `origofs trash retention 7d`"
                    ),
                }
            }
            for e in entries {
                let who = e
                    .actor_id
                    .map(|a| format!("actor={a}"))
                    .unwrap_or_else(|| "actor=-".to_string());
                println!(
                    "#{:<5} {:<6} {:<10} {who}\t{}",
                    e.id,
                    e.kind.as_str(),
                    e.size,
                    e.path
                );
            }
        }
        TrashCmd::Restore { id, actor } => {
            // Attributed: a restore is a write to the working tree, so it is
            // blamed like any other. `cli_ctx` opens a session so the restore can
            // itself be reverted.
            let ctx = match resolve_actor(actor)? {
                Some(a) => cli_ctx(ws, a).await?,
                None => {
                    ws.ensure_attributed("trash restore").await?;
                    WriteCtx::actor(0)
                }
            };
            let path = ws.restore_trash(id, ctx).await?;
            println!("restored {path}");
        }
        TrashCmd::Purge { id, all } => match (id, all) {
            (Some(_), true) => anyhow::bail!("pass an id or --all, not both"),
            (None, false) => anyhow::bail!("pass an entry id, or --all to drop everything"),
            (Some(id), false) => {
                if ws.purge_trash(id).await? {
                    println!("purged entry #{id}");
                } else {
                    println!("no trash entry #{id}");
                }
            }
            (None, true) => println!("purged {} entries", ws.empty_trash().await?),
        },
        TrashCmd::Retention { setting } => match setting.as_deref() {
            None => match ws.trash_retention().await? {
                Some(secs) => println!("trash retention is {}", format_retention(secs)),
                None => println!("trash is disabled"),
            },
            Some(v) => {
                let secs = parse_retention(v)?;
                ws.set_trash_retention(secs).await?;
                match secs {
                    Some(s) => println!("trash retention is now {}", format_retention(s)),
                    None => println!(
                        "trash is now disabled; already-retained entries are kept \
                         (use `origofs trash purge --all` to drop them)"
                    ),
                }
            }
        },
    }
    Ok(())
}

/// Parse the `on`/`off` argument the two ACL switches share.
fn parse_switch(v: &str) -> anyhow::Result<bool> {
    match v {
        "on" | "true" | "1" => Ok(true),
        "off" | "false" | "0" => Ok(false),
        other => Err(anyhow::anyhow!(
            "unknown setting {other:?} (expected `on` or `off`)"
        )),
    }
}

/// Run one `origofs acl …` subcommand.
///
/// Every mutating arm takes the same shape: with an actor, call the gated `_as`
/// form; without one, call the raw form and say out loud that nothing checked it.
/// The raw forms are not a fallback for convenience — they exist because
/// provisioning has no actor, and a caller who reaches them by forgetting
/// `--by` should be able to tell from the output that they did.
async fn run_acl(ws: &Workspace, cmd: AclCmd) -> anyhow::Result<()> {
    use origofs_sdk::Perms;
    let unchecked = "(unchecked: no --by given, so this ran as provisioning)";
    match cmd {
        AclCmd::Show { actor } => {
            println!(
                "default-deny is {}; read enforcement is {}",
                if ws.acl_default_deny().await? {
                    "on"
                } else {
                    "off"
                },
                if ws.acl_enforce_reads().await? {
                    "on"
                } else {
                    "off"
                },
            );
            let grants = ws.list_grants(actor).await?;
            if grants.is_empty() {
                println!("no grants");
            }
            for g in grants {
                let prefix = if g.path_prefix.is_empty() {
                    "/"
                } else {
                    &g.path_prefix
                };
                let by = g.granted_by.map(|b| format!(" by={b}")).unwrap_or_default();
                println!("actor={:<5} {:<30} {}{by}", g.actor_id, prefix, g.perms);
            }
        }
        AclCmd::Check { actor, path } => {
            let perms = ws.effective_perms(actor, &path).await?;
            println!("actor {actor} at {path}: {perms}");
        }
        AclCmd::Grant {
            actor,
            prefix,
            perms,
            by,
        } => {
            let p = Perms::parse(&perms).ok_or_else(|| {
                anyhow::anyhow!(
                    "unknown permissions {perms:?} (expected `read`, `write`, `propose`, \
                     `none`, or a combination like `read+write`)"
                )
            })?;
            match resolve_actor(by)? {
                Some(granter) => {
                    ws.grant_as(WriteCtx::actor(granter), actor, &prefix, p)
                        .await?;
                    println!("granted {p} at {prefix} to actor {actor} (by actor {granter})");
                }
                None => {
                    ws.grant(actor, &prefix, p, None).await?;
                    println!("granted {p} at {prefix} to actor {actor} {unchecked}");
                }
            }
        }
        AclCmd::Revoke { actor, prefix, by } => {
            let existed = match resolve_actor(by)? {
                Some(revoker) => {
                    ws.revoke_as(WriteCtx::actor(revoker), actor, &prefix)
                        .await?
                }
                None => ws.revoke(actor, &prefix, None).await?,
            };
            if existed {
                println!("revoked actor {actor}'s grant at {prefix}");
            } else {
                println!("actor {actor} had no grant at exactly {prefix}");
            }
        }
        AclCmd::DefaultDeny { setting, by } => match setting.as_deref() {
            None => println!(
                "acl default-deny is {}",
                if ws.acl_default_deny().await? {
                    "on"
                } else {
                    "off"
                }
            ),
            Some(v) => {
                let on = parse_switch(v)?;
                match resolve_actor(by)? {
                    Some(a) => ws.set_acl_default_deny_as(WriteCtx::actor(a), on).await?,
                    None => ws.set_acl_default_deny(on).await?,
                }
                println!("acl default-deny is now {}", if on { "on" } else { "off" });
            }
        },
        AclCmd::EnforceReads { setting, by } => match setting.as_deref() {
            None => println!(
                "acl read enforcement is {}",
                if ws.acl_enforce_reads().await? {
                    "on"
                } else {
                    "off"
                }
            ),
            Some(v) => {
                let on = parse_switch(v)?;
                match resolve_actor(by)? {
                    Some(a) => ws.set_acl_enforce_reads_as(WriteCtx::actor(a), on).await?,
                    None => ws.set_acl_enforce_reads(on).await?,
                }
                println!(
                    "acl read enforcement is now {}",
                    if on { "on" } else { "off" }
                );
            }
        },
    }
    Ok(())
}

/// The context a **read** runs under, or `None` for an unattributed read.
///
/// No session, unlike [`cli_ctx`]: a read records nothing, so opening a session
/// row per `origofs ls` would be a write in service of a read.
///
/// `None` is a real answer rather than a failure. Reads are open unless a
/// workspace turns `acl_enforce_reads` on, and the overwhelming majority of CLI
/// use is a developer looking at their own workspace, where demanding an actor id
/// would be friction with nothing behind it. Where enforcement *is* on, the
/// engine refuses an unattributed read on its own — the flag is how you get an
/// answer, not how you get past the check.
///
/// It is not an identity check either, and cannot be: whoever writes the argv
/// writes the environment, and a local process holding the workspace directory
/// has `meta.db` and the CAS on disk anyway. What it buys is that
/// `origofs read --actor 7` answers what actor 7 would actually be served, so an
/// ACL can be verified with the same binary that enforces it.
fn read_ctx(flag: Option<i64>) -> anyhow::Result<Option<WriteCtx>> {
    Ok(resolve_actor(flag)?.map(WriteCtx::actor))
}

/// Open a CLI session for `actor` and return the write context to act under.
///
/// Every attributed CLI command opens its own session labelled `cli`, matching
/// what `write` already did, so a `revert-session` can undo one command's work.
async fn cli_ctx(ws: &origofs_sdk::Workspace, actor: i64) -> anyhow::Result<WriteCtx> {
    let session = ws.create_session(actor, Some("cli")).await?;
    Ok(WriteCtx::session(actor, session))
}

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

/// Parse a byte count with an optional binary suffix: `4096`, `8K`, `64M`, `2G`.
///
/// `origofs bench --size` is the one place the CLI takes a number big enough that
/// spelling it in bytes is a source of zero-counting mistakes — and a mistyped
/// `--size 8000000000` is not a typo you notice until the run has been going for a
/// while. Suffixes are binary (`K` = 1024), matching every size origofs reports.
fn parse_size(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim();
    // Strip the unit tail (`B`, `iB`) *before* looking for the scale letter, so
    // `8MiB` — the spelling this program's own output uses — is read as `8M` and
    // not as a number ending in `B`.
    let body = s.strip_suffix(['B', 'b']).unwrap_or(s);
    let body = body.strip_suffix(['I', 'i']).unwrap_or(body);
    let (digits, shift) = match body.chars().last() {
        Some('K') | Some('k') => (&body[..body.len() - 1], 10),
        Some('M') | Some('m') => (&body[..body.len() - 1], 20),
        Some('G') | Some('g') => (&body[..body.len() - 1], 30),
        _ => (body, 0),
    };
    let n: u64 = digits
        .trim_end()
        .parse()
        .map_err(|_| format!("{s:?} is not a byte count (try 4096, 8K, 64M, 2G)"))?;
    n.checked_shl(shift)
        .filter(|v| *v >> shift == n)
        .ok_or_else(|| format!("{s:?} overflows a 64-bit byte count"))
}

/// Render `bytes` for a human, in the binary units the rest of origofs reports.
///
/// Reporting alongside the exact figure rather than instead of it: a benchmark is
/// read to be compared with another one, and `8.0 MiB` cannot be subtracted.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit + 1 < UNITS.len() {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// Render a duration at a fixed three significant figures, picking the unit.
fn human_dur(d: std::time::Duration) -> String {
    let s = d.as_secs_f64();
    if s >= 1.0 {
        format!("{s:.2}s")
    } else if s >= 0.001 {
        format!("{:.1}ms", s * 1e3)
    } else {
        format!("{:.0}us", s * 1e6)
    }
}

/// Print `origofs info` (issue #118).
///
/// The caveats are printed, not just documented, because this output is what gets
/// pasted into an issue as evidence. A "dedup 1.4x" line that travels without the
/// sentence saying it counts only repetition *inside this file* is how a
/// measurement turns into a wrong claim about the store.
fn print_info(path: &str, info: &origofs_sdk::FileLayout) {
    let (min, avg, max) = info.chunker;
    println!("path            {path}");
    println!("size            {} ({})", info.size, human_bytes(info.size));
    match info.manifest {
        Some(h) => println!("manifest        {}", h.to_hex()),
        None => println!("manifest        (none: empty file, so there is no body to read)"),
    }
    if info.chunks == 0 {
        println!("chunks          0 — nothing to fetch");
        return;
    }
    println!(
        "chunks          {} refs, {} distinct — a whole-file read fetches {} objects",
        info.chunks, info.distinct_chunks, info.chunks
    );
    let fmt = |v: Option<u32>| v.map(|v| human_bytes(u64::from(v))).unwrap_or_default();
    println!(
        "chunk sizes     min {}, median {}, mean {}, max {}",
        fmt(info.smallest),
        fmt(info.median),
        info.mean().map(human_bytes).unwrap_or_default(),
        fmt(info.largest),
    );
    let widest = info
        .histogram
        .iter()
        .map(|(_, n)| *n)
        .max()
        .unwrap_or(1)
        .max(1);
    for (bound, count) in &info.histogram {
        // Bar width is relative to the fullest bucket, so the shape is readable
        // whether the file has 5 chunks or 5 million.
        let bar = "#".repeat((*count as f64 / widest as f64 * 32.0).round() as usize);
        println!(
            "  <= {:>9}  {:>10}  {bar}",
            human_bytes(u64::from(*bound)),
            count
        );
    }
    println!(
        "distinct bytes  {} ({:.2}x self-dedup)",
        human_bytes(info.distinct_bytes),
        info.self_dedup()
    );
    println!("                repetition *within this file* only — what it also shares with");
    println!("                other files is not measured (that means reading every manifest)");
    match &info.residency {
        None => println!("residency       not probed (--no-probe)"),
        Some(r) => {
            println!(
                "residency       {}/{} distinct chunks present in the content store",
                r.present, info.distinct_chunks
            );
            println!(
                "                presence, not cache residency: a tiered store answers from \
                 either tier"
            );
            if r.missing > 0 {
                println!(
                    "  WARNING: {} chunk(s) are GONE — this file cannot be read. First few:",
                    r.missing
                );
                for h in &r.missing_sample {
                    println!("    {}", h.to_hex());
                }
            }
        }
    }
    println!(
        "chunker         min {} / avg {} / max {}",
        human_bytes(u64::from(min)),
        human_bytes(u64::from(avg)),
        human_bytes(u64::from(max))
    );
}

/// Print `origofs bench` (issue #118). See [`print_info`] on why the caveats are
/// part of the output rather than only of the docs.
fn print_bench(report: &origofs_sdk::BenchReport) {
    let (min, avg, max) = report.chunker;
    println!(
        "bench: {} files x {} = {}",
        report.opts.files,
        human_bytes(report.opts.file_size),
        human_bytes(report.total_bytes)
    );
    println!(
        "  chunker             min {} / avg {} / max {}",
        human_bytes(u64::from(min)),
        human_bytes(u64::from(avg)),
        human_bytes(u64::from(max))
    );
    println!(
        "  chunks produced     {} refs, {} distinct",
        report.chunks, report.distinct_chunks
    );
    for t in [report.upload_concurrency, report.fetch_concurrency] {
        match t.value {
            Some(n) => println!("  {:<19} {n} ({})", concurrency_label(t.var), t.var),
            None => println!(
                "  {:<19} engine default ({} unset)",
                concurrency_label(t.var),
                t.var
            ),
        }
    }
    println!("  seed                {}", report.opts.seed);
    println!();
    println!(
        "{:<8}{:>14}{:>10}{:>10}{:>10}   ({} ops each)",
        "phase", "throughput", "p50", "p95", "max", report.write.ops
    );
    for (label, stage) in [
        ("write", &report.write),
        ("read", &report.read),
        ("read#2", &report.reread),
    ] {
        println!(
            "{label:<8}{:>14}{:>10}{:>10}{:>10}",
            format!("{}/s", human_bytes(stage.bytes_per_sec() as u64)),
            human_dur(stage.quantile(0.5)),
            human_dur(stage.quantile(0.95)),
            human_dur(stage.quantile(1.0)),
        );
    }
    println!();
    println!("note: `read` and `read#2` are the first and second pass, NOT cold and warm:");
    println!("      nothing here evicts a page cache or a cache tier, so both ran over");
    println!("      bytes this run had just written.");
    println!("note: writes are unattributed, so no edit-op or blame-index update is on");
    println!("      the clock; an attributed write costs a little more than this says.");
    if report.distinct_chunks < report.chunks {
        println!(
            "note: only {} of {} chunks were distinct, so some writes deduplicated and",
            report.distinct_chunks, report.chunks
        );
        println!("      the write figure is OVERSTATED. Re-run against an empty --dir.");
    }
    if report.kept {
        println!(
            "note: --keep, so the sample files are still in {}.",
            report.opts.dir
        );
    } else {
        println!("note: sample files removed; their chunks stay in the content store until");
        println!("      a `gc` past the grace period reclaims them.");
    }
}

/// The human name for a concurrency knob, from its environment variable.
fn concurrency_label(var: &str) -> &'static str {
    if var.contains("UPLOAD") {
        "upload concurrency"
    } else {
        "fetch concurrency"
    }
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
        Cmd::Mkdir { path, actor } => match resolve_actor(actor)? {
            Some(actor) => {
                let ctx = cli_ctx(&ws, actor).await?;
                ws.mkdir_as(ctx, &path).await?;
            }
            None => {
                ws.ensure_attributed("mkdir").await?;
                ws.mkdir_p(&path).await?;
            }
        },
        Cmd::Write { path, from, actor } => {
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
                            match ws.write_or_propose(ctx, &path, &data, None).await? {
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
        }
        Cmd::Read { path, actor } => {
            let bytes = match read_ctx(actor)? {
                Some(ctx) => ws.read_as(ctx, &path).await?,
                None => ws.read(&path).await?,
            };
            std::io::stdout().write_all(&bytes)?;
        }
        Cmd::Ls { path, actor } => {
            let entries = match read_ctx(actor)? {
                Some(ctx) => ws.ls_as(ctx, &path).await?,
                None => ws.ls(&path).await?,
            };
            for e in entries {
                println!("{}\t{}", e.kind.as_str(), e.name);
            }
        }
        Cmd::Stat { path, actor } => {
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
        }
        Cmd::Info { path, no_probe } => {
            let info = ws.file_layout(&path, !no_probe).await?;
            print_info(&path, &info);
        }
        Cmd::Bench {
            dir,
            files,
            size,
            seed,
            keep,
            force,
        } => {
            let mut opts = origofs_sdk::BenchOpts::new();
            opts.dir = dir;
            opts.files = files;
            opts.file_size = size;
            opts.seed = seed.unwrap_or(opts.seed);
            opts.keep = keep;
            opts.force = force;
            print_bench(&ws.bench(&opts).await?);
        }
        Cmd::Rm { path, actor } => match resolve_actor(actor)? {
            Some(actor) => {
                let ctx = cli_ctx(&ws, actor).await?;
                // `remove_or_propose`, not `remove`: a propose-only actor's delete
                // is queued for review rather than refused, which is how `write`
                // already behaves. Refusing would make the two inconsistent in the
                // opposite direction.
                match ws.remove_or_propose(ctx, &path, None).await? {
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
        },
        Cmd::Mv { from, to, actor } => match resolve_actor(actor)? {
            Some(actor) => {
                let ctx = cli_ctx(&ws, actor).await?;
                ws.rename_as(ctx, &from, &to).await?;
            }
            None => {
                ws.ensure_attributed("mv").await?;
                ws.rename(&from, &to).await?;
            }
        },
        Cmd::Commit {
            message,
            author,
            actor,
        } => {
            let hash = match resolve_actor(actor)? {
                Some(actor) => {
                    let ctx = cli_ctx(&ws, actor).await?;
                    ws.commit_as(ctx, &author, &message).await?
                }
                None => {
                    ws.ensure_attributed("commit").await?;
                    ws.commit(&author, &message).await?
                }
            };
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
        Cmd::Diff {
            from,
            to,
            path,
            actor,
        } => match path {
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
        Cmd::Suggestions {
            status,
            path,
            actor,
        } => {
            let st = match status.as_deref() {
                Some(s) => Some(
                    SuggestionStatus::parse(s)
                        .ok_or_else(|| anyhow::anyhow!("unknown status {s:?}"))?,
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
        }
        Cmd::SuggestionDiff { id, actor } => {
            let patch = match read_ctx(actor)? {
                Some(ctx) => ws.suggestion_diff_as(ctx, id).await?,
                None => ws.suggestion_diff(id).await?,
            };
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
        Cmd::RevertSession {
            actor,
            session,
            by,
            path_prefix,
        } => {
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
        }
        Cmd::Dump { out } => {
            let n = if out == "-" {
                let stdout = std::io::stdout();
                ws.dump(std::io::BufWriter::new(stdout.lock())).await?
            } else {
                let f = std::fs::File::create(&out)?;
                let n = ws.dump(std::io::BufWriter::new(f)).await?;
                println!("dumped {n} records to {out}");
                n
            };
            let _ = n;
        }
        Cmd::Load { input } => {
            let report = if input == "-" {
                let stdin = std::io::stdin();
                ws.load(std::io::BufReader::new(stdin.lock())).await?
            } else {
                let f = std::fs::File::open(&input)?;
                ws.load(std::io::BufReader::new(f)).await?
            };
            println!(
                "restored {} rows (dump taken at schema v{})",
                report.total_rows(),
                report.source_schema_version
            );
            for (table, n) in &report.tables {
                println!("  {table}: {n}");
            }
            // The single most likely way to be confused by a successful load: the
            // names and the blame are all here, and every read fails, because the
            // bytes were never in the dump. Say so at the moment it matters rather
            // than letting the user meet `content missing for hash ...` cold.
            println!(
                "note: this restored metadata only. File bytes live in the content \
                 store, which a dump references by hash and does not carry — point \
                 this workspace at the same content store, or reads will fail."
            );
            if !report.skipped_tables.is_empty() {
                // A dump from a newer build may carry tables this one does not
                // know. Skipping is deliberate (see `Fs::load`), but silence
                // would let a partial restore look complete.
                println!(
                    "  skipped unknown tables: {}",
                    report.skipped_tables.join(", ")
                );
            }
        }
        Cmd::Du { path, actor } => {
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
        }
        Cmd::Quota { bytes, inodes } => {
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
        }
        Cmd::Trash { cmd } => run_trash(&ws, cmd).await?,
        Cmd::Acl { cmd } => run_acl(&ws, cmd).await?,
        Cmd::RequireAttribution { setting } => match setting.as_deref() {
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
        },
        Cmd::PosixLocks { setting, path } => {
            if let Some(path) = path {
                let held = ws.posix_locks(&path).await?;
                if held.is_empty() {
                    // Distinguishes "nothing holds this" from "not collecting",
                    // the same way `trash list` has to.
                    let on = ws.posix_locks_enabled().await?;
                    println!(
                        "no advisory locks on {path} (locking is {})",
                        if on { "on" } else { "off" }
                    );
                } else {
                    for l in held {
                        let end = if l.end == origofs_sdk::posixlock::LOCK_EOF {
                            "EOF".to_string()
                        } else {
                            l.end.to_string()
                        };
                        println!(
                            "{}\t{}-{}\tpid {}\towner {}\tmount {}",
                            if l.exclusive { "WRITE" } else { "READ " },
                            l.start,
                            end,
                            l.pid,
                            l.owner,
                            l.holder
                        );
                    }
                }
            } else {
                match setting.as_deref() {
                    None => {
                        let on = ws.posix_locks_enabled().await?;
                        println!("posix-locks is {}", if on { "on" } else { "off" });
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
                        ws.set_posix_locks_enabled(on).await?;
                        println!("posix-locks is {}", if on { "on" } else { "off" });
                    }
                }
            }
        }
        Cmd::Blame { path, actor } => {
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
        }
        Cmd::Sandbox {
            actor,
            discard,
            isolate,
            cmd,
        } => {
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
        }
        Cmd::Overlay {
            actor,
            sync_ms,
            isolate,
            cmd,
        } => {
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
        }
        Cmd::Mount { mountpoint, actor } => {
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
                    println!(
                        "  (anonymous mount: path ACLs do not apply — pass --actor to bind one)"
                    );
                }
                let handle =
                    std::thread::spawn(move || origofs_sdk::fuse::mount_as(ws, &mountpoint, ctx));
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("mount thread panicked"))??;
            }
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
            if stats.skipped_young > 0 {
                println!(
                    "  {} unreferenced object(s) left for now: younger than the {}s grace \n  period, which is what keeps a collection safe alongside live writers",
                    stats.skipped_young,
                    origofs_sdk::DEFAULT_GC_GRACE_SECS
                );
            }
            if stats.skipped_undated > 0 {
                println!(
                    "  warning: {} unreferenced object(s) could not be dated by this content \n  backend, so they were left alone — this store cannot be collected safely",
                    stats.skipped_undated
                );
            }
        }
        Cmd::Repack => {
            let freed = ws.repack().await?;
            println!("repack: {freed} bytes reclaimed");
        }
        Cmd::Flush => {
            ws.flush().await?;
            println!("flushed buffered writes to durable storage");
        }
        Cmd::Migrate => {
            let (before, after) = ws.migrate().await?;
            if before == after {
                println!("schema already at v{after}; nothing to apply");
            } else {
                println!("migrated schema v{before} -> v{after}");
            }
        }
        Cmd::SchemaVersion => {
            let current = ws.schema_version().await?;
            let latest = ws.latest_schema_version();
            println!("schema version: v{current} (this binary knows up to v{latest})");
            if current < latest {
                println!(
                    "  run `origofs migrate` to apply {} step(s)",
                    latest - current
                );
            } else if current > latest {
                println!(
                    "  warning: the store is NEWER than this binary; a newer origofs wrote it \n  and this one may not understand every column. Upgrade before writing."
                );
            }
        }
        Cmd::Backup { dest } => {
            let what = ws.backup_metadata(&dest).await?;
            println!("{what}");
            println!(
                "note: this is the metadata store only — content lives in the content store \n  and is already durable there. Blame, the audit log, actors, and uncommitted \n  edits exist ONLY in this file."
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
            // Only reachable on the dry run: `--rebuild` refuses outright when an
            // object it can't read would change what gets restored.
            if report.unsupported > 0 {
                let kinds = report
                    .unsupported_kinds
                    .iter()
                    .map(|(kind, v)| format!("{kind} v{v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                println!(
                    "  WARNING: {} object(s) written by a NEWER origofs ({kinds}) — \
                     upgrade origofs before rebuilding, or history it can't read will be lost",
                    report.unsupported
                );
            }
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
        Cmd::Presence { window, actor } => {
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
        }
        Cmd::Serve {
            addr,
            auth_tokens,
            gate_reads,
            root,
            cors_origins,
            max_body_bytes,
            request_timeout,
            max_concurrent_requests,
            metrics,
        } => {
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
            println!(
                "serving origofs at http://{addr} (SIGTERM/Ctrl-C to stop; in-flight requests drain)"
            );
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
        }
        Cmd::Nfs { addr, actor } => {
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
        }
    }
    Ok(())
}

/// The error for a subcommand whose surface only exists on Unix.
///
/// `mount` (FUSE), `nfs` (NFSv3), and `sandbox`/`overlay` (overlayfs edit-capture)
/// each drive a kernel interface Windows does not have, so the corresponding
/// `origofs-sdk` modules are `#[cfg(all(unix, …))]` and are simply absent there.
///
/// The clap definitions stay on every platform anyway, deliberately: `--help`,
/// argument parsing, and the docs then read the same everywhere, and someone
/// running one of these on Windows gets a sentence explaining *why* it cannot
/// work and what to use instead — rather than clap's "unrecognized subcommand",
/// which reads like a typo or a broken install. This mirrors what the Python
/// bindings already do (`Workspace.mount()` / `serve_nfs()` raise a clear error
/// off-platform rather than vanishing from the class). #107.
#[cfg(not(unix))]
fn unix_only(subcommand: &str, surface: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "`origofs {subcommand}` is not available on this platform: it is built on {surface}, \
         which Windows does not provide. The portable surfaces work here — `origofs serve` \
         (HTTP API), `origofs mcp`, and `origofs git` — as do all the ordinary file, \
         versioning, and attribution commands."
    )
}

/// Build the HTTP API authenticator from `--auth-token` specs. origofs never trusts a
/// client-named actor, so the server must resolve identity itself. With no specs:
/// refuse to expose a non-loopback address, and on loopback attribute all writes
/// Periodic housekeeping for a long-running server.
///
/// Two maintenance operations shipped with no caller anywhere in the tree:
///
///   * `reap_presence` drops presence rows for sessions that stopped
///     heartbeating. Without it the table only ever grows, and `presence()` —
///     which every collaborative UI polls — gets slower forever.
///   * `supersede_stale_suggestions` retires proposals whose base content has
///     already moved on. Left pending, they sit in the review queue looking
///     actionable, and accepting one just fails as superseded.
///
/// A server is the right place for both: it is the only process that lives long
/// enough for either to matter. Every call is best-effort — housekeeping must never
/// take the server down — and the interval is deliberately coarse, because neither
/// operation is urgent and both touch shared tables.
async fn spawn_janitor(ws: std::sync::Arc<origofs_sdk::Workspace>) {
    /// How long a session may go without a heartbeat before its presence row is
    /// reaped. Comfortably longer than any sane heartbeat interval, so a brief
    /// network hiccup does not make a working collaborator vanish from the UI.
    const PRESENCE_GRACE_SECS: i64 = 300;
    const EVERY: std::time::Duration = std::time::Duration::from_secs(60);

    let mut ticker = tokio::time::interval(EVERY);
    // The first tick fires immediately; skip straight to the cadence so startup
    // isn't competing with a maintenance sweep.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        match ws.reap_presence(PRESENCE_GRACE_SECS).await {
            Ok(n) if n > 0 => tracing::debug!(reaped = n, "reaped stale presence rows"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "presence reap failed"),
        }
    }
}

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
    Ok(std::sync::Arc::new(parse_auth_specs(specs)?))
}

/// Merge `--auth-token` arguments with `ORIGOFS_AUTH_TOKENS`.
///
/// Bearer tokens on the command line are readable by every process on the host
/// through `ps` and are written to shell history — the same objection that keeps
/// `ORIGOFS_ENCRYPTION_KEY` out of argv. The flag stays, because it is what makes
/// the README's one-liner work, but a real deployment should use the environment.
///
/// Entries are separated by newlines or commas, so both a here-doc and a one-line
/// value work. A token itself may contain `=` (base64 padding) but not a comma or
/// a newline; blank entries are skipped so trailing separators are harmless.
fn auth_tokens_with_env(mut specs: Vec<String>) -> Vec<String> {
    if let Ok(raw) = std::env::var("ORIGOFS_AUTH_TOKENS") {
        specs.extend(
            raw.split(['\n', ','])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        );
    }
    specs
}

/// Parse `--auth-token TOKEN=ACTOR_ID[:SESSION_ID]` specs into a token map.
///
/// Split out of [`build_api_auth`] so it is testable without a workspace: this is
/// argument parsing that decides who a request is attributed to, which makes it
/// exactly the part worth pinning.
fn parse_auth_specs(specs: &[String]) -> Result<origofs_sdk::api::BearerAuth> {
    let mut bearer = origofs_sdk::api::BearerAuth::new();
    for spec in specs {
        // Split from the *right*. The actor/session half never contains `=`, but
        // the token half routinely does — base64 pads with it, and a bearer token
        // is very often base64. Splitting on the first `=` made any such token
        // unusable, with an error blaming the actor id.
        let (token, who) = spec.rsplit_once('=').ok_or_else(|| {
            anyhow::anyhow!("bad --auth-token {spec:?}; expected TOKEN=ACTOR_ID[:SESSION_ID]")
        })?;
        if token.is_empty() {
            anyhow::bail!("bad --auth-token {spec:?}: the token is empty");
        }
        let (actor, session) = match who.split_once(':') {
            Some((a, s)) => (
                a.parse().with_context(|| format!("actor id in {spec:?}"))?,
                Some(
                    s.parse()
                        .with_context(|| format!("session id in {spec:?}"))?,
                ),
            ),
            None => (
                who.parse()
                    .with_context(|| format!("actor id in {spec:?}"))?,
                None,
            ),
        };
        bearer = bearer.with_token(token.to_string(), actor, session);
    }
    Ok(bearer)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `--auth-token` decides which actor a request's writes are attributed to, so
    /// its parsing is worth pinning. The binary had no unit tests at all — 1,200
    /// lines including this and the `--config` backend selection — and the Docker
    /// job only exercised the happy path through `serve`.
    #[test]
    fn auth_token_specs_parse() {
        let ok = parse_auth_specs(&[
            "tok-a=7".to_string(),
            "tok-b=9:42".to_string(),
            // A base64 token ends in `=` padding, so the split has to be on the
            // *last* separator — the actor half never contains one.
            "dG9rZW4=:=11".to_string(),
            "cGFk==11".to_string(),
        ])
        .expect("valid specs");
        assert!(!ok.is_empty());
    }

    #[test]
    fn auth_token_specs_reject_malformed_input() {
        let bad = [
            "no-equals-sign",   // missing the separator entirely
            "=7",               // empty token
            "tok=",             // no actor id
            "tok=notanumber",   // actor id is not an integer
            "tok=7:notanumber", // session id is not an integer
            "tok=7:",           // empty session id
        ];
        for spec in bad {
            assert!(
                parse_auth_specs(&[spec.to_string()]).is_err(),
                "{spec:?} should be rejected"
            );
        }
    }

    #[test]
    fn an_empty_spec_list_yields_an_empty_map() {
        // The empty case is what routes `build_api_auth` into its loopback-only
        // dev path, so it must stay distinguishable from "tokens configured".
        assert!(parse_auth_specs(&[]).expect("no specs").is_empty());
    }

    /// `origofs bench --size` is the one flag here that scales, and a suffix that
    /// parsed as the wrong power of two would silently move every throughput
    /// number in the report by 1024x. Units are binary throughout, matching what
    /// `human_bytes` prints back.
    #[test]
    fn size_suffixes_are_binary_and_round_trip_what_we_print() {
        assert_eq!(parse_size("4096"), Ok(4096));
        assert_eq!(parse_size("8K"), Ok(8 << 10));
        assert_eq!(parse_size("64m"), Ok(64 << 20));
        assert_eq!(parse_size("2G"), Ok(2 << 30));
        // The spelling this program's own output uses has to be accepted, or a
        // user cannot paste a figure back into the flag that produced it.
        assert_eq!(parse_size("1MiB"), Ok(1 << 20));
        assert_eq!(parse_size("1MB"), Ok(1 << 20));
        assert_eq!(human_bytes(parse_size("64M").unwrap()), "64.0 MiB");
    }

    /// A size that does not fit is refused rather than wrapping to a small one —
    /// a benchmark that silently ran on 1 MiB after being asked for 16 EiB would
    /// report a number for the wrong experiment.
    #[test]
    fn a_size_that_overflows_is_refused_not_wrapped() {
        assert!(parse_size("99999999999G").is_err());
        assert!(parse_size("banana").is_err());
        assert!(parse_size("").is_err());
        assert!(parse_size("-1").is_err());
    }
}
