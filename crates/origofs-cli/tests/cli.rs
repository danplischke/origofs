//! Black-box tests for the `origofs` binary itself.
//!
//! The engine's *semantics* are covered exhaustively in `origofs-core/tests/`
//! (merge, attribution, recover, durability, integrity). This file covers the
//! layer above them that nothing else did: the **shell**. Before it, the only
//! coverage of `origofs` was a Docker CI job running `--help` and starting
//! `serve` — so every flag-to-field mapping, every default, every exit code,
//! every line of output, and (most importantly) *which engine method each
//! subcommand actually calls* was unverified. A `--actor` plumbed to the wrong
//! argument, an inverted `--rebuild`, or a `gc` wired to a zero grace period
//! would all have shipped silently, and for most users this binary **is**
//! origofs.
//!
//! So the assertions here are deliberately about the *seam*, not the engine:
//! exit status, stdout/stderr text, and the workspace state observable
//! afterwards. Tests are ordered by the issue's blast-radius priority —
//! data-loss-adjacent commands first, then attribution, then the core loop,
//! then read-only reporting.
//!
//! Everything is hermetic: a fresh `tempfile` workspace per test, SQLite +
//! local CAS, no network, no Postgres.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The freshly built `origofs` binary under test.
const BIN: &str = env!("CARGO_BIN_EXE_origofs");

/// The result of one CLI invocation. Exit code is kept as an `Option` so a
/// signal death is distinguishable from a clean non-zero exit.
struct Out {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Out {
    fn ok(&self) -> bool {
        self.code == Some(0)
    }

    /// Assert success, showing *both* streams on failure — a CLI test that only
    /// prints "assertion failed" wastes the one piece of evidence it has.
    #[track_caller]
    fn expect_ok(self, what: &str) -> Self {
        assert!(
            self.ok(),
            "{what} should have succeeded, exited {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.code,
            self.stdout,
            self.stderr
        );
        self
    }

    /// Assert a *runtime* failure: exit 1, the code `main`'s `anyhow::Result`
    /// produces. Deliberately not `!= 0`, so a clap usage error (exit 2) can
    /// never masquerade as the engine error a test meant to provoke.
    #[track_caller]
    fn expect_err(self, what: &str) -> Self {
        assert_eq!(
            self.code,
            Some(1),
            "{what} should have failed with exit 1\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.stdout,
            self.stderr
        );
        self
    }

    // The content assertions borrow rather than consume, so a chain can end in a
    // `let` binding that is inspected further.
    #[track_caller]
    fn stdout_has(&self, needle: &str) -> &Self {
        assert!(
            self.stdout.contains(needle),
            "stdout should contain {needle:?}\n--- stdout ---\n{}",
            self.stdout
        );
        self
    }

    #[track_caller]
    fn stdout_lacks(&self, needle: &str) -> &Self {
        assert!(
            !self.stdout.contains(needle),
            "stdout should NOT contain {needle:?}\n--- stdout ---\n{}",
            self.stdout
        );
        self
    }

    #[track_caller]
    fn stderr_has(&self, needle: &str) -> &Self {
        assert!(
            self.stderr.contains(needle),
            "stderr should contain {needle:?}\n--- stderr ---\n{}",
            self.stderr
        );
        self
    }

    /// stdout with trailing whitespace trimmed — for the many commands whose
    /// entire contract is one short line.
    fn trimmed(&self) -> &str {
        self.stdout.trim_end()
    }

    /// Non-empty stdout lines. Most listing commands are line-oriented, and a
    /// line count is the cheapest way to catch "printed every row twice" or
    /// "printed nothing at all".
    fn lines(&self) -> Vec<&str> {
        self.stdout.lines().filter(|l| !l.is_empty()).collect()
    }
}

/// A temp workspace plus the helpers to drive the binary against it.
struct Ws {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
}

impl Ws {
    /// A workspace directory that exists but has not been `init`ed. Most tests
    /// want [`Ws::init`]; this exists for the tests that are *about* `init`.
    fn bare() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        Self { _tmp: tmp, dir }
    }

    fn init() -> Self {
        let ws = Self::bare();
        ws.run(&["init"]).expect_ok("init");
        ws
    }

    /// Somewhere to park files that are *not* the workspace (a `--from` source,
    /// a `backup` destination), still inside the same temp dir.
    fn scratch(&self, name: &str) -> PathBuf {
        let p = self._tmp.path().join(name);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        p
    }

    fn meta_db(&self) -> PathBuf {
        self.dir.join("meta.db")
    }

    fn run(&self, args: &[&str]) -> Out {
        self.run_with_stdin(args, None)
    }

    fn run_in(&self, args: &[&str], stdin: &str) -> Out {
        self.run_with_stdin(args, Some(stdin.as_bytes()))
    }

    fn run_with_stdin(&self, args: &[&str], stdin: Option<&[u8]>) -> Out {
        let mut full = vec!["--workspace", self.dir.to_str().unwrap()];
        full.extend_from_slice(args);
        raw(None, &full, stdin)
    }

    /// Shorthand for the very common "write these bytes as this actor".
    fn write_as(&self, path: &str, actor: i64, data: &str) -> Out {
        let actor = actor.to_string();
        self.run_in(&["write", path, "--actor", &actor], data)
    }

    /// Register an actor and return the id the CLI printed. `origofs actor`
    /// prints the bare id precisely so a shell can capture it like this, which
    /// makes the parse itself part of what is under test.
    fn actor(&self, args: &[&str]) -> i64 {
        let mut full = vec!["actor"];
        full.extend_from_slice(args);
        let out = self.run(&full).expect_ok("actor");
        out.trimmed().parse().unwrap_or_else(|_| {
            panic!("`origofs actor` must print a bare id, got {:?}", out.stdout)
        })
    }
}

/// Invoke the binary with a controlled environment.
///
/// The environment matters as much as the arguments here:
///
///   * `ORIGOFS_LOG=error` quiets the library's `info` spans. The CLI writes them
///     to **stderr** on purpose, but they carry timestamps and ANSI escapes, so
///     leaving them on would make every stderr assertion flaky.
///   * `RUST_BACKTRACE=0` because cargo sets it for test *children* too, and an
///     `anyhow` error would otherwise bury its one-line message under 30 frames.
///   * The `ORIGOFS_*` knobs that change behavior (`ORIGOFS_ENCRYPTION_KEY`,
///     `ORIGOFS_METRICS`) are cleared so a developer's shell cannot alter the
///     result — an inherited encryption key in particular would make reads fail
///     in a way that looks like a code bug.
fn raw(cwd: Option<&Path>, args: &[&str], stdin: Option<&[u8]>) -> Out {
    let mut cmd = Command::new(BIN);
    cmd.args(args)
        .env("ORIGOFS_LOG", "error")
        .env("RUST_BACKTRACE", "0")
        .env_remove("RUST_LOG")
        .env_remove("ORIGOFS_ENCRYPTION_KEY")
        .env_remove("ORIGOFS_METRICS")
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let mut child = cmd.spawn().expect("the origofs binary must be built");
    if let Some(bytes) = stdin {
        child.stdin.take().unwrap().write_all(bytes).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    Out {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Data-loss-adjacent commands
//
// These are the ones where a mis-wired flag destroys work rather than printing
// the wrong thing, so they come first.
// ─────────────────────────────────────────────────────────────────────────────

/// GC's safety rests entirely on the age gate (CLAUDE.md: content is written
/// *before* the metadata referencing it, so every in-flight write has a window
/// where its chunks look unreachable). `origofs gc` therefore must call
/// `Workspace::gc()` — the default `DEFAULT_GC_GRACE_SECS` grace — and never
/// `gc_with_grace(0)`, which exists next to it and would happily delete the
/// chunks of a concurrent writer.
///
/// A fresh workspace's orphans are seconds old, so the observable signature of
/// the correct call is: **nothing deleted, and the young-skip line printed**.
/// If someone ever "simplifies" the CLI to a zero grace, this test fails.
#[test]
fn gc_leaves_freshly_orphaned_content_alone() {
    let ws = Ws::init();
    // Three overwrites of one path: the first two versions' chunks are orphaned
    // immediately and would be swept by an ungated collection.
    for v in ["one\n", "two\n", "three\n"] {
        ws.run_in(&["write", "/churn.txt"], v).expect_ok("write");
    }

    ws.run(&["gc"])
        .expect_ok("gc")
        .stdout_has("deleted 0")
        .stdout_has("younger than the")
        .stdout_has("grace");

    // And the live content is of course still readable — a gc that "collected
    // nothing" but corrupted the tree would otherwise pass the line above.
    assert_eq!(ws.run(&["read", "/churn.txt"]).stdout, "three\n");
}

/// `repack` is only meaningful for a packed store; against the default local CAS
/// it must be a safe no-op rather than an error or a destructive rewrite. It is
/// the documented step *after* `gc`, so an operator will run it here.
#[test]
fn repack_is_a_harmless_no_op_on_an_unpacked_store() {
    let ws = Ws::init();
    ws.run_in(&["write", "/a.txt"], "keep me\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "seed"]).expect_ok("commit");

    ws.run(&["repack"])
        .expect_ok("repack")
        .stdout_has("0 bytes reclaimed");
    assert_eq!(ws.run(&["read", "/a.txt"]).stdout, "keep me\n");

    // `flush` is the other store-maintenance verb an operator pairs with these;
    // like repack it must succeed and say so on a write-through store.
    ws.run(&["flush"])
        .expect_ok("flush")
        .stdout_has("flushed buffered writes");
}

/// `rm` must delete exactly the path named and nothing adjacent, must refuse a
/// non-empty directory rather than recursing (there is no `-r`, so a silent
/// recursive delete would be catastrophic), and must fail loudly on a missing
/// path instead of exiting 0 and letting a script believe it removed something.
#[test]
fn rm_removes_one_path_and_refuses_to_recurse() {
    let ws = Ws::init();
    ws.run_in(&["write", "/keep.txt"], "keep\n")
        .expect_ok("write");
    ws.run_in(&["write", "/dir/gone.txt"], "gone\n")
        .expect_ok("write");
    ws.run_in(&["write", "/dir/stay.txt"], "stay\n")
        .expect_ok("write");

    // A non-empty directory is refused outright.
    ws.run(&["rm", "/dir"])
        .expect_err("rm on a non-empty dir")
        .stderr_has("directory not empty");
    ws.run(&["ls", "/dir"])
        .expect_ok("ls")
        .stdout_has("gone.txt");

    // The named file goes; its sibling and the unrelated file do not.
    ws.run(&["rm", "/dir/gone.txt"]).expect_ok("rm");
    let listing = ws.run(&["ls", "/dir"]).expect_ok("ls");
    assert_eq!(listing.lines(), vec!["file\tstay.txt"]);
    assert_eq!(ws.run(&["read", "/keep.txt"]).stdout, "keep\n");

    // Now the directory is empty, so it can go.
    ws.run(&["rm", "/dir/stay.txt"]).expect_ok("rm");
    ws.run(&["rm", "/dir"]).expect_ok("rm on an empty dir");
    ws.run(&["ls", "/"]).expect_ok("ls").stdout_lacks("dir");

    // A missing path is an error, not a no-op.
    ws.run(&["rm", "/never-existed.txt"])
        .expect_err("rm on a missing path")
        .stderr_has("not found");
}

/// `revert-session` is the sharpest tool in the box: it edits files in place to
/// remove one actor's lines. The flags decide *whose* work is destroyed, so this
/// pins that `--actor`/`--session` select the target (and not, say, the actor
/// doing the reverting) and that other authors' lines survive untouched.
#[test]
fn revert_session_undoes_only_the_named_actors_lines() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let claude = ws.actor(&["claude", "--agent", "--model", "m"]);

    // Each `write --actor` opens a fresh CLI session, so these are sessions 1
    // and 2 — the ids `--session` has to match.
    ws.write_as("/doc.txt", alice, "a1\na2\n")
        .expect_ok("alice write");
    ws.write_as("/doc.txt", claude, "a1\nAGENT\na2\n")
        .expect_ok("agent write");

    let blame = ws.run(&["blame", "/doc.txt"]).expect_ok("blame");
    assert_eq!(blame.lines().len(), 3, "alice / agent / alice");
    blame.stdout_has("agent:claude");

    ws.run(&[
        "revert-session",
        "--actor",
        &claude.to_string(),
        "--session",
        "2",
    ])
    .expect_ok("revert-session")
    .stdout_has("1 file(s) changed")
    .stdout_has("/doc.txt");

    // Exactly the agent's line is gone; alice's two lines remain, and blame has
    // collapsed back to a single human range.
    assert_eq!(ws.run(&["read", "/doc.txt"]).stdout, "a1\na2\n");
    let blame = ws.run(&["blame", "/doc.txt"]).expect_ok("blame");
    assert_eq!(blame.lines().len(), 1);
    blame.stdout_has("human:alice");
}

/// `--path-prefix` bounds the blast radius, and it matches on *directory
/// boundaries*: `/tenant-a` must cover `/tenant-a/notes.txt` and never
/// `/tenant-abc/...`. A prefix wired as a raw `starts_with` would silently
/// revert a neighbouring tenant's files, which is precisely the failure this
/// flag exists to prevent — so both the miss and the hit are asserted.
#[test]
fn revert_session_path_prefix_matches_on_directory_boundaries() {
    let ws = Ws::init();
    let agent = ws.actor(&["claude", "--agent", "--model", "m"]);
    let a = agent.to_string();

    // One session touching two same-prefix-looking trees. `write` creates the
    // parents itself, so no explicit mkdir is needed.
    ws.write_as("/tenant-a/notes.txt", agent, "in a\n")
        .expect_ok("write a");
    ws.write_as("/tenant-abc/notes.txt", agent, "in abc\n")
        .expect_ok("write abc");

    // A prefix that matches no session file changes nothing — and still exits 0,
    // so "0 file(s) changed" is the only signal a caller gets.
    ws.run(&[
        "revert-session",
        "--actor",
        &a,
        "--session",
        "1",
        "--path-prefix",
        "/tenant-b",
    ])
    .expect_ok("revert-session with a non-matching prefix")
    .stdout_has("0 file(s) changed");

    // `/tenant-a` reverts session 1's file there — and must leave `/tenant-abc`
    // (written in session 2) completely alone regardless.
    ws.run(&[
        "revert-session",
        "--actor",
        &a,
        "--session",
        "1",
        "--path-prefix",
        "/tenant-a",
    ])
    .expect_ok("revert-session")
    .stdout_has("1 file(s) changed")
    .stdout_has("/tenant-a/notes.txt");

    assert_eq!(ws.run(&["read", "/tenant-a/notes.txt"]).stdout, "");
    assert_eq!(
        ws.run(&["read", "/tenant-abc/notes.txt"]).stdout,
        "in abc\n"
    );
}

/// The `Propose` write policy is enforced in the engine, but `revert-session`
/// reaches an *unattributed* engine call — so the CLI has to run the check
/// itself, via the `--by` actor. Without `--by` there is nothing to check
/// against, which makes this the one place where omitting a flag is the
/// difference between "gated" and "ungated": worth pinning explicitly.
#[test]
fn revert_session_by_a_propose_only_actor_is_denied() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let agent = ws.actor(&["claude", "--agent", "--model", "m"]);
    ws.write_as("/doc.txt", agent, "agent line\n")
        .expect_ok("write");

    ws.run(&["write-policy", &agent.to_string(), "propose"])
        .expect_ok("write-policy");

    // A propose-only actor cannot revert anyone — including itself.
    ws.run(&[
        "revert-session",
        "--actor",
        &agent.to_string(),
        "--session",
        "1",
        "--by",
        &agent.to_string(),
    ])
    .expect_err("revert-session --by a propose-only actor")
    .stderr_has("propose-only");

    // Denied means *nothing happened*, not "half reverted".
    assert_eq!(ws.run(&["read", "/doc.txt"]).stdout, "agent line\n");

    // A direct-write actor may. This also proves `--by` is the permission
    // subject while `--actor` stays the target: alice reverts the agent's work.
    ws.run(&[
        "revert-session",
        "--actor",
        &agent.to_string(),
        "--session",
        "1",
        "--by",
        &alice.to_string(),
    ])
    .expect_ok("revert-session --by a direct actor")
    .stdout_has("1 file(s) changed");
    assert_eq!(ws.run(&["read", "/doc.txt"]).stdout, "");
}

/// `checkout` rewrites the working tree, so a wrong branch argument costs
/// uncommitted-looking state. Two things are pinned: the tree really is
/// rematerialized at the target branch's content (not merely a HEAD pointer
/// move), and an unknown branch fails without touching anything.
#[test]
fn checkout_rematerializes_the_working_tree() {
    let ws = Ws::init();
    ws.run_in(&["write", "/f.txt"], "on main\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "base"]).expect_ok("commit");

    ws.run(&["branch", "feature"]).expect_ok("branch");
    ws.run(&["checkout", "feature"])
        .expect_ok("checkout")
        .stdout_has("switched to branch feature");
    ws.run_in(&["write", "/f.txt"], "on feature\n")
        .expect_ok("write");
    ws.run_in(&["write", "/only-on-feature.txt"], "x\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "feature work"])
        .expect_ok("commit");

    // Back to main: the file reverts and the feature-only file disappears.
    ws.run(&["checkout", "main"]).expect_ok("checkout main");
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "on main\n");
    ws.run(&["read", "/only-on-feature.txt"])
        .expect_err("a feature-only file must not exist on main");

    // An unknown branch is a clean failure that leaves the tree where it was.
    ws.run(&["checkout", "no-such-branch"])
        .expect_err("checkout of an unknown branch")
        .stderr_has("not found");
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "on main\n");
    ws.run(&["branch"]).expect_ok("branch").stdout_has("* main");
}

/// The metadata DB is the half that cannot be rebuilt from content (blame, the
/// audit log, actors, uncommitted edits live only there), so `backup` is the
/// command standing between a user and unrecoverable loss. It must produce a
/// real SQLite snapshot and it must **refuse to overwrite** an existing file —
/// clobbering yesterday's backup with today's is itself the data-loss event.
#[test]
fn backup_snapshots_the_metadata_db_and_refuses_to_clobber() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    ws.write_as("/f.txt", alice, "attributed\n")
        .expect_ok("write");

    let dest = ws.scratch("snap/meta-backup.db");
    let dest_s = dest.to_str().unwrap();
    ws.run(&["backup", dest_s])
        .expect_ok("backup")
        .stdout_has("sqlite online backup")
        // The caveat is the point of the command: content is elsewhere, blame is
        // only here. Losing that sentence loses the reason to run it.
        .stdout_has("metadata store only");

    // A real snapshot, not a touched placeholder: SQLite's file magic.
    let bytes = std::fs::read(&dest).expect("the snapshot file must exist");
    assert!(
        bytes.starts_with(b"SQLite format 3\0"),
        "backup must write an actual SQLite database"
    );
    assert!(bytes.len() > 4096, "snapshot is implausibly small");

    // Second run against the same destination must fail rather than overwrite.
    ws.run(&["backup", dest_s])
        .expect_err("backup onto an existing file")
        .stderr_has("already exists");
    assert_eq!(
        std::fs::read(&dest).unwrap().len(),
        bytes.len(),
        "the refused backup must not have truncated the existing snapshot"
    );
}

/// `fsck` without `--rebuild` is documented as read-only. An inverted flag here
/// would rewrite a healthy metadata DB from content on what the user believed
/// was a dry run — silently discarding blame and every uncommitted edit. So:
/// the dry run reports, says it is a dry run, and provably leaves the DB alone.
#[test]
fn fsck_dry_run_reports_without_rebuilding() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    ws.write_as("/a.txt", alice, "hello\n").expect_ok("write");
    ws.run(&["commit", "-m", "seed", "--author", "Dan"])
        .expect_ok("commit");
    // Uncommitted, attribution-only state — exactly what a stray rebuild eats.
    ws.write_as("/uncommitted.txt", alice, "not yet committed\n")
        .expect_ok("write");

    let before = std::fs::metadata(ws.meta_db()).unwrap().len();

    ws.run(&["fsck"])
        .expect_ok("fsck")
        .stdout_has("commit(s)")
        .stdout_has("1 branch(es) via ref mirror")
        .stdout_has("main")
        .stdout_has("(HEAD)")
        .stdout_has("dry run")
        // Nothing was rebuilt, so the rebuild-only lines must be absent.
        .stdout_lacks("rebuilt working tree");

    assert_eq!(
        std::fs::metadata(ws.meta_db()).unwrap().len(),
        before,
        "a dry-run fsck must not rewrite the metadata DB"
    );
    assert_eq!(
        ws.run(&["read", "/uncommitted.txt"]).stdout,
        "not yet committed\n"
    );
    ws.run(&["blame", "/a.txt"])
        .expect_ok("blame")
        .stdout_has("human:alice");
}

/// The disaster-recovery path: the content store is a self-describing Merkle DAG
/// with a mirrored ref table, so `fsck --rebuild` restores committed files,
/// directories, and branches onto a fresh DB from the bucket alone. This drives
/// it the way a user in trouble would — delete `meta.db`, run the command — and
/// checks both what comes back and the honest warning about what does not.
#[test]
fn fsck_rebuild_restores_a_workspace_after_metadata_loss() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    ws.write_as("/a.txt", alice, "hello\n").expect_ok("write");
    ws.run_in(&["write", "/sub/b.txt"], "deep\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "seed", "--author", "Dan"])
        .expect_ok("commit");

    // Lose the metadata half entirely.
    std::fs::remove_file(ws.meta_db()).unwrap();

    ws.run(&["fsck", "--rebuild"])
        .expect_ok("fsck --rebuild")
        .stdout_has("rebuilt working tree @ main")
        .stdout_has("1 dir(s), 2 file(s), 0 symlink(s)")
        // Say plainly what is *not* coming back, so nobody assumes blame did.
        .stdout_has("blame/attribution is not recoverable");

    // Committed content, the directory structure, the branch, and history are
    // all back from the content store alone.
    assert_eq!(ws.run(&["read", "/a.txt"]).stdout, "hello\n");
    assert_eq!(ws.run(&["read", "/sub/b.txt"]).stdout, "deep\n");
    ws.run(&["log"]).expect_ok("log").stdout_has("seed");
    ws.run(&["branch"]).expect_ok("branch").stdout_has("* main");

    // …and blame really is gone, as advertised. If this ever starts passing,
    // the warning above has become a lie and should be removed with it.
    let blame = ws.run(&["blame", "/a.txt"]).expect_ok("blame");
    assert!(
        blame.lines().is_empty(),
        "attribution is DB-only and cannot survive a rebuild, got: {:?}",
        blame.stdout
    );
}

/// An empty workspace must not look like a corrupt one. `fsck` there is a
/// legitimate first thing to run, and it should say "nothing to recover"
/// rather than erroring or reporting phantom branches.
#[test]
fn fsck_on_an_empty_workspace_reports_nothing_to_recover() {
    let ws = Ws::init();
    ws.run(&["fsck"])
        .expect_ok("fsck on an empty workspace")
        .stdout_has("scanned 0 object(s)")
        .stdout_has("no commits to recover");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Attribution
//
// A blame trail is only worth having if the identity behind each write is
// right. These tests check that the CLI's `--actor` really reaches an
// *attributed* engine call, and that the propose gate the CLI's own
// `write-policy` sets actually binds the CLI's own `write`.
// ─────────────────────────────────────────────────────────────────────────────

/// The headline claim of the whole system, through the shell: `--actor` on
/// `write` produces per-line, per-actor blame, and a human and an agent are
/// distinguishable in it. Guards the flag→`WriteCtx` wiring — an `--actor`
/// plumbed to the wrong argument, or dropped on the way to `write_or_propose`,
/// would leave a workspace whose blame quietly names the wrong author.
#[test]
fn write_with_an_actor_produces_per_line_blame() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let claude = ws.actor(&["claude", "--agent", "--model", "claude-opus-4-8"]);

    ws.write_as("/f.txt", alice, "l1\nl2\nl3\n")
        .expect_ok("alice write");
    let blame = ws.run(&["blame", "/f.txt"]).expect_ok("blame");
    assert_eq!(blame.lines(), vec!["   1-3     human:alice"]);

    // The agent edits only the middle line: blame must split into three ranges,
    // not re-attribute the whole file to the last writer.
    ws.write_as("/f.txt", claude, "l1\nCLAUDE\nl3\n")
        .expect_ok("agent write");
    let blame = ws.run(&["blame", "/f.txt"]).expect_ok("blame");
    assert_eq!(
        blame.lines(),
        vec![
            "   1       human:alice",
            "   2       agent:claude",
            "   3       human:alice",
        ]
    );
}

/// The same claim, cross-checked *behind* the CLI's own rendering. `blame`
/// printing plausible text is not proof — if `write --actor` and `blame` were
/// both wrong in the same direction the test above would still pass. So read
/// the op-log directly through the SDK: the append-only `edit_op` row is the
/// ground truth, and it must carry the right actor **and a real session id**
/// (the CLI opens one per invocation; a `WriteCtx::actor` with no session would
/// break `revert-session`, which addresses work by session).
#[tokio::test]
async fn cli_writes_land_in_the_edit_op_log_with_a_session() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    ws.write_as("/f.txt", alice, "hello\n").expect_ok("write");

    // The CLI subprocess has exited, so its SQLite database is ours to open.
    let sdk = origofs_sdk::Workspace::open_local(ws.meta_db(), ws.dir.join("cas"))
        .await
        .unwrap();

    let ops = sdk.edit_ops(alice, None).await.unwrap();
    assert_eq!(ops.len(), 1, "one attributed CLI write, one edit-op");
    assert_eq!(ops[0].actor_id, alice);
    assert_eq!(ops[0].path, "/f.txt");
    assert!(
        ops[0].session_id.is_some(),
        "`origofs write --actor` must open a session, not write bare-actor ops"
    );

    // And the actor registry got what `origofs actor` was told.
    let actor = sdk.get_actor(alice).await.unwrap().expect("alice exists");
    assert_eq!(actor.display_name, "alice");
    assert_eq!(actor.kind, origofs_sdk::ActorKind::Human);
}

/// `origofs actor` is the registration surface, and its flags decide whether an
/// edit is later shown as a person's or a machine's. `--agent`, `--model`, and
/// `--controller` all have to land: the controller link is the provenance chain
/// (which human launched this agent) and is invisible in every other output.
#[tokio::test]
async fn actor_registers_humans_agents_and_the_controller_link() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let claude = ws.actor(&[
        "claude",
        "--agent",
        "--model",
        "claude-opus-4-8",
        "--controller",
        &alice.to_string(),
    ]);
    assert_ne!(alice, claude, "each actor gets its own id");

    let sdk = origofs_sdk::Workspace::open_local(ws.meta_db(), ws.dir.join("cas"))
        .await
        .unwrap();
    let agent = sdk.get_actor(claude).await.unwrap().expect("claude exists");
    assert_eq!(agent.kind, origofs_sdk::ActorKind::Agent);
    assert_eq!(agent.display_name, "claude");
    assert_eq!(
        agent.agent_model.as_deref(),
        Some("claude-opus-4-8"),
        "--model must reach the actor row"
    );
    assert_eq!(
        agent.controller_actor_id,
        Some(alice),
        "--controller is the provenance chain and has nothing else to prove it"
    );

    // Without `--agent`, `--model` must not silently turn a person into a bot.
    let bob = ws.actor(&["bob", "--model", "gpt-9"]);
    let bob = sdk.get_actor(bob).await.unwrap().unwrap();
    assert_eq!(bob.kind, origofs_sdk::ActorKind::Human);
}

/// The regression this test exists for is called out in the CLI source itself:
/// `origofs write` once called the raw attributed `write_as`, which is exempt
/// from the §6 policy by construction — so `origofs write-policy <actor> propose`
/// had no effect on `origofs write`, and the CLI ignored the gate its own
/// subcommand had just set. The observable contract is that the working tree
/// does **not** move and a suggestion appears instead.
#[test]
fn write_policy_propose_routes_cli_writes_into_the_review_queue() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let agent = ws.actor(&["claude", "--agent", "--model", "m"]);
    ws.write_as("/f.txt", alice, "original\n")
        .expect_ok("alice write");

    ws.run(&["write-policy", &agent.to_string(), "propose"])
        .expect_ok("write-policy")
        .stdout_has("write policy set to propose");

    // A propose-only write *succeeds* (exit 0) but queues rather than lands —
    // the exit code must not be an error, or every agent harness breaks.
    ws.write_as("/f.txt", agent, "agent version\n")
        .expect_ok("propose-only write")
        .stdout_has("propose-only")
        .stdout_has("queued suggestion #1");

    // The whole point: the working tree is untouched.
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "original\n");
    ws.run(&["suggestions"])
        .expect_ok("suggestions")
        .stdout_has("pending")
        .stdout_has(&format!("actor={agent}"))
        .stdout_has("/f.txt");

    // A propose-only actor must not be able to create a file either — a "new
    // path" write is still a write.
    ws.write_as("/brand-new.txt", agent, "sneaky\n")
        .expect_ok("propose-only write to a new path")
        .stdout_has("queued suggestion #2");
    ws.run(&["read", "/brand-new.txt"])
        .expect_err("a proposed new file must not exist yet");

    // Back to `direct` and the same write lands immediately: the policy is a
    // live setting, not a one-way door.
    ws.run(&["write-policy", &agent.to_string(), "direct"])
        .expect_ok("write-policy direct")
        .stdout_has("write policy set to direct");
    ws.write_as("/f.txt", agent, "agent version\n")
        .expect_ok("direct write")
        .stdout_lacks("queued suggestion");
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "agent version\n");
}

/// `write-policy` is actor-agnostic and takes its policy as a free string, so a
/// typo must be rejected loudly. Silently ignoring `propse` would leave an agent
/// the operator believed was gated writing directly forever.
#[test]
fn write_policy_rejects_an_unknown_policy() {
    let ws = Ws::init();
    let agent = ws.actor(&["claude", "--agent", "--model", "m"]);
    ws.run(&["write-policy", &agent.to_string(), "propse"])
        .expect_err("a misspelled policy")
        .stderr_has("unknown write policy");

    // …and the actor is still on the default `direct`, i.e. the failed call did
    // not half-apply something.
    ws.write_as("/f.txt", agent, "lands\n")
        .expect_ok("write")
        .stdout_lacks("queued suggestion");
}

/// Review, end to end through the CLI. Two invariants that only show up here:
/// an accepted edit lands **attributed to the original author**, not to the
/// approver who ran the command; and a reviewer must differ from the author, so
/// an agent cannot rubber-stamp its own proposal by calling `accept` itself.
#[test]
fn accept_lands_the_edit_attributed_to_the_author_not_the_approver() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let claude = ws.actor(&["claude", "--agent", "--model", "m"]);
    ws.write_as("/f.txt", alice, "original\n")
        .expect_ok("alice write");

    ws.run_in(
        &[
            "suggest",
            "/f.txt",
            "--actor",
            &claude.to_string(),
            "--summary",
            "tweak it",
        ],
        "agent version\n",
    )
    .expect_ok("suggest")
    .stdout_has("suggestion #1 created");

    // The reviewer needs to see what they are approving.
    ws.run(&["suggestion-diff", "1"])
        .expect_ok("suggestion-diff")
        .stdout_has("-original")
        .stdout_has("+agent version");

    // Self-approval is refused: the reviewer must differ from the author, which
    // is what stops the review path degenerating into a slower direct write.
    ws.run(&["accept", "1", "--actor", &claude.to_string()])
        .expect_err("an author accepting their own suggestion")
        .stderr_has("cannot be accepted by its author");
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "original\n");

    // Alice approves — and blame credits the agent, not alice.
    ws.run(&["accept", "1", "--actor", &alice.to_string()])
        .expect_ok("accept")
        .stdout_has("accepted suggestion #1");
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "agent version\n");
    ws.run(&["blame", "/f.txt"])
        .expect_ok("blame")
        .stdout_has("agent:claude")
        .stdout_lacks("human:alice");

    // A suggestion is single-use: re-accepting must not replay the write.
    ws.run(&["accept", "1", "--actor", &alice.to_string()])
        .expect_err("re-accepting a settled suggestion")
        .stderr_has("already");
}

/// `reject` must settle the proposal without touching the tree, and
/// `suggest --delete` proposes a *removal* — the destructive half of the review
/// path, where a mis-wired `--delete` flag means accepting a review deletes a
/// file nobody meant to delete.
#[test]
fn reject_leaves_the_tree_alone_and_suggest_delete_proposes_a_removal() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let claude = ws.actor(&["claude", "--agent", "--model", "m"]);
    ws.write_as("/f.txt", alice, "original\n")
        .expect_ok("write");

    ws.run_in(
        &["suggest", "/f.txt", "--actor", &claude.to_string()],
        "rejected version\n",
    )
    .expect_ok("suggest");
    ws.run(&["reject", "1", "--actor", &alice.to_string()])
        .expect_ok("reject")
        .stdout_has("rejected suggestion #1");
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "original\n");
    ws.run(&["suggestions", "--status", "rejected"])
        .expect_ok("suggestions --status rejected")
        .stdout_has("#1")
        .stdout_has("rejected");

    // A proposed deletion does not delete until accepted…
    ws.run(&[
        "suggest",
        "/f.txt",
        "--actor",
        &claude.to_string(),
        "--delete",
        "--summary",
        "drop it",
    ])
    .expect_ok("suggest --delete")
    .stdout_has("suggestion #2 created");
    assert_eq!(ws.run(&["read", "/f.txt"]).stdout, "original\n");

    // …and then it does.
    ws.run(&["accept", "2", "--actor", &alice.to_string()])
        .expect_ok("accept the deletion");
    ws.run(&["read", "/f.txt"])
        .expect_err("an accepted delete-suggestion must remove the file")
        .stderr_has("not found");
}

/// `write --from FILE --actor` is a distinct code path: attribution and
/// streaming were once mutually exclusive, so a large file could only be written
/// by giving up the attribution that is the point of the system. The CLI now
/// streams straight from the file when the actor may write directly, and falls
/// back to buffering when it may not (a proposal has to hold its bytes). Both
/// halves of that branch are exercised here, because they diverge *inside* one
/// match arm and nothing else would catch a swap.
#[test]
fn write_from_a_file_is_attributed_and_still_honours_the_propose_gate() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let src = ws.scratch("payload.txt");
    std::fs::write(&src, "from a file\n").unwrap();
    let src_s = src.to_str().unwrap();

    // The streaming branch: direct-write actor, `--from`.
    ws.run(&[
        "write",
        "/streamed.txt",
        "--from",
        src_s,
        "--actor",
        &alice.to_string(),
    ])
    .expect_ok("write --from --actor");
    assert_eq!(ws.run(&["read", "/streamed.txt"]).stdout, "from a file\n");
    ws.run(&["blame", "/streamed.txt"])
        .expect_ok("blame")
        .stdout_has("human:alice");

    // The buffering branch: same flags, propose-only actor — must queue, not
    // stream past the gate.
    ws.run(&["write-policy", &alice.to_string(), "propose"])
        .expect_ok("write-policy");
    ws.run(&[
        "write",
        "/proposed.txt",
        "--from",
        src_s,
        "--actor",
        &alice.to_string(),
    ])
    .expect_ok("propose-only write --from")
    .stdout_has("queued suggestion");
    ws.run(&["read", "/proposed.txt"])
        .expect_err("a propose-only --from write must not land");

    // `--from` on a missing file is a clean error, not a panic or an empty write.
    ws.run(&["write", "/nope.txt", "--from", "/definitely/not/here.txt"])
        .expect_err("write --from a missing file");
    ws.run(&["read", "/nope.txt"])
        .expect_err("a failed --from must not create the target");
}

/// The complement of the attributed path: plain `write` (no `--actor`) is
/// deliberately unattributed. If it ever started inventing an actor, every
/// blame trail would gain phantom authorship — so the *absence* of blame is
/// itself a contract worth pinning.
#[test]
fn write_without_an_actor_records_no_blame() {
    let ws = Ws::init();
    ws.run_in(&["write", "/anon.txt"], "who wrote this\n")
        .expect_ok("write");
    assert_eq!(ws.run(&["read", "/anon.txt"]).stdout, "who wrote this\n");

    let blame = ws.run(&["blame", "/anon.txt"]).expect_ok("blame");
    assert!(
        blame.lines().is_empty(),
        "an unattributed write must produce no blame ranges, got: {:?}",
        blame.stdout
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. The core loop
// ─────────────────────────────────────────────────────────────────────────────

/// `init` must create a usable workspace and be safe to re-run — people type it
/// again when unsure, and an `init` that reset an existing workspace would be
/// the worst bug in the tree.
#[test]
fn init_creates_a_workspace_and_is_idempotent() {
    let ws = Ws::bare();
    ws.run(&["init"])
        .expect_ok("init")
        .stdout_has("initialized origofs workspace");
    assert!(ws.meta_db().is_file(), "init must create meta.db");
    assert!(ws.dir.join("cas").is_dir(), "init must create cas/");

    ws.run_in(&["write", "/f.txt"], "content\n")
        .expect_ok("write");
    ws.run(&["init"]).expect_ok("re-init");
    assert_eq!(
        ws.run(&["read", "/f.txt"]).stdout,
        "content\n",
        "a second init must not wipe the workspace"
    );
}

/// `--workspace` defaults to `.origofs` in the current directory. That default is
/// what every README example relies on, and it is invisible in the source of
/// any single subcommand — it lives once on the top-level `Cli` struct, so a
/// rename there would silently point every un-flagged invocation somewhere new.
#[test]
fn the_workspace_defaults_to_dot_origofs_in_the_cwd() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();

    raw(Some(cwd), &["init"], None)
        .expect_ok("init with no --workspace")
        .stdout_has(".origofs");
    assert!(cwd.join(".origofs/meta.db").is_file());

    raw(Some(cwd), &["write", "/f.txt"], Some(b"default ws\n")).expect_ok("write");
    let out = raw(Some(cwd), &["read", "/f.txt"], None).expect_ok("read");
    assert_eq!(out.stdout, "default ws\n");
}

/// A CLI-only convenience with no engine equivalent: `write` creates the parent
/// directories of its target. Without it every example in the README would need
/// a paired `mkdir`, and losing it would break scripts silently (the write would
/// just start failing). Path traversal is checked alongside, because the same
/// path-splitting code is what would have to mishandle `..` for a poisoned name
/// to reach the metadata store.
#[test]
fn write_creates_missing_parents_but_never_escapes_the_root() {
    let ws = Ws::init();
    ws.run_in(&["write", "/deep/nest/f.txt"], "x\n")
        .expect_ok("write into a missing tree");
    assert_eq!(
        ws.run(&["ls", "/deep"]).expect_ok("ls").lines(),
        vec!["dir\tnest"]
    );
    assert_eq!(ws.run(&["read", "/deep/nest/f.txt"]).stdout, "x\n");

    // `..` is refused at the metadata boundary, so it can never be *stored* —
    // which is what stops it escaping later during host materialization.
    ws.run_in(&["write", "/../escape.txt"], "nope\n")
        .expect_err("a traversing path")
        .stderr_has("invalid path");
    ws.run_in(&["mkdir", "/a/../../b"], "")
        .expect_err("a traversing mkdir");
}

/// The loop from the crate docs, in one test: init → write → read → commit →
/// log. It is the path every user takes first, and it crosses four engine calls
/// whose output formats (`[branch abcdef123456] message`, the 12-hex log line)
/// are themselves the CLI's contract with anyone scripting it.
#[test]
fn the_core_loop_writes_reads_commits_and_logs() {
    let ws = Ws::init();
    ws.run_in(&["write", "/notes/a.txt"], "first version\n")
        .expect_ok("write");
    assert_eq!(ws.run(&["read", "/notes/a.txt"]).stdout, "first version\n");

    let commit = ws
        .run(&[
            "commit",
            "-m",
            "initial",
            "--author",
            "Dan <dan@example.com>",
        ])
        .expect_ok("commit");
    commit.stdout_has("[main ").stdout_has("] initial");

    // The abbreviated hash is 12 hex chars — the format `log` echoes back.
    let hash = commit
        .trimmed()
        .trim_start_matches("[main ")
        .split(']')
        .next()
        .unwrap()
        .to_string();
    assert_eq!(hash.len(), 12, "commit prints a 12-char abbreviated hash");
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

    let log = ws.run(&["log"]).expect_ok("log");
    assert_eq!(log.lines().len(), 1);
    log.stdout_has(&hash)
        .stdout_has("Dan <dan@example.com>")
        .stdout_has("initial");

    // A second commit stacks on top, newest first.
    ws.run_in(&["write", "/notes/a.txt"], "second version\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "follow-up"]).expect_ok("commit");
    let log = ws.run(&["log"]).expect_ok("log");
    let lines = log.lines();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("follow-up"), "log is newest-first");
    assert!(lines[1].contains("initial"));
    // `--author` defaults to `origofs`, and the default must not leak into the
    // commit that *was* given an author.
    assert!(lines[0].contains("origofs"));
    assert!(lines[1].contains("Dan <dan@example.com>"));
}

/// `diff` has two distinct shapes behind one subcommand: a changed-path listing,
/// and — with `--path` — one file's unified patch. The argument order
/// (`from`, then `to`) decides the sign of every hunk, so a swap would render
/// every diff backwards while still looking perfectly plausible.
#[test]
fn diff_lists_changed_paths_and_renders_one_files_patch() {
    let ws = Ws::init();
    ws.run_in(&["write", "/f.txt"], "l1\nl2\nl3\n")
        .expect_ok("write");
    ws.run_in(&["write", "/untouched.txt"], "same\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "base"]).expect_ok("commit");

    ws.run(&["branch", "feature"]).expect_ok("branch");
    ws.run(&["checkout", "feature"]).expect_ok("checkout");
    ws.run_in(&["write", "/f.txt"], "l1\nCHANGED\nl3\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "edit"]).expect_ok("commit");

    // Path listing: only the file that moved.
    let listed = ws.run(&["diff", "main", "feature"]).expect_ok("diff");
    assert_eq!(listed.lines(), vec!["M /f.txt"]);

    // Directional: main→feature adds CHANGED and removes l2.
    let patch = ws
        .run(&["diff", "main", "feature", "--path", "/f.txt"])
        .expect_ok("diff --path");
    patch.stdout_has("-l2").stdout_has("+CHANGED");

    // Reversed arguments must reverse the patch — this is the assertion that
    // actually catches a `from`/`to` swap.
    ws.run(&["diff", "feature", "main", "--path", "/f.txt"])
        .expect_ok("reversed diff")
        .stdout_has("+l2")
        .stdout_has("-CHANGED");

    // An unchanged path and identical refs each get their own message rather
    // than empty output a script would misread as an error.
    ws.run(&["diff", "main", "feature", "--path", "/untouched.txt"])
        .expect_ok("diff of an unchanged path")
        .stdout_has("unchanged between main and feature");
    ws.run(&["diff", "main", "main"])
        .expect_ok("diff of a ref against itself")
        .stdout_has("no differences");

    // An unknown ref fails rather than silently diffing against nothing.
    ws.run(&["diff", "main", "no-such-ref"])
        .expect_err("diff against an unknown ref")
        .stderr_has("not found");
}

/// `branch` is overloaded on arity: with a name it creates, without one it
/// lists — and the listing's `* ` marker is the only way a user knows where they
/// are. A marker computed against the wrong branch is a genuinely dangerous
/// display bug, since the next `commit` goes somewhere else.
#[test]
fn branch_creates_with_a_name_and_lists_with_the_current_marker() {
    let ws = Ws::init();
    ws.run_in(&["write", "/f.txt"], "x\n").expect_ok("write");
    ws.run(&["commit", "-m", "base"]).expect_ok("commit");

    let listing = ws.run(&["branch"]).expect_ok("branch");
    assert_eq!(listing.lines().len(), 1);
    listing.stdout_has("* main");

    ws.run(&["branch", "feature"])
        .expect_ok("branch feature")
        .stdout_has("created branch feature");

    // Created, but *not* switched to — creating a branch must not move HEAD.
    let listing = ws.run(&["branch"]).expect_ok("branch");
    assert_eq!(listing.lines().len(), 2);
    listing.stdout_has("* main").stdout_has("  feature");

    ws.run(&["checkout", "feature"]).expect_ok("checkout");
    let listing = ws.run(&["branch"]).expect_ok("branch");
    listing.stdout_has("* feature").stdout_has("  main");
}

/// Merge has three reportable outcomes and the CLI must distinguish them,
/// because they demand different next actions: up-to-date (do nothing),
/// fast-forward (done), and conflicts (**resolve, then commit**). The conflict
/// case leaves diff3 markers in the working tree, so a user who misreads the
/// message commits them.
#[test]
fn merge_reports_up_to_date_fast_forward_and_conflicts() {
    let ws = Ws::init();
    ws.run_in(&["write", "/c.txt"], "base\n").expect_ok("write");
    ws.run(&["commit", "-m", "base"]).expect_ok("commit");
    ws.run(&["branch", "feature"]).expect_ok("branch");

    // Nothing has diverged yet.
    ws.run(&["merge", "feature"])
        .expect_ok("merge")
        .stdout_has("already up to date");

    // Only feature moves → fast-forward.
    ws.run(&["checkout", "feature"]).expect_ok("checkout");
    ws.run_in(&["write", "/c.txt"], "feature\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "feature"]).expect_ok("commit");
    ws.run(&["checkout", "main"]).expect_ok("checkout");
    ws.run(&["merge", "feature"])
        .expect_ok("merge")
        .stdout_has("fast-forward to");
    assert_eq!(ws.run(&["read", "/c.txt"]).stdout, "feature\n");
    ws.run(&["conflicts"])
        .expect_ok("conflicts")
        .stdout_lacks("/c.txt");

    // Both sides move on the same lines → conflict.
    ws.run(&["branch", "other"]).expect_ok("branch");
    ws.run(&["checkout", "other"]).expect_ok("checkout");
    ws.run_in(&["write", "/c.txt"], "theirs\n")
        .expect_ok("write");
    ws.run(&["commit", "-m", "theirs"]).expect_ok("commit");
    ws.run(&["checkout", "main"]).expect_ok("checkout");
    ws.run_in(&["write", "/c.txt"], "ours\n").expect_ok("write");
    ws.run(&["commit", "-m", "ours"]).expect_ok("commit");

    // NOTE: a conflicted merge exits **0** today. The message is the only
    // signal, so it is what this pins; see the `merge --message` case below for
    // the flag wiring.
    ws.run(&["merge", "other", "--message", "custom merge"])
        .expect_ok("merge with conflicts")
        .stdout_has("merge stopped with 1 conflict(s)")
        .stdout_has("content /c.txt");

    // The conflict is recorded for `origofs conflicts`, and the working tree
    // holds diff3 markers naming all three sides.
    ws.run(&["conflicts"])
        .expect_ok("conflicts")
        .stdout_has("content\t/c.txt");
    let body = ws.run(&["read", "/c.txt"]).expect_ok("read").stdout;
    for marker in ["<<<<<<< ours", "||||||| original", ">>>>>>> theirs"] {
        assert!(body.contains(marker), "expected {marker:?} in:\n{body}");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Read-only reporting
//
// Nothing here can destroy data, but everything here is what a user reads
// *before* deciding to. Wrong output drives wrong decisions.
// ─────────────────────────────────────────────────────────────────────────────

/// `ls` and `stat` are the two ways to look at the tree, and both render
/// tab-separated / `key=value` output that scripts parse. `stat`'s mode digits
/// in particular are the only place the CLI shows a file-vs-directory bit
/// numerically.
#[test]
fn ls_and_stat_render_kinds_modes_and_sizes() {
    let ws = Ws::init();
    ws.run_in(&["write", "/dir/f.txt"], "hello\n")
        .expect_ok("write");

    // `ls` defaults to `/` when given no path.
    let root = ws.run(&["ls"]).expect_ok("ls with no argument");
    assert_eq!(root.lines(), vec!["dir\tdir"]);
    assert_eq!(
        ws.run(&["ls", "/dir"]).expect_ok("ls").lines(),
        vec!["file\tf.txt"]
    );

    let file = ws.run(&["stat", "/dir/f.txt"]).expect_ok("stat");
    file.stdout_has("kind=file")
        .stdout_has("mode=100644")
        .stdout_has("size=6")
        .stdout_has("nlink=1");
    ws.run(&["stat", "/dir"])
        .expect_ok("stat a directory")
        .stdout_has("kind=dir")
        .stdout_has("mode=40755");

    // `ls` on a file is an error, not an empty listing that reads as "no files".
    ws.run(&["ls", "/dir/f.txt"])
        .expect_err("ls on a file")
        .stderr_has("not a directory");
    ws.run(&["stat", "/missing"])
        .expect_err("stat on a missing path")
        .stderr_has("not found");
}

/// `status` answers "what would I be committing" — the question asked right
/// before the destructive step. Both the sigils and, crucially, the clean-tree
/// sentence matter: a clean tree printing *nothing* would be indistinguishable
/// from a crashed command.
#[test]
fn status_reports_sigils_and_says_so_when_clean() {
    let ws = Ws::init();
    ws.run_in(&["write", "/kept.txt"], "v1\n")
        .expect_ok("write");
    ws.run_in(&["write", "/deleted.txt"], "bye\n")
        .expect_ok("write");
    ws.run(&["status"])
        .expect_ok("status")
        .stdout_has("A /kept.txt")
        .stdout_has("A /deleted.txt");

    ws.run(&["commit", "-m", "base"]).expect_ok("commit");
    ws.run(&["status"])
        .expect_ok("status")
        .stdout_has("clean (working tree matches HEAD)");

    ws.run_in(&["write", "/kept.txt"], "v2\n")
        .expect_ok("write");
    ws.run_in(&["write", "/added.txt"], "new\n")
        .expect_ok("write");
    ws.run(&["rm", "/deleted.txt"]).expect_ok("rm");
    let status = ws.run(&["status"]).expect_ok("status");
    status
        .stdout_has("M /kept.txt")
        .stdout_has("A /added.txt")
        .stdout_has("D /deleted.txt")
        .stdout_lacks("clean (");
    assert_eq!(
        status.lines().len(),
        3,
        "one line per change, no duplicates"
    );
}

/// `suggestions` is the review queue, and its two filters decide what a reviewer
/// sees. A filter wired to the wrong field would hide pending work — the queue
/// looks empty and proposals rot — so both the positive and negative cases of
/// each filter are checked, plus the empty-queue sentence.
#[test]
fn suggestions_filters_by_status_and_path() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    let claude = ws.actor(&["claude", "--agent", "--model", "m"]);
    ws.write_as("/a.txt", alice, "a\n").expect_ok("write");
    ws.write_as("/b.txt", alice, "b\n").expect_ok("write");

    ws.run(&["suggestions"])
        .expect_ok("suggestions on an empty queue")
        .stdout_has("no suggestions");

    let c = claude.to_string();
    ws.run_in(&["suggest", "/a.txt", "--actor", &c], "a2\n")
        .expect_ok("suggest a");
    ws.run_in(&["suggest", "/b.txt", "--actor", &c], "b2\n")
        .expect_ok("suggest b");
    ws.run(&["reject", "1", "--actor", &alice.to_string()])
        .expect_ok("reject");

    assert_eq!(
        ws.run(&["suggestions"])
            .expect_ok("suggestions")
            .lines()
            .len(),
        2,
        "unfiltered lists every suggestion regardless of status"
    );

    let pending = ws
        .run(&["suggestions", "--status", "pending"])
        .expect_ok("--status pending");
    assert_eq!(pending.lines().len(), 1);
    pending.stdout_has("/b.txt").stdout_lacks("/a.txt");

    let by_path = ws
        .run(&["suggestions", "--path", "/a.txt"])
        .expect_ok("--path");
    assert_eq!(by_path.lines().len(), 1);
    by_path.stdout_has("rejected").stdout_has("/a.txt");

    ws.run(&["suggestions", "--path", "/nothing-here.txt"])
        .expect_ok("--path with no matches")
        .stdout_has("no suggestions");

    // A typo'd status must not quietly degrade to "no filter" — that would show
    // a reviewer everything and read as if the filter worked.
    ws.run(&["suggestions", "--status", "pendign"])
        .expect_err("an unknown status")
        .stderr_has("unknown status");
}

/// Locks are the LFS-style guard for binaries that cannot be three-way merged,
/// so `--owner` is the whole mechanism: it decides who holds the lock and who
/// may release it. Note both contended cases report on **stdout with exit 0**;
/// that is pinned here as current behavior, because it means a script must read
/// the text rather than the status.
#[test]
fn lock_and_unlock_are_owner_scoped() {
    let ws = Ws::init();
    ws.run_in(&["write", "/binary.bin"], "data\n")
        .expect_ok("write");

    ws.run(&["locks"])
        .expect_ok("locks")
        .stdout_lacks("/binary.bin");

    ws.run(&["lock", "/binary.bin", "--owner", "alice"])
        .expect_ok("lock")
        .stdout_has("locked /binary.bin");
    assert_eq!(
        ws.run(&["locks"]).expect_ok("locks").lines(),
        vec!["alice\t/binary.bin"]
    );

    // Someone else cannot take it, and cannot release it either.
    ws.run(&["lock", "/binary.bin", "--owner", "bob"])
        .expect_ok("a contended lock still exits 0")
        .stdout_has("already locked");
    ws.run(&["unlock", "/binary.bin", "--owner", "bob"])
        .expect_ok("a foreign unlock still exits 0")
        .stdout_has("not your lock");
    assert_eq!(
        ws.run(&["locks"]).expect_ok("locks").lines(),
        vec!["alice\t/binary.bin"],
        "a refused steal must not have changed the holder"
    );

    ws.run(&["unlock", "/binary.bin", "--owner", "alice"])
        .expect_ok("unlock")
        .stdout_has("unlocked /binary.bin");
    assert!(ws.run(&["locks"]).expect_ok("locks").lines().is_empty());
}

/// `schema-version` is what an operator reads before a deploy, and `migrate` is
/// what they run after. On a freshly opened workspace the two must agree — an
/// `open` already migrates — so the honest answer here is "already at vN,
/// nothing to apply", and *no* upgrade nag.
#[test]
fn schema_version_agrees_with_migrate_on_a_fresh_workspace() {
    let ws = Ws::init();
    let version = ws
        .run(&["schema-version"])
        .expect_ok("schema-version")
        .stdout_has("schema version: v")
        .stdout_has("this binary knows up to v")
        // A fresh workspace is never behind or ahead of the binary that made it.
        .stdout_lacks("run `origofs migrate`")
        .stdout_lacks("NEWER than this binary")
        .trimmed()
        .to_string();

    // The two numbers in that line are the same version, printed twice.
    let after = |prefix: &str| -> String {
        let rest = version
            .split_once(prefix)
            .unwrap_or_else(|| panic!("expected {prefix:?} in {version:?}"))
            .1;
        rest.chars().take_while(char::is_ascii_digit).collect()
    };
    let current = after("schema version: v");
    let latest = after("knows up to v");
    assert!(!current.is_empty() && !latest.is_empty(), "{version:?}");
    assert_eq!(current, latest, "a fresh workspace is at the latest schema");

    ws.run(&["migrate"])
        .expect_ok("migrate")
        .stdout_has(&format!("schema already at v{latest}"))
        .stdout_has("nothing to apply");
}

/// `presence` shows *live* collaborators, and the CLI is not one: each
/// invocation opens a session and exits without ever heartbeating. So an empty
/// listing is correct, and the regression it guards is the opposite — presence
/// starting to report every past CLI session as an active collaborator, which
/// would make the feature useless in exactly the multi-writer setting it exists
/// for. The change feed (`watch`), by contrast, *does* record that write.
#[test]
fn presence_is_empty_without_heartbeats_but_the_change_feed_is_not() {
    let ws = Ws::init();
    let alice = ws.actor(&["alice"]);
    ws.write_as("/f.txt", alice, "x\n").expect_ok("write");

    let presence = ws.run(&["presence"]).expect_ok("presence");
    assert!(
        presence.lines().is_empty(),
        "a CLI session never heartbeats, so it is not present: {:?}",
        presence.stdout
    );
    ws.run(&["presence", "--window", "3600"])
        .expect_ok("presence --window");

    // The write is nonetheless on the change feed, attributed.
    let feed = ws.run(&["watch"]).expect_ok("watch");
    assert_eq!(feed.lines().len(), 1);
    feed.stdout_has("write")
        .stdout_has(&format!("actor:{alice}"))
        .stdout_has("/f.txt");

    // `--since` is a cursor, not a count: past the last seq the feed is empty.
    let seq: i64 = feed.trimmed().split('\t').next().unwrap().parse().unwrap();
    let after = ws
        .run(&["watch", "--since", &seq.to_string()])
        .expect_ok("watch --since");
    assert!(
        after.lines().is_empty(),
        "--since must exclude the event it names, got: {:?}",
        after.stdout
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-cutting CLI behavior
// ─────────────────────────────────────────────────────────────────────────────

/// A usage error and a runtime error must not look alike. clap exits **2** for
/// an unparseable command line; `main`'s `anyhow::Result` exits **1** for
/// everything the engine refuses. A script distinguishing "I called it wrong"
/// from "it said no" relies on that, and it is easy to break by, say, catching
/// clap's error and re-raising it through `anyhow`.
#[test]
fn usage_errors_and_runtime_errors_have_distinct_exit_codes() {
    let ws = Ws::init();

    let unknown = ws.run(&["definitely-not-a-command"]);
    assert_eq!(unknown.code, Some(2), "clap usage error");
    assert!(unknown.stderr.contains("unrecognized subcommand"));

    // A missing required argument is likewise a usage error.
    assert_eq!(ws.run(&["accept", "1"]).code, Some(2), "missing --actor");
    // …and a non-integer where an id is expected.
    assert_eq!(
        ws.run(&["accept", "1", "--actor", "alice"]).code,
        Some(2),
        "--actor is typed as an integer, so clap rejects a name"
    );

    // Whereas an engine refusal is exit 1.
    ws.run(&["read", "/nope.txt"])
        .expect_err("a runtime error is exit 1");

    // `--help` succeeds and lists the subcommands (the docker CI job's only
    // check, kept here so it is covered by `cargo test` too).
    raw(None, &["--help"], None)
        .expect_ok("--help")
        .stdout_has("Usage: origofs")
        .stdout_has("Commands:");
}

/// The library emits `tracing` records; the CLI is the only thing that renders
/// them, and it must render them to **stderr**. `origofs read` writes raw file
/// bytes to stdout and `origofs mcp` speaks JSON-RPC there, so a single log line
/// on stdout corrupts a data channel. `--log-format json` is checked in the same
/// test because it is the mode a log pipeline uses, where the corruption would
/// be silent.
#[test]
fn tracing_output_stays_on_stderr_in_both_log_formats() {
    let ws = Ws::init();
    ws.run_in(&["write", "/f.txt"], "payload\n")
        .expect_ok("write");

    for format in ["text", "json"] {
        // `info` is the CLI's own default level; force it back on (the harness
        // otherwise quiets it) so there is something to misplace.
        let mut cmd = Command::new(BIN);
        let out = cmd
            .args([
                "--workspace",
                ws.dir.to_str().unwrap(),
                "--log-format",
                format,
                "read",
                "/f.txt",
            ])
            .env("ORIGOFS_LOG", "info")
            .env("RUST_BACKTRACE", "0")
            .env_remove("RUST_LOG")
            .env_remove("ORIGOFS_ENCRYPTION_KEY")
            .output()
            .unwrap();
        assert!(out.status.success(), "read with --log-format {format}");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            "payload\n",
            "--log-format {format} must not put a log record on stdout"
        );
    }

    // And the JSON format really is JSON — one object per line, on stderr.
    let mut cmd = Command::new(BIN);
    let out = cmd
        .args([
            "--workspace",
            ws.dir.to_str().unwrap(),
            "--log-format",
            "json",
            "commit",
            "-m",
            "logged",
        ])
        .env("ORIGOFS_LOG", "info")
        .env("RUST_BACKTRACE", "0")
        .env_remove("RUST_LOG")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    let record = stderr
        .lines()
        .find(|l| l.starts_with('{'))
        .unwrap_or_else(|| panic!("expected a JSON log record on stderr, got:\n{stderr}"));
    assert!(record.contains("\"level\":\"INFO\""), "got {record}");
    assert!(record.ends_with('}'), "one JSON object per line: {record}");
}

/// `--config` selects the metadata/content backends. A malformed file must fail
/// before any workspace is opened, naming the file — the alternative is falling
/// back to the default local SQLite workspace, which would look like it worked
/// while writing to entirely the wrong store.
#[test]
fn a_malformed_config_file_is_refused_rather_than_ignored() {
    let ws = Ws::init();
    let bad = ws.scratch("bad-config.toml");
    std::fs::write(&bad, "this is not [valid toml {{{\n").unwrap();

    ws.run(&["--config", bad.to_str().unwrap(), "log"])
        .expect_err("a malformed --config")
        .stderr_has("bad-config.toml");

    let missing = ws.scratch("no-such-config.toml");
    ws.run(&["--config", missing.to_str().unwrap(), "log"])
        .expect_err("a missing --config file");
}

// ── info / bench (issue #118) ────────────────────────────────────────────────

/// Deterministic, high-entropy bytes. Entropy matters: a low-entropy body gives
/// FastCDC no cut points, so every chunk runs to `MAX_CHUNK` and a test written
/// over it would be asserting about a file that chunks nothing like a real one.
fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// The numbers have to be right for a file whose layout the test already knows.
/// A body under `MIN_CHUNK` is exactly one chunk, that chunk is the whole file,
/// and nothing about it deduplicates — so every figure in the report is pinned,
/// not merely plausible.
#[test]
fn info_reports_the_layout_of_a_single_chunk_file() {
    let ws = Ws::init();
    ws.run_in(&["write", "/small.txt"], "hello origofs")
        .expect_ok("write");

    let out = ws.run(&["info", "/small.txt"]).expect_ok("info");
    out.stdout_has("size            13 (13 B)");
    out.stdout_has("chunks          1 refs, 1 distinct");
    out.stdout_has("a whole-file read fetches 1 objects");
    out.stdout_has("chunk sizes     min 13 B, median 13 B, mean 13 B, max 13 B");
    out.stdout_has("distinct bytes  13 B (1.00x self-dedup)");
    out.stdout_has("residency       1/1 distinct chunks present");
    // The chunker settings travel with the report, so a number pasted into an
    // issue carries the parameters it was produced under.
    out.stdout_has("chunker         min 16.0 KiB / avg 64.0 KiB / max 256.0 KiB");
}

/// A multi-megabyte file is the case `info` exists for: the report must show the
/// read amplification (many chunks, all present) and a histogram that accounts for
/// every one of them.
#[test]
fn info_reports_a_chunk_histogram_that_accounts_for_every_chunk() {
    let ws = Ws::init();
    let src = ws.scratch("big.bin");
    std::fs::write(&src, pseudo_random(4 << 20, 0xC0FFEE)).unwrap();
    ws.run(&["write", "/big.bin", "--from", src.to_str().unwrap()])
        .expect_ok("write");

    let out = ws.run(&["info", "/big.bin"]).expect_ok("info");
    let chunks: u64 = out
        .lines()
        .iter()
        .find_map(|l| l.strip_prefix("chunks          "))
        .and_then(|l| l.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no chunk count in:\n{}", out.stdout));
    assert!(chunks > 16, "4 MiB should span many chunks, got {chunks}");

    // Every histogram bucket count must sum back to the chunk count — a bucket
    // that silently dropped chunks would make the distribution a lie.
    let binned: u64 = out
        .lines()
        .iter()
        .filter_map(|l| l.trim().strip_prefix("<= "))
        .filter_map(|l| l.split_whitespace().nth(2))
        .filter_map(|n| n.parse::<u64>().ok())
        .sum();
    assert_eq!(
        binned, chunks,
        "histogram must bin every chunk:\n{}",
        out.stdout
    );
    out.stdout_has(&format!(
        "residency       {chunks}/{chunks} distinct chunks present"
    ));
}

/// Self-dedup has to be measured. Two copies of the same block share chunks, so
/// the report must show fewer distinct chunks than references — and must still say
/// out loud that this counts only repetition inside the file, because that caveat
/// is what stops the figure being read as a claim about the whole store.
#[test]
fn info_reports_self_dedup_and_says_what_it_excludes() {
    let ws = Ws::init();
    let block = pseudo_random(1 << 20, 11);
    let src = ws.scratch("dup.bin");
    std::fs::write(&src, [block.clone(), block].concat()).unwrap();
    ws.run(&["write", "/dup.bin", "--from", src.to_str().unwrap()])
        .expect_ok("write");

    let out = ws.run(&["info", "/dup.bin"]).expect_ok("info");
    let line = out
        .lines()
        .into_iter()
        .find(|l| l.starts_with("chunks          "))
        .unwrap()
        .to_string();
    let nums: Vec<u64> = line
        .split_whitespace()
        .filter_map(|t| t.parse().ok())
        .collect();
    assert!(
        nums[1] < nums[0],
        "two copies of one block must share chunks: {line}"
    );
    out.stdout_has("self-dedup)");
    out.stdout_has("repetition *within this file* only");
    out.stdout_has("presence, not cache residency");
}

/// The probe is the only part of `info` that touches the content backend, so
/// `--no-probe` must genuinely omit the residency line rather than print a guess.
#[test]
fn info_no_probe_omits_residency_rather_than_guessing() {
    let ws = Ws::init();
    ws.run_in(&["write", "/a.txt"], "x").expect_ok("write");

    ws.run(&["info", "/a.txt", "--no-probe"])
        .expect_ok("info --no-probe")
        .stdout_has("residency       not probed")
        .stdout_lacks("distinct chunks present");
}

/// `info` is a diagnosis of the read path, so it must refuse exactly what a read
/// refuses — a directory and a missing path — instead of inventing a report.
#[test]
fn info_refuses_what_read_refuses() {
    let ws = Ws::init();
    ws.run(&["mkdir", "/d"]).expect_ok("mkdir");
    ws.run(&["info", "/d"]).expect_err("info on a directory");
    ws.run(&["info", "/nope"])
        .expect_err("info on a missing path");
}

/// The end-to-end smoke test: `bench` runs all three phases, reports the settings
/// it ran under, prints the caveats, and leaves the workspace as it found it.
#[test]
fn bench_runs_all_phases_and_cleans_up_after_itself() {
    let ws = Ws::init();
    let out = ws
        .run(&["bench", "--dir", "/bench", "--files", "2", "--size", "1M"])
        .expect_ok("bench");

    out.stdout_has("bench: 2 files x 1.0 MiB = 2.0 MiB");
    out.stdout_has("chunker             min 16.0 KiB / avg 64.0 KiB / max 256.0 KiB");
    out.stdout_has("chunks produced");
    // Unset in the test environment, and reported as unset rather than as a copy
    // of the engine's default that could drift away from it.
    out.stdout_has("upload concurrency  engine default (ORIGOFS_UPLOAD_CONCURRENCY unset)");
    out.stdout_has("fetch concurrency   engine default (ORIGOFS_FETCH_CONCURRENCY unset)");
    for phase in ["write", "read", "read#2"] {
        assert!(
            out.lines().iter().any(|l| l.starts_with(phase)),
            "missing the {phase} row:\n{}",
            out.stdout
        );
    }
    out.stdout_has("MiB/s");
    out.stdout_has("NOT cold and warm");
    out.stdout_has("sample files removed");

    // Nothing left behind — a benchmark that grows the workspace every time it is
    // run is one nobody can run twice.
    ws.run(&["ls", "/"]).expect_ok("ls /").stdout_lacks("bench");
}

/// The one destructive-surface guarantee, at the shell: `bench` refuses a
/// directory that already holds something, names the escape hatch, and writes
/// nothing on the way out.
#[test]
fn bench_refuses_a_populated_directory_and_names_the_escape_hatch() {
    let ws = Ws::init();
    ws.run_in(&["write", "/bench/precious.txt"], "keep me")
        .expect_ok("write");

    ws.run(&["bench", "--dir", "/bench", "--files", "1", "--size", "64K"])
        .expect_err("bench in a populated directory")
        .stderr_has("force");
    assert_eq!(
        ws.run(&["ls", "/bench"]).expect_ok("ls").lines().len(),
        1,
        "a refusal must not have written a sample file first"
    );

    ws.run(&[
        "bench", "--dir", "/bench", "--files", "1", "--size", "64K", "--force",
    ])
    .expect_ok("bench --force");
    // `--force` licenses running there, never deleting someone else's file.
    ws.run(&["read", "/bench/precious.txt"])
        .expect_ok("the pre-existing file survived")
        .stdout_has("keep me");
}

/// `--keep` is the opposite promise, and it has to hold too: the sample stays so a
/// slow file can be looked at with `info` afterwards.
#[test]
fn bench_keep_leaves_the_sample_for_inspection() {
    let ws = Ws::init();
    ws.run(&[
        "bench", "--dir", "/kept", "--files", "2", "--size", "64K", "--keep",
    ])
    .expect_ok("bench --keep")
    .stdout_has("--keep, so the sample files are still in /kept");

    assert_eq!(ws.run(&["ls", "/kept"]).expect_ok("ls").lines().len(), 2);
    ws.run(&["info", "/kept/bench-0000.bin"])
        .expect_ok("info on a kept sample");
}

/// `--size` is the flag most likely to be typed as a wrong number of zeroes, so
/// the suffix forms have to work and an unparseable one has to be a clap usage
/// error (exit 2), not a run that starts and then fails.
#[test]
fn bench_size_accepts_binary_suffixes_and_rejects_nonsense() {
    let ws = Ws::init();
    for (size, expected) in [
        ("65536", "64.0 KiB"),
        ("64K", "64.0 KiB"),
        ("1MiB", "1.0 MiB"),
    ] {
        ws.run(&["bench", "--dir", "/s", "--files", "1", "--size", size])
            .expect_ok(size)
            .stdout_has(&format!("1 files x {expected}"));
    }
    let bad = ws.run(&["bench", "--size", "banana"]);
    assert_eq!(
        bad.code,
        Some(2),
        "a bad --size is a usage error, not a failed run: {}",
        bad.stderr
    );
}
