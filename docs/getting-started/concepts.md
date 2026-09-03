# Core concepts

Six ideas explain most of origofs's behaviour. None of them is complicated on its
own; the combination is what makes the system different from a directory.

## A workspace is metadata plus content, split

The single architectural idea everything hangs on: **the metadata store and the
content store are separate, and never mixed.**

- The **content store** holds the *bytes*. Files are cut into content-defined
  chunks (FastCDC), each addressed by its BLAKE3 hash, alongside the immutable
  commit/tree/blob objects that form a Merkle DAG. It is immutable,
  deduplicated, and integrity-verified on read.
- The **metadata store** holds the *names and versions*: directories, inodes,
  refs, the attribution log, the audit log, the change feed, presence. It stores
  content only as hash references — it never holds large file bytes.

Each half picks its backend independently. Solo, that is a SQLite file and a
local directory. For a team, Postgres and an object store. See
[Storage backends](../reference/storage-backends.md).

The practical consequence: **the content store can rebuild the database, but not
attribution.** The object graph is self-describing, so
[`origofs fsck --rebuild`](../guides/backup-and-recovery.md) can restore committed
files, directories and branches from the bucket alone. Blame, the audit log,
actors and uncommitted edits live only in the database — so
[the database is the thing to back up](../guides/backup-and-recovery.md).

## The working tree is an overlay on a commit

Files you can change are mutable. Commit objects are immutable. origofs resolves
that the way git's index does: the working tree is an **overlay whose base is a
commit tree**.

Reads fall through the working tree, to the base tree, to content chunks. Writes
copy up. Committing crystallizes the working tree into new immutable tree and
commit objects.

This is why `status` can show you changes against `HEAD` without a separate
staging step, and why a checkout can rematerialize a file without rewriting
history.

## Actors and sessions

An **actor** is a registered identity — a human or an agent. An agent carries its
model, and optionally the human `--controller` who launched it, so a trail leads
back to a person.

A **session** groups one actor's work into an episode: one agent run, one
editing sitting. Attribution records both, which is what makes
[`revert-session`](../guides/attribution.md#undo-one-session) possible — undoing
"what the agent did in that run" is a question about a session, not about a
commit range.

```bash
DAN=$(origofs --workspace "$WS" actor dan)
BOT=$(origofs --workspace "$WS" actor bot --agent --model claude-opus-4 --controller "$DAN")
```

## Attributed writes, and unattributed ones

Every mutating operation has two forms, and the difference is deliberate.

- **Attributed** — carries an actor. Records an append-only edit-op (the ground
  truth) and updates the materialized blame index. On the CLI that is
  `--actor`; in Rust a `WriteCtx`; on the HTTP API it comes from the credential.
- **Unattributed** — records nothing. These exist because internal machinery
  needs them: checkout, merge materialization, applying an accepted suggestion.
  They are exempt by construction, not by oversight.

An unattributed write is a real write that leaves no trail. If you want that to
be an error rather than a possibility, turn it off:

```bash
origofs --workspace "$WS" require-attribution on
```

That is a **completeness** switch, not access control — see below.

## Permission is checked in the engine

Access control is not implemented per surface. Every attributed mutation runs one
of four checks inside the engine, so a new endpoint on any surface inherits it
and cannot forget it. Refusals surface as `Denied`, and as `403` over HTTP.

Two things follow that surprise people:

- **Reads are open unless you opt in.** `READ` is enforced only where a workspace
  sets `acl enforce-reads on`, because reads have never been checked and turning
  enforcement on by default would stop every actor in every existing workspace at
  once. Once on, listings filter per entry — a directory listing that names what
  a `stat` would refuse is exactly the existence oracle the refusal exists to
  prevent.
- **An operation with no single path still touches everything.** `commit`,
  `checkout`, `create_branch` and an unbounded `revert-session` are checked at
  `/`, not exempted for lack of a path.

See [Operating a workspace](../guides/operating.md#scope-what-an-agent-can-reach).

## Identity is asserted here, verified at the boundary

`--actor` on the CLI, and `ORIGOFS_ACTOR` behind it, are **assertions**. Whoever
writes the argv writes the environment, and a local process holding the workspace
directory has `meta.db` and the content store on disk anyway. The CLI is not a
security boundary and does not pretend to be one.

The boundary is the [HTTP surface](../reference/http-api.md), where identity is
resolved server-side from a credential and the request body never names an actor.
`origofs serve` refuses to bind a non-loopback address without authentication
configured.

A [mount](../guides/mounts.md) sits in between: it is bound to one actor for its
lifetime, which **authorizes** but does not **attribute** — the kernel never says
which process issued a request, so one actor covers everything on that
mountpoint.

What the CLI flags buy you is that attribution *gets recorded*.

## Content is immutable, so churn leaves garbage

Nothing is ever overwritten; a changed file writes new chunks and orphans the old
ones. [`origofs gc`](../guides/backup-and-recovery.md#reclaiming-space) reclaims
them by mark-and-sweep from live refs, and packed stores additionally need
`repack` to return the space.

GC is safe to run alongside active writers. It does not quiesce the store — it
skips objects younger than a grace period, because content is written before the
metadata that references it, so every write has a window where its chunks look
unreferenced.
