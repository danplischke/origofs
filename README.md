<div align="center">

# origofs

**A filesystem where humans and AI agents share the same files —
and every byte knows who wrote it.**

[![CI](https://github.com/danplischke/origofs/actions/workflows/ci.yml/badge.svg)](https://github.com/danplischke/origofs/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![rust](https://img.shields.io/badge/rust-1.88%2B-dea584)](#install)
[![design](https://img.shields.io/badge/docs-DESIGN.md-informational)](docs/DESIGN.md)

[**Quickstart**](#quickstart) · [**Agents**](#working-with-agents) ·
[**Attribution**](#know-who-did-what) · [**Versioning**](#versioning) ·
[**Teams**](#running-for-a-team) · [**Python**](#python) ·
[**Backends**](#storage-backends) · [**Design**](docs/DESIGN.md)

</div>

---

Point an agent at your files and let it work. Then ask what it did — not the
chat log, not a diff you hope is complete. Ask the **filesystem**:

```bash
origofs --workspace ./ws init
AGENT=$(origofs --workspace ./ws actor claude --agent --model claude-opus-4-8)

# the agent edits in a fast native mount; every change streams into origofs, attributed, live
origofs --workspace ./ws overlay --actor "$AGENT" -- claude -p "refactor the parser"

# who wrote which lines?
origofs --workspace ./ws blame /src/parser.rs
```

```text
   1-40    human:dan
  41-58    agent:claude      ← the agent's work, down to the line
  59-72    human:dan
```

Don't like the result? `ws.revert_session(agent, session)` undoes **everything
that agent did in that session** — across every file it touched — and leaves the
human edits standing.

origofs is content-addressed storage with a real metadata database (Postgres or
SQLite), opt-in Git-style versioning, and per-actor attribution recorded in the
write path itself. It isn't a wrapper over `git` or a VFS shim — it's a storage
engine, exposed through a CLI, a Rust SDK, Python bindings, an HTTP API, MCP, and
real filesystem mounts (FUSE/NFS).

> **Pre-1.0 and moving fast.** Build from source ([Install](#install)); the
> `overlay` and `sandbox` commands need Linux (unprivileged overlayfs).

## Why origofs

Six questions a directory of files can't answer:

|  |  |
|---|---|
| **Who wrote this line — a person or an agent?** | Every attributed write records the actor, session, and tool-call behind it. `origofs blame` reports it per line; the record survives commits, branch switches, and reformatting. |
| **Can I undo just the agent's work?** | Revert an agent's entire session across every file it touched, keeping everyone else's edits intact. |
| **Can I review before it lands?** | Agents can *propose* edits into a review queue instead of applying them; a human accepts (credited to the agent) or rejects. An actor can even be made *propose-only*, so its writes are gated into that queue automatically. |
| **Can people and agents edit together, live?** | Opt-in CRDT co-editing lets humans, agents, and browser editors type into the same document at once and converge — with every character still attributed to whoever wrote it. |
| **Will it hold up for a team?** | The Postgres backend is built for many concurrent writers — humans and agents — sharing one workspace, with a live change feed and presence so every client sees edits as they happen. |
| **Can I trust what I read back?** | Content is BLAKE3-addressed and verified on every read: silent bit-rot or tampering in object storage surfaces as an error instead of being served as if it were real. |

## Install

origofs is a Rust workspace. Build the `origofs` CLI with a recent stable toolchain:

```bash
cargo install --path crates/origofs-cli     # installs the `origofs` binary
# or, without installing:
cargo build --release                    # ./target/release/origofs
```

A workspace is just a directory origofs manages (metadata DB + content store). For a
team deployment, point it at Postgres and object storage instead — see
[Running for a team](#running-for-a-team) and [Storage backends](#storage-backends).

## Quickstart

```bash
WS=./ws
origofs --workspace "$WS" init
echo 'hello from origofs' | origofs --workspace "$WS" write /notes/a.txt
origofs --workspace "$WS" ls   /notes
origofs --workspace "$WS" read /notes/a.txt
origofs --workspace "$WS" stat /notes/a.txt
```

From Rust:

```rust
use origofs_sdk::Workspace;

let ws = Workspace::open_local("meta.db", "cas").await?;   // or open_pg(dsn, cas)
ws.mkdir_p("/notes").await?;
ws.write("/notes/a.txt", b"hello").await?;
let bytes = ws.read("/notes/a.txt").await?;
```

## Working with agents

### A native mount agents edit in place

The fastest way to put an agent to work is a **live overlay mount**. origofs sets up
an unprivileged kernel overlay over the workspace, runs your agent inside it, and
streams the agent's changes back into origofs — *attributed, as they happen*, not
only when the process exits:

```bash
origofs --workspace "$WS" overlay --actor "$AGENT" --sync-ms 500 -- \
    some-agent --do-the-thing
```

The agent sees an ordinary directory and reads/writes at native speed; origofs
captures each change (create, modify, delete) into the content store and records
it against `--actor`. When it finishes, `origofs blame` and the change feed already
reflect everything it did. This is how agents are meant to interact with origofs day
to day.

Prefer a protocol integration? origofs also speaks **MCP** (Model Context Protocol)
over stdio, so an agent can call filesystem tools directly — and every write is
attributed to the agent:

```bash
origofs --workspace "$WS" mcp --agent-name claude --model claude-opus-4-8
```

### Propose-and-review, not just apply

An agent can submit an edit for human review instead of applying it. The proposed
bytes go straight into the content store (deduplicated, diffable); the working
tree doesn't change until someone accepts:

```bash
echo "patched" | origofs --workspace "$WS" suggest /main.rs --actor "$AGENT" --summary "fix bug"
origofs --workspace "$WS" suggestions --status pending
origofs --workspace "$WS" suggestion-diff 1              # base → proposed, unified diff
origofs --workspace "$WS" accept 1 --actor "$HUMAN"      # applies it, credited to the agent
```

`accept` lands the edit **attributed to the authoring agent** (so blame stays
honest) and records the approver; it refuses if the file moved since the proposal
(a stale base) — and retires that proposal as `superseded` rather than leaving it
pending forever. A *successful* accept does the same to the other pending
proposals on that path whose base it just moved. `reject` discards it.

A proposal comes in two **kinds**, because "stale" means different things:

| kind | base | proposal | accepting it |
|---|---|---|---|
| `bytes` (default) | the file's content hash | a whole file body | a conditional whole-file write — refused, and superseded, if the file moved |
| `crdt` | the document's Yjs **state vector** | an opaque `encodeStateAsUpdate` blob | an `applyUpdate` **merge** |

A CRDT merge is defined for *any* pair of states, so a `crdt` proposal against a
[live co-edited](#live-co-editing-crdt) document is never stale: a colleague's
concurrent edit elsewhere in the file neither invalidates it nor gets clobbered by
it. `crdt` proposals are therefore never swept as superseded. Either way the
review gate is the same — the merged text is credited to its original author, the
approver is recorded, and nobody accepts their own proposal.

```rust
let doc = ws.open_coedit(ctx, "/notes.md").await?;
doc.insert(ctx, 0, "a suggestion");
let id = ws.suggest_coedit(ctx, "/notes.md", &doc, Some("reword the intro")).await?;
```

The queue is **actor-agnostic** — it's a change-request workflow between people
just as much as an agent-proposal one. Whether an actor *must* propose (rather
than write directly) is its **write policy**: a bounded trust gate that's a
property of the actor, not its kind. A trusted agent stays `direct`; an untrusted
contributor is set `propose`-only and every write it makes — on *any* surface — is
routed into the review queue instead of landing:

```bash
origofs --workspace "$WS" write-policy "$AGENT" propose   # now its writes must be reviewed
```

Over **MCP**, an agent gets the whole loop as tools — `origofs_read`,
`origofs_write`, `origofs_edit` (exact string search-and-replace), `origofs_suggest`,
`origofs_suggestion_diff`, `origofs_accept`, `origofs_reject` — under the same
server-side attribution and policy enforcement (and it can't accept its own
proposals). `origofs_live` tells it whether a path has a live editing session open
(so it knows its read may lag), and — on a server built with the `coedit` feature —
`origofs_suggest_coedit` proposes into such a document as a CRDT merge instead of a
file body, which is the right shape when a human has the file open.

## Know who did what

Every attributed write (`write_as`) records an append-only edit-op — actor,
session, tool-call, before/after content — and updates a per-line authorship map.
`origofs blame` then reports, per line range, whether a **human** or an **agent** wrote
it:

```bash
origofs --workspace "$WS" blame /src/parser.rs
#    1-40   human:dan
#   41-58   agent:claude
#   59-72   human:dan
```

Blame is keyed by **content**, so it stays correct where naïve line-tracking
breaks: it survives commits and branch checkouts (the map travels with the bytes,
never desyncing from the file), a re-indent or a moved block keeps its original
author rather than being credited to whoever reformatted, and content produced
outside the attributed path simply blames to nothing instead of showing stale
authorship.

Undo an agent's work without touching anyone else's — `revert_session` walks
every file the agent touched in that session and removes exactly the lines it
authored, leaving surrounding human edits in place:

```rust
let files_changed = ws.revert_session(agent_id, session_id).await?;
```

## Versioning

Versioning is opt-in and Git-shaped — a real commit DAG, branches, checkout, log,
status, three-way merge, and locks — but backed by origofs's content-addressed store,
so snapshots are incremental (only changed chunks are stored) and identical trees
are shared across commits for free.

```bash
origofs --workspace "$WS" commit -m "initial" --author "Dan <dan@example.com>"
origofs --workspace "$WS" branch feature
origofs --workspace "$WS" checkout feature
origofs --workspace "$WS" diff main feature                # changed-path list
origofs --workspace "$WS" diff main feature --path /x.rs   # one file's line diff
```

Branch comparison works on content addresses, not file reads: equal hashes mean
an identical file (a 32-byte compare), so a diff only ever reads the paths that
actually changed — the metadata trees *are* the index.

### Real-`git` interop

origofs stays BLAKE3-native internally, but its history projects to — and imports
from — genuine git objects, so you can keep using the `git` CLI and hosts like
GitHub:

```bash
# origofs history → a real git repo the `git` binary reads directly
origofs --workspace "$WS" git export ./repo --format sha256   # or sha1 for GitHub
git -C ./repo log --oneline
git -C ./repo fsck --strict                                # clean

# a real git repo → origofs history
origofs --workspace "$WS2" git import ./repo --branch main
```

With `git-remote-origofs` on your `PATH`, the real `git` can even clone, fetch, and
push an origofs workspace over `origofs://` URLs — no export step:

```bash
git clone origofs://"$WS" checkout
cd checkout && echo hi >> readme.md && git commit -am edit && git push origin main
```

Large files can be exported as git-LFS pointer blobs (`--lfs-threshold <bytes>`),
backed by origofs's chunk store.

## Running for a team

For a shared human+agent workspace, run origofs on **Postgres** — the backend built
for many concurrent writers. Atomic-create is serialized so racing writers never
leave orphaned inodes, and the whole write path is transactional: content is made
durable first, then metadata, blame, and the audit log commit together, so a
crash can never leave a half-recorded edit.

```rust
let ws = Workspace::open_pg("host=db port=5432 user=origofs dbname=origofs", content).await?;
```

### Live collaboration

Every operation lands on an append-only **change feed** (who touched what, who
committed), and each session heartbeats its **presence** (which actor, which
path). Tail the feed by cursor, or — on Postgres — let `LISTEN/NOTIFY` push new
events so clients never poll:

```bash
origofs --workspace "$WS" watch --follow    # live feed: seq  kind  actor  path
origofs --workspace "$WS" presence          # who's active right now
```

The feed is **exactly-once and in commit order** even under concurrent writers,
and every event is **branch-scoped**, so a UI showing `main` filters to one
branch. From Rust, `PostgresMetadataStore::subscribe(after_seq, branch)` returns
a blocking `LISTEN`-backed subscription whose `recv()` wakes on every committed
change — a real push, not a poll.

### Working offline, then rejoining

SQLite mode is the offline mode: one portable file, full speed, no server. When
you reconnect, `resync` reconciles what you did with what everyone else did —
through the same three-way merge that powers `origofs merge`, not a separate
code path:

```bash
origofs --workspace ./offline resync --remote ./shared --branch main -m "back online"
```

```text
main: merged as 3a9f21c8b4d0 and pushed
  fetched 41 object(s), 2203648 B (6 already present)
  pushed  17 object(s), 856064 B (3 already present)
  blame carried: 12 in, 5 out
```

It works between **different backends** — your offline SQLite + local content
store against the team's Postgres + object storage — because that is the whole
point. A conflicting merge records conflicts exactly as an ordinary merge does
and leaves the shared branch untouched until you resolve them, and the shared
branch only ever moves under a compare-and-swap, so a teammate who pushed while
you were merging is never clobbered.

Crucially, **your attribution comes with you**: the lines an agent wrote offline
are still credited to that agent on the server afterwards, with its identity
mapped into the shared workspace rather than blindly copied onto whichever actor
happens to hold that id there.

### Live co-editing (CRDT)

Opt-in (the `coedit` feature): humans, agents, and browser editors type into the
same document **concurrently** and converge — a CRDT (`yrs`) under the hood, with
authorship tracked per character. It speaks the standard Yjs **y-sync** protocol,
so an unmodified Yjs client connects to the co-editing WebSocket with no custom
server code — a plain-text or Markdown editor bound to the shared text (over
`y-websocket`) collaborates out of the box. The shared document is a flat-text
CRDT, so a *structural* rich-text editor such as PlateJS shapes its Yjs document
to match rather than binding the flat text directly.

The server stays the sole authority on **who wrote what**: whatever a client's
bytes claim, each inserted run is attributed to the *authenticated* actor. When a
session checkpoints, that character-level, interleaved authorship lands in the
*same* byte-range blame index as ordinary writes — so two people editing one line
show up as two spans, not one collapsed line.

```rust
let doc = ws.open_coedit(ctx, "/notes.md").await?;    // resume the live CRDT
// … clients exchange y-sync updates over the WebSocket …
ws.checkpoint_coedit(ctx, "/notes.md", &doc).await?;  // crystallize into blame
```

Across workers it stays consistent: when a document is edited on two processes at
once (behind a load balancer), a Postgres `LISTEN/NOTIFY` relay fans each update
out so every worker's replica converges. The co-editing endpoint is served as a
WebSocket at `GET /coedit/{path}` by both the HTTP API and the FastAPI router.

While a document is open, its stored bytes are the last **checkpoint** — real,
fully attributed, but possibly behind what people are typing. origofs records that
as a per-path **live marker**, and the rule is to *surface* the staleness, never to
block on it:

```rust
let (bytes, live) = ws.read_live("/notes.md").await?;   // never blocks, never fails
if live.is_some() { /* these bytes may lag an open editor */ }
for doc in ws.live_paths().await? { … }                 // everything open right now
```

`read` keeps its contract unchanged and always answers; a reader is simply told
whether the answer may be behind. Nothing forces a checkpoint on a reader's
behalf — a read must not write, a checkpoint needs an actor to attribute it to,
and the live document is in-process room state the engine cannot reach anyway. A
caller that needs the freshest bytes (a release build, a `git` export) checkpoints
the co-editing coordinator first, then reads; `origofs`'s own git export warns and
lists any live path rather than exporting stale bytes silently. `end_coedit` clears
the marker once the final checkpoint has landed.

### HTTP API

Every operation is available over HTTP/JSON — files as raw bytes, everything else
as JSON — so any client or service can drive a workspace. Writes go through the
same path as every other surface, so they land on the change feed and carry
attribution:

```bash
origofs --workspace "$WS" serve --addr 127.0.0.1:8080 --auth-token "$TOKEN=$ACTOR" &
AUTH=(-H "Authorization: Bearer $TOKEN")

curl "${AUTH[@]}" -X PUT --data-binary 'hello' http://127.0.0.1:8080/v1/files/notes/a.txt
curl 'http://127.0.0.1:8080/v1/files/notes/a.txt'                    # → hello
curl "${AUTH[@]}" -X POST -d '{"message":"first"}' http://127.0.0.1:8080/v1/commit
curl 'http://127.0.0.1:8080/v1/events?since=0'                       # the change feed
curl "${AUTH[@]}" -X POST -d '{"path":"/notes/a.txt"}' \
     http://127.0.0.1:8080/v1/presence                               # heartbeat: I'm here
curl 'http://127.0.0.1:8080/v1/presence'                             # who's active
curl 'http://127.0.0.1:8080/readyz'                                  # backends reachable?
```

The data surface is versioned under **`/v1`**; liveness (`/health`) and readiness
(`/readyz`) stay at the root so an orchestrator probes them independent of the API
version. Full routes cover files, dirs, stat, blame, rename, commit/log,
branches/checkout, events, presence (`GET` to list, `POST` to heartbeat), actors,
sessions, diff, suggestions, and the live co-editing WebSocket.

`POST /v1/presence` takes an optional `{"path": …}` and nothing else: the actor
and session come from the credential, so a browser client can keep itself visible
in `GET /v1/presence` without an in-process SDK bridge, and cannot heartbeat
anyone but itself. Presence is keyed by session, so a credential not bound to one
gets a `400` telling it to create a session rather than having one minted per
heartbeat.

**Attribution never comes from the request.** A write is attributed to the actor
the *credential* resolves to — the request never names an actor, so a client can't
forge blame, and a propose-only actor's `PUT` is routed into the review queue
instead of landing. `--auth-token TOKEN=ACTOR[:SESSION]` is the built-in bearer
mapping; `serve` refuses to bind a non-loopback address without one. Errors come
back as a machine-readable envelope (`{"error":{"code","message","retryable"}}`)
and every response carries an `x-request-id`.

### Metrics

origofs is **emit-only** for observability: the library records `tracing` spans and
numeric measurements but installs no subscriber and no exporter, so embedding it
costs nothing and you wire up your own. The binary opts in — `--log-format json`
for logs, `--metrics` for numbers:

```bash
origofs --workspace "$WS" serve --addr 127.0.0.1:8080 --metrics   # or ORIGOFS_METRICS=1
curl 'http://127.0.0.1:8080/metrics'      # Prometheus text exposition
```

`/metrics` sits at the root next to `/health` and `/readyz`, and gets the same
treatment as `/readyz`: no credential required. Nothing sensitive is exposed —
every label is a closed set (an error `code`/`class`, a fixed operation name, a
*matched route template* like `/v1/files/{*path}`), so no path, actor, hash, or
file content ever reaches a scrape. Without `--metrics` nothing is installed and
the route answers `503 metrics not enabled`.

Series: `origofs_writes_total` / `origofs_write_bytes_total`, `origofs_reads_total` /
`origofs_read_bytes_total`, `origofs_chunks_put_total` / `origofs_chunks_deduped_total`
(dedup hit rate), `origofs_commits_total`, `origofs_gc_objects_deleted_total` /
`origofs_gc_bytes_freed_total`, `origofs_errors_total{code,class}` (keyed off the
same machine-readable code the API error envelope returns), and the
`origofs_op_duration_seconds{op}` / `origofs_http_request_duration_seconds{method,path}`
histograms. Rust embedders record into the same facade — enable
`origofs-core`'s `metrics` feature and install any [`metrics`](https://docs.rs/metrics)
recorder.

## Python

`origofs-py` is the binding layer for the surface most people build on top of:
a Python service that already knows *which user or agent* is calling. Every I/O
method is a coroutine, so it drops straight into `async def` handlers, and
structured results come back as plain JSON-serializable `dict`/`list`.

```bash
cd crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin && maturin develop     # builds the extension + installs `origofs`
```

Wheels are abi3 (`maturin build --release` — one wheel works on CPython ≥ 3.9).
The integrations below ship as extras: `fastapi`, `fsspec`, `upath`,
`llamaindex`, `markitdown`, `db`.

```python
import origofs

ws = await origofs.Workspace.open_local("meta.db", "cas")
# ...or the production combo — Postgres metadata + S3-shared content:
#   cfg = origofs.S3Config(bucket="my-bucket", region="us-east-1")
#   ws  = await origofs.Workspace.open_pg_s3(dsn, cfg)   # GcsConfig/open_pg_gcs too

# map your app's user id onto an origofs actor (idempotent — no side table needed)
actor = await ws.find_or_create_human("user_42", "Dan")
ctx   = origofs.WriteCtx.session(actor, await ws.create_session(actor))

await ws.write_as(ctx, "/notes.txt", b"hello")   # attributed → blame + audit
await ws.blame("/notes.txt")   # [{"byte_start","byte_end","line_start","actor",…}, …]

sid = await ws.suggest(ctx, "/main.rs", b"patched", "fix bug")   # proposed, not applied
await ws.accept_suggestion(sid, origofs.WriteCtx.actor(reviewer))
```

The same [write policy](#propose-and-review-not-just-apply) applies here, because
it lives in the engine: `write_or_propose` on a propose-only actor queues a
suggestion instead of writing, and returns a `WriteOutcome` telling you which
happened. Errors map onto Python's own: a missing path raises `FileNotFoundError`,
a bad argument `ValueError`, a stale suggestion base `origofs.ConflictError`.

### FastAPI — bring your own auth

origofs ships no authentication, on purpose: a blame trail is only trustworthy if
the identity behind each write is, and only your app knows how to resolve it.
`origofs.fastapi.build_router` wires up every workspace endpoint against an auth
dependency **you** provide:

```python
from fastapi import FastAPI, Header
from origofs.fastapi import build_router
import origofs

async def authn(x_actor_id: int = Header(...)) -> origofs.WriteCtx:
    # decode your JWT / session / agent token → the actor to attribute to
    return origofs.WriteCtx.actor(x_actor_id)

app = FastAPI()
app.include_router(build_router(ws, authn=authn), prefix="/fs")
```

Every mutating route depends on `authn` and hands its `WriteCtx` to the
workspace — the request body never names an actor, so a client can't forge
attribution. Reads are open by default (`reader=` gates them, `dependencies=[…]`
gates everything); `GET /files/{path}` streams rather than buffering and honors a
`Range` header. The [co-editing](#live-co-editing-crdt) WebSocket is served here
too, at `GET /coedit/{path}`.

### The PyData stack reads origofs paths directly

`origofs.fsspec.OrigoFileSystem` is a genuine `fsspec.asyn.AsyncFileSystem` — usable
synchronously *and* by awaiting the `_`-prefixed coroutines on your own loop — so
pandas, Dask, PyArrow, and Zarr work against a workspace with no glue:

```python
import origofs.fsspec                                    # registers "origofs://"
import pandas as pd

df = pd.read_parquet("origofs:///data/events.parquet",
                     storage_options={"db_path": "meta.db", "cas_dir": "cas"})

fs = origofs.fsspec.OrigoFileSystem(db_path="meta.db", cas_dir="cas", actor=actor)
fs.pipe_file("/notes.txt", b"hello")      # attributed — fs.blame() credits it
fs.cat_file("/notes.txt", start=0, end=5) # ranged: reads only the covering chunks
```

It passes fsspec's own conformance suite, and with the `upath` extra a workspace
is a `pathlib`-shaped `UPath("origofs:///notes.txt", db_path=…, cas_dir=…)`.

### RAG that knows who wrote each passage

Retrieval over origofs isn't "object storage + embeddings": each passage carries
**who wrote it** (from blame) and is keyed by a content hash, so only
genuinely-changed passages need re-embedding. `content_defined` segmentation is
the default because it puts boundaries where the bytes decide — an edit near the
top of a file doesn't reshuffle every later passage's hash:

```python
from origofs.rag import read_passages
for p in await read_passages(ws, root="/docs", segmentation="content_defined"):
    p.text, p.hash, p.authors        # per-byte authorship, not a file-level guess

from origofs.llamaindex import SimpleWorkspaceReader     # one Document per passage
docs = SimpleWorkspaceReader(ws, root="/docs", convert="markitdown").load_data()
```

Non-text documents (PDF, DOCX, …) go through a pluggable converter to Markdown
first; their provenance is document-level, since the extracted text no longer
maps to the original bytes.

### Feeds, agents, and mounts

```python
sub = await ws.subscribe(after_seq=0, branch="main")   # Postgres LISTEN/NOTIFY push
events = await sub.recv()          # blocks until the next batch — ideal behind SSE

from origofs.overlay import run    # agent in a live overlay, attributed as it types
code = await run("./ws", agent_actor, ["claude", "-p", "refactor the parser"])

with ws.mount("/mnt/origofs") as m: ...   # FUSE (Linux); NFSv3 elsewhere:
await ws.serve_nfs("127.0.0.1:11111")  # a task that runs until cancelled
```

`origofs.db` additionally declares the whole metadata schema as SQLAlchemy models
with packaged **Alembic** migrations, for when a deploy step (not the engine's own
automatic on-open migration) should own the database — and for read-only
reporting queries alongside the async API.

Full details, the complete API surface, and the example apps live in
[`crates/origofs-py/README.md`](crates/origofs-py/README.md) — start with
[`examples/collab_app.py`](crates/origofs-py/examples/collab_app.py), which runs
the whole human-writes → agent-suggests → reviewer-accepts story end to end with
no server or curl needed.

## Built to not lose or corrupt data

Because agents can generate a lot of churn against shared storage, correctness
under failure is a first-class concern:

- **Corruption never passes as real.** Content is BLAKE3-addressed and re-hashed
  on read against the address it was fetched by. A flipped bit, a truncated
  object, or tampering in object storage surfaces as a precise error — down to the
  offending chunk — instead of being handed back as authentic. Compaction
  re-verifies every surviving chunk before dropping the old copy.
- **Writes are atomic and durable.** Content is flushed before the metadata that
  references it commits, and an edit's inode, content, blame, and audit entry all
  land in one transaction — or none of them do.
- **Blame can't lie.** Authorship is tied to content, so it can never drift out
  of sync with the file it annotates (see [Know who did what](#know-who-did-what)).
- **The bucket can rebuild the database.** Content is stored as a self-describing
  git-style graph (commit → tree → file manifest → chunks), and the branch table
  is mirrored alongside it, so if the metadata DB is lost you can point a fresh one
  at the surviving object store and `origofs fsck --rebuild` to recover every committed
  file, directory, and branch — chunking and all. (Blame and the audit log live
  only in the DB — see [Backing up](#backing-up).)

## Backing up

Two stores, two very different jobs.

**The content store needs no backup from origofs.** It is immutable and
content-addressed, so whatever durability your bucket or filesystem already
provides is the whole story.

**The metadata database is the irreplaceable half.** `origofs fsck --rebuild`
reconstructs every committed file, directory, symlink, and branch from the
content store alone — but **blame, the audit log, the actor registry, and every
uncommitted edit exist only in the database**. Losing it loses the thing origofs
is for.

```bash
origofs --workspace ./ws backup ./backups/meta-$(date +%F).db
```

SQLite is snapshotted with SQLite's own online backup API, so writers keep
running while it is taken. Do not substitute `cp meta.db`: a live database has a
`-wal` sidecar and may be mid-transaction, and the copy often restores — which is
what makes it dangerous. The command refuses to overwrite an existing file, so a
scheduled backup cannot quietly destroy the previous one.

To restore, put the snapshot back where the workspace expects it, alongside the
**same content store** (the snapshot is metadata only):

```bash
origofs --workspace ./ws serve ...   # stop first
rm -f ./ws/meta.db ./ws/meta.db-wal ./ws/meta.db-shm
cp ./backups/meta-2026-01-31.db ./ws/meta.db
origofs --workspace ./ws schema-version   # sanity-check, then restart
```

**Postgres**: `origofs backup` deliberately refuses rather than producing
something that merely resembles a backup. Use `pg_dump` or continuous archiving
(PITR) — both give a consistent snapshot of a live database, and PITR additionally
bounds how much you can lose. The restore procedure is the same shape: restore the
database, keep the content store as it is, then check `origofs schema-version`
before starting the new binaries.

## Storage backends

Content addressing means a chunk's identity is its BLAKE3 hash, so dedup,
versioning, and integrity hold no matter where bytes live.

- **Local** — a sharded content-addressed directory. `Workspace::open_local`.
- **Object storage** — Content-defined chunking keeps edits cheap (only changed
  chunks re-upload), and a **pack layer** (`open_s3_packed` / `open_gcs_packed`)
  batches chunks into large pack objects so you make a few big PUTs instead of
  thousands of tiny ones, with a small local index for single ranged-GET reads.
  `repack()` reclaims space from deleted chunks. Two flavours:
  - **S3 / R2 / MinIO** (and GCS via its S3-interop API with HMAC keys) —
    `Workspace::open_s3`.
  - **Google Cloud Storage, natively** — `Workspace::open_gcs`, over GCS's JSON
    API with OAuth2: a service-account key/file, Application Default Credentials
    (`GOOGLE_APPLICATION_CREDENTIALS` / `gcloud`), or GKE workload identity — no
    HMAC keys needed.
- **Encryption at rest** — wrap any backend so content is encrypted
  (XChaCha20-Poly1305) before it touches disk or the network, transparently to
  the engine. The address stays the plaintext hash, so **dedup still works**
  (convergent encryption). Set `ORIGOFS_ENCRYPTION_KEY` or use
  `Workspace::open_local_encrypted`.

Content is addressed and never overwritten, so churn leaves orphaned chunks
behind; mark-and-sweep garbage collection reclaims them:

```bash
origofs --workspace "$WS" gc     # run when idle — not safe alongside active writers
```

## Interfaces

| Surface | Use it for |
|---|---|
| **`origofs` CLI** | Scripting and day-to-day workspace operations |
| **Rust SDK** (`origofs-sdk`) | Embedding origofs in a Rust service |
| **[Python](#python)** (`origofs-py`) | Async-native PyO3 bindings — FastAPI, fsspec, RAG; resolve identity yourself |
| **HTTP API** (`origofs-sdk` `api` feature) | Any language / any client over JSON |
| **MCP** (`origofs-sdk` `mcp` feature) | Agents calling filesystem tools directly, attributed |
| **Overlay mount** | Running an agent live in a fast native mount |
| **FUSE / NFS** | Mounting the workspace as a POSIX filesystem |

They all funnel into the same engine, so a write lands on the change feed and
carries attribution no matter which one it came through. [**Python**](#python)
gets its own section above — it's the surface most services are built on.

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
```

The Postgres backend tests self-skip unless `ORIGOFS_PG_TEST_URL` points at a
reachable database:

```bash
ORIGOFS_PG_TEST_URL="host=127.0.0.1 port=5432 user=postgres dbname=origofs" cargo test --workspace
```

### Performance

Criterion micro-benchmarks cover the hot paths (chunk + BLAKE3 write, whole-file
read, commit/tree building, encryption overhead) over an in-memory store, so they
reflect origofs's own CPU cost rather than disk or network:

```bash
cargo bench -p origofs-core
```

Indicative single-threaded numbers (release, in-memory store): writes chunk + hash
at ~1.3 GiB/s and reads reassemble at ~10 GiB/s; encryption at rest costs roughly
2× on write and is decrypt-bound on read.

## Design

The full design and rationale — the metadata/content split, the versioning model,
attribution, and the failure-surface work — live in
[`docs/DESIGN.md`](docs/DESIGN.md).

## License

MIT OR Apache-2.0
