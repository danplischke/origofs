<div align="center">

!!This is experimental and heavily vibe coded. Please do not use it.

# origofs

**A filesystem where humans and AI agents share the same files —
and every byte knows who wrote it.**

[![CI](https://github.com/danplischke/origofs/actions/workflows/ci.yml/badge.svg)](https://github.com/danplischke/origofs/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue)](#license)
[![rust](https://img.shields.io/badge/rust-1.88%2B-dea584)](#install)
[![design](https://img.shields.io/badge/docs-DESIGN.md-informational)](docs/DESIGN.md)

[**Documentation**](docs/index.md) ·
[**What it is**](#what-origofs-is) · [**Quickstart**](#quickstart) ·
[**Agents**](#working-with-agents) ·
[**Attribution**](#know-who-did-what) · [**Versioning**](#versioning) ·
[**Teams**](#running-for-a-team) · [**Python**](#python) ·
[**Backends**](#storage-backends) · [**Design**](docs/DESIGN.md)

</div>

---

> **Documentation.** The user-facing docs live in [`docs/`](docs/index.md) and
> build into a site with `zensical serve`. This README is the tour; the docs are
> where the guides and the reference live.

## What origofs is

origofs is a **storage engine for files**. You keep your files in an origofs
*workspace* instead of a plain directory, and read and write them through a CLI,
a Rust or Python SDK, an HTTP API, or a real POSIX mount.

What it adds to a directory is **identity**. Every write names an *actor* — a
person, or a specific agent — and origofs records that per byte, permanently. So a
file here can answer a question a plain directory can't: *who wrote this line, a
human or an agent?*

### See it in one minute

Two writers editing one file:

```bash
origofs --workspace ./ws init

# register the two actors; each command prints an id
DAN=$(origofs --workspace ./ws actor dan)                                  # a person
BOT=$(origofs --workspace ./ws actor claude --agent --model claude-opus-4-8)   # an agent

# dan writes two lines; the agent later rewrites the file with a third
printf 'alpha\nbeta\n'        | origofs --workspace ./ws write /notes.md --actor "$DAN"
printf 'alpha\nbeta\ngamma\n' | origofs --workspace ./ws write /notes.md --actor "$BOT"

origofs --workspace ./ws blame /notes.md
```

```text
   1-2     human:dan
   3       agent:claude     ← credited only with the line it actually added
```

The agent submitted the *whole* file, but blame is keyed by **content**, not by
line number — so dan keeps his lines and the agent is credited with exactly what
it changed. That holds up when lines move, when the file is reformatted, and
across commits and branch switches.

### In practice: let the agent work, then check what it did

You don't hand an agent's writes to origofs yourself. Run the agent *inside* the
workspace and its edits stream in as it works, already attributed:

```bash
origofs --workspace ./ws overlay --actor "$BOT" -- claude -p "refactor the parser"
origofs --workspace ./ws blame /src/parser.rs
```

```text
   1-40    human:dan
  41-58    agent:claude      ← the agent's work, down to the line
  59-72    human:dan
```

Don't like the result? `ws.revert_session(agent, session)` undoes **everything
that agent did in that session** — across every file it touched — and leaves the
human edits standing. Or don't let it land in the first place: an agent can be
made *propose-only*, so its writes queue up for [review](#propose-and-review-not-just-apply)
instead of applying.

### Under the hood

origofs is content-addressed storage (BLAKE3-hashed chunks, deduplicated and
verified on read) over a real metadata database (SQLite solo, Postgres for a
team), with opt-in Git-style versioning and attribution recorded in the write
path itself. It is **not** a wrapper over `git` or a VFS shim — it's a storage
engine, exposed through a CLI, a Rust SDK, Python bindings, an HTTP API, MCP, and
real filesystem mounts (FUSE/NFS).

> **Pre-1.0 and moving fast.** Build from source ([Install](#install)); the
> `overlay` and `sandbox` commands need Linux (unprivileged overlayfs).

## What you get

Six questions a directory of files can't answer:

| Question | origofs's answer |
|---|---|
| **Who wrote this line — a person or an agent?** | `origofs blame` reports it per line. Every attributed write also records the session and tool-call behind it. |
| **Can I undo just the agent's work?** | Revert one agent's whole session across every file it touched; everyone else's edits stay. |
| **Can I review before it lands?** | Agents can *propose* edits into a review queue; a human accepts (credited to the agent) or rejects. |
| **Can people and agents edit together, live?** | Opt-in CRDT co-editing: humans, agents, and browser editors type into one document and converge, still attributed per character. |
| **Will it hold up for a team?** | Postgres backs many concurrent writers on one workspace, with a live change feed and presence. |
| **Can I trust what I read back?** | Content is verified against its hash on every read, so bit-rot or tampering is an error, never silently served. |

## Install

origofs is a Rust workspace. Build the `origofs` CLI with a recent stable toolchain:

```bash
cargo install --path crates/origofs-cli     # installs the `origofs` binary
# or, without installing:
cargo build --release                    # ./target/release/origofs
```

A workspace is a directory origofs manages: a metadata database (`meta.db`) next to
a content store (`cas/`). You never edit those by hand — the paths you use
(`/notes/a.txt`) live *inside* the workspace, not on your disk. For a team
deployment, point it at Postgres and object storage instead — see
[Running for a team](#running-for-a-team) and [Storage backends](#storage-backends).

## Quickstart

The ordinary file operations, from the shell. `write` takes its bytes from stdin
(or `--from <file>`), and paths are absolute within the workspace:

```bash
WS=./ws
origofs --workspace "$WS" init                     # create the workspace

echo 'hello from origofs' | origofs --workspace "$WS" write /notes/a.txt
origofs --workspace "$WS" ls   /notes              # list a directory
origofs --workspace "$WS" read /notes/a.txt        # bytes to stdout
origofs --workspace "$WS" stat /notes/a.txt        # size, kind, timestamps

# ...and the same write, on the record: `--actor` is what makes blame possible
DAN=$(origofs --workspace "$WS" actor dan)
echo 'hello from dan' | origofs --workspace "$WS" write /notes/a.txt --actor "$DAN"
origofs --workspace "$WS" blame /notes/a.txt
```

The same thing from Rust — `write` is unattributed, `write_as` carries an
identity, and everything else is the same call:

```rust
use origofs_sdk::{Workspace, WriteCtx};

let ws = Workspace::open_local("meta.db", "cas").await?;   // or open_pg(dsn, cas)
ws.mkdir_p("/notes").await?;

let dan = ws.create_human("dan", None).await?;             // an actor id
let ctx = WriteCtx::session(dan, ws.create_session(dan, None).await?);
ws.write_as(ctx, "/notes/a.txt", b"hello").await?;         // attributed

let bytes = ws.read("/notes/a.txt").await?;
let spans = ws.blame("/notes/a.txt").await?;               // who wrote which bytes
```

Large files stream on the write side too: `write_reader_as` (Rust) and
`write_path_as` (Python) chunk incrementally *and* record blame, so attribution no
longer costs you the ability to write a file larger than memory. See
[`docs/LIMITS.md`](docs/LIMITS.md).

Python mirrors this API with `await` on every call — see [Python](#python). The
binding covers the workspace, versioning, merge, attribution, suggestions, the
change feed, multi-workspace, maintenance (`gc`/`flush`/`repack`/
`backup_metadata`), and encryption at rest. Genuinely Rust-only today: the
`resync` reconciliation flow, direct object push/fetch between workspaces, and
assembling a custom backend stack by hand.

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

The gate applies to any mutation that **names an actor** — `write`, `rm`, `mv`,
`mkdir` and `commit` all take `--actor` (or read `ORIGOFS_ACTOR`), and route
through the same engine check. A command that names nobody is unattributed and
records no blame; to make that an error rather than a silent gap:

```bash
origofs --workspace "$WS" require-attribution on   # every mutation must say who
```

That is an attribution-completeness switch, **not** access control. An actor id on
a command line is asserted by whoever writes the command line, and a process that
can reach the workspace directory can reach `meta.db` directly. Identity is only
*verified* where something resolves it server-side — the HTTP API, which refuses
to run unauthenticated off-loopback.

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
let changed: Vec<String> = ws.revert_session(agent_id, session_id, None).await?;
```

Reachable from every surface:

```bash
origofs revert-session --actor 7 --session 42 --by 1   # --by is checked against the write policy
```

```python
changed = await ws.revert_session(agent_id, session_id)
```

```http
POST /v1/revert-session   {"actor": 7, "session": 42}
```

It returns the paths it changed, not just a count, so a caller can invalidate
exactly the caches that went stale. Pass a **path scope** to bound the blast
radius — what a multi-tenant host needs, since an "undo the agent's work" button
lives in one tenant's UI while the session it reverts may have written anywhere:

```python
changed = await ws.revert_session(agent_id, session_id, path_prefix="/tenant-a")
```

The prefix matches on directory boundaries, so `/tenant-a` covers
`/tenant-a/notes.txt` and never `/tenant-abc/notes.txt`. Scoping inside the call
is also what makes it safe: pre-flighting with `edit_ops` and then reverting
reads the session's reach and acts on it in two steps, so a write landing in
between is reverted without ever having been checked.

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
`y-websocket`) collaborates out of the box.

A document comes in **two shapes**, and a client picks the one its editor speaks:

| Shape | Endpoint | Bind with |
|---|---|---|
| **flat** — one `Y.Text` | `GET /coedit/{path}` | `y-websocket`, any plain-text/Markdown editor |
| **tree** — a `Y.XmlFragment` | `GET /coedit-tree/{path}` | `@platejs/yjs`/`@slate-yjs/core`, `y-prosemirror`, TipTap |

The flat shape is the right one for source files and anything a diff tool reads.
The tree shape is for rich-text editors, which bind to a structured document
natively — see [Structured co-editing](#structured-co-editing-rich-text-editors).

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

A browser can't set headers on a WebSocket upgrade, so the credential rides in
the one header it *can* set — the subprotocol list:

```js
new WebSocket(`wss://host/v1/coedit/notes.md`, ["origofs", token])
```

The server echoes back `origofs` (never the token) and authenticates it through
the same hook as every other route. A `?token=` query param also works and stays
supported, but a URL is the worst place for a credential — it lands in access and
proxy logs by default. Each connection is bound to a **session**, opened for it
if the credential didn't name one, so a sitting's live edits can be undone as a
unit with `revert_session`.

**Frames carry the y-websocket envelope** — an outer message tag (`messageSync`
= 0) wrapping the y-sync payload — which is what `y-websocket` and `y-protocols`
emit, so a browser client needs no thought here. It matters if you write a client
against y-sync *directly*: a bare update frame starts with `messageYjsUpdate` = 2,
which is `messageAuth` in the envelope, so it decodes cleanly and does nothing.
The socket then connects, handshakes, reports the right peer count and never
converges. origofs logs any such frame at `warn` and reports its tag in
`SyncReply.unhandled`, so the mismatch is diagnosable rather than a mystery.

#### Ctrl+Z — `POST /coedit-undo/{path}`

Undo pops **one actor's own** most recent action, never a collaborator's
paragraph and never an edit that arrived from another worker. Send
`{"redo": true}` for the other direction:

```js
await fetch(`/v1/coedit-undo/notes.md`, {
  method: "POST",
  headers: { authorization: `Bearer ${token}` },
  body: JSON.stringify({ redo: false }),
})
```

It is a request *beside* the socket rather than a message on it, because the
result already travels the room's y-sync fan-out — only the request needed a
channel, and a new message tag would break the unmodified Yjs clients the socket
exists to serve. The response says `changed`, distinguishing "undone" from
"nothing to undo" so you can grey the button out, and `available` (see below).

Four things to know before building on it:

- **An undo is a write**, so it takes `WRITE` at the path exactly as opening the
  document does. A propose-only actor is refused (`403`), not silently ignored —
  there is no such thing as a proposed undo.
- **Undo does not rewrite authorship.** Undoing a deletion gives the text back to
  *its* author, not to whoever pressed the key, and the op-log keeps every edit
  that happened. An undo that erased evidence would be a way to launder an
  agent's edits out of the audit trail, which is the opposite of the point.
- **A stack does not outlive the room.** Undo is an editor affordance, not
  history: it does not survive a reconnect, a `checkout`, or a worker restart,
  and your stack goes when your last socket on that document closes. Two tabs
  share one stack. For anything durable, reach for `revert_session` (a whole
  sitting), the trash (a delete), or `checkout` (a commit).
- **Both shapes work.** A tree document names its `XmlFragment` root in the
  request (`{"redo": false, "root": "content"}`) — the same `root` its socket was
  opened with. Both shapes can hold one path at once, so the root is what picks
  the room, and a request without one addresses the flat shape. The tree's *file*
  still moves only when your next `coedit-tree-checkpoint` lands bytes, because
  origofs cannot serialize a tree; the live document moves immediately.

**Across workers, exactly one holds an actor's stack.** A room lives in one
process's memory and so does its undo stack, so behind a load balancer one
person with two tabs can land on two workers. Letting both keep a stack is not
safe: origofs's author stamp is written in the same undo step as the insert it
describes, so one worker's undo can strip a stamp the other's restore needs, and
the restored text comes back **unattributed** for the next checkpoint to credit
to whoever triggered it.

So a worker **claims** `(path, actor)` before it starts recording undo, and only
one can hold it. The response says `available` alongside `changed`, and they are
different answers: `available: false` means the actor's history exists but lives
on another worker — show "undo is active in another window", not "nothing to
undo". The claim carries a 60-second lease a live worker renews, so a crashed one
frees the actor rather than denying them undo forever, and it is released as soon
as the holding worker's last socket for that actor closes. Nothing changes for a
single-worker deployment: two tabs there are the same holder and share one stack.

You can avoid ever seeing `available: false` by routing an actor's sockets for
one path to one worker (sticky sessions on actor+path).

While a document is open, its stored bytes are the last **checkpoint** — real,
fully attributed, but possibly behind what people are typing. origofs records that
as a per-path **live marker**, and the rule is to *surface* the staleness, never to
block on it:

```rust
let (bytes, live) = ws.read_live("/notes.md").await?;   // never blocks, never fails
if live.is_some() { /* these bytes may lag an open editor */ }
for doc in ws.live_paths().await? { … }                 // everything open right now
```

**How far behind, and for how long.** A room's CRDT lives in the server's memory,
so the durable bytes are only as fresh as the last checkpoint. By default a room
is checkpointed **5 seconds after it goes quiet** and **at least every 60 seconds**
while it stays busy — so the window a crash could lose is bounded by that, not by
how long someone leaves a tab open. Both triggers are configurable (and can be
turned off, for an embedder driving checkpoints itself):

```rust
use origofs_sdk::api::{ApiOptions, CheckpointPolicy};
let options = ApiOptions {
    checkpoint: CheckpointPolicy { idle_after: Some(Duration::from_secs(2)), ..Default::default() },
    ..Default::default()
};
```

```python
build_router(ws, authn=authn, checkpoint=CheckpointPolicy(idle_after=2.0))
```

The live marker carries `checkpointed_at`, so a UI can show "last saved 3 minutes
ago" rather than only "this may be stale".

`read` keeps its contract unchanged and always answers; a reader is simply told
whether the answer may be behind. Nothing forces a checkpoint on a reader's
behalf — a read must not write, a checkpoint needs an actor to attribute it to,
and the live document is in-process room state the engine cannot reach anyway. A
caller that needs the freshest bytes (a release build, a `git` export) checkpoints
the co-editing coordinator first, then reads; `origofs`'s own git export warns and
lists any live path rather than exporting stale bytes silently. `end_coedit` clears
the marker once the final checkpoint has landed.

#### Structured co-editing (rich-text editors)

Every mainstream rich-text CRDT binding — `@platejs/yjs`/`@slate-yjs/core`,
`y-prosemirror`, TipTap — binds to a nested XML **tree**, not a flat `Y.Text`. Point
one at `GET /coedit-tree/{path}?root=content` and it binds directly:

```js
const doc = new Y.Doc()
new WebsocketProvider(`wss://host/v1/coedit-tree`, "notes.md", doc, {
  params: { root: "content" },
  protocols: ["origofs", token],
})
// …bind doc.getXmlFragment("content") with your editor's Yjs plugin…
// …or, for PlateJS/Slate: doc.get("content", Y.XmlText), which is what
//    @slate-yjs/core binds. Both address the same branch — see below.
```

**Slate and Plate root at a `Y.XmlText`, and that is fine.** origofs binds the
room as a `Y.XmlFragment`, which looks like a mismatch, but Yjs keys root types by
*name*: `doc.get(name, T)` binds a view of whatever branch is already there rather
than asserting a type the peer must match, so both sides address the same branch
and converge. Pinned against bytes from a real `@slate-yjs/core` client in
`crates/origofs-core/tests/coedit_tree_slate.rs`.

The one thing such a host must handle: origofs's `a` (author) and `n` (node id)
stamps are ordinary Yjs *formatting attributes*, and on the Slate binding a
formatting attribute is a **mark** — so every text node arrives with two marks it
did not author.

```json
{"a": "7,0", "n": "3f2a.0", "bold": true, "text": "world"}
```

That is deliberate: `n` is the token you cite in the span map at checkpoint time,
so it has to be readable from the client. Configure your schema to **ignore**
`a`/`n` rather than strip them — the server re-asserts them on every apply, so a
normalizer that removes them will fight it.

Content is attributed exactly as on the flat shape — server-side, to the socket's
authenticated actor, whatever the bytes claim. What differs is how bytes reach
disk. **origofs does not own your document schema**, so it will not serialize a
tree: picking Markdown or HTML would make the engine responsible for a document
model and a dialect. Instead you serialize, and say which byte ranges came from
which co-edit node:

```js
await fetch(`/v1/coedit-tree-checkpoint/notes.md`, {
  method: "POST",
  headers: { authorization: `Bearer ${token}` },
  body: JSON.stringify({ body, spans }),   // spans: [{start, end, node}]
})
```

Each `node` is an id **origofs assigned** when it stamped that run — read it off
`ytext.toDelta()` (`attributes.n`) or `element.getAttribute("n")`. You name byte
ranges and nodes; origofs resolves each node to the author it stamped itself and
lands the result in the same byte-range blame index as every other write. A
request can never name an author, and an id origofs never issued resolves to
nobody. Bytes no span covers — your serializer's own punctuation — are attributed
to the caller.

Three consequences worth knowing before you build on it:

- **A wrong span map means wrong blame.** origofs validates that spans are ordered,
  non-overlapping, in range, and on character boundaries, but it cannot check that
  you mapped the right bytes to the right node — it cannot read your serializer.
  That is the price of not owning the schema.
- **A stale sidecar opens an empty document — and it will not overwrite one.**
  origofs can rebuild a flat document from the file's text and blame; it cannot
  rebuild a *tree*, because parsing bytes back into nodes needs your schema. So
  check `resumed()`, seed from `read(path)` when it is false, and say so with
  `seeded_from(body)`. Until you do, a checkpoint over a **non-empty** file is
  refused (`ForeignWriteError`, `409`) rather than performed. It used to be
  performed: an empty body landed over real content with nothing failing at any
  earlier point. Over HTTP the same declaration is `"seeded_from_file": true` in
  the checkpoint body.
- **An out-of-band write is refused, not merged.** The flat path folds a foreign
  write in by diffing text into CRDT operations; a tree cannot be reconciled that
  way, so the checkpoint fails with `ForeignWriteError`. Re-read, reseed,
  checkpoint again. The comparison is against the *document's* own base, so it
  holds for a socket-less checkpoint too, and re-opening the room before each
  checkpoint no longer resets it.

Durability is split to match: the server persists the CRDT sidecar on the same
sweeper tick as a flat room — so a crash costs no editing history — while the file
and its blame move only when you checkpoint. From Rust or Python:

```python
doc = await ws.open_coedit_tree(ctx, "/notes.md", "content")
if not await doc.resumed():
    seed = await ws.read("/notes.md")
    ...                             # parse it into the tree — your schema, your parse
    await doc.seeded_from(seed)     # ...and now origofs knows the document covers it
await ws.checkpoint_coedit_tree(ctx, "/notes.md", doc, body, spans)
await ws.persist_coedit_tree("/notes.md", doc)   # CRDT only; the file stays put
```

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
sessions, diff, suggestions, and the live co-editing WebSockets (flat and
[tree](#structured-co-editing-rich-text-editors)).

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
mapping; `serve` refuses to bind a non-loopback address without one. Set the same
specs in **`ORIGOFS_AUTH_TOKENS`** (newline- or comma-separated) to keep tokens out
of `ps` and shell history, as `ORIGOFS_ENCRYPTION_KEY` already is. Errors come
back as a machine-readable envelope (`{"error":{"code","message","retryable"}}`)
and every response carries an `x-request-id`.

**Reads are open unless you close them.** Writes always need a credential; reads
do not, which is why the `curl` calls above fetch files, events and presence with
no `AUTH`. That is the right default for a loopback dev server and the wrong one
for anything else — an open read serves file bytes, blame, the audit log and the
review queue. Pass **`--gate-reads`** to require the same credential on reads, and
**`--root /tenant-a`** to restrict what the surface can address at all. `serve`
warns when it binds a non-loopback address without read gating.

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
pip install origofs
```

Wheels are **abi3**, so one per platform covers CPython ≥ 3.9 — no Rust toolchain
at install time. They're built for manylinux (x86_64/aarch64), macOS
(arm64/x86_64) and Windows x64, and attached to every
[release](https://github.com/danplischke/origofs/releases) as well as published
to PyPI. The integrations below ship as extras: `fastapi`, `fsspec`, `upath`,
`llamaindex`, `markitdown`, `db`.

To build it yourself instead — a platform without a wheel, or working on the
bindings:

```bash
cd crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin && maturin develop     # builds the extension + installs `origofs`
```

`Workspace.mount()` (FUSE) is Linux-only in the published wheel: `fuser` needs
macFUSE on macOS, which is a kernel extension a wheel cannot carry. macOS mounts
over NFSv3 with `serve_nfs` instead. On Windows neither mount path exists — both
methods raise a clear error — and the rest of the binding works normally. The
Windows wheel is built and exercised on every pull request, not just at release
time.

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
a bad argument `ValueError`, a stale suggestion base `origofs.StaleBaseError` and
a foreign write around a co-edit document `origofs.ForeignWriteError` — both
subclasses of `origofs.ConflictError`, so catching that still catches either, and
the type tells you which recovery to run (re-diff and re-suggest, versus reseed
the document and checkpoint again). `accept_suggestion` returns the content
address it landed, so you can confirm what is now at the path without re-reading.

### FastAPI — bring your own auth

origofs ships no authentication, on purpose: a blame trail is only trustworthy if
the identity behind each write is, and only your app knows how to resolve it.
`origofs.fastapi.build_router` wires up every workspace endpoint against an auth
dependency **you** provide:

```python
# app.py — pip install "origofs[fastapi]"
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
import origofs
from origofs.fastapi import build_router


async def authn(
    x_actor_id: Optional[int] = Header(default=None),
    x_session_id: Optional[int] = Header(default=None),
) -> origofs.WriteCtx:
    """Resolve the request's principal to the actor to attribute it to."""
    # swap this for your real auth: decode a JWT, look up a session, validate
    # an agent token — then map that principal to an origofs actor id
    if x_actor_id is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    if x_session_id is not None:
        return origofs.WriteCtx.session(x_actor_id, x_session_id)
    return origofs.WriteCtx.actor(x_actor_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    ws = await origofs.Workspace.open_local("meta.db", "cas")   # or open_pg_s3(dsn, cfg)
    app.include_router(build_router(ws, authn=authn), prefix="/fs")
    app.state.ws = ws
    yield


app = FastAPI(lifespan=lifespan)


# your own endpoints sit alongside it — e.g. onboarding, which maps your user id
# to the actor `authn` will later resolve to (idempotent, so no side table)
@app.post("/users")
async def upsert_user(external_id: str, name: str):
    return {"actor_id": await app.state.ws.find_or_create_human(external_id, name)}
```

```bash
uvicorn app:app --reload

curl -X PUT --data-binary 'hello' -H 'X-Actor-Id: 1' \
     http://127.0.0.1:8000/fs/files/notes.txt
curl http://127.0.0.1:8000/fs/files/notes.txt        # → hello
curl http://127.0.0.1:8000/fs/blame/notes.txt        # credited to actor 1
curl -X PUT --data-binary 'x' http://127.0.0.1:8000/fs/files/y   # 401: no identity
```

One `build_router` call mounts the whole workspace: files, dirs, stat, rename,
blame, commit/log/status, diff, branches/checkout, the suggestion review queue,
the change feed, presence, actors/sessions, and the
[co-editing](#live-co-editing-crdt) WebSockets at `/coedit/{path}` and
`/coedit-tree/{path}` with its `/coedit-tree-checkpoint/{path}` (long-lived rooms
are created once per router, not per request), and per-actor
[undo/redo](#ctrlz--post-coedit-undopath) at `/coedit-undo/{path}`.

Mount a subset with `include=`/`exclude=`, naming route *groups* rather than
paths — `files`, `blame`, `history`, `suggestions`, `revert`, `actors`,
`presence`, `coedit-ws`, `coedit-checkpoint`, `trash`, `health` (plus `coedit`
for both co-editing groups). `build_coedit_router(ws, authn=...)` is the named
shortcut for the co-editing surface alone, and
`build_coedit_router(..., checkpoint_route=False)` gives you the sockets without
origofs's tree-checkpoint route — for a host that enforces its own authorization
on body writes and wants exactly one such path, its own.

One workspace can hold many tenants under scoped paths. Pass `root=` — fixed, or
a dependency resolving it per request — and the router scopes itself:

```python
app.include_router(build_router(ws, authn=authn, root="/tenants/acme"))
```

A caller then asks for `/files/notes.md` and gets `/tenants/acme/notes.md`; there
is no representable request for another tenant's file, because the root is
prepended rather than compared against. Listing routes (`/status`, `/diff`,
`/events`, `/presence`, `/suggestions`) are filtered to the root, and the
id-addressed suggestion routes answer `404` outside it — `404`, not `403`, so a
caller can't probe which ids exist. Operations no filter can narrow — commit,
branches, checkout, and the commit log — are refused with `403`; mount an
unscoped router for the operator surface that needs them.

Every mutating route depends on `authn` and hands its `WriteCtx` straight to an
**attributed** workspace call — the request body never names an actor, so a
client can't forge attribution, and the caller's write policy is enforced by the
engine rather than route by route. A propose-only actor's `PUT` or `DELETE`
lands in the review queue instead of the working tree; rename, mkdir, commit,
branch, checkout and registering actors are refused with `403`. Namespace
mutations carry an actor too, so "who deleted this file" has an answer.
Reads are open by default: pass `reader=<dependency>` to
gate them, or `dependencies=[…]` (forwarded to `APIRouter`, along with `tags`
and the rest) to gate everything. `GET /files/{path}` streams rather than
buffering a whole file, and honors a single-range `Range` header (`206`/`416`),
so large files behave like a static file server.

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

## Operating a workspace

Four things you configure rather than call: what may be recovered, who may reach
what, how much space is used, and whether file locks coordinate between mounts.
All four are **off or unlimited by default** — turning one on changes behaviour
for everyone using that workspace, so none of them arrives with an upgrade.

### Undo a delete — `origofs trash`

A committed file can always be read back out of history. An **uncommitted** one
could not be recovered at all, which matters more here than on an ordinary
filesystem: the users are agents, and `rm -rf` on a bad path is a routine failure
mode.

```bash
origofs trash retention          # "trash is disabled" — the default
origofs trash retention 7d       # start collecting, keep entries a week

origofs rm /draft.md --actor 2
origofs trash list               # #1  file  5  actor=2  /draft.md
origofs trash restore 1 --actor 2
```

Entries record **who deleted them**, so a restore is attributed and the deletion
is already in the op-log beside it. Retention is off by default because turning
it on silently would change when space is reclaimed for every existing
deployment, and the first anyone would learn of it is a storage bill. An empty
listing therefore tells you *which* empty it is — nothing deleted, or not
collecting.

### Scope what an agent can reach — `origofs acl`

Grants are `(actor, path prefix) → permissions`, longest prefix wins, matched on
directory boundaries so `/tenant-a` never covers `/tenant-abc`.

```bash
origofs acl grant 1 /src read+write
origofs acl check 1 /src/main.rs      # actor 1 at /src/main.rs: read+write
origofs acl show
```

**A grant on its own restricts nothing.** With `default-deny` off — the default —
an actor with no matching grant falls back to its write policy, which for an
ordinary actor is full access:

```bash
origofs acl check 1 /secrets.txt      # actor 1 at /secrets.txt: read+write+propose
origofs acl default-deny on
origofs acl check 1 /secrets.txt      # actor 1 at /secrets.txt: none
```

That is what `acl check` is for: it answers the question an ACL bug is actually
asking, after prefix matching *and* the fallback. Reads are a separate switch
(`origofs acl enforce-reads on`) because reads have never been checked — turning
it on without writing read grants first stops every actor at once.

Grants are enforced in the engine, so they apply the same over MCP, the HTTP API
and a mount. Pass `--by` to grant *as* an actor: that requires `WRITE` at the
prefix and refuses to hand out a bit the granter does not hold there. Omitting it
provisions as the workspace owner and says so.

### Measure and cap — `origofs du` / `origofs quota`

```bash
origofs du /                     # /   2 inodes   6 bytes
origofs quota                    # bytes:  6 / unlimited
                                 # inodes: 2 / unlimited
```

Both count an inode with several names once, and sum **logical** size — never
deduplicated bytes. A quota measured in physical bytes would move under a user
who changed nothing, because someone else's write can dedup against theirs.

### Share file locks between mounts — `origofs posix-locks`

```bash
origofs posix-locks              # posix-locks is off
origofs posix-locks on
origofs posix-locks --path /notes.md
```

This is **not** "locking on/off". A FUSE mount that does not answer `fcntl` locks
still has working advisory locks — the kernel serves them locally, per mount — so
a single mount already coordinates its own processes. What this adds is
coordination *between* mounts: two processes, on two machines, against one
workspace. It also takes locking over from the kernel for that mount, which is
why it is a deliberate switch rather than a default.

Locks are taken by mounts, not by the CLI, and they carry a lease so a mount that
dies does not hold a byte range forever. Mounts read the setting once, at mount
time — remount to pick up a change. NFS exports do not support this: NFSv3
locking is a separate protocol (NLM) that origofs does not speak.

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

### Upgrading origofs

**Nothing in the content store is ever migrated.** Objects are immutable and
addressed by the hash of their own bytes, so a format change adds new objects and
leaves every existing one valid and readable. That includes encrypted stores:
objects carry a version header, the Argon2id parameters are recorded next to the
salt, and both are read back exactly as written no matter which build wrote them.
Upgrading origofs never means rewriting a bucket.

**The metadata database is migrated in place, forward only.** There are no
down-migrations, deliberately: a step that dropped a column the newer build had
been filling would destroy everything written since the upgrade. So the way back
is a snapshot taken before the step, which is what `--backup` is for:

```bash
origofs --workspace ./ws migrate --check                   # what would be applied?
origofs --workspace ./ws migrate --backup ./pre-upgrade.db # snapshot, then apply
```

Both read the database *before* migrating it — opening a workspace applies pending
steps, so anything asked afterwards can only describe what it just did.

**Rolling back the binaries is safe.** A build that meets a database from a newer
origofs refuses to open it (`unsupported_version`, "upgrade origofs") rather than
working against a schema it does not know, and the refusal leaves the database
untouched — so rolling forward again is a recovery, not a repair. To actually run
the older build, restore the pre-upgrade snapshot; the content store needs no
rollback.

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
    `Workspace::open_s3`. Exercised in CI against a live MinIO on every push.
  - **Google Cloud Storage, natively** — `Workspace::open_gcs`, over GCS's JSON
    API with OAuth2: a service-account key/file, Application Default Credentials
    (`GOOGLE_APPLICATION_CREDENTIALS` / `gcloud`), or GKE workload identity — no
    HMAC keys needed.
    **Caveat: this one is not exercised against a live backend.** Its builder —
    credential precedence, plaintext-endpoint handling — has unit coverage, and
    everything past construction is the same `ObjectContentStore` code the MinIO
    leg runs end-to-end. But no CI job has ever pointed native GCS at a real
    bucket, because no GCS emulator can stand in for one: `object_store` writes
    objects with a bare XML-API `PUT`, and the emulators
    (`fsouza/fake-gcs-server`, `oittaa/gcp-storage-emulator`) don't serve that
    shape — every write is rejected before it stores anything. The suite is
    there and passes against a real bucket (`ORIGOFS_GCS_TEST_*`, see
    `crates/origofs-core/tests/content_backends.rs`); it needs credentials CI
    doesn't have. Prefer the S3-interop flavour above if you want the path with
    continuous coverage, and validate `open_gcs` against your own bucket before
    relying on it.
- **Encryption at rest** — wrap any backend so content is encrypted
  (XChaCha20-Poly1305) before it touches disk or the network, transparently to
  the engine. The address stays the plaintext hash, so **dedup still works**
  (convergent encryption). Set `ORIGOFS_ENCRYPTION_KEY` or use
  `Workspace::open_local_encrypted`.

Content is addressed and never overwritten, so churn leaves orphaned chunks
behind; mark-and-sweep garbage collection reclaims them:

```bash
origofs --workspace "$WS" gc     # safe alongside writers (age-gated); cheapest when idle
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

**Platforms.** Everything above runs on Linux and macOS. On **Windows** the
portable surfaces — CLI, Rust SDK, Python, HTTP API, MCP and git interop — work
as documented, over either metadata backend. The three that are kernel
interfaces Windows has no equivalent of are compiled out there: FUSE, NFS, and
the overlay/sandbox mount (which is built on overlayfs whiteouts). `origofs
mount`, `origofs nfs`, `origofs sandbox` and `origofs overlay` still exist as
subcommands and exit with a message explaining why they can't run, rather than
disappearing from `--help`. Each platform is built and tested in CI.

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

MIT — see [`LICENSE`](LICENSE).
