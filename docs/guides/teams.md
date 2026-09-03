# Running for a team

Solo, a workspace is a SQLite file and a local directory. For a shared
human-and-agent workspace, run origofs on **Postgres** — the backend built for
many concurrent writers — with content in an object store.

```bash
origofs --config team.toml serve --addr 0.0.0.0:8080 --auth-token "$TOKEN=$ACTOR"
```

See [Configuration](../reference/configuration.md) for the TOML, and
[Storage backends](../reference/storage-backends.md) for the content side.

## Why Postgres

Atomic-create is serialized, so racing writers never leave orphaned inodes, and
the whole write path is transactional: content is made durable first, then
metadata, blame and the audit log commit together. A crash cannot leave a
half-recorded edit.

```rust
let ws = Workspace::open_pg("host=db port=5432 user=origofs dbname=origofs", content).await?;
```

SQLite remains the right choice for solo and offline work: one portable file,
full speed, no server.

## The change feed

Every operation lands on an append-only feed — who touched what, who committed —
and it is **exactly-once and in commit order** even under concurrent writers.
Every event is **branch-scoped**, so a UI showing `main` filters to one branch.

```bash
origofs --workspace "$WS" watch --follow    # seq  kind  actor  path
origofs --workspace "$WS" presence          # who is active right now
```

Over HTTP that is `GET /v1/events?since=N`. On Postgres, clients can be *pushed*
to rather than polling: `PostgresMetadataStore::subscribe(after_seq, branch)`
returns a `LISTEN`-backed subscription whose `recv()` wakes on every committed
change.

!!! note

    `subscribe` is Postgres-only and deliberately not on the object-safe
    `MetadataStore` trait. SQLite callers use `watch`, which polls.

## Presence

Each session heartbeats which actor is where, so a UI can show who else is in a
file:

```bash
curl -H "Authorization: Bearer $TOKEN" \
     -X POST -d '{"path":"/notes/a.txt"}' http://host:8080/v1/presence   # I'm here
curl http://host:8080/v1/presence                                        # who's active
```

The heartbeat takes an optional path and nothing else — the actor and session
come from the credential, so a browser client cannot heartbeat anyone but itself.
Presence is keyed by session, so a credential not bound to one gets a `400`
telling it to create a session, rather than having one minted per heartbeat.

## Working offline, then rejoining

`resync` reconciles a solo workspace with the shared one through the same
three-way merge that powers `origofs merge` — not a separate code path:

```bash
origofs --workspace ./offline resync --remote ./shared --branch main -m "back online"
```

```text
main: merged as 3a9f21c8b4d0 and pushed
  fetched 41 object(s), 2203648 B (6 already present)
  pushed  17 object(s), 856064 B (3 already present)
  blame carried: 12 in, 5 out
```

It works **between different backends** — your offline SQLite and local content
store against the team's Postgres and object storage — because that is the whole
point. A conflicting merge records conflicts exactly as an ordinary merge does and
leaves the shared branch untouched until you resolve them, and the shared branch
only ever moves under a compare-and-swap, so a teammate who pushed while you were
merging is never clobbered.

**Your attribution comes with you.** The lines an agent wrote offline are still
credited to that agent on the server afterwards, with its identity *mapped* into
the shared workspace rather than blindly copied onto whichever actor happens to
hold that id there.

## Live co-editing

Opt-in, behind the `coedit` feature: humans, agents and browser editors type into
the same document concurrently and converge, with authorship tracked **per
character**. It speaks the standard Yjs *y-sync* protocol, so an unmodified Yjs
client connects with no custom server code.

A document comes in two shapes, and a client picks the one its editor speaks:

| Shape | Endpoint | Bind with |
|---|---|---|
| **flat** — one `Y.Text` | `GET /coedit/{path}` | `y-websocket`; any plain-text or Markdown editor |
| **tree** — a `Y.XmlFragment` | `GET /coedit-tree/{path}` | `@platejs/yjs`, `y-prosemirror`, `y-slate`, TipTap |

Use the flat shape for source files and anything a diff tool reads; the tree
shape for rich-text editors, which bind to a structured document natively.

**The server stays the sole authority on who wrote what.** Whatever a client's
bytes claim, each inserted run is attributed to the *authenticated* actor. When a
session checkpoints, that character-level interleaved authorship lands in the
same byte-range blame index as ordinary writes — so two people editing one line
show up as two spans, not one collapsed line.

```rust
let doc = ws.open_coedit(ctx, "/notes.md").await?;    // resume the live CRDT
// … clients exchange y-sync updates over the WebSocket …
ws.checkpoint_coedit(ctx, "/notes.md", &doc).await?;  // crystallize into blame
```

### Authenticating a browser

A browser cannot set headers on a WebSocket upgrade, so the credential rides in
the one header it *can* set — the subprotocol list:

```js
new WebSocket(`wss://host/v1/coedit/notes.md`, ["origofs", token])
```

The server echoes back `origofs`, never the token. A `?token=` query parameter
also works and stays supported, but a URL is the worst place for a credential —
it lands in access and proxy logs by default.

Each connection is bound to a **session**, opened for it if the credential did
not name one, so a sitting's live edits can be undone as a unit with
[`revert-session`](attribution.md#undo-one-session).

Opening a co-editing socket requires `WRITE` at the path, not merely a valid
credential: the upgrade authenticates but does not authorize. To build a
*proposal* against a co-edited document without holding write, use the
[propose path](review.md#two-kinds-of-proposal) instead.

### Staleness is surfaced, never blocked on

While a document is open, its stored bytes are the last **checkpoint** — real and
fully attributed, but possibly behind what people are typing. origofs records
that as a per-path live marker:

```rust
let (bytes, live) = ws.read_live("/notes.md").await?;   // never blocks, never fails
if live.is_some() { /* these bytes may lag an open editor */ }
for doc in ws.live_paths().await? { /* everything open right now */ }
```

Over MCP an agent asks the same question with `origofs_live`, so it knows its read
may lag.

**How far behind, and for how long.** A room's CRDT lives in the server's memory,
so the durable bytes are only as fresh as the last checkpoint. By default a room
is checkpointed **5 seconds after it goes quiet** and **at least every 60
seconds** while it stays busy — so the window a crash could lose is bounded by
that, not by how long someone leaves a tab open. Both triggers are configurable,
and can be turned off for an embedder driving checkpoints itself:

```python
build_router(ws, authn=authn, checkpoint=CheckpointPolicy(idle_after=2.0))
```

### Across workers

When a document is edited on two processes at once, behind a load balancer, a
Postgres `LISTEN`/`NOTIFY` relay fans each update out so every worker's replica
converges.

!!! warning "A checkpoint never overwrites a foreign write"

    If a file changed underneath a live document, the flat shape folds the
    foreign write in by replaying the CRDT sidecar, and **refuses when it
    cannot** — a missing sidecar, a removed file, bytes that are no longer UTF-8.
    The tree shape always refuses, because origofs cannot parse arbitrary bytes
    back into nodes.

    A [branch checkout](versioning.md) is the case that makes this bite: it
    rematerializes the file *and* swaps away the sidecar, which lives in the
    working tree, while the live marker survives. Re-open the document after
    switching branches.

## Multi-tenancy

Serving several tenants from one deployment has its own set of rules — scoping,
id-addressed resources, and what a refusal is allowed to reveal. See
[Multi-tenancy](../MULTI_TENANCY.md).
