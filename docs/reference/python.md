# Python

`origofs-py` is the binding layer for the surface most people build on: a service
that already knows *which user or agent* is calling. Every I/O method is a
coroutine, so it drops straight into `async def` handlers, and structured results
come back as plain JSON-serializable `dict` and `list`.

```bash
pip install origofs
```

See [Install](../getting-started/install.md#python) for wheels, extras and
building from source.

## The workspace

```python
import asyncio, origofs

async def main():
    ws  = await origofs.Workspace.open_local("meta.db", "cas")
    dan = await ws.create_human("dan")
    ctx = origofs.WriteCtx.session(dan, await ws.create_session(dan))

    await ws.write_as(ctx, "/notes/a.txt", b"hello")
    print(await ws.read("/notes/a.txt"))

    for span in await ws.blame("/notes/a.txt"):
        print(span["line_start"], span["line_end"], span["actor"]["name"])

asyncio.run(main())
```

Or the production pairing — Postgres metadata with S3 content:

```python
cfg = origofs.S3Config(bucket="my-bucket", region="us-east-1")
ws  = await origofs.Workspace.open_pg_s3(dsn, cfg)      # GcsConfig / open_pg_gcs too
```

`WriteCtx` is how identity travels. `WriteCtx.actor(id)` attributes to an actor;
`WriteCtx.session(id, session)` also records the session, which is what makes
[`revert_session`](../guides/attribution.md#undo-one-session) able to undo a
whole run.

To map your application's user id onto an origofs actor, without keeping a side
table:

```python
actor_id = await ws.find_or_create_human(external_id, name)   # idempotent
```

The binding covers the workspace, versioning, merge, attribution, suggestions,
the change feed, multi-workspace, maintenance (`gc`, `flush`, `repack`,
`backup_metadata`) and encryption at rest. Genuinely Rust-only today: the
[`resync`](../guides/teams.md#working-offline-then-rejoining) reconciliation flow,
direct object push/fetch between workspaces, and assembling a custom backend stack
by hand.

## Large files

`write_path_as` chunks incrementally *and* records blame, so attribution does not
cost you the ability to write a file larger than memory. See
[Limits](../LIMITS.md).

## FastAPI

origofs ships **no authentication, on purpose**: a blame trail is only trustworthy
if the identity behind each write is, and only your application knows how to
resolve it. `build_router` wires up every workspace endpoint against an auth
dependency you provide.

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
    # swap this for your real auth: decode a JWT, look up a session, validate an
    # agent token — then map that principal to an origofs actor id
    if x_actor_id is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    if x_session_id is not None:
        return origofs.WriteCtx.session(x_actor_id, x_session_id)
    return origofs.WriteCtx.actor(x_actor_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    ws = await origofs.Workspace.open_local("meta.db", "cas")
    app.include_router(build_router(ws, authn=authn), prefix="/fs")
    app.state.ws = ws
    yield


app = FastAPI(lifespan=lifespan)
```

```bash
uvicorn app:app --reload

curl -X PUT --data-binary 'hello' -H 'X-Actor-Id: 1' \
     http://127.0.0.1:8000/fs/files/notes.txt
curl http://127.0.0.1:8000/fs/blame/notes.txt        # credited to actor 1
curl -X PUT --data-binary 'x' http://127.0.0.1:8000/fs/files/y   # 401: no identity
```

One `build_router` call mounts the whole workspace: files, dirs, stat, rename,
blame, commit/log/status, diff, branches, checkout, the review queue, the change
feed, presence, actors and sessions, and the co-editing WebSockets. Long-lived
co-editing rooms are created once per router, not per request.

Every mutating route depends on `authn` and hands its `WriteCtx` straight to an
attributed workspace call — **the request body never names an actor**, so a client
cannot forge attribution, and the caller's write policy is enforced by the engine
rather than route by route.

Reads are open by default; pass `reader=<dependency>` to gate them, or
`dependencies=[…]` (forwarded to `APIRouter`) to gate everything. `GET
/files/{path}` streams rather than buffering, and honours a single-range `Range`
header, so large files behave like a static file server.

### Multi-tenancy

One workspace can hold many tenants under scoped paths. Pass `root=` — fixed, or
a dependency resolving it per request:

```python
app.include_router(build_router(ws, authn=authn, root="/tenants/acme"))
```

A caller asks for `/files/notes.md` and gets `/tenants/acme/notes.md`. There is no
representable request for another tenant's file, because the root is
**prepended** rather than compared against.

Listing routes are filtered to the root, and id-addressed suggestion routes answer
`404` outside it — `404`, not `403`, so a caller cannot probe which ids exist.
Operations no filter can narrow — commit, branches, checkout, the commit log — are
refused with `403`; mount an unscoped router for the operator surface that needs
them.

See [Multi-tenancy](../MULTI_TENANCY.md).

## The PyData stack

`origofs.fsspec.OrigoFileSystem` is a genuine `fsspec.asyn.AsyncFileSystem` —
usable synchronously *and* by awaiting the `_`-prefixed coroutines on your own
loop — so pandas, Dask, PyArrow and Zarr work against a workspace with no glue:

```python
import origofs.fsspec                                    # registers "origofs://"
import pandas as pd

df = pd.read_parquet("origofs:///data/events.parquet",
                     storage_options={"db_path": "meta.db", "cas_dir": "cas"})

fs = origofs.fsspec.OrigoFileSystem(db_path="meta.db", cas_dir="cas", actor=actor)
fs.pipe_file("/notes.txt", b"hello")        # attributed — fs.blame() credits it
fs.cat_file("/notes.txt", start=0, end=5)   # ranged: reads only the covering chunks
```

It passes fsspec's own conformance suite. With the `upath` extra, a workspace is a
`pathlib`-shaped `UPath("origofs:///notes.txt", db_path=…, cas_dir=…)`.

## RAG with provenance

Retrieval over origofs is not "object storage plus embeddings": each passage
carries **who wrote it**, from blame, and is keyed by a content hash — so only
genuinely changed passages need re-embedding.

```python
from origofs.rag import read_passages
for p in await read_passages(ws, root="/docs", segmentation="content_defined"):
    p.text, p.hash, p.authors        # per-byte authorship, not a file-level guess

from origofs.llamaindex import SimpleWorkspaceReader     # one Document per passage
docs = SimpleWorkspaceReader(ws, root="/docs", convert="markitdown").load_data()
```

`content_defined` segmentation is the default because it puts boundaries where the
bytes decide — an edit near the top of a file does not reshuffle every later
passage's hash.

Non-text documents (PDF, DOCX, …) go through a pluggable converter to Markdown
first. Their provenance is **document-level**, because the extracted text no
longer maps to the original bytes.

## Feeds, agents and mounts

```python
sub = await ws.subscribe(after_seq=0, branch="main")   # Postgres LISTEN/NOTIFY push
events = await sub.recv()          # blocks until the next batch — ideal behind SSE

from origofs.overlay import run    # agent in a live overlay, attributed as it types
code = await run("./ws", agent_actor, ["claude", "-p", "refactor the parser"])

with ws.mount("/mnt/origofs", ctx=ctx) as m: ...   # FUSE (Linux)
await ws.serve_nfs("127.0.0.1:11111", ctx=ctx)     # a task that runs until cancelled
```

`ctx=` binds the mount to an actor, exactly as `--actor` does on the CLI. Passing
`None` is the anonymous mount that bypasses ACLs — see
[POSIX mounts](../guides/mounts.md#a-mount-is-bound-to-one-actor).

The bindings enable the SDK's `coedit` feature always, `nfs` on Unix, and `fuse`
on **Linux only** — narrower than Unix, because macFUSE is a kernel extension a
wheel cannot carry. macOS mounts over NFSv3 instead.

## SQLAlchemy and Alembic

`origofs.db` declares the whole metadata schema as SQLAlchemy models with packaged
Alembic migrations, for when a deploy step — rather than the engine's automatic
on-open migration — should own the database, and for read-only reporting queries
alongside the async API.

## Worked example

`crates/origofs-py/examples/collab_app.py` runs the whole story end to end — a
human writes, an agent suggests, a reviewer accepts — with no server and no
`curl`.
