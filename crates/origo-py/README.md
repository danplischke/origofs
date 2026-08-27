# origo-py — Python bindings for origo

Async-native bindings so you can drive an origo workspace from Python — write your
own FastAPI endpoints, inject the authenticated user/agent behind each write,
and orchestrate FUSE/NFS mounts.

Every I/O method returns an **awaitable**, so it drops straight into `async def`
handlers. Structured results come back as plain `dict`/`list` (JSON-serializable).

## Install

```bash
pip install origofs
```

The distribution is **`origofs`** — `origo` on PyPI belongs to an unrelated
project — but the import name is `origo`:

```python
import origo
```

Wheels ship for CPython ≥ 3.9 on Linux (x86_64, aarch64 — manylinux_2_28),
macOS (Intel and Apple Silicon), and Windows x64. They are `abi3`, so one wheel
per platform serves every supported Python. Anywhere else, pip falls back to the
sdist and builds from source, which needs a Rust toolchain (≥ 1.85) and a C
compiler (SQLite is compiled in).

The FastAPI router integration is an extra: `pip install "origofs[fastapi]"`.

> FUSE mounting (`Workspace.mount`) is Linux-only. On macOS, use the NFS surface
> (`Workspace.serve_nfs`) instead — the macOS wheels are deliberately built
> without a FUSE mount implementation, so they need no macFUSE install.

## Build from a checkout

```bash
cd crates/origo-py
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop            # builds the extension module + installs it
pytest tests/              # end-to-end test
```

Wheels: `maturin build --release` (abi3, one wheel works on CPython ≥ 3.9).

## Releasing

`crates/origo-py/Cargo.toml`’s `version` is the single source of truth. Bump it,
commit, then tag:

```bash
git tag py-v0.1.0 && git push origin py-v0.1.0
```

`.github/workflows/release-py.yml` builds every wheel plus the sdist, smoke-tests
each one by importing it, and publishes to PyPI via Trusted Publishing (OIDC —
no API token is stored in the repo). It refuses to publish if the tag and the
manifest version disagree. A `workflow_dispatch` run can target TestPyPI for a
dry run.

## Use

```python
import origo

ws = await origo.Workspace.open_local("meta.db", "cas")
# ...or multi-writer on Postgres with S3-shared content (the production combo):
#   cfg = origo.S3Config(bucket="my-bucket", region="us-east-1")   # + endpoint/keys
#   ws  = await origo.Workspace.open_pg_s3(dsn, cfg)               # or open_pg_s3_packed
# object-store constructors verify content integrity on read (a bit-rotted object
# errors instead of being served). open_object_memory(db) runs the same adapter
# with no network, for local dev/tests.

# Map your app's user id to an origo actor (idempotent; no side table needed):
actor_id = await ws.find_or_create_human("user_42", "Dan")   # your id -> origo actor
# then inject the identity you resolved in your endpoint:
ctx = origo.WriteCtx.session(actor_id, session_id)   # or origo.WriteCtx.actor(actor_id)
await ws.write_as(ctx, "/notes.txt", b"hello")      # attributed -> blame + audit

diff = await ws.diff("main", "feature")             # [{"path","status"}, ...]
sid  = await ws.suggest(ctx, "/x", b"proposed")     # agent proposes; not applied
await ws.accept_suggestion(sid, origo.WriteCtx.actor(reviewer))  # applied, credited
```

Errors map to Python exceptions: missing path → `FileNotFoundError`, bad arg →
`ValueError`, a stale suggestion base → `origo.ConflictError`.

## FastAPI router (bring your own auth)

origo has no built-in authentication — a blame trail is only trustworthy if the
identity behind each write is, and that's yours to own. `origo.fastapi.build_router`
gives you every workspace endpoint wired up, with attribution driven by an auth
dependency **you** provide:

```python
from fastapi import FastAPI, Header, HTTPException
import origo
from origo.fastapi import build_router

async def authn(x_actor_id: int = Header(...)) -> origo.WriteCtx:
    # decode your JWT / session / agent token -> the origo actor to attribute to
    if x_actor_id is None:
        raise HTTPException(401)
    return origo.WriteCtx.actor(x_actor_id)

app = FastAPI()
app.include_router(build_router(ws, authn=authn), prefix="/fs")
```

Every mutating route depends on `authn` and passes its `WriteCtx` straight to the
workspace — the request body never names an actor, so a client can't forge
attribution. Reads are open by default; pass `reader=<dependency>` to gate them,
or `dependencies=[...]` (forwarded to `APIRouter`) to gate everything. Needs the
`fastapi` extra (`pip install "origofs[fastapi]"`). See `examples/fastapi_router.py`.

## Live change feed (push)

On Postgres, `subscribe` gives a real push feed (LISTEN/NOTIFY) — `await recv()`
blocks until the next batch instead of polling `watch`. Ideal behind a FastAPI
SSE/WebSocket endpoint:

```python
sub = await ws.subscribe(after_seq=0, branch="main")   # PG only; raises on SQLite
while True:
    events = await sub.recv()      # woken by NOTIFY; [] once the connection closes
    if not events:
        break
    for e in events:
        ...                        # push to the client
```

## Run an agent in a live overlay

`origo.overlay.run` launches an agent in a fast native kernel overlay while its
edits stream into origo, attributed to an actor — the way agents are meant to work
day to day. It shells out to the `origo` CLI (the overlay is host orchestration,
not embedded in the extension), operating on a workspace **directory** the API
also opens:

```python
import origo
from origo.overlay import run

ws_dir = "./ws"
api   = await origo.Workspace.open_local(f"{ws_dir}/meta.db", f"{ws_dir}/cas")
actor = await api.find_or_create_agent("agent-token", "claude", "opus")
code  = await run(ws_dir, actor, ["claude", "-p", "refactor the parser"])
```

Requires the `origo` binary on PATH and a Linux host with unprivileged
user-namespace overlays.

## Mount orchestration

```python
mount = ws.mount("/mnt/origo")        # FUSE, in the background; returns a handle
mount.unmount()                      # or use `with ws.mount(...) as m:`

import asyncio
nfs = asyncio.create_task(ws.serve_nfs("127.0.0.1:11111"))  # runs until cancelled
nfs.cancel()
```

## Recover from the content store (if the DB is lost)

Your files live in the content store as a self-describing graph; the metadata DB
holds refs + attribution. If the DB is lost, point a **fresh** one at the same
content store and rebuild — committed files, directories, and branch names come
back (blame/attribution and uncommitted edits don't; they're DB-only):

```python
# same S3/dir as before, brand-new metadata DB:
ws = await origo.Workspace.open_pg_s3(new_dsn, cfg)      # or open_local(new_db, cas)
info = await ws.scan()                                  # read-only: what's recoverable
#   {"commits_found": 12, "used_mirror": True, "branches": [("main", "…"), …], …}
report = await ws.rebuild()                             # restores refs + working tree
#   {"files": 340, "dirs": 27, "checked_out": "main", "used_mirror": True, …}
```

Reading every object also integrity-checks it (`report["corrupt"]` counts any that
failed). The DB stays the thing to back up — so also run Postgres PITR / a replica.

## Examples

- **`examples/collab_app.py`** — the one to start from. A complete little
  service (bearer-token auth mapped to actors, the full workspace API, a live
  SSE feed) that also **runs itself**: `python examples/collab_app.py` plays
  the whole story end to end — a human writes, an agent *suggests*, a reviewer
  accepts, and blame ends up crediting both — with no server or curl needed.
- `examples/fastapi_router.py` — the minimal `build_router` one-liner with your
  own header auth.
- `examples/fastapi_app.py` — the same surface written out as hand-rolled
  endpoints, if you'd rather own each route.
- **`examples/web/`** (repo root) — a full-stack **React + PlateJS** editor over
  this router, showing per-block and per-line **attribution** (blame), version
  **lineage** (commit history + diffs), and the agent-suggestion review queue,
  with a live SSE feed. The visual companion to `collab_app.py`.

## API surface

`Workspace`: `open_local` · `open_local_packed` · `open_pg` · `open_s3` ·
`open_s3_packed` · `open_pg_s3` · `open_pg_s3_packed` · `open_object_memory` ·
`read` · `write` ·
`write_as` · `mkdir_p` · `ls` · `stat` · `remove` · `rename` · `commit` · `log` ·
`status` · `diff` · `diff_file` · `create_branch` · `checkout` · `branches` ·
`current_branch` · `rebuild` · `scan` ·
`create_human` · `create_agent` · `actor_by_subject` · `actor` · `list_actors` ·
`find_or_create_human` · `find_or_create_agent` · `create_session` · `blame` ·
`watch` · `subscribe` · `presence` · `touch` · `suggest` · `suggest_delete` ·
`list_suggestions` ·
`get_suggestion` · `suggestion_diff` · `suggestion_content` · `accept_suggestion` ·
`reject_suggestion` ·
`mount` · `serve_nfs`. Plus `WriteCtx`, `S3Config`, `Mount`, `fuse_mountable()`.
