# origofs-py — Python bindings for origofs

Async-native bindings so you can drive an origofs workspace from Python — write your
own FastAPI endpoints, inject the authenticated user/agent behind each write,
and orchestrate FUSE/NFS mounts.

Every I/O method returns an **awaitable**, so it drops straight into `async def`
handlers. Structured results come back as plain `dict`/`list` (JSON-serializable).

## Install

```bash
pip install origofs
```

abi3 wheels, so one per platform covers CPython ≥ 3.9 and no Rust toolchain is
needed at install time.

## Build from source

For a platform without a wheel, or to work on the bindings:

```bash
cd crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop            # builds the extension module + installs it
pytest tests/              # end-to-end test
```

Wheels: `maturin build --release` (abi3, one wheel works on CPython ≥ 3.9).

## Use

```python
import origofs

ws = await origofs.Workspace.open_local("meta.db", "cas")
# ...or multi-writer on Postgres with S3-shared content (the production combo):
#   cfg = origofs.S3Config(bucket="my-bucket", region="us-east-1")   # + endpoint/keys
#   ws  = await origofs.Workspace.open_pg_s3(dsn, cfg)               # or open_pg_s3_packed
# object-store constructors verify content integrity on read (a bit-rotted object
# errors instead of being served). open_object_memory(db) runs the same adapter
# with no network, for local dev/tests.

# Map your app's user id to an origofs actor (idempotent; no side table needed):
actor_id = await ws.find_or_create_human("user_42", "Dan")   # your id -> origofs actor
# then inject the identity you resolved in your endpoint:
ctx = origofs.WriteCtx.session(actor_id, session_id)   # or origofs.WriteCtx.actor(actor_id)
await ws.write_as(ctx, "/notes.txt", b"hello")      # attributed -> blame + audit

diff = await ws.diff("main", "feature")             # [{"path","status"}, ...]
sid  = await ws.suggest(ctx, "/x", b"proposed")     # agent proposes; not applied
await ws.accept_suggestion(sid, origofs.WriteCtx.actor(reviewer))  # applied, credited
```

Errors map to Python exceptions: missing path → `FileNotFoundError`, bad arg →
`ValueError`, a stale suggestion base → `origofs.ConflictError`.

## FastAPI router (bring your own auth)

origofs has no built-in authentication — a blame trail is only trustworthy if the
identity behind each write is, and that's yours to own. `origofs.fastapi.build_router`
gives you every workspace endpoint wired up, with attribution driven by an auth
dependency **you** provide:

```python
from fastapi import FastAPI, Header, HTTPException
import origofs
from origofs.fastapi import build_router

async def authn(x_actor_id: int = Header(...)) -> origofs.WriteCtx:
    # decode your JWT / session / agent token -> the origofs actor to attribute to
    if x_actor_id is None:
        raise HTTPException(401)
    return origofs.WriteCtx.actor(x_actor_id)

app = FastAPI()
app.include_router(build_router(ws, authn=authn), prefix="/fs")
```

Every mutating route depends on `authn` and passes its `WriteCtx` straight to the
workspace — the request body never names an actor, so a client can't forge
attribution. Reads are open by default; pass `reader=<dependency>` to gate them,
or `dependencies=[...]` (forwarded to `APIRouter`) to gate everything. `POST
/actors`/`POST /sessions` mint new actor/session ids the same way — an
already-authenticated caller (e.g. a trusted backend onboarding a new user)
provisions them, not anonymous self-registration. `GET /files/{path}` streams
(never buffers a whole file in memory) and honors a single-range `Range` header
(`206`/`416`), so large files and partial fetches behave like a static file
server. Needs the `fastapi` extra (`pip install "origofs[fastapi]"`). See
`examples/fastapi_router.py`.

## fsspec filesystem (pandas / Dask / PyArrow / Zarr)

`origofs.fsspec.OrigoFileSystem` exposes a workspace as an
[fsspec](https://filesystem-spec.readthedocs.io/) filesystem, so the PyData stack
can read and write origofs paths directly — and because every origofs I/O method is
already a coroutine, it's a genuine `fsspec.asyn.AsyncFileSystem`: the same
filesystem is usable synchronously (`fs.ls`, `fs.cat_file`, …) *and* by awaiting
the `_`-prefixed coroutines on your own loop.

```python
import origofs.fsspec            # registers the "origofs://" protocol
import pandas as pd

# read/write straight from your usual tools:
df = pd.read_parquet("origofs:///data/events.parquet",
                     storage_options={"db_path": "meta.db", "cas_dir": "cas"})

from origofs.fsspec import OrigoFileSystem
fs = OrigoFileSystem(db_path="meta.db", cas_dir="cas")   # sync (fsspec loop)
fs.pipe_file("/notes.txt", b"hello")
fs.cat_file("/notes.txt", start=0, end=5)                # ranged read (only the covering chunks)

afs = OrigoFileSystem(db_path="meta.db", cas_dir="cas", asynchronous=True)
await afs._pipe_file("/notes.txt", b"hello")             # await on your loop
```

Point it at any backend with connection kwargs (`backend="pg_s3", dsn=…, s3={…}`)
or hand it an already-open workspace (`OrigoFileSystem(ws=ws)`) to share one store
with the rest of your app. **Attribution rides along**: pass `actor=`/`session=`
(or a `ctx=origofs.WriteCtx`), and every write lands attributed — `fs.blame(path)`
credits it — with the same per-call override (`fs.pipe_file(p, data, actor=42)`)
and server-owns-identity discipline as the rest of origofs. Ranged reads go through
`read_range`, so a large file isn't slurped whole. Listing caching is off by
default (an origofs working tree is live and multi-writer). Needs the `fsspec` extra
(`pip install "origofs[fsspec]"`).

It passes fsspec's own conformance suite (`fsspec.tests.abstract` — copy/get/put/
pipe/open, including the recursive, trailing-slash, and glob edge cases); see
`tests/test_fsspec_compliance.py`.

**Pathlib API** — because it's a well-behaved filesystem, it also works with
[universal-pathlib](https://github.com/fsspec/universal_pathlib) as a first-class,
explicitly-registered protocol (`pip install "origofs[upath]"`):

```python
from upath import UPath
root = UPath("origofs:///", db_path="meta.db", cas_dir="cas")   # or storage_options=…
(root / "notes.txt").write_text("hello")
for child in root.iterdir():
    print(child, child.stat().st_size)
```

## RAG with provenance (passages that know who wrote them)

Retrieval over origofs isn't "S3 + embeddings": every passage carries **who wrote
it** (blame) and **where it came from**, and is keyed by a content hash so only
genuinely-changed passages need re-embedding. The technology-agnostic half lives in
the Rust core (`Workspace.passages`) — no embeddings, no vector store, no framework
types — so any stack consumes the same records.

```python
# framework-neutral: provenance-carrying passage records
from origofs.rag import read_passages
passages = await read_passages(ws, root="/docs", segmentation="content_defined")
for p in passages:
    p.text, p.hash, p.authors        # -> who wrote this passage (precise, per-byte)

# or drop straight into LlamaIndex — one Document per passage, provenance in metadata
from origofs.llamaindex import SimpleWorkspaceReader
from llama_index.core import VectorStoreIndex
docs  = SimpleWorkspaceReader(ws, root="/docs", convert="markitdown").load_data()
index = VectorStoreIndex.from_documents(docs)   # node.metadata = {path, authors, passage_hash, …}
```

**Segmentation** is a real choice: `content_defined` (the default) puts boundaries
where the bytes decide, so an edit near the top of a file doesn't reshuffle every
later passage's hash — that edit-stability is what makes incremental re-embedding
cheap. Also `fixed`, `lines`, `whole_file`, or a custom splitter.

**Non-text documents** (PDF, DOCX, images, …) go through a pluggable `Converter`
that projects them to Markdown first; a converted passage's provenance is
*document-level* (source path + who added it + which converter), since the
extracted text no longer maps to the original bytes. `MarkItDownConverter` ships as
the batteries-included one (`pip install "origofs[markitdown]"`), but you can pass
`unstructured`, `pandoc`, an LLM, or a plain `callable(path, data, mime) -> str`.

Needs the relevant extras: `pip install "origofs[llamaindex,markitdown]"` (the
`origofs.rag` core needs none). See **`examples/rag_provenance.py`** — it plays the
whole story (a human + an agent co-author docs, a PDF is converted, and each
retrieved answer names its author and source) with no API keys.

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

`origofs.overlay.run` launches an agent in a fast native kernel overlay while its
edits stream into origofs, attributed to an actor — the way agents are meant to work
day to day. It shells out to the `origofs` CLI (the overlay is host orchestration,
not embedded in the extension), operating on a workspace **directory** the API
also opens:

```python
import origofs
from origofs.overlay import run

ws_dir = "./ws"
api   = await origofs.Workspace.open_local(f"{ws_dir}/meta.db", f"{ws_dir}/cas")
actor = await api.find_or_create_agent("agent-token", "claude", "opus")
code  = await run(ws_dir, actor, ["claude", "-p", "refactor the parser"])
```

Requires the `origofs` binary on PATH and a Linux host with unprivileged
user-namespace overlays.

## Mount orchestration

```python
mount = ws.mount("/mnt/origofs")        # FUSE, in the background; returns a handle
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
ws = await origofs.Workspace.open_pg_s3(new_dsn, cfg)      # or open_local(new_db, cas)
info = await ws.scan()                                  # read-only: what's recoverable
#   {"commits_found": 12, "used_mirror": True, "branches": [("main", "…"), …], …}
report = await ws.rebuild()                             # restores refs + working tree
#   {"files": 340, "dirs": 27, "checked_out": "main", "used_mirror": True, …}
```

Reading every object also integrity-checks it (`report["corrupt"]` counts any that
failed). The DB stays the thing to back up — so also run Postgres PITR / a replica.

## Alembic migrations for the metadata schema

A workspace already migrates its own metadata schema forward automatically on
open (`ws.schema_version()` / `ws.migrate()`) — `origofs.db` is for the times
you want that schema managed by **Alembic** instead: a CI-driven migration
step, a schema-diffing tool, or provisioning a fresh database before the
engine ever touches it. `origofs.db.models` declares every table from
`crates/origofs-core/src/migrations.rs` as SQLAlchemy models (the single
source of truth `origofs.db`'s packaged migrations autogenerate against), so
a database Alembic creates is fully interoperable with one the engine creates.
Needs the `db` extra (`pip install "origofs[db]"`) plus a driver for your
backend (Postgres: `psycopg[binary]`; SQLite's `sqlite3` is stdlib).

**Provision or upgrade a database** — run this once (a deploy step, an init
container, or before the first `origofs.Workspace.open_*`) and the workspace
API opens straight into an already-migrated store:

```python
import origofs.db

origofs.db.upgrade("sqlite:///meta.db")                          # dev/solo
origofs.db.upgrade("postgresql+psycopg://user:pass@host/dbname")  # multi-writer/production

# ...or skip passing a URL at all and set it once in the environment:
#   os.environ["ORIGOFS_DATABASE_URL"] = "postgresql+psycopg://…"
#   origofs.db.upgrade()
```

**Roll back or inspect history** — `get_alembic_config` hands you a real
`alembic.config.Config` for anything beyond upgrade/downgrade:

```python
origofs.db.downgrade("sqlite:///meta.db")           # one revision back
origofs.db.downgrade("sqlite:///meta.db", "base")   # drop every origofs table

from alembic import command
cfg = origofs.db.get_alembic_config("sqlite:///meta.db")
command.current(cfg)   # the revision(s) currently applied
command.history(cfg)   # the full revision list
```

**Two migration ledgers, one schema.** Alembic tracks its own progress in
`alembic_version`; the engine tracks its own in `schema_meta`
(`crates/origofs-core/src/migrations.rs`). The packaged initial revision
creates every table *and* stamps `schema_meta` through the latest version the
engine knows about, so both ledgers agree from the moment Alembic creates the
database — the engine never re-runs (or, for the destructive V11/V13 table
rebuilds, re-applies) a migration Alembic already handled. Whichever tool
creates the database, `ws.schema_version()` reports `up_to_date: True`.

**Query the schema directly** — every table is a plain SQLAlchemy model
(`origofs.db.Actor`, `.EditOp`, `.BlobBlame`, `.Suggestion`, `.FsEvent`, …),
handy for read-only reporting/analytics queries that sit alongside the async
workspace API:

```python
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session as DbSession   # origofs.db.Session is the *table* model
from origofs.db import Actor, EditOp

engine = create_engine("sqlite:///meta.db")
with DbSession(engine) as db:
    edits_by_actor = db.execute(
        select(Actor.display_name, EditOp.path)
        .join(EditOp, EditOp.actor_id == Actor.id)
    ).all()
```

**Developing origofs itself** — after editing `python/origofs/db/models.py`,
draft the next migration from the `origofs-py` crate root (uses the
`alembic.ini` there, not `origofs.db`'s programmatic config):

```bash
cd crates/origofs-py
alembic -x db_url=sqlite:///./dev.db revision --autogenerate -m "…"
alembic -x db_url=sqlite:///./dev.db upgrade head
```

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

**`python/origofs/__init__.pyi` is the complete, authoritative list** — every
method with its signature, its return shape as a `TypedDict`, and a note on what
it is for. Your editor already reads it. This section is a map, not an index:
enumerating the methods here means maintaining a second copy that silently falls
behind, which is how the bindings drifted from the engine in the first place.

Three tests keep the stub honest rather than aspirational.
`test_parity.py::test_every_sdk_method_is_bound_or_has_a_reason` diffs the Rust
`Workspace` against the pyo3 one, so an engine method with no binding fails
rather than going unnoticed; `test_the_type_stub_declares_every_binding` diffs
the stub against the bindings; and `test_stub_records.py` builds one live
instance of every declared record and compares its keys.

`Workspace` covers, roughly in the order you meet them:

- **opening** — `open_local` and `open_pg` for a local store; `open_s3` /
  `open_gcs` and their `open_pg_*` forms for object storage, each with
  `_packed` (few big PUTs), `_encrypted` (at rest), and `_cached` (a bounded
  local read tier) variants; `open_object_memory` for tests.
- **files** — `read` · `read_range` · `read_to_path` · `write` · `write_path` ·
  `ls` · `stat` · `mkdir_p` · `remove` · `rename` · `symlink` · `link` ·
  `chmod` · `chown` · the `*xattr` family · `statfs`.
- **attribution** — the `_as` form of every mutation (`write_as`,
  `mkdir_as`, `remove_or_propose`, …), `write_as_blamed` for explicit byte-range
  authorship, `blame`, `edit_ops`, and `revert_session_as`.
- **identity and permission** — `create_human` / `create_agent` /
  `find_or_create_*` · `create_session` · `set_write_policy` ·
  `grant` / `revoke` / `list_grants` / `effective_perms` ·
  `ensure_may_write_at` / `ensure_may_write_workspace` ·
  `require_attribution` and `ensure_attributed`.
- **versioning** — `commit_as` · `log` · `status` · `diff` · branches ·
  `checkout_as` · `merge` / `merge_branch` · `conflicts` · locks.
- **review** — `suggest` · `list_suggestions` · `suggestion_diff` ·
  `accept_suggestion` / `reject_suggestion`.
- **collaboration** — `watch` · `subscribe` · `record_event` · `presence` ·
  the `coedit` family (CRDT documents, tree documents, the cross-worker relay).
- **operations** — `gc` · `flush` · `repack` · `trash` · `usage` / `quota` ·
  `backup_metadata` · `dump_as` / `load` · `resync` / `push_objects` /
  `fetch_objects` · `rebuild` / `scan` · `migrate` · `ready` · `bench` ·
  `file_layout`.
- **mounting** — `mount` (FUSE, Linux) · `serve_nfs` (Unix).

Plus `WriteCtx`, `Scope`, `S3Config`, `GcsConfig`, `CacheConfig`, `Mount`,
`content_hash()` and `fuse_mountable()`.

One asymmetry worth knowing: **`dump_as` takes a `WriteCtx` where the Rust
`dump` does not**, and the unauthorized form is not bound. A dump is
whole-*store* — every workspace, every actor including its `auth_subject`, every
ACL grant, all blame — and none of it is path-scoped, so no `Scope` narrows it
and no subtree grant bounds it. It is checked as `write` at `/`. `load` has no
`_as` counterpart for the opposite reason: it cannot be ACL-gated, because the
identities a check would consult are the ones it installs, so it refuses any
store that already has actors or grants.

Integrations (own extras): `origofs.fastapi` (HTTP router) · `origofs.fsspec`
(`OrigoFileSystem`, the fsspec filesystem — also a `UPath("origofs://…")` via
universal-pathlib) · `origofs.rag` (provenance-carrying passages) +
`origofs.llamaindex` (`SimpleWorkspaceReader`) + `origofs.converters`
(`MarkItDownConverter`) · `origofs.overlay` (agent overlay) · `origofs.db`
(SQLAlchemy models + Alembic migrations for the metadata schema).
