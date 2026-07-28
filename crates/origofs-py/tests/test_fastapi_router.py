"""Tests for origofs.fastapi.build_router.

Two layers:
  * unit tests against a fake workspace — auth enforcement, forge-prevention,
    error mapping — with no I/O;
  * an integration test against a real origofs.Workspace, proving an attributed
    write made through the router shows up in blame credited to the actor that
    `authn` resolved (and to no one else).

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install fastapi httpx
    python tests/test_fastapi_router.py        # or: pytest tests/
"""
import asyncio
import os
import tempfile

import origofs
from origofs.fastapi import build_router

from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient


# --- an authn dependency: resolve headers -> the actor to attribute to -------
# This stands in for real auth (JWT, session cookie, agent token). The request
# body never names an actor; identity comes only from here.

async def header_authn(
    x_actor_id: int = Header(default=None),
    x_session_id: int = Header(default=None),
) -> origofs.WriteCtx:
    if x_actor_id is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    if x_session_id is not None:
        return origofs.WriteCtx.session(x_actor_id, x_session_id)
    return origofs.WriteCtx.actor(x_actor_id)


# --- fake workspace ---------------------------------------------------------

class FakeWs:
    """Minimal async stand-in recording how the router calls it."""

    def __init__(self):
        self.files = {}
        self.writes = []          # (ctx, path, data)
        self.accepts = []         # (sid, ctx)

    async def read(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    async def mkdir_p(self, path):
        pass

    async def write_as(self, ctx, path, data):
        self.writes.append((ctx, path, data))
        self.files[path] = data

    async def remove(self, path):
        self.files.pop(path, None)

    async def ls(self, path):
        return [{"name": k.lstrip("/")} for k in self.files]

    async def blame(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return [{"line_start": 1, "line_end": 1, "actor": {"id": 7}}]

    async def suggest(self, ctx, path, data, summary):
        self.writes.append((ctx, path, data))
        return 1

    async def accept_suggestion(self, sid, ctx):
        self.accepts.append((sid, ctx))
        if sid == 999:
            raise origofs.ConflictError("stale base")


def _client(ws, **kw):
    app = FastAPI()
    app.include_router(build_router(ws, authn=header_authn, **kw))
    return TestClient(app)


# --- unit tests -------------------------------------------------------------

def test_write_requires_auth():
    c = _client(FakeWs())
    r = c.put("/files/notes.txt", content=b"hi")   # no X-Actor-Id
    assert r.status_code == 401, r.text


def test_write_is_attributed_to_authn_ctx():
    ws = FakeWs()
    c = _client(ws)
    r = c.put("/files/notes.txt", content=b"hello",
              headers={"X-Actor-Id": "42", "X-Session-Id": "9"})
    assert r.status_code == 200, r.text
    ctx, path, data = ws.writes[-1]
    assert ctx.actor_id == 42 and ctx.session_id == 9
    assert path == "/notes.txt" and data == b"hello"


def test_client_cannot_forge_attribution():
    # Even if the client tacks on ?actor=1, the route has no such parameter —
    # attribution is whatever `authn` returned (actor 42), nothing else.
    ws = FakeWs()
    c = _client(ws)
    r = c.put("/files/x?actor=1&session=1", content=b"z",
              headers={"X-Actor-Id": "42"})
    assert r.status_code == 200, r.text
    ctx, _, _ = ws.writes[-1]
    assert ctx.actor_id == 42


def test_delete_requires_auth():
    c = _client(FakeWs())
    assert c.delete("/files/x").status_code == 401


def test_reads_open_by_default():
    ws = FakeWs()
    ws.files["/a.txt"] = b"data"
    c = _client(ws)
    r = c.get("/files/a.txt")            # no auth header
    assert r.status_code == 200 and r.content == b"data"


def test_reader_gate_rejects_reads():
    async def deny():
        raise HTTPException(status_code=403, detail="no reads for you")

    ws = FakeWs()
    ws.files["/a.txt"] = b"data"
    c = _client(ws, reader=deny)
    assert c.get("/files/a.txt").status_code == 403


def test_missing_file_maps_404():
    c = _client(FakeWs())
    assert c.get("/files/nope.txt").status_code == 404


def test_conflict_maps_409():
    c = _client(FakeWs())
    r = c.post("/suggestions/999/accept", headers={"X-Actor-Id": "1"})
    assert r.status_code == 409, r.text


# --- integration test (real workspace) --------------------------------------

def test_integration_attribution_end_to_end():
    d = tempfile.mkdtemp()

    async def _setup():
        # The origofs awaitables bind to the running loop, so create them inside one.
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        dan = await ws.create_human("dan", "dan@example.com")
        sess = await ws.create_session(dan, "fastapi")
        return ws, dan, sess

    ws, dan, sess = asyncio.run(_setup())

    c = _client(ws)
    hdr = {"X-Actor-Id": str(dan), "X-Session-Id": str(sess)}

    r = c.put("/files/src/app.py", content=b"print('hi')\n", headers=hdr)
    assert r.status_code == 200, r.text

    # read it back through the router
    assert c.get("/files/src/app.py").content == b"print('hi')\n"

    # blame credits the actor authn resolved — not a client-supplied id
    bl = c.get("/blame/src/app.py").json()
    assert bl and bl[0]["actor"]["id"] == dan, bl
    assert bl[0]["actor"]["kind"] == "human"


def _run_all():
    import inspect
    mod = globals()
    for name, fn in sorted(mod.items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            fn()
            print("ok  ", name)
    print("ALL OK")


if __name__ == "__main__":
    _run_all()
