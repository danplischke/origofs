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
from types import SimpleNamespace

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

    async def write_or_propose(self, ctx, path, data, summary=None):
        # The fake actor is direct, so this records a write like write_as and
        # reports it landed.
        self.writes.append((ctx, path, data))
        self.files[path] = data
        return SimpleNamespace(wrote=True, suggestion_id=None)

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


# --- integration tests (real workspace) --------------------------------------
#
# FakeWs above is enough for auth/attribution/error-mapping unit tests, but it
# has no real directory/versioning/presence semantics -- these use a real
# origofs.Workspace, the same pattern test_integration_attribution_end_to_end
# below already uses.

def _real_client_with_actor(**router_kw):
    """A TestClient over a real workspace, plus a provisioned actor's auth header."""
    d = tempfile.mkdtemp()

    async def _setup():
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        dan = await ws.create_human("dan", None)
        sess = await ws.create_session(dan, "test")
        return ws, dan, sess

    ws, dan, sess = asyncio.run(_setup())
    c = _client(ws, **router_kw)
    hdr = {"X-Actor-Id": str(dan), "X-Session-Id": str(sess)}
    return c, ws, dan, sess, hdr


def test_removing_a_nonempty_directory_maps_409_not_500():
    # Regression test: DirectoryNotEmpty is the one origofs error the Rust
    # binding maps to a plain OSError (see `to_pyerr` in origofs-py/src/lib.rs),
    # rather than one of the specific subclasses _run() already handled -- it
    # used to fall through uncaught and surface as a bare 500.
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    r = c.put("/files/adir/f.txt", content=b"hi", headers=hdr)
    assert r.status_code == 200, r.text

    r = c.delete("/files/adir", headers=hdr)
    assert r.status_code == 409, r.text
    assert r.json()["detail"]  # a real message, not an empty/generic 500 body


def test_directory_and_stat_routes():
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    assert c.post("/dirs/docs", headers=hdr).status_code == 200
    assert c.put("/files/docs/a.txt", content=b"hello", headers=hdr).status_code == 200

    listing = c.get("/dirs/docs").json()
    assert any(e["name"] == "a.txt" for e in listing), listing

    st = c.get("/stat/docs/a.txt").json()
    assert st["kind"] == "file" and st["size"] == 5, st

    r = c.post("/rename", json={"from": "/docs/a.txt", "to": "/docs/b.txt"}, headers=hdr)
    assert r.status_code == 200, r.text
    assert c.get("/files/docs/a.txt").status_code == 404
    assert c.get("/files/docs/b.txt").content == b"hello"


def test_versioning_routes_through_router():
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    c.put("/files/notes.txt", content=b"v1", headers=hdr)

    r = c.post("/commit", json={"message": "base", "author": "dan"}, headers=hdr)
    assert r.status_code == 200 and r.json()["hash"], r.text

    log = c.get("/log").json()
    assert len(log) == 1 and log[0]["message"] == "base", log

    assert c.get("/status").json() == []  # clean working tree right after commit

    assert c.post("/branches", json={"name": "feature"}, headers=hdr).status_code == 200
    assert c.post("/checkout", json={"name": "feature"}, headers=hdr).status_code == 200
    branches = {b["name"] for b in c.get("/branches").json()}
    assert {"main", "feature"} <= branches, branches

    c.put("/files/notes.txt", content=b"v2", headers=hdr)
    c.post("/commit", json={"message": "work", "author": "dan"}, headers=hdr)

    changes = {d["path"]: d["status"] for d in c.get("/diff", params={"from": "main", "to": "feature"}).json()}
    assert changes == {"/notes.txt": "modified"}, changes

    patch = c.get("/diff/file", params={"from": "main", "to": "feature", "path": "/notes.txt"}).text
    assert "-v1" in patch and "+v2" in patch, patch


def test_presence_and_events_routes():
    c, _ws, dan, sess, hdr = _real_client_with_actor()
    c.put("/files/notes.txt", content=b"hi", headers=hdr)

    r = c.post("/presence/touch", json={"path": "/notes.txt"}, headers=hdr)
    assert r.status_code == 200, r.text
    present = c.get("/presence").json()
    assert any(p["session_id"] == sess and p["actor_id"] == dan for p in present), present

    events = c.get("/events").json()
    assert any(e["path"] == "/notes.txt" for e in events), events
    # `since` filters out everything already seen
    assert c.get("/events", params={"since": events[-1]["seq"]}).json() == []


def test_presence_touch_requires_a_session():
    # touch() needs a session_id; a session-less WriteCtx.actor(...) is a clean
    # 400, not a crash reaching into ws.touch() with session_id=None.
    c = _client(FakeWs())
    r = c.post("/presence/touch", json={"path": "/x"}, headers={"X-Actor-Id": "1"})
    assert r.status_code == 400, r.text


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


# A propose-only actor's write through the router is queued for review, not
# applied — and only a different actor can accept it. This is the bounded,
# actor-agnostic write policy reaching the FastAPI surface.
def test_propose_only_actor_write_is_queued_via_router():
    d = tempfile.mkdtemp()

    async def _setup():
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        author = await ws.create_human("ext", None)  # an untrusted contributor
        author_s = await ws.create_session(author, "web")
        reviewer = await ws.create_human("dan", None)
        reviewer_s = await ws.create_session(reviewer, "web")
        await ws.set_write_policy(author, "propose")
        return ws, author, author_s, reviewer, reviewer_s

    ws, author, author_s, reviewer, reviewer_s = asyncio.run(_setup())
    c = _client(ws)

    # The propose-only actor's PUT is queued, not applied.
    r = c.put(
        "/files/notes.txt",
        content=b"proposed",
        headers={"X-Actor-Id": str(author), "X-Session-Id": str(author_s)},
    )
    assert r.status_code == 200, r.text
    sid = r.json()["proposed"]
    assert sid is not None
    assert c.get("/files/notes.txt").status_code == 404  # nothing landed

    # A different actor reviews and accepts — now it lands, credited to its author.
    ra = c.post(
        f"/suggestions/{sid}/accept",
        headers={"X-Actor-Id": str(reviewer), "X-Session-Id": str(reviewer_s)},
    )
    assert ra.status_code == 200, ra.text
    assert c.get("/files/notes.txt").content == b"proposed"
    bl = c.get("/blame/notes.txt").json()
    assert bl and bl[0]["actor"]["id"] == author


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
