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
from origofs.fastapi import SPOOL_MAX, build_router

import pytest
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
    """Minimal async stand-in recording how the router calls it.

    Only the *attributed* methods are defined. That is deliberate: a router that
    reaches for an unattributed engine op (``remove``, ``mkdir_p``, ``rename``,
    ``commit``) hits an ``AttributeError`` here rather than quietly succeeding,
    so the fake itself is part of the guard described in
    ``test_every_mutating_route_passes_its_principal_to_an_attributed_call``.
    """

    def __init__(self):
        self.files = {}
        self.writes = []          # (ctx, path, data)
        self.accepts = []         # (sid, ctx)
        self.removes = []         # (ctx, path)
        self.mkdirs = []          # (ctx, path)
        self.renames = []         # (ctx, from, to)
        self.commits = []         # (ctx, author, message)
        self.sessions = []        # (actor_id, client)

    async def read(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    async def stat(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return {"kind": "file", "size": len(self.files[path])}

    async def read_range(self, path, off, length):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path][off : off + length]

    async def mkdir_as(self, ctx, path):
        self.mkdirs.append((ctx, path))

    async def write_as(self, ctx, path, data):
        self.writes.append((ctx, path, data))
        self.files[path] = data

    async def write_or_propose(self, ctx, path, data, summary=None):
        # The fake actor is direct, so this records a write like write_as and
        # reports it landed.
        self.writes.append((ctx, path, data))
        self.files[path] = data
        return SimpleNamespace(wrote=True, suggestion_id=None)

    async def remove_or_propose(self, ctx, path, summary=None):
        self.removes.append((ctx, path))
        self.files.pop(path, None)
        return SimpleNamespace(wrote=True, suggestion_id=None)

    async def rename_as(self, ctx, from_, to):
        self.renames.append((ctx, from_, to))
        self.files[to] = self.files.pop(from_, b"")

    async def commit_as(self, ctx, author, message):
        self.commits.append((ctx, author, message))
        return "0" * 64

    async def create_branch_as(self, ctx, name):
        pass

    async def checkout_as(self, ctx, name):
        pass

    async def create_human(self, name, model):
        return 1

    async def create_agent(self, name, model, controller):
        return 2

    async def create_session(self, actor_id, client):
        self.sessions.append((actor_id, client))
        return 100 + actor_id

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


def test_presence_heartbeat_route_matches_the_rust_api():
    # POST /presence is the parity route for the Rust HTTP API's POST
    # /v1/presence: an optional body carrying only a path, and a response
    # echoing the *server-resolved* session/actor.
    c, _ws, dan, sess, hdr = _real_client_with_actor()

    r = c.post("/presence", json={"path": "notes.txt"}, headers=hdr)
    assert r.status_code == 200, r.text
    assert r.json() == {"session_id": sess, "actor_id": dan, "path": "/notes.txt"}

    present = c.get("/presence").json()
    assert any(p["session_id"] == sess and p["path"] == "/notes.txt" for p in present), present

    # The body is optional entirely, and an empty path means "no current path".
    assert c.post("/presence", headers=hdr).json()["path"] is None
    assert c.post("/presence", json={"path": "  "}, headers=hdr).json()["path"] is None


def test_presence_heartbeat_ignores_an_actor_named_in_the_body():
    # Identity is resolved server-side from the credential; a body that tries to
    # name someone else changes nothing (the field simply isn't part of the
    # request model), so a client can only ever heartbeat itself.
    c, _ws, dan, sess, hdr = _real_client_with_actor()
    r = c.post("/presence", json={"actor": 999, "session": 999, "path": "/x"}, headers=hdr)
    assert r.status_code == 200, r.text
    assert r.json()["actor_id"] == dan and r.json()["session_id"] == sess
    assert all(p["actor_id"] != 999 for p in c.get("/presence").json())


def test_presence_heartbeat_requires_a_session():
    # A credential bound to an actor but no session gets a 400 telling it to
    # create one -- the Rust handler refuses to mint a session per heartbeat.
    c = _client(FakeWs())
    r = c.post("/presence", json={"path": "/x"}, headers={"X-Actor-Id": "1"})
    assert r.status_code == 400, r.text
    assert "session" in r.json()["detail"]


def test_presence_heartbeat_requires_auth():
    c = _client(FakeWs())
    assert c.post("/presence", json={"path": "/x"}).status_code == 401


def test_read_file_streams_large_content_correctly():
    # A file spanning several internal read_range() chunks (_STREAM_CHUNK is
    # 1 MiB) -- regression coverage for the chunked reassembly loop that
    # replaced buffering the whole file in one `ws.read()` call.
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    content = bytes((i % 251) for i in range(2 * 1024 * 1024 + 12345))
    assert c.put("/files/big.bin", content=content, headers=hdr).status_code == 200
    r = c.get("/files/big.bin")
    assert r.status_code == 200
    assert r.headers["content-length"] == str(len(content))
    assert r.content == content


class _FlakyReadRangeProxy:
    """Forwards everything to a real Workspace except read_range, which fails
    on its Nth call -- simulating a concurrent delete/change mid-stream. A
    real compiled Workspace doesn't allow attribute assignment (`ws.read_range
    = ...` raises AttributeError: read-only), so this wraps it instead;
    build_router accepts "any object with the same async methods"."""

    def __init__(self, real, fail_on_call: int):
        self._real = real
        self._fail_on_call = fail_on_call
        self.calls = 0

    def __getattr__(self, name):
        return getattr(self._real, name)

    async def read_range(self, path, off, length):
        self.calls += 1
        if self.calls == self._fail_on_call:
            raise FileNotFoundError(path)
        return await self._real.read_range(path, off, length)


def test_read_file_streaming_ends_cleanly_on_a_mid_stream_error():
    # Regression test: the chunked-reassembly generator's read_range() call
    # wasn't wrapped in error handling, so a concurrent delete/change between
    # stat() and a later chunk (origofs is multi-writer by design) propagated
    # uncaught into the ASGI layer instead of just ending the response.
    _c, ws, _dan, sess, hdr = _real_client_with_actor()
    content = bytes((i % 251) for i in range(3 * 1024 * 1024))
    _c.put("/files/big.bin", content=content, headers=hdr)

    proxy = _FlakyReadRangeProxy(ws, fail_on_call=2)
    c = _client(proxy)
    r = c.get("/files/big.bin")

    # Whatever exact status/body FastAPI produces for a generator failing
    # after headers may already be sent (version-dependent), the important
    # thing already happened: this line was reached at all -- an uncaught
    # exception from the generator would have propagated out of .get() itself
    # (TestClient's default raise_server_exceptions=True) instead of
    # returning a response.
    assert proxy.calls >= 2
    assert len(r.content) < len(content)


def test_read_file_range_requests():
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    body = b"0123456789" * 5  # 50 bytes
    c.put("/files/small.txt", content=body, headers=hdr)

    r = c.get("/files/small.txt", headers={"Range": "bytes=10-19"})
    assert r.status_code == 206 and r.content == body[10:20]
    assert r.headers["content-range"] == "bytes 10-19/50"

    r = c.get("/files/small.txt", headers={"Range": "bytes=40-"})
    assert r.status_code == 206 and r.content == body[40:]

    r = c.get("/files/small.txt", headers={"Range": "bytes=-10"})
    assert r.status_code == 206 and r.content == body[-10:]

    # unsatisfiable -- starts past EOF
    r = c.get("/files/small.txt", headers={"Range": "bytes=1000-2000"})
    assert r.status_code == 416
    assert r.headers["content-range"] == "bytes */50"

    # a Range we don't parse (multi-range, bad unit, ...) falls back to a
    # full 200 response rather than erroring -- RFC 7233 permits ignoring it
    r = c.get("/files/small.txt", headers={"Range": "not-a-range"})
    assert r.status_code == 200 and r.content == body


def test_read_directory_maps_409():
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    c.post("/dirs/adir", headers=hdr)
    r = c.get("/files/adir")
    assert r.status_code == 409, r.text


def test_create_actor_and_session_via_router():
    c, _ws, dan, _sess, hdr = _real_client_with_actor()

    r = c.post("/actors", json={"name": "new-user"}, headers=hdr)
    assert r.status_code == 200, r.text
    new_id = r.json()["id"]
    assert new_id != dan

    r = c.post(
        "/actors",
        json={"name": "claude", "agent": True, "model": "opus", "controller": dan},
        headers=hdr,
    )
    assert r.status_code == 200, r.text
    assert r.json()["id"] != new_id

    # A session belongs to whoever the credential resolves to, so the new actor
    # mints its own with its own credential -- dan cannot mint one on its behalf.
    r = c.post("/sessions", json={"client": "web"}, headers={"X-Actor-Id": str(new_id)})
    assert r.status_code == 200, r.text
    assert r.json()["actor"] == new_id
    new_sid = r.json()["id"]

    # the freshly-minted actor/session pair is immediately usable
    r = c.post(
        "/presence/touch",
        json={"path": "/x"},
        headers={"X-Actor-Id": str(new_id), "X-Session-Id": str(new_sid)},
    )
    assert r.status_code == 200, r.text


def test_create_agent_preserves_an_explicit_empty_model():
    # Regression test: `req.model or "unknown"` would also replace an
    # explicit "" (falsy), diverging from the Rust API's
    # `req.model.as_deref().unwrap_or("unknown")`, which only substitutes on
    # None. "" is a deliberately odd input, but the point is `is not None` is
    # the correct check either way.
    c, ws, dan, _sess, hdr = _real_client_with_actor()
    r = c.post("/actors", json={"name": "claude", "agent": True, "model": ""}, headers=hdr)
    assert r.status_code == 200, r.text
    agent_id = r.json()["id"]

    async def _check():
        return await ws.actor(agent_id)

    info = asyncio.run(_check())
    assert info["agent_model"] == ""


def test_actor_and_session_routes_require_auth():
    # Mirrors the Rust HTTP API: minting a new actor/session requires an
    # already-authenticated caller (a trusted backend provisioning identities
    # for new users), not anonymous self-registration.
    c = _client(FakeWs())
    assert c.post("/actors", json={"name": "x"}).status_code == 401
    assert c.post("/sessions", json={}).status_code == 401


def test_suggest_delete_via_router():
    c, ws, dan, _sess, hdr = _real_client_with_actor()
    c.put("/files/deleteme.txt", content=b"bye", headers=hdr)

    async def _make_reviewer():
        return await ws.create_human("reviewer", None)

    reviewer = asyncio.run(_make_reviewer())

    r = c.post(
        "/suggestions",
        params={"path": "/deleteme.txt", "delete": "true", "summary": "cleanup"},
        headers=hdr,
    )
    assert r.status_code == 200, r.text
    sid = r.json()["id"]
    # not applied yet -- still a review-queue entry, not a landed delete
    assert c.get("/files/deleteme.txt").status_code == 200

    r = c.post(f"/suggestions/{sid}/accept", headers={"X-Actor-Id": str(reviewer)})
    assert r.status_code == 200, r.text
    assert c.get("/files/deleteme.txt").status_code == 404


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


# --- the write policy reaches this surface too (issue #99) -------------------
#
# The router used to authenticate every mutating route and then throw the
# principal away, calling the *unattributed* engine ops (`remove`, `mkdir_p`,
# `rename`, `commit`). Those take no WriteCtx, so they skip `ensure_may_write`
# and record no edit_op: a propose-only actor could not overwrite a file through
# `PUT`, but could delete it and commit the deletion — the exact hop issue #78
# closed on the Rust surfaces, still open on this one.


def test_delete_is_attributed_and_policy_governed():
    ws = FakeWs()
    ws.files["/x.txt"] = b"bye"
    c = _client(ws)
    r = c.delete("/files/x.txt", headers={"X-Actor-Id": "42", "X-Session-Id": "9"})
    assert r.status_code == 200, r.text
    assert r.json() == {"removed": "/x.txt"}
    ctx, path = ws.removes[-1]
    assert ctx.actor_id == 42 and ctx.session_id == 9 and path == "/x.txt"


def test_mkdir_rename_and_commit_are_attributed():
    ws = FakeWs()
    ws.files["/a.txt"] = b"data"
    c = _client(ws)
    hdr = {"X-Actor-Id": "42", "X-Session-Id": "9"}

    assert c.post("/dirs/docs", headers=hdr).status_code == 200
    assert c.post("/rename", json={"from": "/a.txt", "to": "/b.txt"}, headers=hdr).status_code == 200
    assert c.post("/commit", json={"message": "m", "author": "dan"}, headers=hdr).status_code == 200

    assert ws.mkdirs[-1][0].actor_id == 42
    assert ws.renames[-1][0].actor_id == 42
    ctx, author, message = ws.commits[-1]
    # The git-level author string still rides the body; the *identity* the policy
    # gate and the audit log see comes from the credential.
    assert ctx.actor_id == 42 and author == "dan" and message == "m"


def test_a_policy_refusal_maps_403_not_409():
    # PermissionError is a subclass of OSError, which the router maps to 409 for
    # a non-empty directory. A write-policy refusal is an authorization outcome
    # and has to sort ahead of that arm.
    class Denying(FakeWs):
        async def mkdir_as(self, ctx, path):
            raise PermissionError("actor 42 is propose-only and may not create a directory")

    c = _client(Denying())
    r = c.post("/dirs/docs", headers={"X-Actor-Id": "42"})
    assert r.status_code == 403, r.text
    assert "propose-only" in r.json()["detail"]


def test_a_session_belongs_to_the_authenticated_actor():
    # The body carries no actor. It used to, which let an authenticated caller
    # mint a session belonging to somebody else.
    ws = FakeWs()
    c = _client(ws)
    r = c.post("/sessions", json={"actor": 999, "client": "web"}, headers={"X-Actor-Id": "42"})
    assert r.status_code == 200, r.text
    assert r.json() == {"id": 142, "actor": 42}
    assert ws.sessions[-1] == (42, "web")


def test_a_queued_write_creates_no_directories():
    # Parity with `tests/mcp.rs::a_queued_write_creates_no_directories`: parents
    # are created only once the policy decision is known, so a proposal leaves
    # the working tree untouched.
    class Proposing(FakeWs):
        async def ensure_may_write(self, ctx, what):
            raise PermissionError("propose-only")

        async def write_or_propose(self, ctx, path, data, summary=None):
            return SimpleNamespace(wrote=False, suggestion_id=7)

    ws = Proposing()
    c = _client(ws)
    r = c.put("/files/deep/nested/note.txt", content=b"hi", headers={"X-Actor-Id": "42"})
    assert r.status_code == 200, r.text
    assert r.json()["proposed"] == 7
    assert ws.mkdirs == [], ws.mkdirs


def test_every_mutating_route_passes_its_principal_to_an_attributed_call():
    """Every mutating route is accounted for, and none of them throws its
    identity away.

    The Python counterpart of
    ``origofs-sdk/tests/api_write_policy.rs::every_mutating_route_binds_its_principal``.
    It parses the router source, pairs each ``POST``/``PUT``/``DELETE``
    registration with its handler, and requires that handler to *bind* the
    principal (``ctx: Any = Depends(authn)``) and actually *use* it — rather than
    discard it as ``_ctx`` and call an unattributed engine method.

    Discarding it is the precise shape of the bug: authentication passes, the
    actor is dropped, and the call skips ``ensure_may_write`` and records no
    ``edit_op``. A route that genuinely needs no actor must be named in
    ``NO_ACTOR_NEEDED`` with a reason, which is the moment to notice whether that
    is actually true.
    """
    import ast
    import inspect
    from origofs import fastapi as router_mod

    # Empty today. An entry here claims the operation mutates nothing an actor
    # could be blamed for.
    NO_ACTOR_NEEDED: set[str] = set()

    tree = ast.parse(inspect.getsource(router_mod))
    build = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "build_router"
    )

    def mutating(fn):
        """The route methods this handler is registered under, if any."""
        verbs = set()
        for dec in fn.decorator_list:
            # @router.post("/x") -> Call(func=Attribute(attr='post'))
            if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
                if dec.func.attr in {"post", "put", "delete"}:
                    verbs.add(dec.func.attr)
        return verbs

    handlers = [
        n for n in build.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and mutating(n)
    ]
    assert len(handlers) >= 10, (
        f"route scan found only {len(handlers)} mutating handlers — the scan is "
        f"broken, not the router: {[h.name for h in handlers]}"
    )

    offenders = []
    for fn in handlers:
        if fn.name in NO_ACTOR_NEEDED:
            continue
        args = fn.args
        # The principal is whichever parameter defaults to `Depends(authn)`.
        bound = None
        defaults = dict(zip([a.arg for a in args.args][-len(args.defaults):] if args.defaults else [],
                            args.defaults))
        defaults.update({
            a.arg: d for a, d in zip(args.kwonlyargs, args.kw_defaults) if d is not None
        })
        for name, default in defaults.items():
            if (
                isinstance(default, ast.Call)
                and getattr(default.func, "id", None) == "Depends"
                and default.args
                and getattr(default.args[0], "id", None) == "authn"
            ):
                bound = name
        if bound is None:
            offenders.append(f"{fn.name}: does not depend on authn at all")
            continue
        if bound.startswith("_"):
            offenders.append(f"{fn.name}: binds the principal as `{bound}` (discarded)")
            continue
        used = any(
            isinstance(n, ast.Name) and n.id == bound for n in ast.walk(ast.Module(fn.body, []))
        )
        if not used:
            offenders.append(f"{fn.name}: binds `{bound}` but never passes it on")

    assert not offenders, (
        "these mutating routes do not pass their authenticated principal to an "
        "attributed workspace call, so they cannot be attributed or policy-gated:\n  "
        + "\n  ".join(offenders)
        + "\n\nBind `ctx: Any = Depends(authn)` and call the `*_as` variant, or add "
          "the route to NO_ACTOR_NEEDED with a reason."
    )


# --- acceptance criteria, against a real workspace ---------------------------


def test_propose_only_actor_is_refused_every_direct_mutation_via_router():
    d = tempfile.mkdtemp()

    async def _setup():
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        reviewer = await ws.create_human("dan", None)
        reviewer_s = await ws.create_session(reviewer, "web")
        agent = await ws.create_agent("restricted", "opus", reviewer)
        agent_s = await ws.create_session(agent, "mcp")
        await ws.write("/doomed.txt", b"original")
        await ws.write("/movable.txt", b"original")
        await ws.commit("setup", "base")
        await ws.create_branch("side")
        await ws.set_write_policy(agent, "propose")
        return ws, agent, agent_s, reviewer, reviewer_s

    ws, agent, agent_s, reviewer, reviewer_s = asyncio.run(_setup())
    c = _client(ws)
    hdr = {"X-Actor-Id": str(agent), "X-Session-Id": str(agent_s)}

    # A delete has a propose-shaped equivalent, so it is *queued*, not refused —
    # and the file is still there.
    r = c.delete("/files/doomed.txt", headers=hdr)
    assert r.status_code == 200, r.text
    sid = r.json()["proposed"]
    assert sid is not None
    assert c.get("/files/doomed.txt").content == b"original"

    # The rest have no propose-shaped equivalent, so they are refused outright.
    assert c.post("/rename", json={"from": "/movable.txt", "to": "/moved.txt"},
                  headers=hdr).status_code == 403
    assert c.post("/dirs/newdir", headers=hdr).status_code == 403
    assert c.post("/commit", json={"message": "sneaky", "author": "x"},
                  headers=hdr).status_code == 403
    assert c.post("/branches", json={"name": "sneaky"}, headers=hdr).status_code == 403
    assert c.post("/checkout", json={"name": "side"}, headers=hdr).status_code == 403
    assert c.post("/actors", json={"name": "puppet"}, headers=hdr).status_code == 403

    # Nothing landed.
    assert c.get("/files/movable.txt").status_code == 200
    assert c.get("/files/moved.txt").status_code == 404

    # A direct actor accepts the queued deletion: it applies, and the audit trail
    # credits the actor that asked for it.
    rhdr = {"X-Actor-Id": str(reviewer), "X-Session-Id": str(reviewer_s)}
    assert c.post(f"/suggestions/{sid}/accept", headers=rhdr).status_code == 200
    assert c.get("/files/doomed.txt").status_code == 404


def test_a_direct_actor_still_performs_all_of_them():
    # The mirror image: the gate refuses a propose-only actor without getting in
    # a normal one's way.
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    assert c.put("/files/a.txt", content=b"v1", headers=hdr).status_code == 200
    assert c.post("/dirs/docs", headers=hdr).status_code == 200
    assert c.post("/rename", json={"from": "/a.txt", "to": "/b.txt"}, headers=hdr).status_code == 200
    assert c.post("/commit", json={"message": "work", "author": "dan"}, headers=hdr).status_code == 200
    assert c.post("/branches", json={"name": "feature"}, headers=hdr).status_code == 200
    assert c.post("/checkout", json={"name": "feature"}, headers=hdr).status_code == 200
    assert c.delete("/files/b.txt", headers=hdr).json() == {"removed": "/b.txt"}
    assert c.post("/actors", json={"name": "colleague"}, headers=hdr).status_code == 200


def test_namespace_mutations_through_the_router_carry_an_actor():
    # "Who deleted this file" had no answer on this surface: remove/rename/mkdir
    # took no WriteCtx, so they recorded no edit_op.
    c, ws, dan, sess, hdr = _real_client_with_actor()
    c.put("/files/gone.txt", content=b"x", headers=hdr)
    assert c.delete("/files/gone.txt", headers=hdr).status_code == 200

    async def _ops():
        return await ws.edit_ops(dan, sess)

    ops = asyncio.run(_ops())
    assert any(op["path"] == "/gone.txt" for op in ops), ops


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


# --- routes the Rust API had and this router did not -------------------------
#
# `origofs.fastapi` is a separate hand-written surface rather than a binding of the
# axum router, so the two drift independently. These are what the diff turned up.


def test_revert_session_undoes_one_actors_sitting():
    """The headline "undo just the agent's work" operation had no route here.

    It is the one thing a host running agents most needs from an HTTP surface,
    and a Python service had to reach past the router to get it.
    """
    c, ws, dan, sess, hdr = _real_client_with_actor()

    async def _setup():
        agent = await ws.create_agent("claude", "opus", dan)
        asess = await ws.create_session(agent, "run-1")
        actx = origofs.WriteCtx.session(agent, asess)
        await ws.write_as(origofs.WriteCtx.session(dan, sess), "/notes.md", b"human\n")
        await ws.write_as(actx, "/notes.md", b"human\nagent\n")
        return agent, asess

    agent, asess = asyncio.run(_setup())

    r = c.post(
        "/revert-session", json={"actor": agent, "session": asess}, headers=hdr
    )
    assert r.status_code == 200, r.text
    assert r.json()["reverted"] == ["/notes.md"]
    # Exactly the agent's line went; the human's stayed.
    assert c.get("/files/notes.md").content == b"human\n"


def test_revert_session_needs_a_credential_and_an_absolute_prefix():
    c, _ws, _dan, _sess, hdr = _real_client_with_actor()
    assert c.post("/revert-session", json={"actor": 1, "session": 1}).status_code == 401
    r = c.post(
        "/revert-session",
        json={"actor": 1, "session": 1, "path_prefix": "docs"},
        headers=hdr,
    )
    assert r.status_code == 400, r.text
    assert "absolute" in r.json()["detail"]


def test_a_scoped_revert_defaults_to_the_scope_not_everywhere():
    """Omitting the prefix on a scoped router must not mean "every tenant".

    This is the one place a `None` filter must not stay `None`: an unbounded
    revert walks every file the session touched, across every tenant in the
    workspace.
    """
    c, ws, dan, sess, hdr = _real_client_with_actor(root="/tenant-a")

    async def _setup():
        ctx = origofs.WriteCtx.session(dan, sess)
        await ws.mkdir_as(ctx, "/tenant-a")
        await ws.mkdir_as(ctx, "/tenant-b")
        await ws.write_as(ctx, "/tenant-a/mine.txt", b"a\n")
        await ws.write_as(ctx, "/tenant-b/theirs.txt", b"b\n")

    asyncio.run(_setup())

    r = c.post("/revert-session", json={"actor": dan, "session": sess}, headers=hdr)
    assert r.status_code == 200, r.text
    # Only the in-scope file was touched.
    assert r.json()["reverted"] == ["/tenant-a/mine.txt"]

    async def _other_tenant():
        return bytes(await ws.read("/tenant-b/theirs.txt"))

    assert asyncio.run(_other_tenant()) == b"b\n"


def test_a_single_suggestion_is_readable_by_id():
    c, ws, dan, sess, hdr = _real_client_with_actor()

    async def _setup():
        agent = await ws.create_agent("claude", "opus", dan)
        actx = origofs.WriteCtx.actor(agent)
        await ws.write_as(origofs.WriteCtx.actor(dan), "/f.txt", b"one\n")
        return await ws.suggest(actx, "/f.txt", b"one\ntwo\n", "add a line")

    sid = asyncio.run(_setup())

    row = c.get(f"/suggestions/{sid}").json()
    assert row["id"] == sid
    assert row["path"] == "/f.txt"
    assert row["status"] == "pending"
    assert c.get("/suggestions/99999").status_code == 404


def test_health_and_readyz_are_distinct_and_ungated():
    """A probe has no bearer token, so a health route that answers 401 reads as
    an unhealthy backend. Both sit outside the read gate, matching the Rust API
    where they sit outside `/v1`."""
    c, _ws, _dan, _sess, _hdr = _real_client_with_actor(
        reader=_denying_reader()
    )
    assert c.get("/health").json() == {"status": "ok"}
    r = c.get("/readyz")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ready"] is True
    assert body["metadata"] is None and body["content"] is None
    # …while an ordinary read is gated, so the two really are on different paths.
    assert c.get("/stat/anything").status_code == 401


def _denying_reader():
    async def _reader() -> None:
        raise HTTPException(status_code=401, detail="unauthenticated")

    return _reader


# --- ranged reads stream ------------------------------------------------------


def test_a_ranged_read_does_not_materialize_the_range():
    """`docs/LIMITS.md` claims ranged responses stream and names this router.

    It did not: the 206 branch answered with one `read_range(start, len)` into a
    `Response` body, and the open-ended `bytes=0-` a <video> element sends first
    parses to the whole file — so the exact request the doc uses as its example
    materialized every byte, through pyo3's copy.
    """
    ws = FakeWs()
    # Deliberately several times `_STREAM_CHUNK`: at exactly one chunk the old
    # whole-range read and the new streaming one are indistinguishable, so the
    # test would pass against the bug it exists to catch.
    ws.files["/big.bin"] = bytes(range(256)) * 16384  # 4 MiB
    reads = []
    inner = ws.read_range

    async def counting_read_range(path, off, length):
        reads.append(length)
        return await inner(path, off, length)

    ws.read_range = counting_read_range
    c = _client(ws)

    r = c.get("/files/big.bin", headers={"Range": "bytes=0-"})
    assert r.status_code == 206
    size = 4 << 20
    assert r.headers["Content-Range"] == f"bytes 0-{size - 1}/{size}"
    assert r.headers["Content-Length"] == str(size)
    assert len(r.content) == size
    assert r.content == ws.files["/big.bin"]
    # The body arrived in bounded pieces rather than one whole-file read.
    assert len(reads) == 4 and max(reads) == 1 << 20, reads

    # A genuinely small range is still one read, and exact.
    reads.clear()
    r = c.get("/files/big.bin", headers={"Range": "bytes=10-19"})
    assert r.status_code == 206
    assert r.content == ws.files["/big.bin"][10:20]
    assert reads == [10]


def test_an_empty_file_still_answers_cleanly():
    ws = FakeWs()
    ws.files["/empty.txt"] = b""
    c = _client(ws)
    r = c.get("/files/empty.txt")
    assert r.status_code == 200 and r.content == b""
    # A range against a zero-length file is unsatisfiable.
    assert c.get("/files/empty.txt", headers={"Range": "bytes=0-"}).status_code == 416


def _grant_scoped_proposer():
    """A real workspace where bob's *policy* is Direct but his *grant* at /x
    allows PROPOSE only — the case that separates the two write checks."""
    d = tempfile.mkdtemp()

    async def _setup():
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        owner = await ws.create_human("owner", None)
        await ws.grant(owner, "/", "read+write", None)
        await ws.mkdir_as(origofs.WriteCtx.actor(owner), "/x")
        bob = await ws.create_human("bob", None)
        await ws.grant(bob, "/x", "read+propose", owner)
        return ws, bob

    ws, bob = asyncio.run(_setup())
    return _client(ws), ws, {"X-Actor-Id": str(bob)}


def test_a_grant_scoped_proposer_gets_a_suggestion_not_a_403():
    """The router must not reconstruct the write-or-propose fork from the wrong check.

    `write_or_propose` forks on the *path-scoped* check, where the grant covering
    the path decides and the write policy is only the fallback. The router asked
    the path-*less* `ensure_may_write`, which consults the policy alone — so for an
    actor whose policy is Direct but whose grant here allows PROPOSE only it
    answered "may write directly", took the direct path, and the engine's real
    check refused it. The engine queues a suggestion for this actor; the router
    returned 403.
    """
    c, ws, hdr = _grant_scoped_proposer()
    r = c.put("/files/x/small.md", content=b"a" * 16, headers=hdr)
    assert r.status_code == 200, r.text
    assert r.json().get("proposed"), f"expected a queued suggestion, got {r.json()}"

    # And the working tree is untouched: a proposal is not a write.
    async def _read():
        return await ws.read("/x/small.md")

    with pytest.raises(FileNotFoundError):
        asyncio.run(_read())


def test_the_propose_fork_is_the_same_either_side_of_spool_max():
    """A body over SPOOL_MAX must fork the same way a small one does.

    The streaming branch is the only caller that relies on the probe, so a wrong
    answer showed up as a size-dependent status for one actor and path.
    """
    c, _ws, hdr = _grant_scoped_proposer()
    small = c.put("/files/x/small.md", content=b"a" * 16, headers=hdr)
    large = c.put("/files/x/big.md", content=b"a" * (SPOOL_MAX + 1024), headers=hdr)
    assert small.status_code == large.status_code == 200, (small.text, large.text)
    assert small.json().get("proposed") and large.json().get("proposed"), (
        small.json(), large.json()
    )


def test_a_write_granted_actor_still_writes_directly():
    """The counterpart: the fix must not turn real writes into proposals."""
    d = tempfile.mkdtemp()

    async def _setup():
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        owner = await ws.create_human("owner", None)
        await ws.grant(owner, "/", "read+write", None)
        return ws, owner

    ws, owner = asyncio.run(_setup())
    c = _client(ws)
    hdr = {"X-Actor-Id": str(owner)}

    r = c.put("/files/deep/dir/note.md", content=b"hello", headers=hdr)
    assert r.status_code == 200, r.text
    assert r.json().get("written") == 5, r.json()

    async def _small():
        return await ws.read("/deep/dir/note.md")

    assert asyncio.run(_small()) == b"hello"

    big = b"b" * (SPOOL_MAX + 1024)
    r = c.put("/files/deep/dir/big.md", content=big, headers=hdr)
    assert r.status_code == 200, r.text
    assert r.json().get("written") == len(big), r.json()

    async def _big():
        return await ws.read("/deep/dir/big.md")

    assert len(asyncio.run(_big())) == len(big)


# --- route groups (#153) ---------------------------------------------------
#
# `build_router` mounted the whole REST surface or nothing, so a host that stores
# bodies in origofs but owns its own access model had to reach into
# `router.routes` and re-register the one route it wanted — coupling to a path
# string and a route class. These pin the supported way to say it.


def _paths(router):
    """(method, path) for every route on `router`; method None for a websocket."""
    out = set()
    for r in router.routes:
        methods = getattr(r, "methods", None)
        if methods:
            out |= {(m, r.path) for m in methods if m not in ("HEAD", "OPTIONS")}
        else:
            out.add((None, r.path))
    return out


def test_include_mounts_only_the_named_groups():
    """The headline: ask for `coedit` and the mutating REST surface is not there."""
    from origofs.fastapi import build_coedit_router

    full = _paths(build_router(FakeWs(), authn=header_authn))
    only = _paths(build_router(FakeWs(), authn=header_authn, include=["coedit"]))

    assert (None, "/coedit/{path:path}") in only
    assert (None, "/coedit-tree/{path:path}") in only
    # `coedit` is an alias for both halves since #160, so it still means what it
    # meant: sockets *and* the checkpoint route.
    assert ("POST", "/coedit-tree-checkpoint/{path:path}") in only
    assert only < full

    # And the routes whose reachability was the actual bug are gone: a caller who
    # satisfies `authn` can no longer PUT over a body the app's own endpoints
    # protect with more than `authn` checks.
    for gone in [
        ("PUT", "/files/{path:path}"),
        ("DELETE", "/files/{path:path}"),
        ("POST", "/rename"),
        ("POST", "/commit"),
        ("POST", "/revert-session"),
        ("POST", "/actors"),
        ("DELETE", "/trash/{sid}"),
    ]:
        assert gone not in only, f"{gone} should not be on a coedit-only router"

    # The named helper is exactly the same surface.
    assert _paths(build_coedit_router(FakeWs(), authn=header_authn)) == only


def test_exclude_is_the_complement_of_include():
    full = _paths(build_router(FakeWs(), authn=header_authn))
    without = _paths(build_router(FakeWs(), authn=header_authn, exclude=["history"]))
    assert ("POST", "/commit") in full and ("POST", "/commit") not in without
    assert ("GET", "/log") not in without
    # Everything else survives.
    assert ("PUT", "/files/{path:path}") in without
    assert (None, "/coedit/{path:path}") in without


def test_the_default_router_is_unfiltered_and_unchanged():
    """No `include`/`exclude` must mean exactly the surface every caller has today.

    The filter only runs when filtering was asked for, so a gap in the group table
    can never silently change an existing deployment's routes.
    """
    from origofs.fastapi import ROUTE_GROUPS

    default = _paths(build_router(FakeWs(), authn=header_authn))
    everything = _paths(
        build_router(FakeWs(), authn=header_authn, include=sorted(ROUTE_GROUPS))
    )
    assert default == everything


def test_a_typo_in_a_group_name_raises_rather_than_mounting_the_wrong_surface():
    """The quiet failure is the dangerous one.

    A typo in `include` mounts nothing; a typo in `exclude` mounts everything.
    Both look like a working call, and one of them is an auth bypass.
    """
    with pytest.raises(ValueError, match="unknown route group"):
        build_router(FakeWs(), authn=header_authn, include=["coedits"])
    with pytest.raises(ValueError, match="unknown route group"):
        build_router(FakeWs(), authn=header_authn, exclude=["histry"])
    with pytest.raises(ValueError, match="not both"):
        build_router(FakeWs(), authn=header_authn, include=["coedit"], exclude=["history"])


def test_every_route_belongs_to_exactly_one_group():
    """The guard that keeps the table honest as routes are added.

    A new route with no group is invisible to the tests above — it would just
    quietly appear on, or vanish from, a filtered router. Here it fails: the
    group table is part of adding a route, the same way the MCP tool
    classification and the CLI subcommand classification are.
    """
    from origofs.fastapi import _GROUP_OF, _ROUTE_GROUPS, _route_keys

    router = build_router(FakeWs(), authn=header_authn)
    unclaimed = sorted(
        k for r in router.routes for k in _route_keys(r) if k not in _GROUP_OF
    )
    assert not unclaimed, (
        f"routes with no group: {unclaimed}. Add each to `_ROUTE_GROUPS` in "
        f"origofs/fastapi.py — a route nobody classified cannot be included or "
        f"excluded, so it ships on every filtered router by accident."
    )

    # …and the reverse: an entry naming a route that no longer exists reads as a
    # considered decision while protecting nothing.
    live = _paths(router)
    stale = sorted(
        k for keys in _ROUTE_GROUPS.values() for k in keys if k not in live
    )
    assert not stale, f"_ROUTE_GROUPS names routes that no longer exist: {stale}"

    # Exactly one group each — a route in two groups would be mounted by either,
    # which makes `include` mean less than it says.
    seen = [k for keys in _ROUTE_GROUPS.values() for k in keys]
    assert len(seen) == len(set(seen)), "a route is listed in more than one group"


def test_a_filtered_router_still_serves_the_routes_it_kept():
    """Filtering must not break the closures the surviving routes depend on.

    The co-editing room registry and its sweeper are built once for the whole
    router; dropping the REST routes must not take them with it.
    """
    app = FastAPI()
    app.include_router(
        build_router(FakeWs(), authn=header_authn, exclude=["files"]), prefix="/v1"
    )
    client = TestClient(app)
    # A kept route answers normally...
    assert client.get("/v1/health").status_code == 200
    # ...and a dropped one is genuinely not routed, rather than 401/403.
    assert client.put(
        "/v1/files/a.txt", content=b"x", headers={"x-actor-id": "1"}
    ).status_code == 404


# --- the coedit split (#160) ------------------------------------------------


def test_the_sockets_can_be_mounted_without_the_checkpoint_route():
    """`coedit` was one all-or-nothing group, so "the sockets, without the mutating
    REST checkpoint" could not be expressed.

    That shape matters: `/coedit-tree-checkpoint` is a mutating body write, and a
    host enforcing its own authorization on body writes needs exactly one such
    path, its own. Mounting origofs's alongside adds a second write path to the
    same bytes, gated differently -- the class of bypass `include` exists to close.
    Note `authn` cannot express it: sockets and checkpoint are all mutating, so
    they all sit behind `authn` and no strictness ordering separates them.
    """
    sockets = _paths(build_router(FakeWs(), authn=header_authn, include=["coedit-ws"]))
    assert (None, "/coedit/{path:path}") in sockets
    assert (None, "/coedit-tree/{path:path}") in sockets
    # Undo is a live-room operation, not a durable write, so it stays with them.
    assert ("POST", "/coedit-undo/{path:path}") in sockets
    assert ("POST", "/coedit-tree-checkpoint/{path:path}") not in sockets

    checkpoint = _paths(
        build_router(FakeWs(), authn=header_authn, include=["coedit-checkpoint"])
    )
    assert checkpoint == {("POST", "/coedit-tree-checkpoint/{path:path}")}


def test_build_coedit_router_can_drop_the_checkpoint_route():
    from origofs.fastapi import build_coedit_router

    both = _paths(build_coedit_router(FakeWs(), authn=header_authn))
    sockets = _paths(
        build_coedit_router(FakeWs(), authn=header_authn, checkpoint_route=False)
    )
    assert ("POST", "/coedit-tree-checkpoint/{path:path}") in both
    assert sockets == both - {("POST", "/coedit-tree-checkpoint/{path:path}")}


def test_coedit_stays_an_alias_for_both_halves():
    """Splitting a group must not change what an existing call mounts."""
    from origofs.fastapi import ROUTE_GROUP_ALIASES

    assert ROUTE_GROUP_ALIASES["coedit"] == {"coedit-ws", "coedit-checkpoint"}

    alias = _paths(build_router(FakeWs(), authn=header_authn, include=["coedit"]))
    halves = _paths(
        build_router(
            FakeWs(), authn=header_authn, include=["coedit-ws", "coedit-checkpoint"]
        )
    )
    assert alias == halves

    # And on the denylist side, where getting it wrong leaves a mutating route
    # mounted on a router whose author asked for it to be gone.
    full = _paths(build_router(FakeWs(), authn=header_authn))
    without = _paths(build_router(FakeWs(), authn=header_authn, exclude=["coedit"]))
    assert without == full - alias
