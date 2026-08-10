"""Tenant scoping for `origofs.fastapi.build_router` (issue #93).

A host that puts many tenants in one workspace — the documented "one workspace,
scoped paths" shape — could authorise the path-carrying routes but had nothing to
authorise the workspace-global ones against: ``/log``, ``/status``, ``/diff``,
``/events``, ``/presence``, ``/branches``, ``/suggestions``, and the id-addressed
suggestion routes. Suggestion ids are workspace-global, so knowing an id was
enough. The only safe move was to refuse all of them and re-implement blame and
suggestion review in front of the SDK.

``root=`` scopes the router instead. These tests are written from the attacker's
side wherever possible: what a caller scoped to tenant A can learn or change
about tenant B.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install fastapi httpx
    pytest tests/test_fastapi_tenant_scope.py
"""
import asyncio
import os
import tempfile

import pytest

import origofs
from origofs.fastapi import build_router

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.testclient import TestClient

ACME = "/tenants/acme"
GLOBEX = "/tenants/globex"


def _setup():
    """A workspace with two tenants' files already in it, plus an actor."""
    d = tempfile.mkdtemp()

    async def _go():
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        dan = await ws.create_human("dan", None)
        sess = await ws.create_session(dan, "web")
        ctx = origofs.WriteCtx.session(dan, sess)
        for root in (ACME, GLOBEX):
            await ws.mkdir_as(ctx, root)
            await ws.write_as(ctx, f"{root}/notes.md", f"{root} secrets\n".encode())
        return ws, dan, sess

    return asyncio.run(_go())


def _client(ws, dan, sess, root):
    async def authn(x_actor_id: int = Header(default=None)) -> origofs.WriteCtx:
        if x_actor_id is None:
            raise HTTPException(status_code=401, detail="unauthenticated")
        return origofs.WriteCtx.session(x_actor_id, sess)

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn, root=root))
    return TestClient(app), {"X-Actor-Id": str(dan)}


def _acme():
    ws, dan, sess = _setup()
    c, hdr = _client(ws, dan, sess, ACME)
    return c, hdr, ws, dan, sess


# --- paths resolve under the root -------------------------------------------


def test_a_path_resolves_under_the_root():
    c, hdr, ws, _dan, _sess = _acme()
    # The caller says `/notes.md`; it means `/tenants/acme/notes.md`.
    assert c.get("/files/notes.md").content == b"/tenants/acme secrets\n"

    assert c.put("/files/new.md", content=b"mine\n", headers=hdr).status_code == 200

    async def _read():
        return bytes(await ws.read(f"{ACME}/new.md"))

    assert asyncio.run(_read()) == b"mine\n"


def test_another_tenants_path_is_not_addressable():
    # There is no representable request for the other tenant's file: the root is
    # prepended, not compared against, so this resolves to
    # /tenants/acme/tenants/globex/notes.md -- which does not exist.
    c, _hdr, _ws, _dan, _sess = _acme()
    assert c.get("/files/tenants/globex/notes.md").status_code == 404


def test_dot_dot_is_refused():
    c, hdr, _ws, _dan, _sess = _acme()
    assert c.get("/files/../globex/notes.md").status_code in (400, 404)
    r = c.post("/rename", json={"from": "/notes.md", "to": "/../globex/x.md"}, headers=hdr)
    assert r.status_code == 400, r.text


def test_a_rename_cannot_move_a_file_across_tenants():
    c, hdr, ws, _dan, _sess = _acme()
    # Both ends are scoped, so the destination lands inside acme too.
    r = c.post("/rename", json={"from": "/notes.md", "to": "/moved.md"}, headers=hdr)
    assert r.status_code == 200, r.text

    async def _check():
        return bytes(await ws.read(f"{ACME}/moved.md")), bytes(await ws.read(f"{GLOBEX}/notes.md"))

    moved, other = asyncio.run(_check())
    assert moved == b"/tenants/acme secrets\n"
    assert other == b"/tenants/globex secrets\n", "the other tenant was disturbed"


def test_a_directory_listing_is_the_tenants_own_root():
    c, _hdr, _ws, _dan, _sess = _acme()
    names = {e["name"] for e in c.get("/dirs/").json()}
    assert "notes.md" in names
    # Not the workspace root -- `tenants` would be there if the scope leaked.
    assert "tenants" not in names, names


# --- listing routes are filtered to the root --------------------------------


def test_status_and_diff_only_report_the_tenants_paths():
    c, hdr, ws, dan, sess = _acme()
    ctx = origofs.WriteCtx.session(dan, sess)

    async def _churn():
        await ws.commit_as(ctx, "dan", "base")
        await ws.write_as(ctx, f"{ACME}/a.md", b"a\n")
        await ws.write_as(ctx, f"{GLOBEX}/b.md", b"b\n")

    asyncio.run(_churn())

    paths = {e["path"] for e in c.get("/status").json()}
    assert paths == {f"{ACME}/a.md"}, paths


def test_the_change_feed_is_filtered():
    c, _hdr, _ws, _dan, _sess = _acme()
    paths = {e["path"] for e in c.get("/events").json()}
    assert paths, "the feed came back empty; the test would be vacuous"
    assert all(p.startswith(ACME) for p in paths), paths


def test_presence_does_not_leak_other_tenants_or_pathless_rows():
    c, hdr, ws, dan, sess = _acme()

    async def _elsewhere():
        # Somebody working in the other tenant, and somebody with no path at all.
        other = await ws.create_human("eve", None)
        other_s = await ws.create_session(other, "web")
        await ws.touch(other, other_s, f"{GLOBEX}/notes.md")
        idle = await ws.create_human("idle", None)
        idle_s = await ws.create_session(idle, "web")
        await ws.touch(idle, idle_s, None)

    asyncio.run(_elsewhere())
    assert c.post("/presence", json={"path": "notes.md"}, headers=hdr).status_code == 200

    rows = c.get("/presence").json()
    assert rows, "presence came back empty; the test would be vacuous"
    assert all(r["path"] and r["path"].startswith(ACME) for r in rows), rows
    # A row with no path still says "somebody is active" -- filtered out too.
    assert all(r["actor_id"] == dan for r in rows), rows


def test_a_heartbeat_cannot_advertise_a_path_in_another_tenant():
    c, hdr, _ws, _dan, _sess = _acme()
    r = c.post("/presence", json={"path": "/tenants/globex/notes.md"}, headers=hdr)
    assert r.status_code == 200, r.text
    # Scoped like any other caller-supplied path.
    assert r.json()["path"] == f"{ACME}/tenants/globex/notes.md"


# --- the suggestion queue ----------------------------------------------------


def test_the_suggestion_queue_is_filtered_even_without_a_path_filter():
    c, hdr, ws, dan, sess = _acme()

    async def _propose():
        agent = await ws.create_agent("claude", "opus", dan)
        a_sess = await ws.create_session(agent, "mcp")
        actx = origofs.WriteCtx.session(agent, a_sess)
        mine = await ws.suggest(actx, f"{ACME}/notes.md", b"acme edit\n", None)
        theirs = await ws.suggest(actx, f"{GLOBEX}/notes.md", b"globex edit\n", None)
        return mine, theirs

    mine, theirs = asyncio.run(_propose())

    # No `path` query at all -- which is exactly how the leak used to happen.
    ids = {s["id"] for s in c.get("/suggestions").json()}
    assert ids == {mine}, ids


def test_another_tenants_suggestion_is_404_by_id():
    # Suggestion ids are workspace-global, so knowing an id used to be enough to
    # read, accept or reject somebody else's proposal.
    c, hdr, ws, dan, sess = _acme()

    async def _propose():
        agent = await ws.create_agent("claude", "opus", dan)
        a_sess = await ws.create_session(agent, "mcp")
        actx = origofs.WriteCtx.session(agent, a_sess)
        return await ws.suggest(actx, f"{GLOBEX}/notes.md", b"globex edit\n", None)

    theirs = asyncio.run(_propose())

    # 404, not 403: a caller must not be able to tell "exists but not yours" from
    # "no such id", or it can walk the id space.
    assert c.get(f"/suggestions/{theirs}/diff").status_code == 404
    assert c.post(f"/suggestions/{theirs}/accept", headers=hdr).status_code == 404
    assert c.post(f"/suggestions/{theirs}/reject", headers=hdr).status_code == 404

    async def _still_pending():
        return (await ws.get_suggestion(theirs))["status"]

    assert asyncio.run(_still_pending()) == "pending"


def test_a_suggestion_made_through_a_scoped_router_lands_in_the_tenant():
    c, hdr, ws, _dan, _sess = _acme()
    r = c.post("/suggestions", params={"path": "/notes.md"}, content=b"proposed\n", headers=hdr)
    assert r.status_code == 200, r.text

    async def _path_of():
        return (await ws.get_suggestion(r.json()["id"]))["path"]

    assert asyncio.run(_path_of()) == f"{ACME}/notes.md"


# --- operations no filter can narrow ----------------------------------------


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("get", "/log", None),
        ("get", "/branches", None),
        ("post", "/commit", {"message": "m", "author": "dan"}),
        ("post", "/branches", {"name": "feature"}),
        ("post", "/checkout", {"name": "main"}),
    ],
)
def test_whole_workspace_operations_are_refused_on_a_scoped_router(method, path, body):
    # A checkout rematerializes *every* tenant's files; the commit log is a
    # shared history. There is no filter that makes these tenant-scoped, so they
    # are refused rather than silently acting workspace-wide.
    c, hdr, _ws, _dan, _sess = _acme()
    r = getattr(c, method)(path, json=body, headers=hdr) if body else getattr(c, method)(path, headers=hdr)
    assert r.status_code == 403, r.text
    assert "whole workspace" in r.json()["detail"]


# --- a root resolved per request --------------------------------------------


def test_the_root_can_be_resolved_from_the_request():
    # One router, many tenants: the host maps the credential to a root.
    ws, dan, sess = _setup()

    async def authn(x_actor_id: int = Header(default=None)) -> origofs.WriteCtx:
        if x_actor_id is None:
            raise HTTPException(status_code=401, detail="unauthenticated")
        return origofs.WriteCtx.session(x_actor_id, sess)

    async def tenant_root(request: Request) -> str:
        tenant = request.headers.get("x-tenant")
        if tenant is None:
            raise HTTPException(status_code=401, detail="no tenant")
        return f"/tenants/{tenant}"

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn, root=tenant_root))
    c = TestClient(app)

    assert c.get("/files/notes.md", headers={"X-Tenant": "acme"}).content == b"/tenants/acme secrets\n"
    assert c.get("/files/notes.md", headers={"X-Tenant": "globex"}).content == b"/tenants/globex secrets\n"
    # The resolver rejects, so the request does.
    assert c.get("/files/notes.md").status_code == 401


# --- the unscoped router is unchanged ---------------------------------------


def test_without_a_root_nothing_changes():
    # The single-tenant default must be exactly as it was: absolute paths, no
    # filtering, and the whole-workspace operations available.
    ws, dan, sess = _setup()
    c, hdr = _client(ws, dan, sess, None)

    assert c.get(f"/files{ACME}/notes.md").content == b"/tenants/acme secrets\n"
    assert c.get(f"/files{GLOBEX}/notes.md").content == b"/tenants/globex secrets\n"
    assert c.post("/commit", json={"message": "m", "author": "dan"}, headers=hdr).status_code == 200
    assert c.get("/log").status_code == 200
    assert c.get("/branches").status_code == 200
    paths = {e["path"] for e in c.get("/events").json()}
    assert any(p.startswith(GLOBEX) for p in paths), paths


if __name__ == "__main__":
    import inspect

    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            if hasattr(fn, "pytestmark"):
                continue  # parametrized; run it under pytest
            fn()
            print("ok  ", name)
    print("ALL OK")
