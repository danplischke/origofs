"""Revising a proposal instead of stacking a sibling (issue #164).

`write_or_propose` had no update-in-place, so an agent told "revise your proposal"
could only propose again. origofs resolved the resulting pair correctly on accept
and wrongly on **reject**: the abandoned earlier draft stayed pending with a base
that still matched the file, so it accepted cleanly and landed text the author had
replaced and the reviewer never chose.
"""
import asyncio
import functools
import os
import tempfile

import pytest
from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient

import origofs
from origofs.fastapi import build_router


def asyncio_test(fn):
    @functools.wraps(fn)
    def wrapper(*a, **kw):
        return asyncio.run(fn(*a, **kw))

    return wrapper


async def _fixture():
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )
    human = await ws.create_human("h", None)
    agent = await ws.create_agent("a", "opus", human)
    await ws.set_write_policy(agent, "propose")
    h, a = origofs.WriteCtx.actor(human), origofs.WriteCtx.actor(agent)
    await ws.write_as(h, "/n.md", b"base\n")
    return ws, h, a


async def _status(ws, sid):
    return (await ws.get_suggestion(sid))["status"]


@asyncio_test
async def test_replaces_retires_the_draft_it_revises():
    ws, h, a = await _fixture()
    v1 = await ws.suggest(a, "/n.md", b"v1 draft\n", None)
    v2 = await ws.suggest(a, "/n.md", b"v2\n", None, replaces=v1)

    assert await _status(ws, v1) == "superseded"
    assert await _status(ws, v2) == "pending"

    await ws.reject_suggestion(v2, h)
    # The reviewer said no to the proposal, and nothing is quietly waiting to be
    # said yes to.
    assert await ws.list_suggestions("pending", "/n.md") == []
    with pytest.raises(ValueError):
        await ws.accept_suggestion(v1, h)
    assert await ws.read("/n.md") == b"base\n"


@asyncio_test
async def test_without_replaces_the_siblings_and_the_bug_are_both_still_there():
    """The negative control. `supersede_stale_suggestions` cannot close this gap:
    both bases match the file, so by its measure neither draft is stale."""
    ws, h, a = await _fixture()
    v1 = await ws.suggest(a, "/n.md", b"v1\n", None)
    v2 = await ws.suggest(a, "/n.md", b"v2\n", None)
    assert await _status(ws, v1) == "pending"
    assert await _status(ws, v2) == "pending"
    assert await ws.supersede_stale_suggestions("/n.md") == 0


@asyncio_test
async def test_an_author_may_withdraw_a_draft_and_it_is_not_a_rejection():
    ws, _h, a = await _fixture()
    sid = await ws.suggest(a, "/n.md", b"never mind\n", None)
    # A propose-only actor may retire its *own* draft: withdrawing is not review.
    await ws.supersede_suggestion(sid, a, "changed my mind")
    assert await _status(ws, sid) == "superseded"


@asyncio_test
async def test_write_or_propose_carries_replaces():
    ws, _h, a = await _fixture()
    first = await ws.write_or_propose(a, "/n.md", b"v1\n", None)
    assert first.wrote is False
    second = await ws.write_or_propose(
        a, "/n.md", b"v2\n", None, replaces=first.suggestion_id
    )
    assert await _status(ws, first.suggestion_id) == "superseded"
    assert await _status(ws, second.suggestion_id) == "pending"


_LOOP = asyncio.new_event_loop()


def _run(make):
    """Drive a coroutine on one long-lived loop.

    The bindings need a *running* loop to create their future, so the coroutine is
    built inside `run_until_complete` rather than before it — the convention the
    rest of this suite uses.
    """

    async def _go():
        return await make()

    return _LOOP.run_until_complete(_go())


def _app():
    d = tempfile.mkdtemp()
    ws = _run(
        lambda: origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
    )
    human = _run(lambda: ws.create_human("h", None))
    agent = _run(lambda: ws.create_agent("a", "opus", human))
    _run(lambda: ws.set_write_policy(agent, "propose"))
    tokens = {
        "h": origofs.WriteCtx.actor(human),
        "a": origofs.WriteCtx.actor(agent),
    }

    async def authn(token: str = Query(...)) -> origofs.WriteCtx:
        resolved = tokens.get(token)
        if resolved is None:
            raise HTTPException(status_code=401, detail="bad token")
        return resolved

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn))
    return app, ws, tokens["h"]


def test_the_route_takes_replaces_and_offers_a_standalone_supersede():
    app, ws, h = _app()
    _run(lambda: ws.write_as(h, "/n.md", b"base\n"))

    with TestClient(app) as tc:
        v1 = tc.post("/suggestions?path=/n.md&token=a", content=b"v1\n").json()["id"]
        v2 = tc.post(
            f"/suggestions?path=/n.md&token=a&replaces={v1}", content=b"v2\n"
        ).json()["id"]
        assert tc.get(f"/suggestions/{v1}?token=h").json()["status"] == "superseded"

        assert tc.post(f"/suggestions/{v2}/reject?token=h").status_code == 200
        assert tc.get("/suggestions?status=pending&token=h").json() == []

        # And the standalone withdrawal, for a draft with no replacement.
        v3 = tc.post("/suggestions?path=/n.md&token=a", content=b"v3\n").json()["id"]
        r = tc.post(f"/suggestions/{v3}/supersede?token=a&reason=changed+my+mind")
        assert r.status_code == 200, r.text
        assert tc.get(f"/suggestions/{v3}?token=h").json()["status"] == "superseded"
