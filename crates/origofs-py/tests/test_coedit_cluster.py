"""Cross-worker live co-editing through FastAPI (roadmap M8).

Two FastAPI apps over the *same* Postgres database (and content store) — two
workers behind a load balancer — each host a socket editing the same document. An
edit on one worker must reach the other worker's room over the Postgres relay.
Bob pulls his worker's state (a fresh y-sync SyncStep1) and finds Alice's edit,
which could only have arrived via the cross-worker relay.

Self-skips unless ORIGOFS_PG_TEST_URL is set. Build + run (from crates/origofs-py):
    maturin develop && pip install fastapi httpx
    ORIGOFS_PG_TEST_URL="host=/var/run/postgresql dbname=origofs" pytest tests/test_coedit_cluster.py
"""
import asyncio
import os
import tempfile
import time
import uuid

import pytest

import origofs
from origofs.fastapi import build_router

from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient

_LOOP = asyncio.new_event_loop()


def _run(make):
    async def _go():
        return await make()

    return _LOOP.run_until_complete(_go())


def _worker(dsn: str, cas: str, tokens: dict):
    """A FastAPI app over a Postgres workspace sharing `dsn` + `cas` with peers."""
    ws = _run(lambda: origofs.Workspace.open_pg(dsn, cas))

    async def authn(token: str = Query(...)) -> origofs.WriteCtx:
        ctx = tokens.get(token)
        if ctx is None:
            raise HTTPException(status_code=401, detail="bad token")
        return ctx

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn))
    return app, ws


def test_edit_on_one_worker_reaches_another():
    dsn = os.environ.get("ORIGOFS_PG_TEST_URL")
    if not dsn:
        pytest.skip("ORIGOFS_PG_TEST_URL unset")

    # A unique path per run keeps the shared (un-reset) relay table from leaking
    # a prior run's ops into this one.
    doc = f"/cluster-{uuid.uuid4().hex}.md"
    cas = tempfile.mkdtemp()

    # Worker A provisions the actors (shared DB); worker B sees them.
    ws_setup = _run(lambda: origofs.Workspace.open_pg(dsn, cas))
    assert ws_setup.is_postgres()
    alice = _run(lambda: ws_setup.create_human("alice", None))
    alice_s = _run(lambda: ws_setup.create_session(alice, "web"))
    bob = _run(lambda: ws_setup.create_human("bob", None))
    bob_s = _run(lambda: ws_setup.create_session(bob, "web"))
    tokens = {
        "alice": origofs.WriteCtx.session(alice, alice_s),
        "bob": origofs.WriteCtx.session(bob, bob_s),
    }

    app_a, _ws_a = _worker(dsn, cas, tokens)
    app_b, _ws_b = _worker(dsn, cas, tokens)

    # Alice's client, with content ready to ride up in the handshake.
    client_a = origofs.CoeditDoc()
    _run(lambda: client_a.insert(tokens["alice"], 0, "hi from worker A"))
    client_b = origofs.CoeditDoc()

    with TestClient(app_a) as ta, TestClient(app_b) as tb:
        with ta.websocket_connect(f"/coedit{doc}?token=alice") as sa, \
             tb.websocket_connect(f"/coedit{doc}?token=bob") as sb:
            # Handshake both (creates + drains the room on each worker).
            greet_a = sa.receive_bytes()
            greet_b = sb.receive_bytes()
            _ = _run(lambda: client_b.handle_sync(tokens["bob"], greet_b))

            # Alice answers her greeting — her content lands on worker A, which
            # attributes it and publishes it to the relay.
            ans_a = _run(lambda: client_a.handle_sync(tokens["alice"], greet_a))
            sa.send_bytes(ans_a.reply)

            # Give the relay a moment to carry the op to worker B's room.
            time.sleep(0.6)

            # Bob pulls worker B's current state with a fresh SyncStep1. Worker B
            # answers from *its* room — which holds Alice's edit only if the relay
            # delivered it across workers. Exactly one read is safe: whichever frame
            # is first (a drain-fanned Update queued during the sleep, or the
            # SyncStep2 answer that's guaranteed after our SyncStep1) carries it.
            sb.send_bytes(_run(lambda: client_b.sync_start()))
            frame = sb.receive_bytes()
            _ = _run(lambda: client_b.handle_sync(tokens["bob"], frame))

    assert _run(lambda: client_b.text()) == "hi from worker A", (
        "the edit did not cross workers over the Postgres relay"
    )
