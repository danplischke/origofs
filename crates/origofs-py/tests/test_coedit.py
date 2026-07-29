"""Live co-editing over the FastAPI WebSocket (roadmap M8).

A client speaking the Yjs **y-sync** protocol connects to ``/coedit/{path}``; its
edit is attributed to the authenticated actor server-side (never trusted from the
bytes) and, when the last socket leaves, checkpointed into the byte-range blame
index. The client here is a local ``origofs.CoeditDoc`` driven through the same
y-sync handshake a browser Yjs client performs.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install fastapi httpx
    pytest tests/test_coedit.py
"""
import asyncio
import os
import tempfile
import time

import pytest

import origofs
from origofs.fastapi import build_router

from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient

# One event loop for the synchronous test to drive the async client doc + reads;
# the app runs on its own loop inside TestClient's portal thread. origofs awaitables
# bind to the loop running when they're *created*, so `_run` builds them inside it.
_LOOP = asyncio.new_event_loop()


def _run(make):
    async def _go():
        return await make()

    return _LOOP.run_until_complete(_go())


def _app_with_alice():
    """A FastAPI app over a fresh workspace, with a ?token= authn resolving to a
    provisioned human 'alice'. Returns (app, ws, alice_id, alice_session)."""
    d = tempfile.mkdtemp()
    ws = _run(lambda: origofs.Workspace.open_local(os.path.join(d, "meta.db"), os.path.join(d, "cas")))
    alice = _run(lambda: ws.create_human("alice", None))
    alice_s = _run(lambda: ws.create_session(alice, "web"))
    ctx = origofs.WriteCtx.session(alice, alice_s)
    tokens = {"alice-token": ctx}

    async def authn(token: str = Query(...)) -> origofs.WriteCtx:
        resolved = tokens.get(token)
        if resolved is None:
            raise HTTPException(status_code=401, detail="bad token")
        return resolved

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn))
    return app, ws, alice, alice_s


def test_coedit_websocket_attributes_content_server_side():
    app, ws, alice, alice_s = _app_with_alice()
    ctx = origofs.WriteCtx.session(alice, alice_s)

    # A y-sync client: a local CoeditDoc that has typed some text.
    client = origofs.CoeditDoc()
    _run(lambda: client.insert(ctx, 0, "hello over websocket"))  # 20 bytes

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice-token") as sock:
            greeting = sock.receive_bytes()  # server → client: SyncStep1
            answer = _run(lambda: client.handle_sync(ctx, greeting))  # SyncStep2 w/ content
            sock.send_bytes(answer.reply)
            time.sleep(0.1)  # let the server apply the frame before we close

        # The socket has closed; poll blame (populated by the last-leave checkpoint)
        # while the app is still alive. The file doesn't exist until the checkpoint
        # lands, so tolerate NotFound while waiting.
        blame = []
        for _ in range(60):
            try:
                blame = _run(lambda: ws.blame("/doc.md"))
            except FileNotFoundError:
                blame = []
            if blame:
                break
            time.sleep(0.05)

    assert blame, "checkpoint on last-leave never populated blame"
    assert len(blame) == 1
    assert blame[0]["actor"]["id"] == alice
    assert blame[0]["actor"]["display_name"] == "alice"
    assert (blame[0]["byte_start"], blame[0]["byte_end"]) == (0, 20)
    assert blame[0]["session"] == alice_s
    assert bytes(_run(lambda: ws.read("/doc.md"))) == b"hello over websocket"


def test_coedit_websocket_rejects_bad_token():
    app, _ws, _alice, _alice_s = _app_with_alice()
    with TestClient(app) as tc:
        # An unauthenticated socket is refused: authn raises, so the connection
        # never opens (or is closed immediately).
        with pytest.raises(Exception):
            with tc.websocket_connect("/coedit/doc.md?token=nope") as sock:
                sock.receive_bytes()


def test_coedit_websocket_closes_cleanly_on_malformed_frame():
    # Regression test: a corrupt/malformed y-sync frame used to raise inside
    # the endpoint uncaught, propagating out of the ASGI app instead of
    # closing the socket -- the client saw a hard reset with no diagnostic,
    # and the room's writer task / registry entry were left to the generic
    # `finally` cleanup rather than a clean protocol-error close.
    from starlette.testclient import WebSocketDisconnect

    app, ws, _alice, _alice_s = _app_with_alice()
    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice-token") as sock:
            sock.receive_bytes()  # SyncStep1 greeting
            sock.send_bytes(b"\xff\xff\xff not a y-sync frame at all \xff\xff")
            with pytest.raises(WebSocketDisconnect) as exc_info:
                sock.receive_bytes()
        assert exc_info.value.code == 1002, exc_info.value

        # The room still got torn down (checkpointed + evicted) despite the
        # abnormal close -- a second connection to the same path starts fresh
        # rather than hanging on a half-cleaned-up room.
        with tc.websocket_connect("/coedit/doc.md?token=alice-token") as sock2:
            sock2.receive_bytes()  # would hang/error if the old room leaked


if __name__ == "__main__":
    test_coedit_websocket_attributes_content_server_side()
    print("ok   attributes_content_server_side")
    test_coedit_websocket_rejects_bad_token()
    print("ok   rejects_bad_token")
    test_coedit_websocket_closes_cleanly_on_malformed_frame()
    print("ok   closes_cleanly_on_malformed_frame")
    print("ALL OK")
