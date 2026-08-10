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

from fastapi import FastAPI, Header, HTTPException, Query
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


# --- the live/dirty marker + CRDT suggestions (issue #75 §3.2, §3.4) ---------


def test_live_marker_is_set_on_open_and_cleared_on_last_leave():
    # `open_coedit` marks the path live so a byte reader can tell "these bytes
    # are the whole truth" from "these bytes may lag an open Y.Doc"; the router
    # clears it after the last socket's final checkpoint. Reading a live path
    # never blocks or fails -- `read_live` just reports the marker alongside the
    # (genuinely checkpointed) bytes.
    app, ws, alice, alice_s = _app_with_alice()
    ctx = origofs.WriteCtx.session(alice, alice_s)
    _run(lambda: ws.write_as(ctx, "/doc.md", b"seed"))

    assert _run(lambda: ws.live_doc("/doc.md")) is None
    assert _run(lambda: ws.live_paths()) == []
    assert _run(lambda: ws.read_live("/doc.md"))[1] is None

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice-token") as sock:
            sock.receive_bytes()  # SyncStep1 greeting: the room is open

            live = _run(lambda: ws.live_doc("/doc.md"))
            assert live is not None, "open_coedit did not mark the path live"
            assert live["path"] == "/doc.md"
            assert live["actor_id"] == alice
            assert live["session_id"] == alice_s
            assert [d["path"] for d in _run(lambda: ws.live_paths())] == ["/doc.md"]

            # A read of a live path is answered, not refused, not blocked -- the
            # durable bytes come back *with* the marker saying they may lag.
            data, mark = _run(lambda: ws.read_live("/doc.md"))
            assert bytes(data) == b"seed"
            assert mark is not None and mark["path"] == "/doc.md"

        # After the last leave: the final checkpoint lands, then the marker goes.
        for _ in range(60):
            if _run(lambda: ws.live_doc("/doc.md")) is None:
                break
            time.sleep(0.05)

    assert _run(lambda: ws.live_doc("/doc.md")) is None, "live marker outlived the room"
    assert _run(lambda: ws.live_paths()) == []
    # Clearing the flag is only about the flag: the bytes are still there.
    data, mark = _run(lambda: ws.read_live("/doc.md"))
    assert bytes(data) == b"seed" and mark is None


def test_suggest_coedit_records_a_crdt_suggestion():
    # A co-edited path is proposed against as a CRDT merge, not a file body: the
    # review row's kind is `crdt`, and accepting it merges rather than clobbering.
    d = tempfile.mkdtemp()
    ws = _run(lambda: origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")))
    alice = _run(lambda: ws.create_human("alice", None))
    alice_s = _run(lambda: ws.create_session(alice, "web"))
    bob = _run(lambda: ws.create_human("bob", None))
    bob_s = _run(lambda: ws.create_session(bob, "web"))
    a_ctx = origofs.WriteCtx.session(alice, alice_s)
    b_ctx = origofs.WriteCtx.session(bob, bob_s)

    # Alice co-edits and checkpoints, so the document has a durable sidecar.
    doc = _run(lambda: ws.open_coedit(a_ctx, "/doc.md"))
    _run(lambda: doc.insert(a_ctx, 0, "base"))
    _run(lambda: ws.checkpoint_coedit(a_ctx, "/doc.md", doc))
    _run(lambda: ws.end_coedit("/doc.md"))

    # Bob forks that document, types, and proposes the result as a CRDT merge.
    fork = _run(lambda: ws.open_coedit(b_ctx, "/doc.md"))
    _run(lambda: fork.insert(b_ctx, 4, " + bob"))
    sid = _run(lambda: ws.suggest_coedit(b_ctx, "/doc.md", fork, "bob's take"))
    _run(lambda: ws.end_coedit("/doc.md"))

    s = _run(lambda: ws.get_suggestion(sid))
    assert s["kind"] == "crdt", s
    assert s["status"] == "pending" and s["actor_id"] == bob
    # Both blobs live in the content store; the row holds only their addresses.
    assert s["base_hash"] and s["proposed_hash"]

    # A plain byte proposal still reports kind == "bytes", so a reviewer UI can
    # tell the two apart (and knows which one a stale base can retire).
    bsid = _run(lambda: ws.suggest(b_ctx, "/other.md", b"hi", None))
    assert _run(lambda: ws.get_suggestion(bsid))["kind"] == "bytes"

    # Accepting merges the update in, credited to its author (bob), not the
    # approver (alice).
    _run(lambda: ws.accept_suggestion(sid, a_ctx))
    assert bytes(_run(lambda: ws.read("/doc.md"))) == b"base + bob"
    assert _run(lambda: ws.get_suggestion(sid))["status"] == "accepted"
    assert any(b["actor"]["id"] == bob for b in _run(lambda: ws.blame("/doc.md")))


def test_suggest_coedit_update_takes_raw_yjs_blobs():
    # The primitive a browser editor uses: it already holds encodeStateVector /
    # encodeStateAsUpdate, so it proposes without the server materializing a doc.
    d = tempfile.mkdtemp()
    ws = _run(lambda: origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")))
    dan = _run(lambda: ws.create_human("dan", None))
    dan_s = _run(lambda: ws.create_session(dan, "web"))
    ctx = origofs.WriteCtx.session(dan, dan_s)

    doc = _run(lambda: ws.open_coedit(ctx, "/doc.md"))
    _run(lambda: doc.insert(ctx, 0, "hello"))
    _run(lambda: ws.checkpoint_coedit(ctx, "/doc.md", doc))
    _run(lambda: ws.end_coedit("/doc.md"))

    fork = _run(lambda: ws.open_coedit(ctx, "/doc.md"))
    base_sv = bytes(_run(lambda: fork.state_vector()))
    _run(lambda: fork.insert(ctx, 5, " world"))
    update = bytes(_run(lambda: fork.state_update()))
    sid = _run(lambda: ws.suggest_coedit_update(ctx, "/doc.md", base_sv, update, None))
    _run(lambda: ws.end_coedit("/doc.md"))
    assert _run(lambda: ws.get_suggestion(sid))["kind"] == "crdt"

    # An empty update proposes nothing and is refused at propose time, rather
    # than becoming a review row nobody can apply.
    with pytest.raises(ValueError):
        _run(lambda: ws.suggest_coedit_update(ctx, "/doc.md", base_sv, b"", None))


# --- credential transport and per-connection sessions (#98) ------------------


def _app_with_subprotocol_auth():
    """An app whose `authn` reads the credential out of `Sec-WebSocket-Protocol`
    -- the one header a browser *can* set on an upgrade, and the reason the router
    has to echo the marker back."""
    d = tempfile.mkdtemp()
    ws = _run(lambda: origofs.Workspace.open_local(os.path.join(d, "meta.db"), os.path.join(d, "cas")))
    alice = _run(lambda: ws.create_human("alice", None))
    alice_s = _run(lambda: ws.create_session(alice, "web"))
    tokens = {"alice-token": origofs.WriteCtx.session(alice, alice_s)}

    async def authn(sec_websocket_protocol: str = Header(default="")) -> origofs.WriteCtx:
        # `new WebSocket(url, ["origofs", token])` arrives as "origofs, <token>".
        parts = [p.strip() for p in sec_websocket_protocol.split(",") if p.strip()]
        resolved = tokens.get(parts[1]) if len(parts) > 1 and parts[0] == "origofs" else None
        if resolved is None:
            raise HTTPException(status_code=401, detail="bad token")
        return resolved

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn))
    return app, ws, alice, alice_s


def test_a_credential_can_ride_the_websocket_subprotocol():
    app, ws, alice, alice_s = _app_with_subprotocol_auth()
    ctx = origofs.WriteCtx.session(alice, alice_s)
    client = origofs.CoeditDoc()
    _run(lambda: client.insert(ctx, 0, "typed over a subprotocol"))  # 24 bytes

    with TestClient(app) as tc:
        # No ?token= in the URL at all -- the credential is in the subprotocol
        # list, and the server has to select "origofs" or a browser would fail
        # the handshake.
        with tc.websocket_connect(
            "/coedit/doc.md", subprotocols=["origofs", "alice-token"]
        ) as sock:
            assert sock.accepted_subprotocol == "origofs"
            greeting = sock.receive_bytes()  # server -> client: SyncStep1
            answer = _run(lambda: client.handle_sync(ctx, greeting))
            sock.send_bytes(answer.reply)
            time.sleep(0.1)  # let the server apply the frame before we close

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
    assert blame[0]["actor"]["id"] == alice
    assert blame[0]["session"] == alice_s


def test_a_socket_offering_no_subprotocol_still_connects():
    # The echo must be conditional: selecting a protocol the client never offered
    # is itself a handshake failure.
    app, _ws, _alice, _alice_s = _app_with_alice()
    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice-token") as sock:
            assert sock.accepted_subprotocol is None
            sock.receive_bytes()


def test_a_session_less_credential_gets_a_session_for_the_connection():
    # A `WriteCtx.actor(...)` connection used to stamp edits (actor, session=None),
    # which `revert_session` can never undo -- on the surface that produces the
    # most edits.
    d = tempfile.mkdtemp()
    ws = _run(lambda: origofs.Workspace.open_local(os.path.join(d, "meta.db"), os.path.join(d, "cas")))
    alice = _run(lambda: ws.create_human("alice", None))

    async def authn(token: str = Query(...)) -> origofs.WriteCtx:
        if token != "alice-token":
            raise HTTPException(status_code=401, detail="bad token")
        return origofs.WriteCtx.actor(alice)  # deliberately session-less

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn))

    ctx = origofs.WriteCtx.actor(alice)
    client = origofs.CoeditDoc()
    _run(lambda: client.insert(ctx, 0, "live edits are revertible"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice-token") as sock:
            greeting = sock.receive_bytes()
            answer = _run(lambda: client.handle_sync(ctx, greeting))
            sock.send_bytes(answer.reply)
            time.sleep(0.1)

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
    session = blame[0]["session"]
    assert session is not None, "a live edit must carry a session, or it can never be reverted"

    # The point of having one.
    changed = _run(lambda: ws.revert_session(alice, session))
    assert changed == ["/doc.md"]
    assert bytes(_run(lambda: ws.read("/doc.md"))) == b""
