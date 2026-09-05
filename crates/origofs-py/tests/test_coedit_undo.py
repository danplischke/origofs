"""Per-actor undo/redo over the FastAPI router (#146).

The Rust API surface got this first; the FastAPI router keeps its own room
registry, so parity here is real code rather than a re-export — which is exactly
the drift `test_parity.py` exists to stop.

What these assert beyond "the route returns 200" is the two properties that are
easy to get wrong in a second implementation: an undo reaches **every** socket
including the one that asked for it (the pop happened on the server, so unlike
its own edits that client has not applied it locally), and an actor's stack is
dropped when *their* last socket leaves rather than when the room empties.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install fastapi httpx
    pytest tests/test_coedit_undo.py
"""
import asyncio
import os
import tempfile
import threading
import time

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


def _app():
    """A FastAPI app over a fresh workspace with two writers, alice and bob."""
    d = tempfile.mkdtemp()
    ws = _run(
        lambda: origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
    )
    alice = _run(lambda: ws.create_human("alice", None))
    alice_s = _run(lambda: ws.create_session(alice, "web"))
    bob = _run(lambda: ws.create_human("bob", None))
    bob_s = _run(lambda: ws.create_session(bob, "web"))
    tokens = {
        "alice": origofs.WriteCtx.session(alice, alice_s),
        "bob": origofs.WriteCtx.session(bob, bob_s),
    }

    async def authn(token: str = Query(...)) -> origofs.WriteCtx:
        resolved = tokens.get(token)
        if resolved is None:
            raise HTTPException(status_code=401, detail="bad token")
        return resolved

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn))
    return app, ws, tokens


def _handshake(sock, doc, ctx):
    """Answer the server's SyncStep1 greeting, pushing whatever `doc` holds.

    Content typed into `doc` *before* connecting rides up in the SyncStep2 answer,
    which is how a real Yjs client delivers what it already has — and the only way
    to push from here, since the Python surface exposes no frame encoder.
    """
    greeting = sock.receive_bytes()
    answer = _run(lambda: doc.handle_sync(ctx, greeting))
    if answer.reply:
        sock.send_bytes(answer.reply)


def _recv(sock, timeout=3.0):
    """One frame, or ``None`` if none arrives within `timeout`.

    `TestClient`'s socket read blocks forever on an empty queue, so a frame that
    never arrives would hang the suite instead of failing it — and "the undo did
    not reach this socket" is exactly the bug these tests exist to catch. Reading
    on a worker thread turns that hang into an assertion. (Verified by breaking
    the fan-out on purpose: without this the run wedges rather than failing.)
    """
    box: list = []

    def pull():
        try:
            box.append(sock.receive_bytes())
        except Exception:
            pass

    t = threading.Thread(target=pull, daemon=True)
    t.start()
    t.join(timeout)
    return box[0] if box else None


def _drain(sock, doc, ctx, want, tries=8):
    """Read frames until `doc` reads `want`, or give up."""
    for _ in range(tries):
        if _run(lambda: doc.text()) == want:
            return
        frame = _recv(sock)
        if frame is None:
            return
        _run(lambda: doc.handle_sync(ctx, frame))


def test_an_undo_reaches_every_socket_including_the_requester():
    app, _ws, tokens = _app()
    a_ctx, b_ctx = tokens["alice"], tokens["bob"]
    a_doc, b_doc = origofs.CoeditDoc(), origofs.CoeditDoc()
    # Typed before connecting, so the handshake's SyncStep2 carries it up.
    _run(lambda: a_doc.insert(a_ctx, 0, "hello from alice"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice") as a_sock, \
             tc.websocket_connect("/coedit/doc.md?token=bob") as b_sock:
            _handshake(a_sock, a_doc, a_ctx)
            _handshake(b_sock, b_doc, b_ctx)
            time.sleep(0.15)
            _drain(b_sock, b_doc, b_ctx, "hello from alice")
            assert _run(lambda: b_doc.text()) == "hello from alice"

            res = tc.post("/coedit-undo/doc.md?token=alice", json={"redo": False})
            assert res.status_code == 200, res.text
            assert res.json()["changed"] is True

            # The requester's own socket must receive it: the pop happened on the
            # server, so unlike her own typing alice has not applied it locally.
            _drain(a_sock, a_doc, a_ctx, "")
            assert _run(lambda: a_doc.text()) == "", (
                "the socket that asked for the undo never received it"
            )
            _drain(b_sock, b_doc, b_ctx, "")
            assert _run(lambda: b_doc.text()) == ""

            # Redo comes back through the same channel.
            res = tc.post("/coedit-undo/doc.md?token=alice", json={"redo": True})
            assert res.status_code == 200, res.text
            assert res.json()["changed"] is True
            _drain(a_sock, a_doc, a_ctx, "hello from alice")
            assert _run(lambda: a_doc.text()) == "hello from alice"


async def ws_read(ws):
    try:
        return bytes(await ws.read('/doc.md'))
    except Exception as e:
        return f'<{e}>'


def test_one_actor_cannot_undo_anothers_work():
    app, ws, tokens = _app()
    a_ctx, b_ctx = tokens["alice"], tokens["bob"]
    a_doc, b_doc = origofs.CoeditDoc(), origofs.CoeditDoc()
    _run(lambda: a_doc.insert(a_ctx, 0, "alice wrote this"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice") as a_sock, \
             tc.websocket_connect("/coedit/doc.md?token=bob") as b_sock:
            _handshake(a_sock, a_doc, a_ctx)
            _handshake(b_sock, b_doc, b_ctx)
            time.sleep(0.15)
            _drain(b_sock, b_doc, b_ctx, "alice wrote this")

            # Bob is authorized — and still has nothing to undo, because the stack
            # is scoped to his own origins.
            res = tc.post("/coedit-undo/doc.md?token=bob", json={})
            assert res.status_code == 200, res.text
            changed = res.json()["changed"]

        # Whatever the pop did, the file and its blame must be untouched: alice
        # still owns every byte she typed. That is the property that matters —
        # `checkpoint_coedit` reads exactly this.
        for _ in range(60):
            try:
                blame = _run(lambda: ws.blame("/doc.md"))
            except FileNotFoundError:
                blame = []
            if blame:
                break
            time.sleep(0.05)
        assert bytes(_run(lambda: ws.read("/doc.md"))) == b"alice wrote this"
        assert [(b["actor"]["id"], b["byte_start"], b["byte_end"]) for b in blame] == [
            (tokens["alice"].actor_id, 0, 16)
        ], f"bob's undo (changed={changed}) altered the authorship of alice's text: {blame}"


def test_undo_is_refused_without_write_at_the_path():
    """An undo is a write, so it takes WRITE at the path and answers 403 — not a
    quiet no-op, which would show an editor the key working while nothing happens.
    """
    app, ws, tokens = _app()
    a_ctx = tokens["alice"]
    alice, bob = a_ctx.actor_id, tokens["bob"].actor_id

    _run(lambda: ws.write_as(a_ctx, "/doc.md", b"seed\n"))
    _run(lambda: ws.set_acl_default_deny(True))
    _run(lambda: ws.grant(alice, "/", ["read", "write"], None))
    _run(lambda: ws.grant(bob, "/", ["read"], alice))

    a_doc = origofs.CoeditDoc()
    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice") as a_sock:
            _handshake(a_sock, a_doc, a_ctx)
            res = tc.post("/coedit-undo/doc.md?token=bob", json={})
            assert res.status_code == 403, res.text


def test_undo_without_a_live_room_changes_nothing():
    """Not an error, and deliberately not an implicit open — opening here would
    mark the path live with no socket whose disconnect ever clears it."""
    app, ws, _tokens = _app()
    with TestClient(app) as tc:
        res = tc.post("/coedit-undo/never-opened.md?token=alice", json={})
        assert res.status_code == 200, res.text
        assert res.json()["changed"] is False
        assert _run(lambda: ws.live_paths()) == []


def test_an_actors_stack_goes_when_their_last_socket_does():
    """Undo is an editor affordance, not history: it does not survive a reconnect.
    And it is dropped per actor, so bob leaving does not cost alice her stack."""
    app, _ws, tokens = _app()
    a_ctx, b_ctx = tokens["alice"], tokens["bob"]
    a_doc, b_doc = origofs.CoeditDoc(), origofs.CoeditDoc()
    _run(lambda: a_doc.insert(a_ctx, 0, "alice typed this"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit/doc.md?token=alice") as a_sock:
            _handshake(a_sock, a_doc, a_ctx)
            time.sleep(0.15)

            # Bob joins and leaves; alice's stack must survive it.
            with tc.websocket_connect("/coedit/doc.md?token=bob") as b_sock:
                _handshake(b_sock, b_doc, b_ctx)
            time.sleep(0.15)

            res = tc.post("/coedit-undo/doc.md?token=alice", json={})
            assert res.status_code == 200, res.text
            assert res.json()["changed"] is True, (
                "bob disconnecting took alice's undo stack with it"
            )


def test_the_tree_shape_undoes_through_the_same_route():
    """Both shapes can hold one path at once, so the request names the root. A
    flat-only default would leave undo unavailable to every rich-text editor."""
    app, _ws, tokens = _app()
    a_ctx = tokens["alice"]
    doc = origofs.CoeditTreeDoc("content")
    _run(lambda: doc.append_text(a_ctx, "p", "alice wrote this"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit-tree/doc.md?token=alice") as sock:
            greeting = sock.receive_bytes()
            answer = _run(lambda: doc.handle_sync(a_ctx, greeting))
            if answer.reply:
                sock.send_bytes(answer.reply)
            time.sleep(0.15)

            # Without a root this addresses the flat room, which nobody opened.
            res = tc.post("/coedit-undo/doc.md?token=alice", json={})
            assert res.status_code == 200, res.text
            assert res.json()["changed"] is False, (
                "a root-less request reached the tree room"
            )

            res = tc.post(
                "/coedit-undo/doc.md?token=alice", json={"root": "content"}
            )
            assert res.status_code == 200, res.text
            assert res.json()["changed"] is True, res.text


def test_a_second_worker_is_refused_the_same_actors_stack():
    """Two routers over one workspace = two workers behind a load balancer.

    At most one may hold an actor's undo stack for a document (#146). Two
    independent stacks can pop items touching the same content, and because the
    author stamp is written in the same undo step as the insert it describes, one
    worker's undo strips a stamp the other's restore needs — leaving text present
    but unattributed for the next checkpoint to credit to whoever triggered it.

    The refused worker reports ``available: false`` rather than "nothing to undo":
    the actor's history exists, it is simply not here.
    """
    app_a, ws, tokens = _app()
    # A second router over the *same* workspace, with its own room registry and
    # its own worker id — what a second process behind a balancer has.
    from fastapi import FastAPI as _FastAPI, HTTPException as _HTTPExc, Query as _Query

    async def authn(token: str = _Query(...)) -> origofs.WriteCtx:
        resolved = tokens.get(token)
        if resolved is None:
            raise _HTTPExc(status_code=401, detail="bad token")
        return resolved

    app_b = _FastAPI()
    app_b.include_router(build_router(ws, authn=authn))

    a_ctx = tokens["alice"]
    doc_a, doc_b = origofs.CoeditDoc(), origofs.CoeditDoc()
    _run(lambda: doc_a.insert(a_ctx, 0, "typed on worker a"))

    with TestClient(app_a) as tc_a, TestClient(app_b) as tc_b:
        with tc_a.websocket_connect("/coedit/doc.md?token=alice") as sock_a:
            _handshake(sock_a, doc_a, a_ctx)
            time.sleep(0.15)

            with tc_b.websocket_connect("/coedit/doc.md?token=alice") as sock_b:
                _handshake(sock_b, doc_b, a_ctx)
                time.sleep(0.15)

                res = tc_a.post("/coedit-undo/doc.md?token=alice", json={})
                assert res.status_code == 200, res.text
                assert res.json()["available"] is True, res.text

                res = tc_b.post("/coedit-undo/doc.md?token=alice", json={})
                assert res.status_code == 200, res.text
                assert res.json()["available"] is False, (
                    f"a second worker was given the same actor's undo stack: {res.text}"
                )

        # Worker A's socket has closed, releasing the claim with the stack it
        # guarded — so worker B can serve alice at once rather than waiting out a
        # lease. The retry happens on the request itself, so no reload is needed.
        time.sleep(0.2)
        with tc_b.websocket_connect("/coedit/doc.md?token=alice") as sock_b:
            _handshake(sock_b, doc_b, a_ctx)
            res = tc_b.post("/coedit-undo/doc.md?token=alice", json={})
            assert res.status_code == 200, res.text
            assert res.json()["available"] is True, (
                f"the claim was not released when the holder's last socket left: {res.text}"
            )


def test_undo_reports_availability_separately_from_change():
    """"Nothing to undo" and "your stack is not here" are different answers, and a
    client drawing the affordance needs to tell them apart."""
    app, _ws, tokens = _app()
    a_ctx = tokens["alice"]
    doc = origofs.CoeditDoc()

    with TestClient(app) as tc:
        # No room open anywhere: not available, nothing changed.
        res = tc.post("/coedit-undo/nothing-open.md?token=alice", json={})
        assert res.json() == {
            "path": "/nothing-open.md",
            "redo": False,
            "changed": False,
            "available": False,
        }

        # A room this worker holds, with an empty stack: available, unchanged.
        with tc.websocket_connect("/coedit/doc.md?token=alice") as sock:
            _handshake(sock, doc, a_ctx)
            time.sleep(0.15)
            res = tc.post("/coedit-undo/doc.md?token=alice", json={})
            assert res.json()["available"] is True, res.text
            assert res.json()["changed"] is False, res.text
