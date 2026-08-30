"""Structured co-editing over the FastAPI surface (issue #92).

A client speaking the Yjs **y-sync** protocol connects to ``/coedit-tree/{path}``
over a ``Y.XmlFragment`` — the shape ``@platejs/yjs`` / ``y-prosemirror`` /
``y-slate`` bind to natively — and its content is attributed to the authenticated
actor server-side. The **host** then lands the bytes by POSTing its own
serialization plus a span map, because origofs does not own the document schema.

The client here is a local ``origofs.CoeditTreeDoc`` driven through the same y-sync
handshake a browser Yjs client performs.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install fastapi httpx
    pytest tests/test_coedit_tree.py
"""
import asyncio
import functools
import os
import tempfile
import time

import pytest

import origofs
from origofs.fastapi import CheckpointPolicy, build_router

from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient

_LOOP = asyncio.new_event_loop()


def _run(make):
    async def _go():
        return await make()

    return _LOOP.run_until_complete(_go())


def _sync(coro_fn):
    """CI runs pytest without pytest-asyncio; drive the coroutine on the
    module loop, matching `_run` above."""

    @functools.wraps(coro_fn)
    def wrapper(*args, **kwargs):
        return _LOOP.run_until_complete(coro_fn(*args, **kwargs))

    return wrapper



def _app(policy=None):
    """A FastAPI app over a fresh workspace with two provisioned humans."""
    d = tempfile.mkdtemp()
    ws = _run(lambda: origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")))
    alice = _run(lambda: ws.create_human("alice", None))
    alice_s = _run(lambda: ws.create_session(alice, "web"))
    bob = _run(lambda: ws.create_human("bob", None))
    bob_s = _run(lambda: ws.create_session(bob, "web"))
    tokens = {
        "alice-token": origofs.WriteCtx.session(alice, alice_s),
        "bob-token": origofs.WriteCtx.session(bob, bob_s),
    }

    async def authn(token: str = Query(...)) -> origofs.WriteCtx:
        resolved = tokens.get(token)
        if resolved is None:
            raise HTTPException(status_code=401, detail="bad token")
        return resolved

    app = FastAPI()
    kwargs = {"authn": authn}
    if policy is not None:
        kwargs["checkpoint"] = policy
    app.include_router(build_router(ws, **kwargs))
    return app, ws, (alice, alice_s), (bob, bob_s)


def _handshake(sock, client, ctx, absorb=0):
    """Answer the server's SyncStep1 greeting with everything the client has, then
    absorb ``absorb`` frames coming back.

    Inbound frames go through ``apply_relayed``, not ``handle_sync``: they carry
    content the *server* has already attributed, and re-attributing it locally would
    credit this connection with someone else's writing. That is exactly what a
    vanilla Yjs client does with ``Y.applyUpdate``.
    """
    greeting = sock.receive_bytes()
    answer = _run(lambda: client.handle_sync(ctx, greeting))
    if answer.reply:
        sock.send_bytes(answer.reply)
    for _ in range(absorb):
        _run(lambda: client.apply_relayed(sock.receive_bytes()))


def _node_of(client, text):
    """The node id origofs stamped on the run whose text is exactly ``text`` — what
    a host reads off its own client to build a span map."""
    runs = _run(lambda: client.runs())
    for run in runs:
        if run["text"] == text:
            assert run["node"], f"run {text!r} carries no node id: {runs}"
            return run["node"]
    raise AssertionError(f"no run {text!r} in {runs}")


# The headline property: a tree client's content is attributed server-side, and the
# host's own serialization carries each author's exact byte ranges into blame.
def test_tree_socket_attributes_content_and_the_host_lands_the_bytes():
    app, ws, (alice, alice_s), (bob, bob_s) = _app()
    a_ctx = origofs.WriteCtx.session(alice, alice_s)
    b_ctx = origofs.WriteCtx.session(bob, bob_s)

    alice_client = origofs.CoeditTreeDoc("content")
    _run(lambda: alice_client.append_text(a_ctx, "p", "hello"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit-tree/notes.md?token=alice-token") as sock:
            # One frame back: the server's attribution delta for what she sent,
            # which is where her run's *server-assigned* node id comes from — the
            # id the client picked locally is overwritten, on purpose.
            _handshake(sock, alice_client, a_ctx, absorb=1)
            hello = _node_of(alice_client, "hello")

            # Bob's client joins, converges on Alice's paragraph through the
            # server's SyncStep2, then adds his own.
            bob_client = origofs.CoeditTreeDoc("content")
            with tc.websocket_connect("/coedit-tree/notes.md?token=bob-token") as b_sock:
                _handshake(b_sock, bob_client, b_ctx)
                b_sock.send_bytes(_run(lambda: bob_client.sync_start()))
                _run(lambda: bob_client.apply_relayed(b_sock.receive_bytes()))
                assert "hello" in _run(lambda: bob_client.plain_text())

                _run(lambda: bob_client.append_text(b_ctx, "p", "world"))
                b_sock.send_bytes(_frame_update(_run(lambda: bob_client.state_update())))
                _run(lambda: bob_client.apply_relayed(b_sock.receive_bytes()))
                world = _node_of(bob_client, "world")

                # The host serializes however it likes and says which bytes came
                # from which node. The heading and the blank lines are its own.
                body = "# Notes\n\nhello\n\nworld\n"
                resp = tc.post(
                    "/coedit-tree-checkpoint/notes.md?token=bob-token",
                    json={
                        "body": body,
                        "spans": [[9, 14, hello], [16, 21, world]],
                    },
                )
                assert resp.status_code == 200, resp.text
                assert resp.json()["bytes"] == len(body)

        assert bytes(_run(lambda: ws.read("/notes.md"))) == body.encode()
        blame = _run(lambda: ws.blame("/notes.md"))

    ranges = [(b["actor"]["id"], b["byte_start"], b["byte_end"]) for b in blame]
    assert ranges == [
        (bob, 0, 9),      # "# Notes\n\n" -- the serializer's, to the checkpointer
        (alice, 9, 14),   # "hello"
        (bob, 14, 22),    # "world" and the punctuation around it
    ], blame
    assert blame[1]["session"] == alice_s


def _frame_update(update: bytes) -> bytes:
    """Wrap a raw Yjs update in a y-sync ``Update`` message (varint-tagged)."""
    return b"\x00\x02" + _varint(len(update)) + update


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


# origofs cannot rebuild a tree from flat bytes -- that needs the host's schema --
# so a document with no coherent sidecar opens empty and says so, rather than
# quietly handing back something a checkpoint would write over the file.
def test_a_tree_document_reports_whether_it_was_resumed():
    _app_unused, ws, (alice, alice_s), _bob = _app()
    ctx = origofs.WriteCtx.session(alice, alice_s)

    doc = _run(lambda: ws.open_coedit_tree(ctx, "/n.md", "content"))
    assert _run(lambda: doc.resumed()) is False
    assert _run(lambda: doc.is_empty()) is True

    node = _run(lambda: doc.append_text(ctx, "p", "seeded"))  # server-side: already stamped
    _run(lambda: ws.checkpoint_coedit_tree(ctx, "/n.md", doc, b"seeded", [(0, 6, node)]))
    _run(lambda: ws.end_coedit("/n.md"))

    again = _run(lambda: ws.open_coedit_tree(ctx, "/n.md", "content"))
    assert _run(lambda: again.resumed()) is True
    assert _run(lambda: again.plain_text()) == "seeded"
    assert _run(lambda: again.authors()) == _run(lambda: doc.authors())

    # A plain write moves the file underneath, so the sidecar stops describing it.
    _run(lambda: ws.write_as(ctx, "/n.md", b"rewritten"))
    stale = _run(lambda: ws.open_coedit_tree(ctx, "/n.md", "content"))
    assert _run(lambda: stale.resumed()) is False


# A tree cannot be reconciled with a foreign write, so it is refused rather than
# clobbered -- and the route says so with a 409 instead of a generic failure.
def test_an_out_of_band_write_is_refused_not_clobbered():
    app, ws, (alice, alice_s), (bob, bob_s) = _app()
    a_ctx = origofs.WriteCtx.session(alice, alice_s)
    b_ctx = origofs.WriteCtx.session(bob, bob_s)

    client = origofs.CoeditTreeDoc("content")
    _run(lambda: client.append_text(a_ctx, "p", "mine"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit-tree/n.md?token=alice-token") as sock:
            _handshake(sock, client, a_ctx, absorb=1)
            node = _node_of(client, "mine")
            # Bob writes the file directly, outside the co-editing session.
            _run(lambda: ws.write_as(b_ctx, "/n.md", b"bob was here\n"))
            resp = tc.post(
                "/coedit-tree-checkpoint/n.md?token=alice-token",
                json={"body": "mine", "spans": [[0, 4, node]]},
            )
    assert resp.status_code == 409, resp.text
    assert bytes(_run(lambda: ws.read("/n.md"))) == b"bob was here\n"


# The server cannot serialize a tree, so it cannot checkpoint one on a timer -- but
# it persists the CRDT, which is what keeps a crash from costing editing history.
def test_the_sweeper_persists_a_tree_room_without_inventing_a_body():
    policy = CheckpointPolicy(idle_after=0.05, max_interval=None, tick=0.02)
    app, ws, (alice, alice_s), _bob = _app(policy)
    ctx = origofs.WriteCtx.session(alice, alice_s)

    client = origofs.CoeditTreeDoc("content")
    _run(lambda: client.append_text(ctx, "p", "unsaved but not lost"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit-tree/doc.md?token=alice-token") as sock:
            _handshake(sock, client, ctx)
            time.sleep(0.4)  # several sweeper ticks, socket still open

            # The file was never invented...
            with pytest.raises(FileNotFoundError):
                _run(lambda: ws.read("/doc.md"))
            # ...and the marker does not claim the bytes were crystallized...
            marker = _run(lambda: ws.live_doc("/doc.md"))
            assert marker is not None and marker["checkpointed_at"] is None

    # ...but the typing survives: a fresh open resumes it.
    resumed = _run(lambda: ws.open_coedit_tree(ctx, "/doc.md", "content"))
    assert _run(lambda: resumed.resumed()) is True
    assert _run(lambda: resumed.plain_text()) == "unsaved but not lost"


# The checkpoint request names byte ranges and node ids, never an actor.
def test_the_checkpoint_request_cannot_name_an_author():
    app, ws, (alice, alice_s), (bob, bob_s) = _app()
    b_ctx = origofs.WriteCtx.session(bob, bob_s)

    client = origofs.CoeditTreeDoc("content")
    _run(lambda: client.append_text(b_ctx, "p", "bob wrote this"))

    with TestClient(app) as tc:
        with tc.websocket_connect("/coedit-tree/doc.md?token=bob-token") as sock:
            _handshake(sock, client, b_ctx, absorb=1)
            node = _node_of(client, "bob wrote this")
            resp = tc.post(
                "/coedit-tree-checkpoint/doc.md?token=bob-token",
                json={
                    "body": "bob wrote thisand this",
                    "spans": [
                        [0, 14, node],
                        # An id shaped like Alice's identity: origofs never issued
                        # it, so it resolves to nobody and falls back to the caller.
                        [14, 22, f"{alice},{alice_s}"],
                    ],
                },
            )
            assert resp.status_code == 200, resp.text
        blame = _run(lambda: ws.blame("/doc.md"))

    assert all(b["actor"]["id"] == bob for b in blame), blame


# A malformed span map is refused with an explanation, not stored as blame nobody
# can render.
def test_a_bad_span_map_is_refused():
    app, ws, _alice, _bob = _app()
    with TestClient(app) as tc:
        resp = tc.post(
            "/coedit-tree-checkpoint/doc.md?token=alice-token",
            json={"body": "abcd", "spans": [[0, 3, "x"], [2, 4, "y"]]},
        )
    assert resp.status_code == 400, resp.text
    assert "non-overlapping" in resp.text
    with pytest.raises(FileNotFoundError):
        _run(lambda: ws.read("/doc.md"))


def test_a_socketless_tree_checkpoint_leaves_no_live_marker():
    """A "Save" with no editor attached must not mark the path live for good.

    The route's no-room fallback used to `open_coedit_tree`, which claims the
    path. The matching clear lives in `leave()`, on the socket disconnect path a
    socket-less checkpoint never reaches -- so every such save leaked a marker
    telling readers (and the git export) the file may lag an editor that is not
    there, and `live_paths` grew without bound.
    """
    app, ws, (alice, alice_s), _ = _app()
    a_ctx = origofs.WriteCtx.session(alice, alice_s)

    client = origofs.CoeditTreeDoc("content")
    _run(lambda: client.append_text(a_ctx, "p", "drafted offline"))

    with TestClient(app) as tc:
        resp = tc.post(
            "/coedit-tree-checkpoint/solo.md?token=alice-token",
            json={"body": "drafted offline\n", "spans": [], "root": "content"},
        )
        assert resp.status_code == 200, resp.text

    assert _run(lambda: ws.read("/solo.md")) == b"drafted offline\n"
    assert _run(lambda: ws.live_doc("/solo.md")) is None
    assert _run(lambda: ws.live_paths()) == []


# --- tree-shaped proposals (issues #75 §3.2, #92) ---------------------------


@_sync
async def test_a_propose_only_agent_can_propose_against_a_tree_document():
    """The gap this closes: on the shape a rich-text editor uses, a propose-only
    agent could not reach the review queue at all. Its options were a byte
    suggestion — whose base goes stale on every keystroke elsewhere in the file
    and whose acceptance discards concurrent work — or nothing."""
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )
    owner = await ws.create_human("owner", None)
    agent = await ws.create_agent("agent", "opus", owner)
    octx, actx = origofs.WriteCtx.actor(owner), origofs.WriteCtx.actor(agent)
    await ws.grant(owner, "/", "read+write", None)
    await ws.grant(agent, "/", "read+propose", owner)
    await ws.set_acl_default_deny(True)

    doc = await ws.open_coedit_tree(octx, "/doc.md")
    doc.append_text(octx, "p", "hello\n")
    await ws.checkpoint_coedit_tree(octx, "/doc.md", doc, b"hello\n", [])
    await ws.end_coedit("/doc.md")

    # The write-shaped doors are shut…
    with pytest.raises(PermissionError):
        await ws.open_coedit_tree(actx, "/doc.md")
    # …and the propose-shaped one is open.
    replica = await ws.load_coedit_tree_to_propose(actx, "/doc.md")
    replica.append_text(actx, "p", "proposed\n")
    sid = await ws.suggest_coedit_tree(actx, "/doc.md", replica, "add a line")

    row = await ws.get_suggestion(sid)
    assert row["kind"] == "crdt-tree" and row["actor_id"] == agent
    assert bytes(await ws.read("/doc.md")) == b"hello\n", "a proposal lands nothing"

    # The reviewer sees the effect of the merge, not the opaque blobs.
    assert "proposed" in await ws.suggestion_diff(sid)

    # origofs cannot serialize a tree, so the ordinary accept refuses and names
    # the call that works.
    with pytest.raises(ValueError, match="accept_coedit_tree_suggestion"):
        await ws.accept_suggestion(sid, octx)

    # The host merges, serializes, and lands it — attributed to the author.
    merged = await ws.merge_coedit_tree_suggestion(sid)
    assert "proposed" in await merged.plain_text()
    await ws.accept_coedit_tree_suggestion(
        octx, sid, merged, b"hello\nproposed\n", []
    )
    assert bytes(await ws.read("/doc.md")) == b"hello\nproposed\n"
    assert (await ws.get_suggestion(sid))["status"] == "accepted"
    assert any(r["actor"]["id"] == agent for r in await ws.blame("/doc.md"))


@_sync
async def test_the_sidecar_reports_its_root_so_a_reviewer_need_not_know_it():
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )
    owner = await ws.create_human("owner", None)
    octx = origofs.WriteCtx.actor(owner)
    doc = await ws.open_coedit_tree(octx, "/doc.md", "prose")
    doc.append_text(octx, "p", "hi\n")
    await ws.checkpoint_coedit_tree(octx, "/doc.md", doc, b"hi\n", [])

    assert await ws.coedit_tree_root("/doc.md") == "prose"
    assert await ws.coedit_tree_root("/nothing.md") is None
