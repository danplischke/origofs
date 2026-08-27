"""End-to-end test of the origofs Python bindings.

Build + run:
    maturin develop            # from crates/origofs-py, in a venv
    pytest tests/              # or: python tests/test_bindings.py
"""
import asyncio
import os
import tempfile

import origofs


async def _exercise():
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(os.path.join(d, "meta.db"), os.path.join(d, "cas"))

    # actors + attributed write (the "inject user/agent" path)
    human = await ws.create_human("dan", "dan@x")
    agent = await ws.create_agent("claude", "opus", human)
    sess = await ws.create_session(agent, "fastapi")
    ctx = origofs.WriteCtx.session(agent, sess)
    await ws.write_as(ctx, "/notes.txt", b"line one\nline two\n")
    assert bytes(await ws.read("/notes.txt")) == b"line one\nline two\n"

    # blame credits the agent
    bl = await ws.blame("/notes.txt")
    assert bl[0]["actor"]["id"] == agent and bl[0]["actor"]["kind"] == "agent", bl

    # ls / stat -> plain dicts
    entries = await ws.ls("/")
    assert any(e["name"] == "notes.txt" for e in entries), entries
    st = await ws.stat("/notes.txt")
    assert st["kind"] == "file" and st["size"] == 18, st

    # versioning + branch diff
    await ws.commit("dan", "base")
    await ws.create_branch("feature")
    await ws.checkout("feature")
    await ws.write("/notes.txt", b"line one\nline TWO\n")
    await ws.write("/new.txt", b"added\n")

    # rename -- called with `from_=` (a keyword), not `from=`: `from` is a
    # Python keyword, so pyo3 exposing an argument literally named `from` would
    # make it impossible to ever pass by keyword (a SyntaxError at the call
    # site). Keyword-calling here pins the binding to the name the .pyi stub
    # actually promises, so a future rename back to `from` fails this test
    # immediately (TypeError: unexpected keyword argument) instead of silently
    # drifting from the stub. Same reason `diff`/`diff_file` below are called
    # with `from_=` too.
    await ws.rename(from_="/new.txt", to="/renamed.txt")
    assert bytes(await ws.read("/renamed.txt")) == b"added\n"

    await ws.commit("dan", "work")
    changes = {c["path"]: c["status"] for c in await ws.diff(from_="main", to="feature")}
    assert changes == {"/notes.txt": "modified", "/renamed.txt": "added"}, changes
    patch = await ws.diff_file(from_="main", to="feature", path="/notes.txt")
    assert "-line two" in patch and "+line TWO" in patch, patch

    # suggestion lifecycle
    await ws.checkout("main")
    sid = await ws.suggest(ctx, "/notes.txt", b"line one\nline 2!\n", "tweak")
    assert bytes(await ws.read("/notes.txt")) == b"line one\nline two\n"  # untouched
    pend = await ws.list_suggestions("pending")
    assert len(pend) == 1 and pend[0]["id"] == sid, pend
    assert "+line 2!" in await ws.suggestion_diff(sid)
    await ws.accept_suggestion(sid, origofs.WriteCtx.actor(human))
    assert bytes(await ws.read("/notes.txt")) == b"line one\nline 2!\n"

    # conflict path -> ConflictError
    sid2 = await ws.suggest(ctx, "/notes.txt", b"proposed\n")
    await ws.write("/notes.txt", b"moved on\n")
    try:
        await ws.accept_suggestion(sid2, origofs.WriteCtx.actor(human))
        raise AssertionError("expected ConflictError")
    except origofs.ConflictError:
        pass

    # feed + presence
    await ws.touch(agent, sess, "/notes.txt")
    assert any(p["session_id"] == sess for p in await ws.presence(60))
    assert len(await ws.watch(0)) > 0

    # errors map to Python builtins
    try:
        await ws.read("/nope")
        raise AssertionError("expected FileNotFoundError")
    except FileNotFoundError:
        pass


def test_bindings():
    asyncio.run(_exercise())


if __name__ == "__main__":
    asyncio.run(_exercise())
    print("OK  fuse_mountable() =", origofs.fuse_mountable())
