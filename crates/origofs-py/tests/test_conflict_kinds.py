"""`ConflictError` covered conditions that demand opposite recoveries (#159).

Two of them, distinguishable only by matching the message string — which then
breaks on any rewording — and the class docstring documented only one:

* a **stale suggestion base** on ``accept_suggestion``: re-diff and re-suggest;
* a **foreign write** since a co-edit document last agreed with the file: reseed
  the document and checkpoint again.

A host that ran the first recovery for the second condition would re-propose
against a document it should have reseeded. Both are now subclasses, so
``except ConflictError`` and the router's 409 mapping are unchanged and a caller
that needs to *act* can branch on the type.
"""
import asyncio
import functools
import os
import tempfile

import pytest

import origofs


def asyncio_test(fn):
    @functools.wraps(fn)
    def wrapper(*a, **kw):
        return asyncio.run(fn(*a, **kw))

    return wrapper


async def _ws():
    d = tempfile.mkdtemp()
    return await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )


def test_the_subclasses_are_conflict_errors():
    """Existing `except ConflictError` handlers must keep catching both."""
    for cls in (origofs.StaleBaseError, origofs.ForeignWriteError):
        assert issubclass(cls, origofs.ConflictError)
        assert issubclass(cls, origofs.OrigoFSError)
    assert origofs.StaleBaseError is not origofs.ForeignWriteError


@asyncio_test
async def test_a_stale_suggestion_base_raises_stale_base_error():
    ws = await _ws()
    agent = await ws.create_agent("agent", "opus", None)
    human = await ws.create_human("h", None)
    hctx = origofs.WriteCtx.actor(human)

    await ws.write_as(hctx, "/n.md", b"base\n")
    sid = await ws.suggest(origofs.WriteCtx.actor(agent), "/n.md", b"proposed\n", None)
    await ws.write_as(hctx, "/n.md", b"moved on\n")

    with pytest.raises(origofs.StaleBaseError):
        await ws.accept_suggestion(sid, hctx)
    # ...and the recovery it asks for is the one the row's state implies.
    assert (await ws.get_suggestion(sid))["status"] == "superseded"
    assert await ws.read("/n.md") == b"moved on\n"

    # `StaleBaseError` is still a `ConflictError`, so nothing that caught the old
    # class stops catching this.
    await ws.write_as(hctx, "/n.md", b"base\n")
    sid = await ws.suggest(origofs.WriteCtx.actor(agent), "/n.md", b"again\n", None)
    await ws.write_as(hctx, "/n.md", b"and again\n")
    with pytest.raises(origofs.ConflictError):
        await ws.accept_suggestion(sid, hctx)


@asyncio_test
async def test_a_foreign_write_raises_foreign_write_error():
    ws = await _ws()
    a = origofs.WriteCtx.actor(await ws.create_human("a", None))
    b = origofs.WriteCtx.actor(await ws.create_human("b", None))

    await ws.write_as(a, "/t.md", b"seed\n")
    doc = await ws.open_coedit_tree(a, "/t.md", "content")
    await doc.seeded_from(b"seed\n")
    await ws.checkpoint_coedit_tree(a, "/t.md", doc, b"seed\nmine\n", [])

    await ws.write_as(b, "/t.md", b"b was here\n")
    with pytest.raises(origofs.ForeignWriteError):
        await ws.checkpoint_coedit_tree(a, "/t.md", doc, b"seed\nmine\nmore\n", [])
    assert await ws.read("/t.md") == b"b was here\n"


@asyncio_test
async def test_accept_returns_the_address_that_landed():
    """`accept_suggestion` returned `None`, so a caller could not confirm what
    landed without re-reading (#163)."""
    ws = await _ws()
    agent = origofs.WriteCtx.actor(await ws.create_agent("agent", "opus", None))
    human = origofs.WriteCtx.actor(await ws.create_human("h", None))

    await ws.write_as(human, "/n.md", b"base\n")
    sid = await ws.suggest(agent, "/n.md", b"proposed\n", None)
    landed = await ws.accept_suggestion(sid, human)

    assert landed is not None
    # The file's *address*, so it matches `stat` — the value a host reconciles
    # against — and not `content_hash(body)`, which is a different hash entirely.
    assert landed == (await ws.stat("/n.md"))["content"]
    assert landed != origofs.content_hash(b"proposed\n")

    # A proposed deletion has no file left to address, and says so.
    sid = await ws.suggest_delete(agent, "/n.md", None)
    assert await ws.accept_suggestion(sid, human) is None


@asyncio_test
async def test_suggestion_content_is_text_as_the_stub_now_says():
    """The stub declared `bytes` and the extension returns `str` (#163), so
    anything typed against it was wrong at runtime and `.decode()` raised."""
    ws = await _ws()
    agent = origofs.WriteCtx.actor(await ws.create_agent("agent", "opus", None))
    human = origofs.WriteCtx.actor(await ws.create_human("h", None))

    await ws.write_as(human, "/n.md", b"hello world\n")
    sid = await ws.suggest(agent, "/n.md", b"hello brave world\n", None)
    content = await ws.suggestion_content(sid)
    assert isinstance(content["base"], str)
    assert isinstance(content["proposed"], str)

    # And the staleness check a caller would otherwise reach for the bodies to
    # answer: the recorded base against the file's current address.
    assert (await ws.get_suggestion(sid))["base_hash"] == (await ws.stat("/n.md"))[
        "content"
    ]
