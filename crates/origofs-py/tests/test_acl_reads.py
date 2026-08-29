"""Read enforcement through the Python bindings and the FastAPI router (#124).

The engine has checked reads since the ACL cache landed, but until now no
surface called the attributed forms — so `acl_enforce_reads` changed nothing for
anyone reaching the workspace over HTTP. These pin the two halves that matter to
a Python caller:

* the bindings expose the `_as` reads, and they behave like their Rust originals;
* the router runs reads *as* the actor its `reader` dependency resolves, and
  refuses an anonymous read once the workspace enforces.
"""
import os
import tempfile

import pytest
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

import origofs


async def _fixture():
    """`owner` reads everything, `bob` reads `/proj`. Two commits, so `diff`
    has two real ends."""
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )
    owner = await ws.create_human("owner", None)
    bob = await ws.create_agent("bob", "opus", None)
    octx = origofs.WriteCtx.actor(owner)

    await ws.grant(owner, "/", "read+write", None)
    await ws.grant(bob, "/proj", "read", None)

    await ws.mkdir_as(octx, "/proj")
    await ws.write_as(octx, "/proj/open.md", b"shared\n")
    await ws.write_as(octx, "/secret.md", b"private\n")
    await ws.commit_as(octx, "owner", "base")
    await ws.write_as(octx, "/secret.md", b"private v2\n")
    await ws.commit_as(octx, "owner", "v2")
    return ws, owner, bob


# --- the bindings ------------------------------------------------------------


@pytest.mark.asyncio
async def test_attributed_reads_are_open_until_the_workspace_opts_in():
    ws, _owner, bob = await _fixture()
    ctx = origofs.WriteCtx.actor(bob)
    await ws.set_acl_default_deny(True)

    assert await ws.acl_enforce_reads() is False
    assert bytes(await ws.read_as(ctx, "/secret.md")) == b"private v2\n"
    assert await ws.stat_as(ctx, "/secret.md")
    assert await ws.blame_as(ctx, "/secret.md") is not None


@pytest.mark.asyncio
async def test_every_attributed_read_is_gated_once_enforced():
    ws, _owner, bob = await _fixture()
    ctx = origofs.WriteCtx.actor(bob)
    await ws.set_acl_default_deny(True)
    await ws.set_acl_enforce_reads(True)

    for call in (
        ws.read_as(ctx, "/secret.md"),
        ws.read_range_as(ctx, "/secret.md", 0, 4),
        ws.stat_as(ctx, "/secret.md"),
        ws.blame_as(ctx, "/secret.md"),
        ws.ls_as(ctx, "/"),
        ws.diff_file_as(ctx, "HEAD", "HEAD", "/secret.md"),
    ):
        with pytest.raises(PermissionError):
            await call

    # …and the subtree bob does hold still answers.
    assert bytes(await ws.read_as(ctx, "/proj/open.md")) == b"shared\n"


@pytest.mark.asyncio
async def test_a_listing_hides_exactly_what_a_stat_refuses():
    # The pair property. A listing that promises more than a stat delivers is
    # useless; one that hides what a stat serves is an existence oracle. Both
    # ask the same resolver the same question, so neither can drift.
    ws, _owner, bob = await _fixture()
    ctx = origofs.WriteCtx.actor(bob)
    await ws.set_acl_default_deny(True)
    await ws.set_acl_enforce_reads(True)
    await ws.grant(bob, "/", "read", None)
    await ws.grant(bob, "/secret.md", "none", None)

    listed = {e["name"] for e in await ws.ls_as(ctx, "/")}
    assert "secret.md" not in listed
    assert "proj" in listed
    with pytest.raises(PermissionError):
        await ws.stat_as(ctx, "/secret.md")


@pytest.mark.asyncio
async def test_the_collection_reads_filter_rather_than_refuse():
    ws, owner, bob = await _fixture()
    octx, bctx = origofs.WriteCtx.actor(owner), origofs.WriteCtx.actor(bob)
    sid = await ws.suggest(octx, "/secret.md", b"proposed\n", None)
    await ws.set_acl_default_deny(True)
    await ws.set_acl_enforce_reads(True)

    log = await ws.log()
    head, base = log[0]["hash"], log[1]["hash"]
    assert await ws.diff_as(bctx, base, head) == []
    assert [d["path"] for d in await ws.diff_as(octx, base, head)] == ["/secret.md"]

    assert await ws.list_suggestions_as(bctx, None, None) == []
    assert len(await ws.list_suggestions_as(octx, None, None)) == 1

    # Not found, not denied: a suggestion id is a guessable global handle, so a
    # refusal would confirm that one exists at it.
    assert await ws.get_suggestion_as(bctx, sid) is None
    assert await ws.get_suggestion_as(octx, sid) is not None
    with pytest.raises(FileNotFoundError):
        await ws.suggestion_diff_as(bctx, sid)

    assert await ws.live_doc_as(bctx, "/secret.md") is None
    assert await ws.live_paths_as(bctx) == []


# --- the FastAPI router ------------------------------------------------------


def _app(ws, owner, bob, *, reader_returns_ctx=True):
    """A router whose `reader` resolves `X-Actor` to a context (or to None,
    the older gate-only shape)."""

    async def authn(x_actor: str = Header(default="")):
        if not x_actor:
            raise HTTPException(status_code=401, detail="no actor")
        return origofs.WriteCtx.actor(int(x_actor))

    async def reader(x_actor: str = Header(default="")):
        if not x_actor:
            return None
        return origofs.WriteCtx.actor(int(x_actor)) if reader_returns_ctx else None

    from origofs.fastapi import build_router

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn, reader=reader))
    return TestClient(app)


@pytest.mark.asyncio
async def test_router_reads_run_as_the_reader_context():
    ws, owner, bob = await _fixture()
    await ws.set_acl_default_deny(True)
    await ws.set_acl_enforce_reads(True)
    c = _app(ws, owner, bob)

    assert c.get("/files/secret.md", headers={"X-Actor": str(bob)}).status_code == 403
    assert c.get("/files/secret.md", headers={"X-Actor": str(owner)}).status_code == 200
    assert (
        c.get("/files/proj/open.md", headers={"X-Actor": str(bob)}).status_code == 200
    )


@pytest.mark.asyncio
async def test_router_refuses_an_anonymous_read_once_enforced():
    # The hole this closes. Reads are open by default, so a read route cannot
    # demand a credential unconditionally — it has to demand one exactly when
    # the workspace has something to check it against.
    ws, _owner, _bob = await _fixture()
    c = _app(ws, _owner, _bob)
    assert c.get("/files/secret.md").status_code == 200

    await ws.set_acl_enforce_reads(True)
    assert c.get("/files/secret.md").status_code == 401


@pytest.mark.asyncio
async def test_a_gate_only_reader_still_works():
    # Backwards compatibility: a `reader` returning None is the documented
    # older shape, and it keeps behaving as a gate whose value is ignored.
    ws, owner, bob = await _fixture()
    c = _app(ws, owner, bob, reader_returns_ctx=False)
    r = c.get("/files/secret.md", headers={"X-Actor": str(bob)})
    assert r.status_code == 200 and r.content == b"private v2\n"
