"""The Workspace surface that had no Python binding at all.

`README` claimed "Python is the same API with `await` on every call", but a large
part of the SDK was Rust-only. The gaps mattered in different ways:

* **Multi-workspace was unreachable.** `workspace`/`workspaces` had no binding, so
  a Python caller got exactly one workspace — the `default` one every `open_*`
  lands in — and the whole workspace layer of `docs/MULTI_TENANCY.md` was
  inaccessible from the surface most services are built on.
* **The write policy was unenforceable.** `set_write_policy` *was* bound, but only
  `write_as`/`write_or_propose` were — `remove`/`rename`/`mkdir_p`/`commit`/
  `checkout` existed solely in their unattributed forms, which are exempt from the
  policy by construction. So marking an agent propose-only looked like it worked
  and did nothing for deletes, renames, or commits, none of which carried blame.
* **A packed store could never be collected.** The packed constructors were bound;
  `gc`/`flush`/`repack` were not.
* **The one unrecoverable half could not be backed up.** No `backup_metadata`.
* **Branching was a one-way door.** `create_branch`/`checkout` bound, `merge` not.
* **`revert_session`** — the headline "undo just the agent's work" feature — existed
  only in the Rust SDK: no CLI subcommand, no HTTP route, no MCP tool, no binding.
"""
import asyncio
import functools
import os
import tempfile

import pytest

import origofs


def asyncio_test(fn):
    """Run an ``async def`` test body via ``asyncio.run``.

    The suite deliberately does not depend on pytest-asyncio — the other test
    files here wrap their bodies in ``asyncio.run`` by hand, so this keeps the
    convention while staying readable.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


async def workspace():
    """A fresh local workspace in a temp dir."""
    d = tempfile.mkdtemp()
    return await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )


# --- multi-workspace -------------------------------------------------------


@asyncio_test
async def test_workspaces_are_isolated_and_listable():
    ws = await workspace()
    await ws.write("/shared-name.txt", b"in default")

    alpha = await ws.workspace("alpha")
    beta = await ws.workspace("beta")

    # Same name, different workspace, different content.
    await alpha.write("/shared-name.txt", b"in alpha")
    await beta.write("/shared-name.txt", b"in beta")

    assert bytes(await ws.read("/shared-name.txt")) == b"in default"
    assert bytes(await alpha.read("/shared-name.txt")) == b"in alpha"
    assert bytes(await beta.read("/shared-name.txt")) == b"in beta"

    # A file in one is simply absent from another.
    await alpha.write("/only-in-alpha.txt", b"x")
    with pytest.raises(FileNotFoundError):
        await beta.read("/only-in-alpha.txt")

    names = await ws.workspaces()
    assert "default" in names and "alpha" in names and "beta" in names


@asyncio_test
async def test_reopening_a_workspace_by_name_finds_the_same_one():
    ws = await workspace()
    a1 = await ws.workspace("proj")
    await a1.write("/f.txt", b"persisted")
    a2 = await ws.workspace("proj")
    assert bytes(await a2.read("/f.txt")) == b"persisted"


@asyncio_test
async def test_an_invalid_workspace_name_is_rejected():
    ws = await workspace()
    for bad in ("", ".", "..", "a/b"):
        with pytest.raises(ValueError):
            await ws.workspace(bad)


# --- the write policy, end to end -----------------------------------------


@asyncio_test
async def test_propose_only_actor_cannot_delete_rename_or_commit():
    ws = await workspace()
    reviewer = await ws.create_human("reviewer", None)
    agent = await ws.create_agent("restricted", "opus", reviewer)
    sess = await ws.create_session(agent, "test")
    ctx = origofs.WriteCtx.session(agent, sess)

    await ws.write("/doomed.txt", b"original")
    await ws.write("/movable.txt", b"original")
    await ws.commit("setup", "base")
    await ws.create_branch("side")

    await ws.set_write_policy(agent, "propose")

    # A delete is *queued*, not applied — the propose-shaped equivalent exists.
    outcome = await ws.remove_or_propose(ctx, "/doomed.txt", "please delete")
    assert not outcome.wrote and outcome.suggestion_id is not None
    assert bytes(await ws.read("/doomed.txt")) == b"original"

    # These have no propose-shaped equivalent, so they are refused outright.
    with pytest.raises(PermissionError):
        await ws.rename_as(ctx, "/movable.txt", "/moved.txt")
    with pytest.raises(PermissionError):
        await ws.mkdir_as(ctx, "/newdir")
    with pytest.raises(PermissionError):
        await ws.commit_as(ctx, "restricted", "sneaky commit")
    with pytest.raises(PermissionError):
        await ws.create_branch_as(ctx, "sneaky")
    # The destructive one.
    with pytest.raises(PermissionError):
        await ws.checkout_as(ctx, "side")

    # Nothing landed.
    assert bytes(await ws.read("/movable.txt")) == b"original"
    with pytest.raises(FileNotFoundError):
        await ws.stat("/newdir")
    assert "sneaky" not in [b["name"] for b in await ws.branches()]


@asyncio_test
async def test_a_direct_actor_can_do_all_of_it():
    ws = await workspace()
    human = await ws.create_human("trusted", None)
    sess = await ws.create_session(human, "test")
    ctx = origofs.WriteCtx.session(human, sess)

    await ws.mkdir_as(ctx, "/dir")
    await ws.write_as(ctx, "/dir/a.txt", b"hello")
    await ws.rename_as(ctx, "/dir/a.txt", "/dir/b.txt")
    commit = await ws.commit_as(ctx, "trusted", "work")
    assert isinstance(commit, str) and len(commit) == 64

    await ws.create_branch_as(ctx, "feature")
    await ws.checkout_as(ctx, "feature")
    assert await ws.current_branch() == "feature"

    outcome = await ws.remove_or_propose(ctx, "/dir/b.txt", None)
    assert outcome.wrote and outcome.suggestion_id is None


@asyncio_test
async def test_ensure_may_write_gates_administrative_operations():
    ws = await workspace()
    human = await ws.create_human("admin", None)
    agent = await ws.create_agent("restricted", "opus", human)
    await ws.set_write_policy(agent, "propose")

    ok = origofs.WriteCtx.actor(human)
    denied = origofs.WriteCtx.actor(agent)

    await ws.ensure_may_write(ok, "register actors")  # no raise
    with pytest.raises(PermissionError):
        await ws.ensure_may_write(denied, "register actors")


# --- maintenance ----------------------------------------------------------


@asyncio_test
async def test_gc_reports_what_it_reclaimed():
    ws = await workspace()
    await ws.write("/churn.txt", b"x" * 200_000)
    await ws.write("/churn.txt", b"y" * 200_000)  # supersedes the first body
    await ws.remove("/churn.txt")

    stats = await ws.gc_with_grace(0)  # quiesced store: no age gate
    assert set(stats) == {
        "reachable",
        "deleted",
        "bytes_freed",
        "skipped_young",
        "skipped_undated",
    }
    assert stats["deleted"] > 0 and stats["bytes_freed"] > 0


@asyncio_test
async def test_a_grace_inside_the_unsafe_band_is_refused():
    ws = await workspace()
    # Between 0 and the dedup-refresh floor there is a window where content is
    # sweepable but was never refreshed — the exact race the age gate closes.
    with pytest.raises(ValueError):
        await ws.gc_with_grace(1)


@asyncio_test
async def test_a_packed_store_can_be_flushed_and_repacked():
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local_packed(
        os.path.join(d, "meta.db"), os.path.join(d, "data"), os.path.join(d, "index")
    )
    for i in range(20):
        await ws.write(f"/f{i}.txt", bytes([i]) * 4096)
    await ws.flush()
    for i in range(0, 20, 2):
        await ws.remove(f"/f{i}.txt")
    await ws.gc_with_grace(0)

    reclaimed = await ws.repack()
    assert isinstance(reclaimed, int)
    # Survivors are still readable through the rewritten packs.
    assert bytes(await ws.read("/f1.txt")) == bytes([1]) * 4096


@asyncio_test
async def test_metadata_can_be_backed_up():
    ws = await workspace()
    await ws.write("/a.txt", b"content")
    human = await ws.create_human("dan", None)
    sess = await ws.create_session(human, "test")
    await ws.write_as(origofs.WriteCtx.session(human, sess), "/b.txt", b"blamed\n")

    dest = os.path.join(tempfile.mkdtemp(), "backup.db")
    result = await ws.backup_metadata(dest)
    assert isinstance(result, str)
    assert os.path.exists(dest) and os.path.getsize(dest) > 0


@asyncio_test
async def test_readiness_reports_both_stores():
    ws = await workspace()
    r = await ws.ready()
    assert r["ready"] is True
    assert r["metadata"] is None and r["content"] is None


@asyncio_test
async def test_housekeeping_is_callable():
    ws = await workspace()
    human = await ws.create_human("dan", None)
    sess = await ws.create_session(human, "test")
    await ws.touch(human, sess, "/a.txt")

    # Nothing is stale yet, so nothing is reaped; a large grace is a no-op.
    assert await ws.reap_presence(3600) == 0
    assert isinstance(await ws.supersede_stale_suggestions("/a.txt"), int)


# --- versioning -----------------------------------------------------------


@asyncio_test
async def test_branches_can_be_merged_not_just_created():
    ws = await workspace()
    await ws.write("/base.txt", b"base\n")
    await ws.commit("dan", "base")

    await ws.create_branch("feature")
    await ws.checkout("feature")
    await ws.write("/feature.txt", b"from the branch\n")
    await ws.commit("dan", "feature work")

    await ws.checkout("main")
    assert (await ws.merge_branch("feature", "dan", "merge feature"))["outcome"] in (
        "fast_forward",
        "merged",
    )
    assert bytes(await ws.read("/feature.txt")) == b"from the branch\n"
    assert await ws.conflicts() == []


@asyncio_test
async def test_merging_an_already_merged_branch_is_a_no_op():
    ws = await workspace()
    await ws.write("/a.txt", b"a")
    await ws.commit("dan", "one")
    await ws.create_branch("stale")
    out = await ws.merge_branch("stale", "dan", None)
    assert out["outcome"] == "already_up_to_date"


@asyncio_test
async def test_versioning_mode_round_trips():
    ws = await workspace()
    assert await ws.versioning_mode() == "native"
    await ws.set_versioning_mode("off")
    assert await ws.versioning_mode() == "off"
    with pytest.raises(ValueError):
        await ws.set_versioning_mode("nonsense")


# --- locks ----------------------------------------------------------------


@asyncio_test
async def test_locks_are_exclusive():
    ws = await workspace()
    await ws.write("/big.bin", b"binary")
    assert await ws.lock("/big.bin", "alice") is True
    assert await ws.lock("/big.bin", "bob") is False

    held = await ws.locks()
    assert any(l["path"] == "/big.bin" and l["owner"] == "alice" for l in held)

    assert await ws.unlock("/big.bin", "bob") is False
    assert await ws.unlock("/big.bin", "alice") is True
    assert await ws.lock("/big.bin", "bob") is True


# --- attribution ----------------------------------------------------------


@asyncio_test
async def test_revert_session_removes_only_that_actors_lines():
    ws = await workspace()
    human = await ws.create_human("dan", None)
    agent = await ws.create_agent("claude", "opus", human)
    h_sess = await ws.create_session(human, "editor")
    a_sess = await ws.create_session(agent, "mcp")

    await ws.write_as(
        origofs.WriteCtx.session(human, h_sess), "/doc.md", b"human line\n"
    )
    await ws.write_as(
        origofs.WriteCtx.session(agent, a_sess),
        "/doc.md",
        b"human line\nagent line\n",
    )
    assert bytes(await ws.read("/doc.md")) == b"human line\nagent line\n"

    changed = await ws.revert_session(agent, a_sess)
    assert changed == ["/doc.md"]
    body = bytes(await ws.read("/doc.md"))
    assert b"human line" in body, "the human's line was collateral damage"
    assert b"agent line" not in body, "the agent's line survived the revert"


@asyncio_test
async def test_revert_session_can_be_scoped_to_one_tenants_subtree():
    # Issue #94: one workspace, tenant-scoped paths. An "undo this agent's work"
    # button lives in one tenant's UI, but the session may have written anywhere.
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)
    sess = await ws.create_session(agent, "mcp")
    ctx = origofs.WriteCtx.session(agent, sess)

    for path in ("/tenant-a/doc.md", "/tenant-b/doc.md", "/tenant-abc/doc.md"):
        await ws.mkdir_p(path.rsplit("/", 1)[0])
        await ws.write_as(ctx, path, b"agent line\n")

    changed = await ws.revert_session(agent, sess, path_prefix="/tenant-a")
    assert changed == ["/tenant-a/doc.md"]

    assert bytes(await ws.read("/tenant-a/doc.md")) == b""
    # Neither the other tenant nor the one whose name merely starts the same way.
    assert bytes(await ws.read("/tenant-b/doc.md")) == b"agent line\n"
    assert bytes(await ws.read("/tenant-abc/doc.md")) == b"agent line\n"

    with pytest.raises(ValueError):
        await ws.revert_session(agent, sess, path_prefix="tenant-a")


@asyncio_test
async def test_edit_ops_are_the_ground_truth_behind_blame():
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)
    sess = await ws.create_session(agent, "mcp")
    ctx = origofs.WriteCtx.session(agent, sess)
    await ws.write_as(ctx, "/a.txt", b"one\n")
    await ws.write_as(ctx, "/b.txt", b"two\n")

    ops = await ws.edit_ops(agent, sess)
    paths = {o["path"] for o in ops}
    assert {"/a.txt", "/b.txt"} <= paths
    for o in ops:
        assert o["actor_id"] == agent and o["session_id"] == sess
        assert isinstance(o["byte_start"], int) and isinstance(o["ts"], int)

    # Filtering by actor alone works too.
    assert len(await ws.edit_ops(agent, None)) >= len(ops)


# --- symlinks -------------------------------------------------------------


@asyncio_test
async def test_symlinks_round_trip():
    ws = await workspace()
    await ws.write("/target.txt", b"pointed at")
    await ws.symlink("/target.txt", "/link.txt")
    assert await ws.readlink("/link.txt") == "/target.txt"

    human = await ws.create_human("dan", None)
    sess = await ws.create_session(human, "test")
    ctx = origofs.WriteCtx.session(human, sess)
    await ws.symlink_as(ctx, "/target.txt", "/link2.txt")
    assert await ws.readlink("/link2.txt") == "/target.txt"


# --- encryption at rest ---------------------------------------------------


@asyncio_test
async def test_encryption_at_rest_round_trips_and_rejects_a_wrong_key():
    d = tempfile.mkdtemp()
    db, cas = os.path.join(d, "meta.db"), os.path.join(d, "cas")

    ws = await origofs.Workspace.open_local_encrypted(db, cas, "correct horse")
    await ws.write("/secret.txt", b"plaintext never on disk")
    assert bytes(await ws.read("/secret.txt")) == b"plaintext never on disk"
    del ws

    # Reopening with the same passphrase works.
    again = await origofs.Workspace.open_local_encrypted(db, cas, "correct horse")
    assert bytes(await again.read("/secret.txt")) == b"plaintext never on disk"
    del again

    # A wrong one fails loudly rather than returning garbage.
    wrong = await origofs.Workspace.open_local_encrypted(db, cas, "wrong passphrase")
    with pytest.raises(Exception):
        await wrong.read("/secret.txt")


# --- permissions -----------------------------------------------------------
# `chmod`/`chown` and the `uid`/`gid` inode fields (migration V17, issues #121
# and #122). Bound here rather than left Rust-only, which is the failure this
# whole file exists to catch.


@asyncio_test
async def test_chmod_changes_the_permission_bits_and_keeps_the_file_type():
    ws = await workspace()
    await ws.write("/build.sh", b"#!/bin/sh\n")

    S_IFREG = 0o100000
    assert (await ws.stat("/build.sh"))["mode"] == S_IFREG | 0o644

    after = await ws.chmod("/build.sh", 0o755)

    # The whole mode word, not just the low bits: an implementation that assigned
    # the mode outright would drop S_IFREG and change the file's kind.
    assert after["mode"] == S_IFREG | 0o755
    assert (await ws.stat("/build.sh"))["mode"] == S_IFREG | 0o755

    # And the content is untouched.
    assert bytes(await ws.read("/build.sh")) == b"#!/bin/sh\n"


@asyncio_test
async def test_chown_sets_ownership_and_leaves_an_omitted_half_alone():
    ws = await workspace()
    await ws.write("/f", b"x")

    # V17 defaults: new inodes are root-owned, so the migration is
    # behaviour-preserving for stores that predate it.
    st = await ws.stat("/f")
    assert (st["uid"], st["gid"]) == (0, 0)

    after = await ws.chown("/f", uid=1000, gid=100)
    assert (after["uid"], after["gid"]) == (1000, 100)

    # `chown :group` and `chown user` are both legal; the omitted half must not be
    # silently reassigned.
    after = await ws.chown("/f", gid=42)
    assert (after["uid"], after["gid"]) == (1000, 42)

    after = await ws.chown("/f", uid=7)
    assert (after["uid"], after["gid"]) == (7, 42)


@asyncio_test
async def test_chmod_and_chown_on_a_missing_path_raise():
    ws = await workspace()
    # Never accepted-and-ignored — that silent success is exactly what #121 was.
    with pytest.raises(FileNotFoundError):
        await ws.chmod("/nope", 0o600)
    with pytest.raises(FileNotFoundError):
        await ws.chown("/nope", uid=1)


@asyncio_test
async def test_a_propose_only_actor_cannot_chmod_or_chown():
    """Metadata is a mutation too.

    ``chmod 000`` on a file an agent may not write is the same denial of service
    one call further along — the shape of #78. There is no propose-shaped
    equivalent of a ``chmod``, so both refuse outright rather than queueing.
    """
    ws = await workspace()
    await ws.write("/f", b"x")

    reviewer = await ws.create_human("reviewer", None)
    agent = await ws.create_agent("claude", "opus", reviewer)
    sess = await ws.create_session(agent, "test")
    ctx = origofs.WriteCtx.session(agent, sess)
    await ws.set_write_policy(agent, "propose")

    with pytest.raises(PermissionError):
        await ws.chmod_as(ctx, "/f", 0o000)
    with pytest.raises(PermissionError):
        await ws.chown_as(ctx, "/f", uid=0)

    # Unchanged by the refused calls.
    st = await ws.stat("/f")
    assert st["mode"] & 0o7777 == 0o644
    assert (st["uid"], st["gid"]) == (0, 0)

    # A direct actor may do both, and the change lands.
    human = await ws.create_human("dan", None)
    hsess = await ws.create_session(human, "test")
    hctx = origofs.WriteCtx.session(human, hsess)
    assert (await ws.chmod_as(hctx, "/f", 0o600))["mode"] & 0o7777 == 0o600
    assert (await ws.chown_as(hctx, "/f", uid=1000))["uid"] == 1000


# --- path-scoped access grants (#123) --------------------------------------


@asyncio_test
async def test_grants_narrow_an_actor_to_a_subtree():
    ws = await workspace()
    await ws.mkdir_p("/src/parser")
    await ws.mkdir_p("/docs")
    await ws.write("/docs/readme.md", b"theirs")
    await ws.write("/src/parser/lex.rs", b"theirs")

    agent = await ws.create_agent("claude", "opus", None)
    sess = await ws.create_session(agent, "test")
    ctx = origofs.WriteCtx.session(agent, sess)

    # Read-only everywhere, writable in one subtree — how deny-by-default is
    # expressed without a separate mode.
    await ws.grant(agent, "/", "read")
    await ws.grant(agent, "/src/parser", "read,write")

    assert await ws.effective_perms(agent, "/docs/readme.md") == "read"
    assert await ws.effective_perms(agent, "/src/parser/lex.rs") == "read,write"

    await ws.write_as(ctx, "/src/parser/lex.rs", b"mine")
    assert bytes(await ws.read("/src/parser/lex.rs")) == b"mine"

    with pytest.raises(PermissionError):
        await ws.write_as(ctx, "/docs/readme.md", b"mine")
    assert bytes(await ws.read("/docs/readme.md")) == b"theirs"


@asyncio_test
async def test_a_grant_does_not_cover_a_sibling_sharing_its_string_prefix():
    """`/tenant-a` must not cover `/tenant-abc` — the classic prefix-scoping bug."""
    ws = await workspace()
    await ws.mkdir_p("/tenant-a")
    await ws.mkdir_p("/tenant-abc")
    await ws.write("/tenant-abc/secrets", b"theirs")

    agent = await ws.create_agent("claude", "opus", None)
    sess = await ws.create_session(agent, "test")
    ctx = origofs.WriteCtx.session(agent, sess)

    await ws.grant(agent, "/", "read")
    await ws.grant(agent, "/tenant-a", "read,write")

    assert await ws.effective_perms(agent, "/tenant-abc/secrets") == "read"
    with pytest.raises(PermissionError):
        await ws.write_as(ctx, "/tenant-abc/secrets", b"mine")


@asyncio_test
async def test_grants_list_longest_first_and_revoke_reports_reality():
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)

    await ws.grant(agent, "/", "read")
    await ws.grant(agent, "/src", "read,write")
    listed = await ws.grants(agent)
    assert [g["prefix"] for g in listed] == ["/src", "/"]
    assert listed[0]["perms"] == "read,write"

    # A revoke against a prefix with no grant must not look like it closed access.
    assert await ws.revoke(agent, "/srcs") is False
    assert await ws.revoke(agent, "/src") is True
    assert [g["prefix"] for g in await ws.grants(agent)] == ["/"]
