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

Then it happened again, which is what issue #120 is about. `origofs-py` is a
hand-written pyo3 surface with its own method list, so an engine feature is only
reachable from Python once somebody writes the binding — and six landed in Rust
without one: trash (#115), usage/quotas/`statfs` (#116, #119), `origofs info` /
`bench` (#118), ownership and chmod/chown (#121, #122), hard links and xattrs
(#119), and the path-scoped write ACLs (#123). The resolution is not a bigger
backlog but a rule: **a pyo3 method and its entry here are part of an engine
feature's definition of done**, not follow-up work. The second half of this file
is that round; `test_the_recent_engine_surface_is_bound` is the cheap guard that
makes the next omission fail rather than go unnoticed.
"""
import asyncio
import functools
import os
import pathlib
import re
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


# ===========================================================================
# The second round (issue #120): engine features that landed in Rust after the
# gaps above were closed, and needed a binding of their own.
# ===========================================================================


# --- the guard ------------------------------------------------------------
#
# This used to be a hand-written set of names, which could only catch the
# *regression* of a binding somebody had already thought of. The failure #120
# describes is the opposite one — an engine method nobody noticed — and an
# allowlist is structurally unable to see it. So the guard reads both surfaces and
# diffs them, and every deliberately-unbound name has to say why.

SDK = pathlib.Path(__file__).resolve().parents[2] / "origofs-sdk" / "src" / "lib.rs"
PYO3 = pathlib.Path(__file__).resolve().parents[1] / "src" / "lib.rs"
STUB = pathlib.Path(__file__).resolve().parents[1] / "python" / "origofs" / "__init__.pyi"

# `Workspace` methods that are Rust-only on purpose, each with the reason. A name
# lands here when binding it would be meaningless (a Rust-idiom constructor), when
# Python reaches the same thing under a better name, or -- one case -- when the
# unauthorized form must not be reachable from a surface.
#
# Adding an entry is the deliberate act the check exists to force. Adding one to
# avoid writing a binding is the thing it exists to prevent, so the reason is
# prose and is expected to argue.
RUST_ONLY = {
    # -- renamed, because the Python spelling says what it does --
    "get_actor": "bound as `actor`",
    "list_branches": "bound as `branches`",
    "open_pg_local": "bound as `open_pg`, whose signature already takes a cas_dir",
    "read_stream": "bound as `read_to_path`; Python has no BoxStream to return",
    "read_to_writer": "bound as `read_to_path`",
    "read_range_stream": "bound as `read_range`, which is already chunk-scoped",
    "write_reader": "bound as `write_path`",
    "write_reader_as": "bound as `write_path_as`",
    "is_ready": "bound as `ready`, which returns the full report rather than a bool",
    # -- Rust idiom with no Python meaning --
    "new": "constructor over Arc<dyn ...> backends a Python caller cannot build",
    "open": "same; the `open_*` constructors are the Python surface",
    "open_encrypted": "same; `open_local_encrypted` and the `_encrypted` forms are bound",
    "open_pg_store": (
        "takes an already-connected Arc<PostgresMetadataStore>, which exists so a "
        "Rust caller assembling its own content stack (the CLI's --config) does not "
        "lose the pg handle that `subscribe` and the co-edit relay need. A Python "
        "caller has no such store to hand over -- `open_pg` is its shape."
    ),
    "encrypted_content": (
        "returns an Arc<dyn ContentStore> for a Rust caller to pair with a metadata "
        "store itself; Python gets the assembled stacks through the `open_*` "
        "constructors"
    ),
    "cached_content": "same",
    "fs": "returns the `Fs` engine handle; the bindings call through it themselves",
    "shutdown_signal": "a Rust shutdown future; `serve_nfs` owns its own lifecycle",
    "latest_schema_version": "a constant, reported inside `migrate`/`schema_version`",
    "open_for_range": "returns a Manifest to stream from; `read_range` is the Python shape",
    "max_bytes": "a `CacheConfig` builder method, exposed as a constructor kwarg",
    "min_free_bytes": "same",
    # -- deliberately not exposed --
    "dump": (
        "only `dump_as` is bound. A dump is whole-store -- every workspace, every "
        "actor's auth_subject, every ACL grant, all blame -- and nothing about it "
        "is path-scoped, so binding the unauthorized form would hand any "
        "authenticated caller of a Python service the entire store."
    ),
}


def _sdk_methods() -> set:
    """Public `Workspace` methods in the Rust SDK."""
    return set(re.findall(r"^\s*pub (?:async )?fn (\w+)", SDK.read_text(), re.M))


def _impl_body(text: str, header: str) -> str:
    """The body of a brace-balanced Rust `impl` block."""
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith(header))
    depth = 0
    out = []
    for i, line in enumerate(lines[start:], start):
        depth += line.count("{") - line.count("}")
        out.append(line)
        if depth == 0 and i > start:
            break
    return "\n".join(out)


def _pyo3_methods() -> set:
    body = _impl_body(PYO3.read_text(), "impl Workspace {")
    return set(re.findall(r"^\s*(?:pub )?fn (\w+)", body, re.M))


def _stub_methods() -> set:
    # Stop at the next *top-level* statement: module-level functions follow the
    # class, and `\s+` would happily eat the newline in front of one.
    m = re.search(r"^class Workspace.*?(?=^class |^def |\Z)", STUB.read_text(), re.M | re.S)
    assert m, "no `class Workspace` in the stub"
    return set(re.findall(r"^[ \t]+(?:async )?def (\w+)", m.group(0), re.M))


def test_every_sdk_method_is_bound_or_has_a_reason():
    """The engine surface and the Python surface, diffed.

    An engine feature is done when a Python caller can reach it. This is what
    makes that a fact rather than an intention: a new `Workspace` method in the
    Rust SDK fails here until it is either bound or listed in `RUST_ONLY` with a
    reason somebody had to write down.
    """
    unbound = _sdk_methods() - _pyo3_methods() - set(RUST_ONLY)
    assert not unbound, (
        f"these SDK methods have no Python binding: {sorted(unbound)}. The pyo3 "
        f"method, its `.pyi` entry and a test here are part of the engine "
        f"feature, not follow-up work (#120). If one is Rust-only on purpose, add "
        f"it to RUST_ONLY with the reason."
    )


def test_rust_only_entries_are_still_real():
    """A stale exemption is worse than none: it reads as a considered decision.

    So an entry that has since been bound, or whose method no longer exists, has
    to be removed rather than left to accumulate.
    """
    sdk, pyo3 = _sdk_methods(), _pyo3_methods()
    gone = sorted(n for n in RUST_ONLY if n not in sdk)
    assert not gone, f"RUST_ONLY names methods the SDK no longer has: {gone}"
    now_bound = sorted(n for n in RUST_ONLY if n in pyo3)
    assert not now_bound, (
        f"RUST_ONLY still excuses methods that are now bound: {now_bound}. Drop "
        f"the entries."
    )


def test_the_type_stub_declares_every_binding():
    """`mypy` checks call sites against the stub and nothing checks the stub.

    A bound method missing from `__init__.pyi` is rejected by a type checker for a
    method that is right there, which is the drift #95 added the stub to prevent.
    `test_stub_records.py` covers the record *shapes*; this covers the method list.
    """
    undeclared = _pyo3_methods() - _stub_methods()
    assert not undeclared, (
        f"bound but missing from __init__.pyi: {sorted(undeclared)}"
    )
    # …and the reverse, which type-checks a call that fails at runtime.
    phantom = _stub_methods() - _pyo3_methods()
    assert not phantom, f"declared in __init__.pyi but not bound: {sorted(phantom)}"


# --- path scoping (#125) --------------------------------------------------


def test_a_scope_prepends_rather_than_compares():
    # A request for another tenant's data is not representable at all, rather than
    # representable and rejected.
    scope = origofs.Scope.at("/tenant-a")
    assert scope.root == "/tenant-a"
    assert scope.is_whole is False
    assert scope.resolve("notes.txt") == "/tenant-a/notes.txt"
    assert scope.resolve("/notes.txt") == "/tenant-a/notes.txt"
    assert scope.resolve("/") == "/tenant-a"
    # There is no path that reaches out of the root...
    assert scope.resolve("/tenant-b/secrets") == "/tenant-a/tenant-b/secrets"
    # ...and the one shape that could is refused before any lookup, so it reveals
    # nothing about what exists.
    with pytest.raises(ValueError):
        scope.resolve("../tenant-b/secrets")


def test_a_scope_matches_on_directory_boundaries():
    scope = origofs.Scope.at("/tenant-a")
    assert scope.contains("/tenant-a") is True
    assert scope.contains("/tenant-a/notes.txt") is True
    # The neighbour a scope exists to exclude, which `startswith` would let in.
    assert scope.contains("/tenant-abc/notes.txt") is False
    # A record naming no path still tells a scoped reader a neighbour exists.
    assert scope.contains(None) is False

    whole = origofs.Scope.whole()
    assert whole.is_whole is True and whole.root == ""
    assert whole.contains("/anything") and whole.contains(None)
    assert whole.resolve("x.txt") == "/x.txt"


def test_out_of_scope_is_not_found_never_forbidden():
    # The exception type is the first place "this exists but is not yours" leaks,
    # so `require` raises FileNotFoundError and deliberately not PermissionError.
    scope = origofs.Scope.at("/tenant-a")
    scope.require("/tenant-a/notes.txt")  # no raise
    with pytest.raises(FileNotFoundError):
        scope.require("/tenant-b/notes.txt")
    with pytest.raises(FileNotFoundError):
        scope.require(None)
    assert not isinstance(FileNotFoundError(), PermissionError)


def test_a_relative_or_traversing_scope_root_is_rejected():
    # A scope that is wrong in this direction fails open, so an ambiguous root is
    # a caller error rather than something guessed at.
    for bad in ("tenant-a", "relative/path"):
        with pytest.raises(ValueError):
            origofs.Scope.at(bad)
    with pytest.raises(ValueError):
        origofs.Scope.at("/tenant-a/../tenant-b")
    # A trailing slash is the same scope; "/" is the whole workspace.
    assert origofs.Scope.at("/tenant-a/").root == "/tenant-a"
    assert origofs.Scope.at("/").is_whole is True


@asyncio_test
async def test_a_scope_drives_real_workspace_calls():
    ws = await workspace()
    scope = origofs.Scope.at("/tenant-a")
    await ws.mkdir_p(scope.resolve("/"))
    await ws.write(scope.resolve("notes.txt"), b"tenant a\n")
    await ws.mkdir_p("/tenant-abc")
    await ws.write("/tenant-abc/notes.txt", b"the neighbour\n")

    assert bytes(await ws.read(scope.resolve("notes.txt"))) == b"tenant a\n"
    # The neighbour is unreachable through the scope rather than merely refused.
    assert scope.resolve("/tenant-abc/notes.txt") == "/tenant-a/tenant-abc/notes.txt"
    with pytest.raises(FileNotFoundError):
        await ws.read(scope.resolve("/tenant-abc/notes.txt"))

    # Filtering a workspace-wide listing — the side door a path-only scope leaves
    # open. `watch` reports every branch's events, including the neighbour's.
    events = await ws.watch(0)
    assert any(e["path"] == "/tenant-abc/notes.txt" for e in events)
    mine = [e for e in events if scope.contains(e["path"])]
    # The root itself is in scope, as are its descendants — and nothing else.
    assert mine and all(
        e["path"] == "/tenant-a" or e["path"].startswith("/tenant-a/") for e in mine
    )
    assert "/tenant-abc/notes.txt" not in [e["path"] for e in mine]


# --- trash (#115) ---------------------------------------------------------


@asyncio_test
async def test_trash_is_off_until_it_is_turned_on():
    # Off is the default on purpose: enabling retention by default would silently
    # change *when* space is reclaimed for every existing deployment.
    ws = await workspace()
    assert await ws.trash_retention() is None

    await ws.write("/gone.txt", b"unrecoverable")
    await ws.remove_trashing("/gone.txt")
    assert await ws.list_trash() == []

    await ws.set_trash_retention(3600)
    assert await ws.trash_retention() == 3600
    await ws.set_trash_retention(None)
    assert await ws.trash_retention() is None


@asyncio_test
async def test_an_uncommitted_delete_is_recoverable():
    ws = await workspace()
    await ws.set_trash_retention(3600)
    await ws.write("/notes.txt", b"never committed\n")
    await ws.remove_trashing("/notes.txt")

    with pytest.raises(FileNotFoundError):
        await ws.read("/notes.txt")

    (entry,) = await ws.list_trash()
    assert entry["path"] == "/notes.txt"
    assert entry["kind"] == "file"
    assert entry["size"] == len(b"never committed\n")
    # An unattributed delete: `remove_trashing` has no actor context.
    assert entry["actor_id"] is None

    human = await ws.create_human("dan", None)
    sess = await ws.create_session(human, "test")
    restored = await ws.restore_trash(entry["id"], origofs.WriteCtx.session(human, sess))
    assert restored == "/notes.txt"
    assert bytes(await ws.read("/notes.txt")) == b"never committed\n"
    # Restoring consumes the entry.
    assert await ws.list_trash() == []


@asyncio_test
async def test_an_attributed_delete_names_who_did_it():
    # The thing a `.trash` directory cannot express: the entry carries the actor
    # and session, so the restore *and* the deletion are both attributable.
    ws = await workspace()
    await ws.set_trash_retention(3600)
    agent = await ws.create_agent("claude", "opus", None)
    sess = await ws.create_session(agent, "mcp")
    ctx = origofs.WriteCtx.session(agent, sess)

    await ws.write_as(ctx, "/oops.txt", b"rm -rf on a bad path\n")
    outcome = await ws.remove_or_propose(ctx, "/oops.txt", None)
    assert outcome.wrote

    (entry,) = await ws.list_trash()
    assert entry["actor_id"] == agent and entry["session_id"] == sess


@asyncio_test
async def test_trash_can_be_purged_one_entry_or_all_of_it():
    ws = await workspace()
    await ws.set_trash_retention(3600)
    for i in range(3):
        await ws.write(f"/f{i}.txt", b"x")
        await ws.remove_trashing(f"/f{i}.txt")

    entries = await ws.list_trash()
    assert len(entries) == 3
    assert await ws.purge_trash(entries[0]["id"]) is True
    # Gone for good — purging the same id twice reports nothing was there.
    assert await ws.purge_trash(entries[0]["id"]) is False

    assert await ws.empty_trash() == 2
    assert await ws.list_trash() == []


@asyncio_test
async def test_disabling_retention_does_not_drop_what_is_already_there():
    # Silently discarding recoverable data as a side effect of a config change
    # would be the opposite of what the feature is for.
    ws = await workspace()
    await ws.set_trash_retention(3600)
    await ws.write("/keep.txt", b"x")
    await ws.remove_trashing("/keep.txt")

    await ws.set_trash_retention(None)
    assert len(await ws.list_trash()) == 1


# --- usage, quotas, statfs (#116, #119) -----------------------------------


@asyncio_test
async def test_usage_and_du_report_logical_bytes():
    ws = await workspace()
    await ws.mkdir_p("/a/b")
    await ws.write("/a/one.txt", b"x" * 100)
    await ws.write("/a/b/two.txt", b"y" * 200)
    await ws.write("/outside.txt", b"z" * 50)

    whole = await ws.usage()
    assert whole["bytes"] == 350
    # Directories are inodes too, so the count is files + dirs + the root.
    assert whole["inodes"] >= 4

    sub = await ws.du("/a")
    assert sub["bytes"] == 300
    assert (await ws.du("/a/b"))["bytes"] == 200

    with pytest.raises(FileNotFoundError):
        await ws.du("/nope")


@asyncio_test
async def test_a_quota_refuses_the_write_that_would_exceed_it():
    ws = await workspace()
    assert await ws.quota() == {"bytes": None, "inodes": None}

    await ws.set_quota(bytes=1000)
    assert await ws.quota() == {"bytes": 1000, "inodes": None}

    await ws.write("/fits.txt", b"x" * 900)
    with pytest.raises(origofs.OrigoFSError):
        await ws.write("/too-big.txt", b"y" * 500)
    # Nothing landed, and the file that fit is untouched.
    with pytest.raises(FileNotFoundError):
        await ws.read("/too-big.txt")
    assert len(bytes(await ws.read("/fits.txt"))) == 900

    # Clearing it lets the same write through.
    await ws.set_quota()
    assert await ws.quota() == {"bytes": None, "inodes": None}
    await ws.write("/too-big.txt", b"y" * 500)


@asyncio_test
async def test_a_quota_below_current_usage_is_allowed_and_not_retroactive():
    # Refusing it would make a quota impossible to introduce on a workspace that
    # already has data, which is the only interesting case.
    ws = await workspace()
    await ws.write("/big.txt", b"x" * 5000)
    await ws.set_quota(bytes=100)

    # Nothing was deleted and the file is still readable...
    assert len(bytes(await ws.read("/big.txt"))) == 5000
    # ...but growth is refused until usage falls back under the limit.
    with pytest.raises(origofs.OrigoFSError):
        await ws.write("/more.txt", b"y")


@asyncio_test
async def test_an_inode_quota_is_separate_from_a_byte_quota():
    ws = await workspace()
    used = (await ws.usage())["inodes"]
    await ws.set_quota(inodes=used + 1)
    await ws.write("/one.txt", b"x")
    with pytest.raises(origofs.OrigoFSError):
        await ws.write("/two.txt", b"x")
    # Overwriting an existing file adds no inode, so it still works.
    await ws.write("/one.txt", b"xx")


@asyncio_test
async def test_statfs_answers_with_and_without_a_quota():
    ws = await workspace()
    await ws.write("/a.txt", b"x" * 8192)

    unquotaed = await ws.statfs()
    assert unquotaed["block_size"] == 4096
    # No quota means no real capacity to report, so the total is synthesized —
    # never zero, or `df` would print a 100%-full filesystem.
    assert unquotaed["total_blocks"] > 0
    assert 0 < unquotaed["free_blocks"] <= unquotaed["total_blocks"]

    await ws.set_quota(bytes=4096 * 10, inodes=100)
    quotaed = await ws.statfs()
    assert quotaed["total_blocks"] == 10
    assert quotaed["total_inodes"] == 100
    assert quotaed["free_blocks"] == 10 - 2  # 8 KiB used, in 4 KiB blocks


# --- ownership, hard links, xattrs (#119, #121, #122) ---------------------


@asyncio_test
async def test_chmod_actually_changes_the_mode():
    # Before #122 both mounts accepted a chmod and did nothing, so `chmod +x` on a
    # script returned success on a false premise.
    ws = await workspace()
    await ws.write("/build.sh", b"#!/bin/sh\n")
    assert (await ws.stat("/build.sh"))["mode"] & 0o777 != 0o755

    returned = await ws.chmod("/build.sh", 0o755)
    # Only the permission bits move: the format bits are the inode's kind, not a
    # caller's to rewrite, so `mode` still reports S_IFREG alongside them.
    assert returned["mode"] & 0o777 == 0o755
    assert returned["mode"] & 0o170000 == 0o100000
    assert (await ws.stat("/build.sh"))["mode"] & 0o777 == 0o755

    with pytest.raises(FileNotFoundError):
        await ws.chmod("/missing.sh", 0o755)


@asyncio_test
async def test_chown_sets_either_half_and_stat_reports_both():
    ws = await workspace()
    await ws.write("/owned.txt", b"x")
    before = await ws.stat("/owned.txt")
    assert before["uid"] == 0 and before["gid"] == 0

    await ws.chown("/owned.txt", 1000, 1000)
    assert (await ws.stat("/owned.txt"))["uid"] == 1000

    # `None` is chown(2)'s -1: leave that half alone. This is how `chgrp` lands.
    await ws.chown("/owned.txt", gid=2000)
    after = await ws.stat("/owned.txt")
    assert after["uid"] == 1000 and after["gid"] == 2000


@asyncio_test
async def test_hard_links_share_one_inode():
    ws = await workspace()
    await ws.write("/original.txt", b"shared body\n")

    linked = await ws.link("/original.txt", "/alias.txt")
    assert linked["nlink"] == 2
    assert (await ws.stat("/alias.txt"))["ino"] == (await ws.stat("/original.txt"))["ino"]

    # A write through one name is visible through the other.
    await ws.write("/alias.txt", b"edited\n")
    assert bytes(await ws.read("/original.txt")) == b"edited\n"

    # The content survives until the last name goes.
    await ws.remove("/original.txt")
    assert bytes(await ws.read("/alias.txt")) == b"edited\n"
    assert (await ws.stat("/alias.txt"))["nlink"] == 1


@asyncio_test
async def test_a_hard_link_to_a_directory_is_refused():
    # POSIX says EPERM: a directory hard link would let the dentry graph form a
    # cycle nothing here — gc, commit, the recursive walks — survives.
    ws = await workspace()
    await ws.mkdir_p("/dir")
    with pytest.raises(PermissionError):
        await ws.link("/dir", "/dir-alias")


@asyncio_test
async def test_xattrs_round_trip():
    ws = await workspace()
    await ws.write("/doc.md", b"body\n")

    assert await ws.listxattr("/doc.md") == []
    assert await ws.getxattr("/doc.md", "user.origin") is None

    await ws.setxattr("/doc.md", "user.origin", b"imported")
    await ws.setxattr("/doc.md", "user.reviewed", b"yes")
    assert bytes(await ws.getxattr("/doc.md", "user.origin")) == b"imported"
    assert await ws.listxattr("/doc.md") == ["user.origin", "user.reviewed"]

    # The boolean is what lets a mount answer ENODATA rather than reporting a
    # removal that removed nothing.
    assert await ws.removexattr("/doc.md", "user.origin") is True
    assert await ws.removexattr("/doc.md", "user.origin") is False
    assert await ws.listxattr("/doc.md") == ["user.reviewed"]


@asyncio_test
async def test_an_oversized_xattr_is_refused_rather_than_stored():
    # An xattr lives in the *metadata* store, and the rule the whole design rests
    # on is that the metadata DB never holds large bytes.
    ws = await workspace()
    await ws.write("/doc.md", b"body\n")
    with pytest.raises(Exception):
        await ws.setxattr("/doc.md", "user.huge", b"x" * (1 << 20))
    assert await ws.listxattr("/doc.md") == []


# --- path-scoped write ACLs (#123) ----------------------------------------


@asyncio_test
async def test_a_grant_makes_permissions_path_scoped():
    # The thing the single `write_policy` column could not express: "may write
    # /docs, may only propose under /src".
    ws = await workspace()
    reviewer = await ws.create_human("reviewer", None)
    agent = await ws.create_agent("claude", "opus", reviewer)
    ctx = origofs.WriteCtx.actor(agent)

    await ws.set_write_policy(agent, "propose")
    await ws.grant(agent, "/docs", ["read", "write"], reviewer)

    assert await ws.effective_perms(agent, "/docs/notes.md") == ["read", "write"]
    # No grant covers /src, so it falls back to the actor's write policy.
    assert await ws.effective_perms(agent, "/src/main.rs") == ["read", "propose"]

    wrote = await ws.write_or_propose(ctx, "/docs/notes.md", b"direct\n", None)
    assert wrote.wrote and wrote.suggestion_id is None
    assert bytes(await ws.read("/docs/notes.md")) == b"direct\n"

    proposed = await ws.write_or_propose(ctx, "/src/main.rs", b"queued\n", None)
    assert not proposed.wrote and proposed.suggestion_id is not None
    with pytest.raises(FileNotFoundError):
        await ws.read("/src/main.rs")


@asyncio_test
async def test_the_longest_matching_prefix_wins():
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)

    await ws.grant(agent, "/", "read+write", None)
    await ws.grant(agent, "/docs/private", [], None)  # an explicit deny

    assert await ws.effective_perms(agent, "/anywhere.txt") == ["read", "write"]
    assert await ws.effective_perms(agent, "/docs/notes.md") == ["read", "write"]
    assert await ws.effective_perms(agent, "/docs/private/secret.md") == []

    # Matched on directory boundaries, so the neighbour a scope exists to exclude
    # is excluded here too.
    assert await ws.effective_perms(agent, "/docs/private-ish/x.md") == ["read", "write"]

    # Neither write nor propose: refused outright rather than queued, which would
    # claim a review that will never happen.
    with pytest.raises(PermissionError):
        await ws.write_or_propose(
            origofs.WriteCtx.actor(agent), "/docs/private/secret.md", b"x", None
        )


# --- checked ACL administration --------------------------------------------


@asyncio_test
async def test_an_agent_cannot_grant_itself_through_the_bindings():
    # `grant`/`revoke` take no authorization at all -- they exist for provisioning,
    # which has no actor to check. A service endpoint built on them would let any
    # authenticated caller grant itself write at `/`, so `grant_as` is the form a
    # surface must use.
    ws = await workspace()
    alice = await ws.create_human("alice", None)
    agent = await ws.create_agent("claude", "opus", alice)
    await ws.set_acl_default_deny(True)
    await ws.grant(alice, "/proj", "read+write", None)

    # Alice narrows her agent: propose-only inside her subtree.
    await ws.grant_as(origofs.WriteCtx.actor(alice), agent, "/proj", "read+propose")
    assert await ws.effective_perms(agent, "/proj/f") == ["read", "propose"]

    ctx = origofs.WriteCtx.actor(agent)
    # It proposes rather than writing...
    outcome = await ws.write_or_propose(ctx, "/proj/f.md", b"x", None)
    assert not outcome.wrote and outcome.suggestion_id is not None

    # ...and cannot promote itself, nor escape the subtree, nor turn the
    # workspace switches off to get around the refusal.
    for call in (
        ws.grant_as(ctx, agent, "/proj", "read+write"),
        ws.grant_as(ctx, agent, "/", "read+write"),
        ws.revoke_as(ctx, alice, "/proj"),
        ws.set_acl_default_deny_as(ctx, False),
        ws.set_acl_enforce_reads_as(ctx, False),
        ws.set_write_policy_as(ctx, agent, "direct"),
    ):
        with pytest.raises(PermissionError):
            await call

    assert await ws.effective_perms(agent, "/proj/f") == ["read", "propose"]
    assert await ws.effective_perms(agent, "/elsewhere") == []


@asyncio_test
async def test_the_granter_is_taken_from_the_context():
    # `granted_by` stops being a caller-supplied claim and becomes who the engine
    # authorized -- the difference between an audit trail and an assertion.
    ws = await workspace()
    alice = await ws.create_human("alice", None)
    agent = await ws.create_agent("claude", "opus", alice)
    await ws.grant(alice, "/proj", "read+write", None)

    await ws.grant_as(origofs.WriteCtx.actor(alice), agent, "/proj", "read")
    grants = await ws.list_grants(agent)
    row = next(g for g in grants if g["path_prefix"] == "/proj")
    assert row["granted_by"] == alice


# --- read enforcement (#124, phase 1) --------------------------------------


@asyncio_test
async def test_reads_are_open_until_the_workspace_opts_in():
    # The migration invariant, through the bindings: adding the check changes
    # nothing for an existing workspace. `Perms.READ` was a bit nothing consulted,
    # so no workspace has read grants and enforcing on upgrade would stop every
    # actor at once.
    ws = await workspace()
    owner = await ws.create_human("owner", None)
    await ws.grant(owner, "/", "read+write", None)
    await ws.write_as(origofs.WriteCtx.actor(owner), "/doc.md", b"secret\n")

    bob = await ws.create_agent("bob", "opus", None)
    ctx = origofs.WriteCtx.actor(bob)
    await ws.set_acl_default_deny(True)  # bob has no grant anywhere

    assert await ws.acl_enforce_reads() is False
    assert bytes(await ws.read_as(ctx, "/doc.md")) == b"secret\n"
    assert await ws.stat_as(ctx, "/doc.md")
    assert await ws.ls_as(ctx, "/")
    assert await ws.blame_as(ctx, "/doc.md")
    # ...while writing is refused, so this is the switch being off rather than an
    # accidental grant.
    with pytest.raises(PermissionError):
        await ws.write_or_propose(ctx, "/doc.md", b"x", None)


@asyncio_test
async def test_read_enforcement_gates_every_attributed_read():
    ws = await workspace()
    owner = await ws.create_human("owner", None)
    octx = origofs.WriteCtx.actor(owner)
    await ws.grant(owner, "/", "read+write", None)
    await ws.write_as(octx, "/doc.md", b"secret\n")
    await ws.symlink_as(octx, "/doc.md", "/link")

    bob = await ws.create_agent("bob", "opus", None)
    ctx = origofs.WriteCtx.actor(bob)
    await ws.set_acl_default_deny(True)
    await ws.set_acl_enforce_reads(True)
    assert await ws.acl_enforce_reads() is True

    for call in (
        ws.read_as(ctx, "/doc.md"),
        ws.read_range_as(ctx, "/doc.md", 0, 3),
        ws.stat_as(ctx, "/doc.md"),
        ws.ls_as(ctx, "/"),
        ws.readlink_as(ctx, "/link"),
        ws.blame_as(ctx, "/doc.md"),
        ws.ensure_may_read_at(ctx, "read", "/doc.md"),
    ):
        with pytest.raises(PermissionError):
            await call

    # A read grant admits all of them, and still is not a write grant.
    await ws.grant(bob, "/", "read", None)
    assert bytes(await ws.read_as(ctx, "/doc.md")) == b"secret\n"
    assert bytes(await ws.read_range_as(ctx, "/doc.md", 0, 3)) == b"sec"
    assert await ws.stat_as(ctx, "/doc.md")
    assert await ws.ls_as(ctx, "/")
    assert await ws.readlink_as(ctx, "/link") == "/doc.md"
    assert await ws.blame_as(ctx, "/doc.md")
    # Returns without raising, which is the whole contract. (It resolves to an
    # empty tuple rather than None, matching its sibling `ensure_may_write_at`.)
    await ws.ensure_may_read_at(ctx, "read", "/doc.md")
    with pytest.raises(PermissionError):
        await ws.write_or_propose(ctx, "/doc.md", b"x", None)


@asyncio_test
async def test_a_denied_read_does_not_reveal_whether_the_path_exists():
    # The property the check is for: an unauthorized read is usually a probe, and
    # a refusal that differed between a real and a missing path would answer it.
    ws = await workspace()
    owner = await ws.create_human("owner", None)
    await ws.grant(owner, "/", "read+write", None)
    await ws.write_as(origofs.WriteCtx.actor(owner), "/doc.md", b"secret\n")

    bob = await ws.create_agent("bob", "opus", None)
    ctx = origofs.WriteCtx.actor(bob)
    await ws.set_acl_default_deny(True)
    await ws.set_acl_enforce_reads(True)

    with pytest.raises(PermissionError) as real:
        await ws.read_as(ctx, "/doc.md")
    with pytest.raises(PermissionError) as ghost:
        await ws.read_as(ctx, "/no-such-file.md")
    assert str(real.value).replace("/doc.md", "<p>") == str(ghost.value).replace(
        "/no-such-file.md", "<p>"
    )


@asyncio_test
async def test_grants_are_listable_and_revocable():
    ws = await workspace()
    admin = await ws.create_human("admin", None)
    agent = await ws.create_agent("claude", "opus", admin)

    await ws.grant(agent, "/docs", ["write"], admin)
    (row,) = await ws.list_grants(agent)
    assert row["actor_id"] == agent
    assert row["path_prefix"] == "/docs"
    assert row["perms"] == ["write"]
    assert row["granted_by"] == admin
    assert isinstance(row["granted_at"], int)

    # A root grant is stored as the empty prefix — length 0, so every more
    # specific grant outranks it.
    await ws.grant(agent, "/", ["read"], admin)
    assert {g["path_prefix"] for g in await ws.list_grants()} == {"/docs", ""}

    assert await ws.revoke(agent, "/docs", admin) is True
    assert await ws.revoke(agent, "/docs", admin) is False
    assert [g["path_prefix"] for g in await ws.list_grants(agent)] == [""]


@asyncio_test
async def test_a_relative_prefix_and_an_unknown_permission_are_rejected():
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)
    # A grant that silently applied to a subtree the operator did not mean would
    # fail open.
    with pytest.raises(ValueError):
        await ws.grant(agent, "docs", ["write"], None)
    with pytest.raises(ValueError):
        await ws.grant(agent, "/docs", ["wrote"], None)


@asyncio_test
async def test_deny_by_default_is_a_deliberate_switch():
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)

    # The default is fallback, so a workspace that has never written a grant
    # behaves exactly as it did before ACLs existed.
    assert await ws.acl_default_deny() is False
    assert await ws.effective_perms(agent, "/a.txt") == ["read", "write", "propose"]

    await ws.set_acl_default_deny(True)
    assert await ws.acl_default_deny() is True
    assert await ws.effective_perms(agent, "/a.txt") == []

    # ...until an operator writes one.
    await ws.grant(agent, "/allowed", ["write"], None)
    assert await ws.effective_perms(agent, "/allowed/x.txt") == ["write"]


@asyncio_test
async def test_ensure_may_write_at_is_the_path_bearing_check():
    ws = await workspace()
    admin = await ws.create_human("admin", None)
    agent = await ws.create_agent("claude", "opus", admin)
    await ws.grant(agent, "/docs", ["write"], admin)
    await ws.grant(agent, "/src", ["propose"], admin)

    ctx = origofs.WriteCtx.actor(agent)
    await ws.ensure_may_write_at(ctx, "write files", "/docs/a.md")  # no raise
    with pytest.raises(PermissionError) as denied:
        await ws.ensure_may_write_at(ctx, "write files", "/src/a.rs")
    # The denial says the actor may not do it, never whether the path exists.
    assert "/src/a.rs" in str(denied.value)


# --- origofs info / bench (#118) ------------------------------------------


@asyncio_test
async def test_file_layout_reports_what_a_read_costs():
    ws = await workspace()
    body = bytes(range(256)) * 4096  # 1 MiB, enough to chunk
    await ws.write("/big.bin", body)

    layout = await ws.file_layout("/big.bin")
    assert layout["size"] == len(body)
    assert layout["manifest"] is not None
    # The read-amplification number: a whole-file read fetches this many objects.
    assert layout["chunks"] >= 1
    assert layout["distinct_chunks"] <= layout["chunks"]
    assert layout["smallest"] <= layout["median"] <= layout["largest"]
    assert layout["self_dedup"] >= 1.0
    assert sum(count for _, count in layout["histogram"]) == layout["chunks"]
    assert len(layout["chunker"]) == 3
    # Probing the store is the one part that touches the backend, so it is off by
    # default.
    assert layout["residency"] is None

    probed = await ws.file_layout("/big.bin", True)
    assert probed["residency"]["present"] == probed["distinct_chunks"]
    assert probed["residency"]["missing"] == 0

    # An empty file has no body and therefore no manifest.
    await ws.write("/empty.txt", b"")
    assert (await ws.file_layout("/empty.txt"))["manifest"] is None

    # It errors the way a read would, so `file_layout` and `read` disagree about a
    # path only when the read path itself is broken.
    await ws.mkdir_p("/dir")
    with pytest.raises(IsADirectoryError):
        await ws.file_layout("/dir")
    with pytest.raises(FileNotFoundError):
        await ws.file_layout("/missing.bin")


@asyncio_test
async def test_bench_measures_this_workspaces_backends():
    ws = await workspace()
    # Deliberately tiny: the defaults (8 x 8 MiB) are sized for a real
    # measurement, not for a test run.
    report = await ws.bench(dir="/.bench", files=2, file_size=8192, seed=7)

    assert report["opts"] == {
        "dir": "/.bench",
        "files": 2,
        "file_size": 8192,
        "seed": 7,
        "keep": False,
        "force": False,
    }
    assert report["total_bytes"] == 2 * 8192
    for phase in ("write", "read", "reread"):
        stage = report[phase]
        assert stage["ops"] == 2
        assert stage["bytes"] == 2 * 8192
        assert stage["p50_secs"] <= stage["max_secs"]
    assert report["chunks"] >= 2
    assert report["kept"] is False

    # It cleans up after itself unless asked not to.
    with pytest.raises(FileNotFoundError):
        await ws.read("/.bench/bench-0000.bin")

    # And it refuses to start in a directory that already holds something.
    await ws.mkdir_p("/occupied")
    await ws.write("/occupied/mine.txt", b"do not touch")
    with pytest.raises(Exception):
        await ws.bench(dir="/occupied", files=1, file_size=4096)
    assert bytes(await ws.read("/occupied/mine.txt")) == b"do not touch"


# --- portable dump / load (#117) -------------------------------------------
#
# The gap that mattered most here is not that `dump`/`load` were unbound but what
# binding them naively would have cost. A dump is whole-**store**: every
# workspace, every actor including its `auth_subject`, every ACL grant, all blame.
# So the binding is the *authorized* form only.


@asyncio_test
async def test_a_dump_round_trips_into_a_fresh_workspace():
    ws = await workspace()
    alice = await ws.create_human("alice", "sub:alice")
    await ws.write_as(origofs.WriteCtx.actor(alice), "/notes.md", b"hello\n")
    await ws.mkdir_as(origofs.WriteCtx.actor(alice), "/sub")

    d = tempfile.mkdtemp()
    dump = os.path.join(d, "dump.jsonl")
    n = await ws.dump_as(origofs.WriteCtx.actor(alice), dump)
    assert n > 0
    # JSON Lines, readable with ordinary tools -- a stated goal of #117.
    with open(dump) as f:
        lines = [line for line in f if line.strip()]
    assert len(lines) == n + 1, "one header record plus one per row"

    dst = await workspace()
    report = await dst.load(dump)
    assert report["total_rows"] == n
    assert report["skipped_tables"] == []
    assert report["tables"]["actor"] == 1
    # Names, structure and the identity registry survived.
    assert [e["name"] for e in await dst.ls("/")] == ["notes.md", "sub"]
    assert [a["display_name"] for a in await dst.list_actors()] == ["alice"]


@asyncio_test
async def test_a_dump_is_checked_at_the_workspace_root():
    """A subtree grant must not buy a dump of the whole store.

    This is the sharpest case for gating a read on a write permission: an actor
    confined to `/tenant-a` that can dump has read every other tenant's metadata,
    every actor's `auth_subject`, and every grant. Nothing narrows a dump, because
    a dump has no path to narrow.
    """
    ws = await workspace()
    admin = await ws.create_human("admin", None)
    agent = await ws.create_agent("claude", "opus", admin)
    await ws.mkdir_as(origofs.WriteCtx.actor(admin), "/tenant-a")
    await ws.write_as(origofs.WriteCtx.actor(admin), "/tenant-a/f.txt", b"x")
    await ws.set_acl_default_deny(True)
    await ws.grant(agent, "/tenant-a", ["read", "write"], admin)
    await ws.grant(admin, "/", ["read", "write"], admin)

    d = tempfile.mkdtemp()
    with pytest.raises(PermissionError) as denied:
        await ws.dump_as(origofs.WriteCtx.actor(agent), os.path.join(d, "no.jsonl"))
    assert "whole workspace" in str(denied.value)

    # The actor holding `/` can, so this is about reach and not about dump being
    # broken.
    ok = os.path.join(d, "yes.jsonl")
    assert await ws.dump_as(origofs.WriteCtx.actor(admin), ok) > 0


@asyncio_test
async def test_a_dump_falls_back_to_the_write_policy_with_no_grants():
    """Binding the check did not break the workspaces that have no ACLs at all."""
    ws = await workspace()
    a = await ws.create_human("a", None)
    await ws.write_as(origofs.WriteCtx.actor(a), "/f.txt", b"x")
    assert not await ws.list_grants()
    d = tempfile.mkdtemp()
    assert await ws.dump_as(origofs.WriteCtx.actor(a), os.path.join(d, "d.jsonl")) > 0


@asyncio_test
async def test_a_load_refuses_a_store_that_is_already_configured():
    """A load replaces the identity registry and every grant with the dump's.

    `ensure_loadable` used to check content and branches only, so a store given
    actors and grants but no files yet counted as pristine -- and that is the
    recommended setup order, since deny-by-default is a deliberate switch thrown
    *after* the grants are written. A load in that window is a silent, total
    takeover of the workspace's authorization state.

    There is deliberately no `load_as`: a load cannot be ACL-gated, because the
    identities a check would consult are the ones it installs. Refusing a store
    that has any is the check.
    """
    src = await workspace()
    mallory = await src.create_human("mallory", "sub:mallory")
    await src.write_as(origofs.WriteCtx.actor(mallory), "/f.txt", b"x")
    await src.grant(mallory, "/", ["read", "write"], mallory)
    d = tempfile.mkdtemp()
    dump = os.path.join(d, "d.jsonl")
    await src.dump_as(origofs.WriteCtx.actor(mallory), dump)

    # The victim, set up exactly as an operator is told to set one up.
    dst = await workspace()
    admin = await dst.create_human("admin", "sub:admin")
    await dst.grant(admin, "/", ["read", "write"], admin)
    await dst.set_acl_default_deny(True)

    with pytest.raises(ValueError) as refused:
        await dst.load(dump)
    assert "identity registry" in str(refused.value)

    # And the refusal left the destination's authorization state as it was.
    assert await dst.acl_default_deny() is True
    grants = await dst.list_grants()
    assert len(grants) == 1 and grants[0]["actor_id"] == admin
    assert [a["display_name"] for a in await dst.list_actors()] == ["admin"]


# --- attribution completeness (#128) ---------------------------------------


@asyncio_test
async def test_require_attribution_is_readable_settable_and_enforceable():
    """All three halves, because two of them are useless alone.

    Enforcement is surface-side by design -- the unattributed engine ops exist for
    internal machinery and are exempt by construction -- so a workspace with the
    switch on is only actually enforced on surfaces that call `ensure_attributed`.
    The CLI does. A Python service could not, because none of this was bound: an
    operator could set the switch with the CLI and a Python server would go on
    accepting unattributed writes without a word.
    """
    ws = await workspace()
    assert await ws.require_attribution() is False
    await ws.ensure_attributed("write")  # off: no raise

    await ws.set_require_attribution(True)
    assert await ws.require_attribution() is True
    with pytest.raises(PermissionError) as denied:
        await ws.ensure_attributed("write")
    assert "requires an actor" in str(denied.value)

    # It is completeness, not access control: an *attributed* write is unaffected.
    a = await ws.create_human("a", None)
    await ws.write_as(origofs.WriteCtx.actor(a), "/f.txt", b"x")

    await ws.set_require_attribution(False)
    await ws.ensure_attributed("write")


@asyncio_test
async def test_ensure_may_write_workspace_is_the_path_less_check():
    ws = await workspace()
    admin = await ws.create_human("admin", None)
    agent = await ws.create_agent("claude", "opus", admin)
    await ws.set_acl_default_deny(True)
    await ws.grant(agent, "/docs", ["read", "write"], admin)
    await ws.grant(admin, "/", ["read", "write"], admin)

    # Having no path is not the same as touching none.
    await ws.ensure_may_write_workspace(origofs.WriteCtx.actor(admin), "commit")
    with pytest.raises(PermissionError) as denied:
        await ws.ensure_may_write_workspace(origofs.WriteCtx.actor(agent), "commit")
    assert "whole workspace" in str(denied.value)


# --- replication between workspaces ----------------------------------------


@asyncio_test
async def test_push_and_fetch_move_objects_without_touching_refs():
    src = await workspace()
    a = await src.create_human("a", "sub:a")
    ctx = origofs.WriteCtx.actor(a)
    await src.write_as(ctx, "/f.txt", b"hello\n")
    head = await src.commit_as(ctx, "seed", "a <a@example.com>")

    dst = await workspace()
    stats = await src.push_objects(dst, head)
    assert stats["objects"] > 0
    # Refs are untouched, which is what makes this safe to run ahead of a resync.
    assert await dst.branches() == []

    # Idempotent: the second pass finds everything already there.
    again = await src.push_objects(dst, head)
    assert again["objects"] == 0 and again["skipped"] > 0

    # And the fetch half is the same walk in the other direction.
    third = await workspace()
    assert (await third.fetch_objects(src, head))["objects"] > 0


@asyncio_test
async def test_resync_reconciles_a_branch_both_ways():
    src = await workspace()
    dst = await workspace()
    a = await src.create_human("a", "sub:a")
    ctx = origofs.WriteCtx.actor(a)
    await src.write_as(ctx, "/f.txt", b"one\n")
    await src.commit_as(ctx, "one", "a <a@example.com>")

    report = await src.resync(dst, "main", "a <a@example.com>", "sync")
    assert report["branch"] == "main"
    assert report["outcome"] == "pushed"
    assert report["head"] is not None
    assert report["pushed"]["objects"] > 0
    assert report["conflicts"] == []
    # Blame travels with the content, which is the point of resync over a plain
    # object copy.
    assert report["blame_pushed"] > 0

    # Nothing new to do the second time.
    assert (await src.resync(dst, "main", "a <a@example.com>", "sync"))[
        "outcome"
    ] == "up-to-date"


@asyncio_test
async def test_a_bad_hash_is_a_value_error_not_a_missing_object():
    src = await workspace()
    dst = await workspace()
    with pytest.raises(ValueError):
        await src.push_objects(dst, "not-a-hash")


# --- the change feed, written to ------------------------------------------


@asyncio_test
async def test_record_event_interleaves_with_file_changes():
    ws = await workspace()
    a = await ws.create_human("a", None)
    await ws.write_as(origofs.WriteCtx.actor(a), "/f.txt", b"x")
    seq = await ws.record_event(
        "task_finished", "/f.txt", actor_id=a, detail="refactored the parser"
    )
    assert seq > 0

    feed = await ws.watch(0)
    kinds = [e["kind"] for e in feed]
    assert "write" in kinds and "task_finished" in kinds
    mine = next(e for e in feed if e["kind"] == "task_finished")
    assert mine["path"] == "/f.txt"
    assert mine["actor_id"] == a
    assert mine["detail"] == "refactored the parser"
    # One ordered stream: the host's own milestone came after the write it follows.
    assert kinds.index("task_finished") > kinds.index("write")


# --- explicit span attribution (M8) ----------------------------------------


@asyncio_test
async def test_write_as_blamed_attributes_sub_line_spans():
    """`write_as` diffs line-by-line; this one is told who wrote which bytes.

    That is the difference between "one collaborator touched this line" and the
    character-level truth a co-editing session actually has, and only
    `checkpoint_coedit` could reach it before -- so an editor integration that was
    not origofs's own CRDT had no way to record what it knew.
    """
    ws = await workspace()
    alice = await ws.create_human("alice", None)
    bob = await ws.create_human("bob", None)
    sa = await ws.create_session(alice, None)
    sb = await ws.create_session(bob, None)

    data = b"aaaabbbb"
    await ws.write_as_blamed(
        origofs.WriteCtx.session(alice, sa),
        "/co.txt",
        data,
        [(alice, sa, 4), (bob, sb, 4)],
    )

    spans = await ws.blame("/co.txt")
    by_actor = {s["actor"]["id"]: (s["byte_start"], s["byte_end"]) for s in spans}
    # `byte_end` is exclusive.
    assert by_actor[alice] == (0, 4)
    assert by_actor[bob] == (4, 8)
    assert bytes(await ws.read("/co.txt")) == data


# --- the bounded read cache (#114) -----------------------------------------


@asyncio_test
async def test_a_cache_config_carries_both_bounds():
    """The tier itself needs an object store to exercise; what is worth pinning
    here is that the config reaches Rust with both bounds, since a cache that
    honours only `max_bytes` is the one that fills someone's disk."""
    c = origofs.CacheConfig("/tmp/origofs-cache")
    assert "8589934592" in repr(c) and "2147483648" in repr(c)
    c = origofs.CacheConfig("/tmp/origofs-cache", max_bytes=1 << 20, min_free_bytes=1 << 21)
    assert "1048576" in repr(c) and "2097152" in repr(c)
