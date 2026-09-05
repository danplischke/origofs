"""The `TypedDict`s in `__init__.pyi` really describe what the extension returns.

`mypy` checks call sites *against* the stub; nothing checks the stub against
reality. So a key renamed in `crates/origofs-py/src/lib.rs` type-checks perfectly
and blows up as a `KeyError` at runtime — which is the failure mode the stub was
added to prevent (issue #95).

This drives a real workspace, collects one instance of each record, and compares
its keys to the `TypedDict` declared for it. The stub is a *stub*, not an
importable module, so the declarations are read out of the `.pyi` with `ast`.

Every `TypedDict` in the stub must be either exercised here or listed in
`NOT_EXERCISED` with a reason — so adding a record forces the choice, rather than
letting coverage quietly lapse.
"""
import ast
import asyncio
import functools
import os
import pathlib
import tempfile

import origofs

STUB = pathlib.Path(__file__).resolve().parents[1] / "python" / "origofs" / "__init__.pyi"

# Records whose shape this test does not build, and why. Each entry is a claim
# that constructing one costs more than the coverage is worth *here* -- not that
# it doesn't matter.
NOT_EXERCISED = {
    # Needs a genuinely conflicting three-way merge; origofs-core/tests/merge.rs
    # is where that shape is pinned, on the Rust side of the same struct.
    "ConflictRecord": "needs a conflicted merge; covered by origofs-core/tests/merge.rs",
    # Reached only through the coedit surface, which test_coedit.py drives.
    "LiveMarker": "co-editing surface; covered by test_coedit.py",
    # Only a *mount* can take one: an advisory lock is owned by an open file
    # description and kept alive by the mount's lease renewer, so there is no
    # library call that creates one and none should be added -- a lock with no
    # renewer expires under its holder. Shape pinned on the Rust side by
    # origofs-core/tests/posix_locks.rs.
    "PosixLockRecord": "needs a mount to take a lock; covered by origofs-core/tests/posix_locks.rs",
}


def asyncio_test(fn):
    """Run an ``async def`` body via ``asyncio.run`` (the convention in this suite)."""

    @functools.wraps(fn)
    def wrapper(*a, **kw):
        return asyncio.run(fn(*a, **kw))

    return wrapper


def stub_records() -> dict:
    """{name: {field names}} for every TypedDict declared in the stub."""
    tree = ast.parse(STUB.read_text())
    out: dict = {}
    for node in tree.body:
        # class Foo(TypedDict): ...
        if isinstance(node, ast.ClassDef) and any(
            isinstance(b, ast.Name) and b.id == "TypedDict" for b in node.bases
        ):
            out[node.name] = {
                s.target.id
                for s in node.body
                if isinstance(s, ast.AnnAssign) and isinstance(s.target, ast.Name)
            }
        # Foo = TypedDict("Foo", {...}) -- the form a reserved-word key forces.
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            call = node.value
            if not (isinstance(call.func, ast.Name) and call.func.id == "TypedDict"):
                continue
            if len(call.args) != 2:
                continue
            name, fields = call.args
            if isinstance(name, ast.Constant) and isinstance(fields, ast.Dict):
                out[str(name.value)] = {
                    k.value for k in fields.keys if isinstance(k, ast.Constant)
                }
    return out


async def _workspace():
    d = tempfile.mkdtemp()
    return await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )


async def _collect() -> dict:
    """One live instance of each record, keyed by its TypedDict name."""
    ws = await _workspace()
    human = await ws.create_human("dan", None)
    sess = await ws.create_session(human, "test")
    ctx = origofs.WriteCtx.session(human, sess)

    agent = await ws.create_agent("claude", "opus", human)
    agent_sess = await ws.create_session(agent, "mcp")
    agent_ctx = origofs.WriteCtx.session(agent, agent_sess)

    await ws.mkdir_as(ctx, "/docs")
    await ws.write_as(ctx, "/docs/notes.txt", b"one\ntwo\n")
    commit = await ws.commit_as(ctx, "dan", "base")
    await ws.create_branch_as(ctx, "side")

    # A pending suggestion from a different actor than the reviewer.
    sid = await ws.suggest(agent_ctx, "/docs/notes.txt", b"one\ntwo\nthree\n", "add a line")

    # An uncommitted change, so `status` has something to report.
    await ws.write_as(ctx, "/docs/notes.txt", b"one\ntwo\nedited\n")

    await ws.touch(human, sess, "/docs/notes.txt")
    await ws.lock("/docs/notes.txt", "dan")

    # A tree co-edited document, for the run shape a host's span map is built from.
    tree = await ws.open_coedit_tree(ctx, "/docs/tree.md", "content")
    await tree.append_text(ctx, "p", "structured")

    # Trash is off by default, so a recoverable delete has to be turned on first.
    await ws.set_trash_retention(3600)
    await ws.write("/docs/doomed.txt", b"delete me\n")
    await ws.remove_trashing("/docs/doomed.txt")

    # One prefix grant, so the ACL record has something to describe.
    await ws.grant(agent, "/docs", ["read", "propose"], human)

    # A tiny benchmark: the defaults (8 x 8 MiB) are sized for a real
    # measurement, which is not what this test is for.
    bench = await ws.bench(dir="/.bench", files=1, file_size=4096)

    # Replication and the portable dump both need a *second* workspace, which is
    # the only reason they are set up here rather than inline in the dict below.
    other = await _workspace()
    head = (await ws.log())[0]["hash"]
    pushed = await ws.push_objects(other, head)

    # `resync` refuses a dirty working tree, and `ws` has an uncommitted change on
    # purpose (so `status` has something to report) -- so the resync record comes
    # from a clean pair of its own rather than from bending the main fixture.
    clean = await _workspace()
    clean_actor = await clean.create_human("dan", "sub:dan")
    clean_ctx = origofs.WriteCtx.actor(clean_actor)
    await clean.write_as(clean_ctx, "/f.txt", b"one\n")
    await clean.commit_as(clean_ctx, "dan", "one")
    resynced = await clean.resync(await _workspace(), "main", "dan", "sync")

    dump = os.path.join(tempfile.mkdtemp(), "d.jsonl")
    await ws.dump_as(ctx, dump)
    loaded = await (await _workspace()).load(dump)

    # The search index has to be built before it can be searched -- an unindexed
    # workspace returns no hits at all, which would make the hit record
    # unbuildable here rather than merely empty.
    index_report = await ws.reindex()
    search_hits = await ws.search("one two")

    records = {
        "ActorRecord": await ws.actor(human),
        "BlameSpan": (await ws.blame("/docs/notes.txt"))[0],
        "StatResult": await ws.stat("/docs/notes.txt"),
        "DirEntry": (await ws.ls("/docs"))[0],
        "CommitRecord": (await ws.log())[0],
        "DiffEntry": (await ws.status())[0],
        "BranchRecord": (await ws.branches())[0],
        "EventRecord": (await ws.watch(0))[0],
        "PresenceRecord": (await ws.presence(60))[0],
        "SuggestionRecord": await ws.get_suggestion(sid),
        "SuggestionContent": await ws.suggestion_content(sid),
        "EditOp": (await ws.edit_ops(human, sess))[0],
        "PassageRecord": (await ws.passages(root="/docs"))[0],
        "LockRecord": (await ws.locks())[0],
        "MergeResult": await ws.merge_branch("side", "dan", "merge"),
        "GcReport": await ws.gc(),
        "RebuildReport": await ws.scan(),
        "SchemaVersion": await ws.schema_version(),
        "MigrateReport": await ws.migrate(),
        "ReadyReport": await ws.ready(),
        "TreeRun": (await tree.runs())[0],
        "TrashRecord": (await ws.list_trash())[0],
        "UsageRecord": await ws.usage(),
        "QuotaRecord": await ws.quota(),
        "FsStatRecord": await ws.statfs(),
        "AclGrantRecord": (await ws.list_grants())[0],
        # `probe=True` so the nested residency record is built rather than None.
        "FileLayoutRecord": await ws.file_layout("/docs/notes.txt", True),
        "ResidencyRecord": (await ws.file_layout("/docs/notes.txt", True))["residency"],
        "BenchReport": bench,
        "BenchOptsRecord": bench["opts"],
        "BenchStageRecord": bench["write"],
        "TunableRecord": bench["upload_concurrency"],
        "TransferStats": pushed,
        "ResyncReport": resynced,
        "LoadReport": loaded,
        "IndexReportRecord": index_report,
        "SearchStatusRecord": await ws.search_status(),
        "SearchHitRecord": search_hits[0],
    }
    assert commit  # the commit above is what `log`/`branches` report on
    return records


@asyncio_test
async def test_every_record_matches_its_typeddict():
    declared = stub_records()
    actual = await _collect()

    mismatches = []
    for name, record in actual.items():
        assert name in declared, f"{name} is collected here but not declared in the stub"
        want, got = declared[name], set(record.keys())
        if want != got:
            mismatches.append(
                f"{name}: stub-only={sorted(want - got)} runtime-only={sorted(got - want)}"
            )

    assert not mismatches, (
        "these TypedDicts no longer describe what the extension returns -- update "
        "__init__.pyi (or the dict builder in src/lib.rs, if the rename was "
        "unintended):\n  " + "\n  ".join(mismatches)
    )


def test_every_declared_record_is_accounted_for():
    # Coverage can't lapse silently: a new TypedDict either gets exercised above
    # or is explicitly excused.
    declared = set(stub_records())
    covered = set(asyncio.run(_collect())) | set(NOT_EXERCISED)
    missing = declared - covered
    assert not missing, (
        f"these records are declared in the stub but never checked against the "
        f"extension: {sorted(missing)}. Add them to _collect(), or to "
        f"NOT_EXERCISED with a reason."
    )


def test_the_stub_parser_actually_found_the_records():
    # A parser that silently matched nothing would make both tests above vacuous.
    declared = stub_records()
    assert len(declared) >= 15, f"only parsed {len(declared)} records from the stub"
    assert "BlameSpan" in declared
    # The functional-syntax record is picked up too (its `from` key is a keyword).
    assert declared["MigrateReport"] == {"from", "to", "migrated"}


@asyncio_test
async def test_blame_inlines_the_actor_record_rather_than_an_id():
    # The question the issue leads with: is `span["actor"]` an id or a record?
    # The wrapper that prompted #95 handled both because the stub didn't say.
    ws = await _workspace()
    dan = await ws.create_human("dan", None)
    sess = await ws.create_session(dan, "test")
    await ws.write_as(origofs.WriteCtx.session(dan, sess), "/a.txt", b"hi\n")

    span = (await ws.blame("/a.txt"))[0]
    assert isinstance(span["actor"], dict), span["actor"]
    assert span["actor"]["id"] == dan
    assert span["actor"]["kind"] == "human"
    # ...and `session` is a bare id, not a record.
    assert span["session"] == sess


@asyncio_test
async def test_literal_values_are_the_ones_the_stub_declares():
    # The Literal unions are only useful if they match the strings the extension
    # actually emits -- a stale one makes every comparison silently false.
    ws = await _workspace()
    dan = await ws.create_human("dan", None)
    sess = await ws.create_session(dan, "test")
    ctx = origofs.WriteCtx.session(dan, sess)
    agent = await ws.create_agent("claude", "opus", dan)

    assert (await ws.actor(dan))["kind"] == "human"
    assert (await ws.actor(agent))["kind"] == "agent"

    await ws.mkdir_as(ctx, "/d")
    await ws.write_as(ctx, "/d/f.txt", b"x")
    assert (await ws.stat("/d"))["kind"] == "dir"
    assert (await ws.stat("/d/f.txt"))["kind"] == "file"

    assert (await ws.status())[0]["status"] == "added"

    sid = await ws.suggest(origofs.WriteCtx.actor(agent), "/d/f.txt", b"y", None)
    s = await ws.get_suggestion(sid)
    assert s["kind"] == "bytes" and s["status"] == "pending"

    await ws.commit_as(ctx, "dan", "base")
    assert (await ws.merge_branch("main", "dan", None))["outcome"] == "already_up_to_date"


if __name__ == "__main__":
    import inspect

    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            fn()
            print("ok  ", name)
    print("ALL OK")
