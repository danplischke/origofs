"""End-to-end tests for the origofs fsspec filesystem (``origofs.fsspec``).

Proves the two faces of the same filesystem — the blocking fsspec API derived
for synchronous callers, and the async ``_``-prefixed coroutines an async caller
awaits directly — plus the origofs-specific payoff: writes carry attribution into
``blame``. Also checks that the ``origofs://`` protocol is discoverable through
fsspec's registry.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install fsspec
    pytest tests/                      # or: python tests/test_fsspec.py
"""
import asyncio
import os
import tempfile

import pytest

fsspec = pytest.importorskip("fsspec")  # skip the module without the fsspec extra

import origofs
from origofs.fsspec import OrigoFileSystem


def _fs(**kwargs):
    d = tempfile.mkdtemp()
    return OrigoFileSystem(
        db_path=os.path.join(d, "meta.db"),
        cas_dir=os.path.join(d, "cas"),
        skip_instance_cache=True,
        **kwargs,
    )


def test_fsspec_sync():
    """The blocking fsspec surface: bytes, ranges, listings, file objects, trees."""
    fs = _fs()

    # write + whole read + ranged reads (the fast path goes through read_range)
    fs.pipe_file("/notes.txt", b"line one\nline two\n")
    assert fs.cat_file("/notes.txt") == b"line one\nline two\n"
    assert fs.cat_file("/notes.txt", start=0, end=4) == b"line"
    assert fs.cat_file("/notes.txt", start=9, end=13) == b"line"
    assert fs.cat_file("/notes.txt", start=-4) == b"two\n"  # suffix read

    # info + listings reflect live state (no stale cache)
    info = fs.info("/notes.txt")
    assert info["type"] == "file" and info["size"] == 18
    assert fs.info("/")["type"] == "directory"
    fs.makedirs("/sub/dir", exist_ok=True)
    fs.pipe_file("/sub/dir/a.txt", b"aaa")
    assert set(fs.ls("/", detail=False)) == {"/notes.txt", "/sub"}
    assert fs.isdir("/sub/dir") and fs.isfile("/sub/dir/a.txt")
    assert fs.exists("/sub/dir/a.txt") and not fs.exists("/missing")

    # find / glob / du over the tree
    assert set(fs.find("/")) == {"/notes.txt", "/sub/dir/a.txt"}
    assert fs.glob("/sub/**/*.txt") == ["/sub/dir/a.txt"]
    assert fs.du("/sub") == 3

    # file objects: seekable ranged reads, buffered write committed on close
    with fs.open("/big.bin", "wb") as f:
        f.write(b"0123456789" * 1000)
    assert fs.info("/big.bin")["size"] == 10000
    with fs.open("/big.bin", "rb") as f:
        f.seek(20)
        assert f.read(5) == b"01234"

    # copy (content dup), native rename, recursive + single remove
    fs.copy("/notes.txt", "/copy.txt")
    assert fs.cat_file("/copy.txt") == fs.cat_file("/notes.txt")
    fs.mv("/copy.txt", "/renamed.txt")
    assert fs.exists("/renamed.txt") and not fs.exists("/copy.txt")
    fs.rm("/sub", recursive=True)
    assert not fs.exists("/sub")
    fs.rm("/renamed.txt")
    assert not fs.exists("/renamed.txt")

    # missing path -> FileNotFoundError (mapped from the workspace error)
    with pytest.raises(FileNotFoundError):
        fs.cat_file("/gone")


def test_fsspec_text_and_append():
    fs = _fs()
    with fs.open("/t.txt", "w") as f:  # text mode (fsspec TextIOWrapper)
        f.write("héllo\nworld\n")
    with fs.open("/t.txt", "r") as f:
        assert f.read() == "héllo\nworld\n"
    fs.pipe_file("/log", b"a\n")
    with fs.open("/log", "ab") as f:  # append seeds from current contents
        f.write(b"b\n")
    assert fs.cat_file("/log") == b"a\nb\n"


def test_fsspec_async():
    """The async-native face: await the coroutines directly on your own loop."""

    async def scenario():
        fs = _fs(asynchronous=True)
        await fs._pipe_file("/a.txt", b"hello world")
        assert await fs._cat_file("/a.txt") == b"hello world"
        assert await fs._cat_file("/a.txt", start=0, end=5) == b"hello"
        assert await fs._exists("/a.txt") and not await fs._exists("/nope")
        infos = await fs._ls("/", detail=True)
        assert [i["name"] for i in infos] == ["/a.txt"]
        # the raw workspace is reachable for everything fsspec can't express
        ws = await fs.get_workspace()
        assert bytes(await ws.read("/a.txt")) == b"hello world"

    asyncio.run(scenario())


def test_fsspec_attribution():
    """Writes made through the filesystem are credited to the actor in blame."""

    async def scenario():
        d = tempfile.mkdtemp()
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        dan = await ws.create_human("dan", "dan@x")
        sam = await ws.create_human("sam", "sam@x")

        # an attributed filesystem sharing the one live workspace
        fs = OrigoFileSystem(ws=ws, actor=dan, asynchronous=True, skip_instance_cache=True)
        await fs._pipe_file("/notes.txt", b"one\ntwo\n")
        bl = await fs._blame("/notes.txt")
        assert bl and bl[0]["actor"]["id"] == dan and bl[0]["actor"]["kind"] == "human"

        # per-call override wins over the instance default
        await fs._pipe_file("/other.txt", b"x\n", actor=sam)
        bl2 = await fs._blame("/other.txt")
        assert bl2[0]["actor"]["id"] == sam

        # without an actor, writes are unattributed
        plain = OrigoFileSystem(ws=ws, asynchronous=True, skip_instance_cache=True)
        await plain._pipe_file("/anon.txt", b"z\n")
        assert await plain._blame("/anon.txt") == []

    asyncio.run(scenario())


def test_fsspec_registry_and_url():
    """The `origofs://` protocol resolves through fsspec, and URLs round-trip."""
    assert fsspec.get_filesystem_class("origofs") is OrigoFileSystem

    d = tempfile.mkdtemp()
    opts = {"db_path": os.path.join(d, "meta.db"), "cas_dir": os.path.join(d, "cas")}
    fs = fsspec.filesystem("origofs", skip_instance_cache=True, **opts)
    fs.pipe_file("/x", b"y")

    with fsspec.open("origofs:///u.txt", "wb", **opts) as f:
        f.write(b"url-write")
    with fsspec.open("origofs:///u.txt", "rb", **opts) as f:
        assert f.read() == b"url-write"


def test_fsspec_memory_backend():
    """The object-store-in-memory backend needs no disk CAS."""
    d = tempfile.mkdtemp()
    fs = OrigoFileSystem(
        backend="memory", db_path=os.path.join(d, "meta.db"), skip_instance_cache=True
    )
    fs.pipe_file("/m.txt", b"in-memory")
    assert fs.cat_file("/m.txt") == b"in-memory"


def test_fsspec_backend_validation():
    with pytest.raises(ValueError):
        OrigoFileSystem()  # neither ws= nor a resolvable backend
    with pytest.raises(ValueError):
        OrigoFileSystem(backend="nonsense", db_path="x")


def test_fsspec_error_edges():
    """Error paths the fsspec compliance suite doesn't pin down for origofs."""
    fs = _fs()

    # empty file round-trips
    fs.pipe_file("/empty", b"")
    assert fs.cat_file("/empty") == b"" and fs.info("/empty")["size"] == 0

    fs.makedirs("/dir")
    with pytest.raises(FileExistsError):
        fs.mkdir("/dir")  # already exists
    with pytest.raises(FileNotFoundError):
        fs.mkdir("/missing/leaf", create_parents=False)  # parent absent

    fs.pipe_file("/dir/f", b"x")
    with pytest.raises(OSError):
        fs.rmdir("/dir")  # not empty
    with pytest.raises(FileNotFoundError):
        fs.rm("/does-not-exist")  # missing target -> raise (POSIX-like)


def test_fsspec_path_traversal_rejected():
    """origofs refuses to store a poisoned path component (`..`, NUL) at the
    metadata boundary — the invariant that stops a name escaping the tree. The
    filesystem must surface that, not paper over it."""
    fs = _fs()
    fs.makedirs("/dir")
    for bad in ("/dir/../escape", "/dir/a\x00b"):
        with pytest.raises(ValueError):
            fs.pipe_file(bad, b"x")
        with pytest.raises(ValueError):
            fs.info(bad)


if __name__ == "__main__":
    test_fsspec_sync()
    test_fsspec_text_and_append()
    test_fsspec_async()
    test_fsspec_attribution()
    test_fsspec_registry_and_url()
    test_fsspec_memory_backend()
    test_fsspec_backend_validation()
    test_fsspec_error_edges()
    test_fsspec_path_traversal_rejected()
    print("OK  origofs.fsspec")
