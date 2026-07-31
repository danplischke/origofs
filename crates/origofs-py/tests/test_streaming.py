"""Streaming writes and reads from Python.

`write`/`write_as` take a ``bytes`` object and pyo3 copies it into a Rust
``Vec<u8>``, so an N-byte write holds roughly 3N transiently — the Python object,
the copy, and the chunker's buffers. Neither ``write_reader`` nor ``read_stream``
was bound at all, so the effective file-size ceiling from Python was available
memory.

The load-bearing test here is ``test_streaming_a_large_file_stays_bounded``: it
asserts peak RSS stays far below the file size *and* that blame and the edit-op
were recorded. That fails against ``write_as`` on memory and against the
unattributed ``write_reader`` on attribution — the whole change in one assertion.
"""
import asyncio
import functools
import os
import resource
import tempfile

import pytest

import origofs


def asyncio_test(fn):
    """Run an ``async def`` body via ``asyncio.run`` (the convention here)."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


async def workspace():
    d = tempfile.mkdtemp()
    return await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )


# Large enough that a buffering implementation is unmistakable against the RSS
# threshold below, small enough not to fill a CI runner's disk. Every temp file is
# cleaned up: an earlier draft used 400 MiB and `mkdtemp`, and left ~2 GB behind
# across a single run.
LARGE = 256 * 1024 * 1024


def peak_rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def write_file(path, size, byte=b"\xa5"):
    """Write ``size`` bytes without holding them: the test must not be the thing
    that blows up memory, or it would mask a buffering regression in the engine."""
    block = byte * (1 << 20)
    with open(path, "wb") as f:
        written = 0
        while written < size:
            n = min(len(block), size - written)
            f.write(block[:n])
            written += n
    return path


# --- the headline ----------------------------------------------------------


@asyncio_test
async def test_streaming_a_large_file_stays_bounded():
    """A 256 MiB attributed write must not cost 256 MiB of RAM — and must still
    record who wrote it."""
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)
    sess = await ws.create_session(agent, "test")
    ctx = origofs.WriteCtx.session(agent, sess)

    size = LARGE
    with tempfile.TemporaryDirectory() as d:
        src = write_file(os.path.join(d, "big.bin"), size)

        before = peak_rss_mb()
        await ws.write_path_as(ctx, "/big.bin", src)
        growth = peak_rss_mb() - before

    # Generous headroom: the point is O(1)-ish vs O(file), not a precise budget.
    # A buffering implementation grows by ~256 MB here; streaming grows by tens.
    assert growth < 96, f"streaming write grew RSS by {growth:.0f} MB for a {size >> 20} MiB file"

    # Size and content are right.
    assert (await ws.stat("/big.bin"))["size"] == size
    assert bytes(await ws.read_range("/big.bin", 0, 8)) == b"\xa5" * 8
    assert bytes(await ws.read_range("/big.bin", size - 8, 8)) == b"\xa5" * 8

    # And it is attributed — the property `write_reader` cannot give you.
    blame = await ws.blame("/big.bin")
    assert blame, "a streamed write recorded no blame"
    assert all(b["actor"]["id"] == agent for b in blame)

    ops = await ws.edit_ops(agent, sess)
    op = next(o for o in ops if o["path"] == "/big.bin")
    assert op["op"] == "write" and op["byte_len"] == size


@asyncio_test
async def test_reading_a_large_file_back_stays_bounded():
    """`read` materializes the whole body; `read_to_path` streams it."""
    ws = await workspace()
    size = LARGE
    with tempfile.TemporaryDirectory() as d:
        src = write_file(os.path.join(d, "in.bin"), size, b"\x5a")
        await ws.write_path("/big.bin", src)

        dest = os.path.join(d, "out.bin")
        before = peak_rss_mb()
        written = await ws.read_to_path("/big.bin", dest)
        growth = peak_rss_mb() - before

        assert written == size
        assert os.path.getsize(dest) == size

        # Spot-check rather than comparing hundreds of MB in Python.
        with open(dest, "rb") as f:
            assert f.read(8) == b"\x5a" * 8
            f.seek(-8, os.SEEK_END)
            assert f.read(8) == b"\x5a" * 8

    assert growth < 96, f"streaming read grew RSS by {growth:.0f} MB"


# --- the policy still applies ----------------------------------------------


@asyncio_test
async def test_a_propose_only_actor_cannot_stream():
    """Streaming must not become a side door around the write policy."""
    ws = await workspace()
    reviewer = await ws.create_human("dan", None)
    agent = await ws.create_agent("restricted", "opus", reviewer)
    sess = await ws.create_session(agent, "test")
    ctx = origofs.WriteCtx.session(agent, sess)
    await ws.set_write_policy(agent, "propose")

    with tempfile.TemporaryDirectory() as d:
        src = write_file(os.path.join(d, "s.bin"), 4096)
        with pytest.raises(PermissionError):
            await ws.write_path_as(ctx, "/denied.bin", src)
        with pytest.raises(FileNotFoundError):
            await ws.stat("/denied.bin")

        # The unattributed form is exempt by construction, as everywhere else.
        await ws.write_path("/unattributed.bin", src)
        assert (await ws.stat("/unattributed.bin"))["size"] == 4096

        await ws.set_write_policy(agent, "direct")
        await ws.write_path_as(ctx, "/allowed.bin", src)
        assert (await ws.stat("/allowed.bin"))["size"] == 4096


# --- ordinary behaviour ----------------------------------------------------


@asyncio_test
async def test_streaming_round_trips_small_files_too():
    ws = await workspace()
    human = await ws.create_human("dan", None)
    sess = await ws.create_session(human, "test")
    ctx = origofs.WriteCtx.session(human, sess)

    d = tempfile.mkdtemp()  # small files; left for post-mortem inspection
    src = os.path.join(d, "small.txt")
    with open(src, "wb") as f:
        f.write(b"hello streaming\n")

    # Parents are not auto-created, exactly as for `write_as` — the surfaces
    # (HTTP, MCP, the FastAPI router) `mkdir_p` first, and streaming is no
    # different. Asserted so the consistency is deliberate rather than assumed.
    with pytest.raises(FileNotFoundError):
        await ws.write_path_as(ctx, "/nested/dir/small.txt", src)
    await ws.mkdir_p("/nested/dir")
    await ws.write_path_as(ctx, "/nested/dir/small.txt", src)
    assert bytes(await ws.read("/nested/dir/small.txt")) == b"hello streaming\n"

    # Empty file: no manifest object, size 0, still creates the file.
    empty = os.path.join(d, "empty.txt")
    open(empty, "wb").close()
    await ws.write_path_as(ctx, "/empty.txt", empty)
    assert (await ws.stat("/empty.txt"))["size"] == 0
    assert bytes(await ws.read("/empty.txt")) == b""


@asyncio_test
async def test_a_missing_source_file_raises_filenotfound():
    ws = await workspace()
    human = await ws.create_human("dan", None)
    sess = await ws.create_session(human, "test")
    ctx = origofs.WriteCtx.session(human, sess)

    with pytest.raises(FileNotFoundError):
        await ws.write_path_as(ctx, "/x.txt", "/no/such/file")
    # Nothing was created for a source that never opened.
    with pytest.raises(FileNotFoundError):
        await ws.stat("/x.txt")


@asyncio_test
async def test_streaming_overwrites_and_records_the_prior_version():
    ws = await workspace()
    agent = await ws.create_agent("claude", "opus", None)
    sess = await ws.create_session(agent, "test")
    ctx = origofs.WriteCtx.session(agent, sess)

    with tempfile.TemporaryDirectory() as d:
        a = write_file(os.path.join(d, "a.bin"), 200_000, b"\x01")
        b = write_file(os.path.join(d, "b.bin"), 300_000, b"\x02")

        await ws.write_path_as(ctx, "/f.bin", a)
        first = (await ws.stat("/f.bin"))["content"]
        await ws.write_path_as(ctx, "/f.bin", b)

    assert (await ws.stat("/f.bin"))["size"] == 300_000
    ops = [o for o in await ws.edit_ops(agent, sess) if o["path"] == "/f.bin"]
    assert ops[-1]["pre_hash"] == first, "the op-log lost what was overwritten"
