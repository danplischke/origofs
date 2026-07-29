"""`serve_nfs` shutdown: nothing may outlive the call.

The observable proof that the listener's fd was released is that the port can be
bound again by a plain socket (a live LISTEN socket blocks that bind even with
SO_REUSEADDR); the proof that the per-connection tasks died with it is that an
open client connection sees EOF.

Note the helpers below stay `async` and yield with `asyncio.sleep`: cancelling an
awaitable only takes effect once the event loop runs its done-callbacks (ordinary
asyncio semantics), so a blocking `time.sleep` in the test would starve the very
teardown it is waiting for.

NFS is Unix-only (`#[cfg(unix)]` in lib.rs), so these self-skip elsewhere.

Build + run (from crates/origofs-py, in a venv):
    maturin develop
    pytest tests/test_serve_nfs_shutdown.py
"""
import asyncio
import os
import socket
import sys
import tempfile
import time

import pytest

import origofs

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="NFS serving is Unix-only"
)


def _free_port() -> int:
    """A port that was free a moment ago (bind-and-release)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _bindable(port: int) -> bool:
    """Whether a plain socket can bind `port` — i.e. no listener holds it.
    SO_REUSEADDR so a lingering TIME_WAIT doesn't masquerade as a leak; a
    still-live LISTEN socket fails the bind regardless."""
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


async def _wait_listening(port: int, timeout: float = 10.0) -> bool:
    """True once something accepts connections on `port`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            await asyncio.sleep(0.05)
    return False


async def _wait_port_free(port: int, timeout: float = 10.0) -> bool:
    """True once the listener fd is gone. Retried, because a cancelled task tears
    the server down in the background (`Runtime::shutdown_background`)."""
    deadline = time.monotonic() + timeout
    while True:
        if _bindable(port):
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.05)


async def _workspace():
    d = tempfile.mkdtemp()
    return await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )


def test_serve_nfs_releases_port_on_cancel():
    """Cancelling the awaiting Python task must drop the listener."""

    async def _exercise():
        ws = await _workspace()
        port = _free_port()
        task = asyncio.ensure_future(ws.serve_nfs(f"127.0.0.1:{port}"))
        assert await _wait_listening(port), "serve_nfs never bound the port"
        assert not _bindable(port), "sanity: the port should be taken while serving"

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await _wait_port_free(port), "listener fd outlived the cancelled serve_nfs"

    asyncio.run(_exercise())


def test_serve_nfs_closes_connections_on_cancel():
    """...and the per-connection tasks/sockets nfsserve spawned go with it."""

    async def _exercise():
        ws = await _workspace()
        port = _free_port()
        task = asyncio.ensure_future(ws.serve_nfs(f"127.0.0.1:{port}"))
        assert await _wait_listening(port), "serve_nfs never bound the port"

        client = socket.create_connection(("127.0.0.1", port), timeout=5)
        client.setblocking(False)
        try:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            deadline = time.monotonic() + 10.0
            while True:
                try:
                    left = client.recv(1)
                    break
                except BlockingIOError:
                    assert time.monotonic() < deadline, (
                        "connection outlived the cancelled server"
                    )
                    await asyncio.sleep(0.05)
            assert left == b"", f"expected EOF from the torn-down server, got {left!r}"
        finally:
            client.close()

    asyncio.run(_exercise())


def test_serve_nfs_shutdown_awaitable_is_graceful():
    """The explicit handle: `shutdown=` takes any awaitable, and awaiting the
    task returns only once teardown is complete (so the port is free at once)."""

    async def _exercise():
        ws = await _workspace()
        port = _free_port()
        stop = asyncio.Event()
        task = asyncio.ensure_future(
            ws.serve_nfs(f"127.0.0.1:{port}", shutdown=stop.wait())
        )
        assert await _wait_listening(port), "serve_nfs never bound the port"

        stop.set()
        await asyncio.wait_for(task, timeout=15)  # returns cleanly, not cancelled
        assert _bindable(port), "graceful shutdown returned with the port still bound"

    asyncio.run(_exercise())


def test_serve_nfs_port_is_reusable_after_shutdown():
    """The end-to-end consequence: the same address can be served again."""

    async def _exercise():
        ws = await _workspace()
        port = _free_port()
        addr = f"127.0.0.1:{port}"

        for _ in range(2):
            stop = asyncio.Event()
            task = asyncio.ensure_future(ws.serve_nfs(addr, shutdown=stop.wait()))
            assert await _wait_listening(port), "serve_nfs never bound the port"
            stop.set()
            await asyncio.wait_for(task, timeout=15)

    asyncio.run(_exercise())


def test_serve_nfs_reports_bind_failure():
    """A bind error still surfaces as an OSError from the awaitable."""

    async def _exercise():
        ws = await _workspace()
        with socket.socket() as taken:
            taken.bind(("127.0.0.1", 0))
            taken.listen(1)
            port = taken.getsockname()[1]
            with pytest.raises(OSError):
                await ws.serve_nfs(f"127.0.0.1:{port}")

    asyncio.run(_exercise())


if __name__ == "__main__":
    import inspect

    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            fn()
            print("ok  ", name)
    print("ALL OK")
