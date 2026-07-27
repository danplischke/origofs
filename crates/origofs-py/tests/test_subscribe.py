"""Change-feed push subscription (Postgres LISTEN/NOTIFY) from Python.

The Postgres part self-skips unless ORIGOFS_PG_TEST_URL is set, e.g.
    ORIGOFS_PG_TEST_URL="host=127.0.0.1 port=5433 user=postgres password=postgres dbname=origofs"

Build + run (from crates/origofs-py, in a venv):
    maturin develop
    python tests/test_subscribe.py       # or: pytest tests/
"""
import asyncio
import os
import tempfile

import origofs


def test_subscribe_requires_postgres():
    async def _exercise():
        d = tempfile.mkdtemp()
        ws = await origofs.Workspace.open_local(
            os.path.join(d, "meta.db"), os.path.join(d, "cas")
        )
        # SQLite has no push feed; subscribe must fail clearly (use watch() there).
        try:
            await ws.subscribe(0)
            raise AssertionError("expected subscribe() to fail on a non-Postgres backend")
        except ValueError:
            pass

    asyncio.run(_exercise())


def test_subscribe_pushes_events():
    dsn = os.environ.get("ORIGOFS_PG_TEST_URL")
    if not dsn:
        print("skip test_subscribe_pushes_events: ORIGOFS_PG_TEST_URL unset")
        return

    async def _exercise():
        d = tempfile.mkdtemp()
        ws = await origofs.Workspace.open_pg(dsn, os.path.join(d, "cas"))

        # Cursor at the current tail, then subscribe (LISTEN is active on return).
        existing = await ws.watch(0)
        cursor = existing[-1]["seq"] if existing else 0
        sub = await ws.subscribe(cursor)

        # A write emits a change-feed event; recv() is woken by the NOTIFY.
        await ws.write("/live.txt", b"hi")
        batch = await asyncio.wait_for(sub.recv(), timeout=5)
        assert batch, "expected at least one pushed event"
        assert any(e["path"] == "/live.txt" for e in batch), batch
        assert all(e["seq"] > cursor for e in batch), batch

    asyncio.run(_exercise())


def _run_all():
    import inspect

    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            fn()
            print("ok  ", name)
    print("ALL OK")


if __name__ == "__main__":
    _run_all()
