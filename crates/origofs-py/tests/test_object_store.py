"""Object-store content backend from Python.

`open_object_memory` runs the *same* object-store adapter as `open_s3` (minus the
network), so this exercises the real object-storage content path — including
read-time integrity verification — without a live bucket. The S3 constructors
(`open_s3`, `open_pg_s3`, …) are the production forms of the same path.

Build + run (from crates/origofs-py, in a venv):
    maturin develop
    python tests/test_object_store.py       # or: pytest tests/
"""
import asyncio
import os
import tempfile

import origofs


def test_s3config_constructs_and_hides_secrets():
    cfg = origofs.S3Config(
        bucket="my-bucket",
        region="us-east-1",
        endpoint="http://localhost:9000",
        allow_http=True,
        access_key_id="AKIA...",
        secret_access_key="super-secret",
        prefix="origofs",
    )
    r = repr(cfg)
    assert "my-bucket" in r and "us-east-1" in r
    assert "super-secret" not in r and "AKIA" not in r  # never leak credentials


def test_gcsconfig_constructs_and_hides_secrets():
    cfg = origofs.GcsConfig(
        bucket="my-bucket",
        service_account_key='{"private_key":"SENSITIVE-KEY","client_email":"x@y.iam"}',
        prefix="origofs",
    )
    r = repr(cfg)
    assert "my-bucket" in r
    assert "SENSITIVE-KEY" not in r  # never leak credentials


def test_object_store_roundtrip_and_attribution():
    async def _exercise():
        d = tempfile.mkdtemp()
        # SQLite metadata + in-memory object store (same adapter as S3).
        ws = await origofs.Workspace.open_object_memory(os.path.join(d, "meta.db"))

        agent = await ws.create_agent("claude", "opus", None)
        sess = await ws.create_session(agent, "test")
        ctx = origofs.WriteCtx.session(agent, sess)

        # write through the object-store content path, then read it back
        await ws.mkdir_p("/a/b")
        await ws.write_as(ctx, "/a/b/c.txt", b"one\ntwo\n")
        assert bytes(await ws.read("/a/b/c.txt")) == b"one\ntwo\n"

        # attribution still works over the object backend
        bl = await ws.blame("/a/b/c.txt")
        assert bl[0]["actor"]["id"] == agent, bl

        # versioning over object storage
        h = await ws.commit("claude", "snapshot")
        assert isinstance(h, str) and len(h) > 0

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
