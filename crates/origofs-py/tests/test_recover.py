"""Disaster recovery from Python: rebuild a workspace's metadata from the
surviving content store after the metadata DB is lost.

This is the recovery path for an embedding (e.g. FastAPI on Postgres + S3) whose
metadata DB is gone: point a fresh DB at the same content store and rebuild.

Run (from crates/origofs-py, after `maturin develop`):
    pytest tests/test_recover.py        # or: python tests/test_recover.py
"""
import asyncio
import tempfile
from pathlib import Path

import origofs


def test_rebuild_recovers_files_and_branches():
    async def _run():
        with tempfile.TemporaryDirectory() as d:
            cas = str(Path(d) / "cas")
            ws = await origofs.Workspace.open_local(str(Path(d) / "meta.db"), cas)
            await ws.mkdir_p("/src")
            await ws.write("/README.md", b"hello")
            await ws.write("/src/app.txt", b"a\nb\n")
            await ws.commit("dan", "initial")
            await ws.create_branch("feature")

            # The metadata DB is lost. Open a FRESH DB over the SAME content store.
            recovered = await origofs.Workspace.open_local(str(Path(d) / "meta2.db"), cas)
            assert recovered.read is not None

            # Dry-run scan is read-only and reports what would come back.
            scan = await recovered.scan()
            assert scan["commits_found"] == 1
            assert scan["used_mirror"] is True
            assert sorted(n for n, _ in scan["branches"]) == ["feature", "main"]

            # Rebuild restores refs + the working tree.
            report = await recovered.rebuild()
            assert report["used_mirror"] is True
            assert report["files"] == 2
            assert report["dirs"] == 1
            assert report["checked_out"] == "main"
            assert sorted(n for n, _ in report["branches"]) == ["feature", "main"]

            # The files (chunked content) are readable again.
            assert bytes(await recovered.read("/README.md")) == b"hello"
            assert bytes(await recovered.read("/src/app.txt")) == b"a\nb\n"

    asyncio.run(_run())


if __name__ == "__main__":
    test_rebuild_recovers_files_and_branches()
    print("ok  test_rebuild_recovers_files_and_branches")
    print("ALL OK")
