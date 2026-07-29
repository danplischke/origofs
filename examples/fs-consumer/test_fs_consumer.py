"""Tests for the generic origofs change consumer — the catch-up guarantees in
particular, proved against an in-memory fake workspace (no network, no origofs
build needed).

Run: ``python -m unittest examples/fs-consumer/test_fs_consumer.py`` (or just
``python examples/fs-consumer/test_fs_consumer.py``).
"""

from __future__ import annotations

import unittest

from origofs_fs_consumer import (
    BlameSpan,
    Change,
    ChangeHandler,
    Consumer,
    DirEntry,
    Event,
    FsSource,
    MemoryCursorStore,
    NotFound,
    Stat,
    TransientError,
)


class FakeSource(FsSource):
    """An in-memory workspace: a current ``{path: bytes}`` map plus an append-only
    event log, mutated through test helpers that mimic origofs's write path."""

    def __init__(self, page_size: int = 100) -> None:
        self.files: dict[str, bytes] = {}
        self.evlog: list[Event] = []
        self._seq = 0
        self.page_size = page_size
        self.read_calls = 0

    # -- test mutators (each appends to the feed, like the real engine does) --

    def _append(self, kind: str, path: str, detail: str | None = None) -> int:
        self._seq += 1
        self.evlog.append(Event(seq=self._seq, kind=kind, path=path, detail=detail, ts=self._seq))
        return self._seq

    def write(self, path: str, data: bytes) -> int:
        self.files[path] = data
        return self._append("write", path)

    def remove(self, path: str) -> int:
        self.files.pop(path, None)
        return self._append("remove", path)

    def rename(self, old: str, new: str) -> int:
        if old in self.files:
            self.files[new] = self.files.pop(old)
        return self._append("rename", old, detail=new)

    def commit(self, msg: str = "snap") -> int:
        return self._append("commit", "/", detail=msg)

    # -- FsSource ------------------------------------------------------------

    def events(self, since: int, branch: str | None = None) -> list[Event]:
        out = [e for e in self.evlog if e.seq > since]
        return out[: self.page_size]  # emulate the server's bounded page

    def read(self, path: str) -> bytes:
        self.read_calls += 1
        try:
            return self.files[path]
        except KeyError:
            raise NotFound(path)

    def blame(self, path: str) -> list[BlameSpan]:
        if path not in self.files:
            raise NotFound(path)
        return [BlameSpan(0, len(self.files[path]), 1, 1, "tester", 1, "human")]

    def stat(self, path: str) -> Stat:
        if path not in self.files:
            raise NotFound(path)
        return Stat("file", len(self.files[path]), 0, 0, 1)

    def list_dir(self, path: str) -> list[DirEntry]:
        base = "" if path == "/" else path
        prefix = base + "/"
        children: dict[str, str] = {}
        for f in self.files:
            if f.startswith(prefix):
                head, sep, tail = f[len(prefix) :].partition("/")
                children[head] = "dir" if sep else "file"
        return [DirEntry(name=n, kind=k) for n, k in sorted(children.items())]


class DictIndex(ChangeHandler):
    """A sink that mirrors the workspace into a ``{path: bytes}`` dict, made
    durable only on ``flush`` — so the tests can assert both the committed state
    and that nothing lands without a flush."""

    def __init__(self) -> None:
        self.index: dict[str, bytes] = {}
        self._buf: list[tuple[str, str, bytes | None]] = []
        self.flush_count = 0
        self.commits: list[int] = []
        self.others: list[tuple[str, str]] = []
        self.snapshots = 0
        self.fail_flushes = 0  # >0: raise TransientError on the next N flushes

    def begin_batch(self) -> None:
        self._buf.clear()

    def on_upsert(self, ch: Change) -> None:
        body = ch.try_read()
        if body is None:
            return  # superseded; the removing event handles it
        self._buf.append(("u", ch.path, body))

    def on_delete(self, ch: Change) -> None:
        self._buf.append(("d", ch.path, None))

    def on_commit(self, ev: Event) -> None:
        self.commits.append(ev.seq)

    def on_other(self, ev: Event) -> None:
        self.others.append((ev.kind, ev.path))

    def on_snapshot_begin(self) -> None:
        self.snapshots += 1

    def flush(self) -> None:
        if self.fail_flushes > 0:
            self.fail_flushes -= 1
            raise TransientError("simulated sink failure")
        for op, path, body in self._buf:
            if op == "u":
                assert body is not None
                self.index[path] = body
            else:
                self.index.pop(path, None)
        self._buf.clear()
        self.flush_count += 1


def consumer(src: FakeSource, sink: DictIndex, cur: MemoryCursorStore, **kw: object) -> Consumer:
    return Consumer(src, sink, cur, poll_interval=0, on_error_backoff=0, **kw)  # type: ignore[arg-type]


class ColdStartSnapshot(unittest.TestCase):
    def test_snapshot_indexes_existing_files_then_tails(self) -> None:
        src = FakeSource()
        src.write("/a.md", b"alpha")
        src.write("/dir/b.md", b"beta")
        cur = MemoryCursorStore()
        sink = DictIndex()

        c = consumer(src, sink, cur)
        c.run_once()

        # Snapshot indexed everything already present, cursor is at the head.
        self.assertEqual(sink.index, {"/a.md": b"alpha", "/dir/b.md": b"beta"})
        self.assertEqual(sink.snapshots, 1)
        self.assertEqual(cur.load(), src._seq)

        # A later live edit is tailed on the next drain — no re-snapshot.
        src.write("/a.md", b"alpha2")
        c.run_once()
        self.assertEqual(sink.index["/a.md"], b"alpha2")
        self.assertEqual(sink.snapshots, 1)

    def test_change_during_snapshot_is_not_lost(self) -> None:
        """The head is captured before the walk, so a write that lands during the
        snapshot is replayed by the tail (idempotently) — no gap."""
        src = FakeSource()
        src.write("/a.md", b"one")
        cur, sink = MemoryCursorStore(), DictIndex()
        c = consumer(src, sink, cur)

        # Splice a concurrent write in *during* the tree walk.
        orig_walk = src.walk_files

        def walk_then_mutate(root: str = "/"):
            yield from orig_walk(root)
            src.write("/late.md", b"late")  # appears after head capture, mid-bootstrap

        src.walk_files = walk_then_mutate  # type: ignore[assignment]
        c.run_once()
        self.assertEqual(sink.index, {"/a.md": b"one", "/late.md": b"late"})


class Resume(unittest.TestCase):
    def test_warm_start_skips_snapshot(self) -> None:
        src = FakeSource()
        src.write("/a.md", b"x")
        head = src._seq
        cur = MemoryCursorStore(head)  # pretend a prior run got this far
        sink = DictIndex()

        consumer(src, sink, cur).run_once()
        self.assertEqual(sink.snapshots, 0)  # no bootstrap
        self.assertEqual(sink.index, {})  # nothing after the cursor yet

        src.write("/b.md", b"y")
        consumer(src, sink, cur).run_once()
        self.assertEqual(sink.index, {"/b.md": b"y"})

    def test_cursor_advances_only_after_flush(self) -> None:
        src = FakeSource()
        src.write("/a.md", b"x")
        cur, sink = MemoryCursorStore(), DictIndex()
        c = consumer(src, sink, cur, backfill="none")  # skip snapshot, tail only
        self.assertEqual(c.run_once(), False)  # nothing yet
        base = cur.load()

        src.write("/b.md", b"y")
        sink.fail_flushes = 99  # sink is "down": every flush raises
        with self.assertRaises(TransientError):
            c.run_once()
        self.assertEqual(cur.load(), base)  # cursor did NOT advance
        self.assertEqual(sink.index, {})  # nothing committed

        sink.fail_flushes = 0  # sink recovers; the same batch re-runs
        c.run_once()
        self.assertEqual(sink.index, {"/b.md": b"y"})
        self.assertGreater(cur.load(), base)


class ContentEvents(unittest.TestCase):
    def test_remove_deletes_from_index(self) -> None:
        src = FakeSource()
        src.write("/a.md", b"x")
        cur, sink = MemoryCursorStore(), DictIndex()
        consumer(src, sink, cur).run_once()
        self.assertIn("/a.md", sink.index)

        src.remove("/a.md")
        consumer(src, sink, cur).run_once()
        self.assertNotIn("/a.md", sink.index)

    def test_rename_is_delete_plus_upsert(self) -> None:
        src = FakeSource()
        src.write("/old.md", b"body")
        cur, sink = MemoryCursorStore(), DictIndex()
        consumer(src, sink, cur).run_once()

        src.rename("/old.md", "/new.md")
        consumer(src, sink, cur).run_once()
        self.assertNotIn("/old.md", sink.index)
        self.assertEqual(sink.index["/new.md"], b"body")

    def test_commit_and_other_events_are_surfaced(self) -> None:
        src = FakeSource()
        cur, sink = MemoryCursorStore(), DictIndex()
        c = consumer(src, sink, cur, backfill="none")
        c.run_once()

        seq = src.commit("release")
        src._append("suggest", "/a.md")
        c.run_once()
        self.assertIn(seq, sink.commits)
        self.assertIn(("suggest", "/a.md"), sink.others)


class Coalescing(unittest.TestCase):
    def test_repeated_writes_collapse_to_one_read(self) -> None:
        src = FakeSource()
        cur, sink = MemoryCursorStore(), DictIndex()
        c = consumer(src, sink, cur, backfill="none")
        c.run_once()

        for i in range(5):
            src.write("/a.md", f"v{i}".encode())
        src.write("/b.md", b"b")
        before = src.read_calls
        c.run_once()  # one drained batch: /a.md written 5x, /b.md once
        # Coalesced: /a.md read once (terminal), /b.md once -> 2 reads, not 6.
        self.assertEqual(src.read_calls - before, 2)
        self.assertEqual(sink.index["/a.md"], b"v4")

    def test_write_then_remove_in_one_batch_ends_deleted(self) -> None:
        src = FakeSource()
        src.write("/keep.md", b"k")
        cur, sink = MemoryCursorStore(), DictIndex()
        consumer(src, sink, cur).run_once()

        src.write("/tmp.md", b"scratch")
        src.remove("/tmp.md")  # same drained batch
        consumer(src, sink, cur).run_once()
        self.assertNotIn("/tmp.md", sink.index)

    def test_exact_order_mode_still_converges(self) -> None:
        src = FakeSource()
        cur, sink = MemoryCursorStore(), DictIndex()
        c = consumer(src, sink, cur, backfill="none", coalesce=False)
        c.run_once()
        src.write("/a.md", b"1")
        src.write("/a.md", b"2")
        src.remove("/a.md")
        src.write("/a.md", b"3")
        c.run_once()
        self.assertEqual(sink.index["/a.md"], b"3")


class SupersededReads(unittest.TestCase):
    def test_stale_upsert_whose_path_is_gone_is_skipped(self) -> None:
        """With coalescing off, an upsert event for a path removed later in the
        same drain reads NotFound — the sink skips it instead of crashing."""
        src = FakeSource()
        cur, sink = MemoryCursorStore(), DictIndex()
        c = consumer(src, sink, cur, backfill="none", coalesce=False)
        c.run_once()

        src.write("/ghost.md", b"boo")  # upsert event...
        src.remove("/ghost.md")  # ...but the file is gone by drain time
        c.run_once()  # must not raise
        self.assertNotIn("/ghost.md", sink.index)


class Backfill(unittest.TestCase):
    def test_replay_from_zero_reconstructs_state_without_snapshot(self) -> None:
        src = FakeSource()
        src.write("/a.md", b"1")
        src.write("/b.md", b"2")
        src.remove("/a.md")
        src.write("/a.md", b"3")
        cur, sink = MemoryCursorStore(), DictIndex()

        consumer(src, sink, cur, backfill="replay").run_once()
        self.assertEqual(sink.snapshots, 0)  # no tree walk
        self.assertEqual(sink.index, {"/a.md": b"3", "/b.md": b"2"})

    def test_reingest_is_idempotent(self) -> None:
        src = FakeSource()
        src.write("/a.md", b"1")
        cur, sink = MemoryCursorStore(), DictIndex()
        consumer(src, sink, cur).run_once()
        snapshot_after_first = dict(sink.index)
        # Re-running from the same cursor changes nothing (feed drained).
        consumer(src, sink, cur).run_once()
        self.assertEqual(sink.index, snapshot_after_first)


class Paging(unittest.TestCase):
    def test_small_pages_drain_completely(self) -> None:
        src = FakeSource(page_size=3)  # force many small pages
        for i in range(10):
            src.write(f"/f{i}.md", str(i).encode())
        cur, sink = MemoryCursorStore(), DictIndex()
        consumer(src, sink, cur, backfill="replay").run_once()
        self.assertEqual(len(sink.index), 10)
        self.assertEqual(cur.load(), src._seq)


if __name__ == "__main__":
    unittest.main(verbosity=2)
