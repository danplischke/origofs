"""A generic, resumable change consumer for an origofs workspace.

origofs is already a change-data-capture source: every user/agent action appends
an event to an **append-only feed** with a monotonic ``seq`` cursor
(``write / remove / rename / mkdir / symlink / commit / lock / unlock /
suggest``), and content is BLAKE3-addressed so the same bytes always hash the
same. This module turns that feed into a **reliable stream of file changes** you
can drive any sink from — a search index (BigQuery, Elasticsearch, …), a
mirror, a webhook — without re-implementing catch-up each time.

What "generic" means here
-------------------------
Three things are pluggable; the catch-up machinery is not:

* :class:`FsSource` — *where changes come from*. :class:`HttpFsSource` talks to
  the origofs HTTP API; you could just as well wrap the ``origofs`` pyo3
  bindings or a test double. It is the only part that knows a transport.
* :class:`CursorStore` — *where the resume point lives*. :class:`FileCursorStore`
  is an atomic JSON file; a production sink usually stores the cursor in the same
  transaction as the data (see the README) so the two can never disagree.
* :class:`ChangeHandler` — *what to do with a change*. Subclass it and implement
  :meth:`~ChangeHandler.on_upsert` / :meth:`~ChangeHandler.on_delete`.

Catch-up (the part you must not get wrong)
------------------------------------------
:class:`Consumer` gives at-least-once delivery with a no-gap bootstrap:

1. **Cold start** captures the feed head *before* snapshotting the tree, walks
   the current files as synthetic upserts, then tails from that captured head.
   Anything that changes *during* the snapshot is replayed by the tail — and
   because upserts are keyed by content, re-applying is idempotent, so the
   overlap is harmless rather than a gap.
2. **Resume** persists the cursor only *after* a batch is durably handled, so a
   crash re-processes the last batch instead of skipping it.
3. Reads reflect the *current* filesystem, so a stale event whose path was since
   removed raises :class:`NotFound` — treated as a no-op, since the removing
   event is (or was) in the feed too. The index is eventually consistent with
   the live tree.

The feed is never pruned, so ``backfill="replay"`` (tail from seq 0, no tree
walk) is an equally-correct, simpler bootstrap when the feed is short.

This module has no third-party dependencies except :class:`HttpFsSource`, which
imports ``httpx`` lazily — the core and its tests run on the standard library
alone.
"""

from __future__ import annotations

import abc
import dataclasses
import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Iterator, Optional

log = logging.getLogger("origofs.consumer")

__all__ = [
    "Consumer",
    "build_http_consumer",
    # sources
    "FsSource",
    "HttpFsSource",
    # cursors
    "CursorStore",
    "FileCursorStore",
    "MemoryCursorStore",
    # handlers
    "ChangeHandler",
    "LoggingHandler",
    "JsonlSink",
    # value types
    "Change",
    "ChangeType",
    "Event",
    "DirEntry",
    "Stat",
    "BlameSpan",
    # errors
    "SourceError",
    "NotFound",
    "TransientError",
]

# The verbs origofs writes into the feed's ``kind`` column (origofs-core
# ``collab.rs``). Only these three move file *bytes*; the rest are structural or
# informational and reach a handler through ``on_commit`` / ``on_other``.
KIND_WRITE = "write"
KIND_REMOVE = "remove"
KIND_RENAME = "rename"
KIND_COMMIT = "commit"


# --- errors -----------------------------------------------------------------


class SourceError(Exception):
    """Base class for anything an :class:`FsSource` raises."""


class NotFound(SourceError):
    """A path does not exist (HTTP 404). On an upsert this means the event was
    superseded by a later remove/rename — the consumer treats it as a no-op."""


class TransientError(SourceError):
    """A retryable failure (timeout, 5xx, connection reset). A source is expected
    to retry these itself; if one still surfaces, the consumer backs off."""


# --- value types ------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    """One row of the change feed (``GET /fs/events``)."""

    seq: int
    kind: str
    path: str
    detail: Optional[str] = None
    ts: int = 0
    actor_id: Optional[int] = None
    session_id: Optional[int] = None
    branch: Optional[str] = None

    @classmethod
    def from_json(cls, o: dict) -> "Event":
        return cls(
            seq=int(o["seq"]),
            kind=o["kind"],
            path=o["path"],
            detail=o.get("detail"),
            ts=int(o.get("ts", 0)),
            actor_id=o.get("actor_id"),
            session_id=o.get("session_id"),
            branch=o.get("branch"),
        )


@dataclass(frozen=True)
class DirEntry:
    name: str
    kind: str  # "file" | "dir" | "symlink"


@dataclass(frozen=True)
class Stat:
    kind: str
    size: int
    mtime: int
    ctime: int
    ino: int


@dataclass(frozen=True)
class BlameSpan:
    """A byte range and the actor that authored it — origofs's attribution, the
    reason to source from it rather than from a plain file crawl."""

    byte_start: int
    byte_end: int
    line_start: int
    line_end: int
    actor: str
    session: Optional[int]
    kind: str  # actor kind: "human" | "agent"


class ChangeType:
    UPSERT = "upsert"
    DELETE = "delete"


@dataclass
class Change:
    """A normalized file change handed to a :class:`ChangeHandler`.

    Content and blame are fetched **lazily** and reflect the *current* file, so a
    handler that only needs the path never pays for a read. Because reads are of
    live state, :meth:`read` on an upsert may raise :class:`NotFound` if the path
    was superseded — catch it and skip.
    """

    type: str
    path: str
    event: Event
    source: "FsSource"
    #: True when this change came from the cold-start tree snapshot rather than a
    #: live feed event (a handler may want to bulk-load snapshot upserts).
    snapshot: bool = False
    _content: Optional[bytes] = field(default=None, repr=False, compare=False)
    _read_done: bool = field(default=False, repr=False, compare=False)

    def read(self) -> bytes:
        """Current file bytes. Cached. Raises :class:`NotFound` if gone."""
        if not self._read_done:
            self._content = self.source.read(self.path)
            self._read_done = True
        assert self._content is not None
        return self._content

    def try_read(self) -> Optional[bytes]:
        """:meth:`read`, but ``None`` instead of raising on a superseded path."""
        try:
            return self.read()
        except NotFound:
            return None

    def blame(self) -> list[BlameSpan]:
        return self.source.blame(self.path)

    def stat(self) -> Stat:
        return self.source.stat(self.path)

    def content_hash(self, algo: str = "sha256") -> str:
        """A stable digest of the current bytes — a good idempotency/dedup key for
        a sink. (This is a client-side hash of the whole file, not origofs's
        internal chunk manifest hash, which the HTTP API does not expose.)"""
        return hashlib.new(algo, self.read()).hexdigest()


# --- the read side (pluggable transport) ------------------------------------


class FsSource(abc.ABC):
    """The read side of a workspace: the feed plus point reads. Swap the
    implementation to change transport (HTTP, in-process bindings, a fake)."""

    @abc.abstractmethod
    def events(self, since: int, branch: Optional[str] = None) -> list[Event]:
        """Feed events strictly after ``since``, oldest first, bounded in size.
        An empty list means the caller has caught up to the head."""

    @abc.abstractmethod
    def read(self, path: str) -> bytes:
        """Current bytes of ``path``. Raise :class:`NotFound` if it is gone."""

    @abc.abstractmethod
    def blame(self, path: str) -> list[BlameSpan]:
        ...

    @abc.abstractmethod
    def stat(self, path: str) -> Stat:
        ...

    @abc.abstractmethod
    def list_dir(self, path: str) -> list[DirEntry]:
        ...

    def walk_files(self, root: str = "/") -> Iterator[str]:
        """Yield every file path under ``root`` (depth-first). Symlinks and dirs
        are traversed but only files are yielded — a file's content arrives via
        its own change, so a content index wants files only."""
        stack = [root]
        while stack:
            d = stack.pop()
            for e in self.list_dir(d):
                child = e.name if d == "/" else f"{d}/{e.name}"
                child = "/" + child if not child.startswith("/") else child
                if e.kind == "dir":
                    stack.append(child)
                elif e.kind == "file":
                    yield child
                # symlink: skip; a handler wanting them can override walk_files


class HttpFsSource(FsSource):
    """An :class:`FsSource` over the origofs HTTP API.

    ``base_url`` is the API mount, including any prefix — e.g. the demo server in
    ``examples/web/server`` mounts the workspace router under ``/fs``, so pass
    ``http://127.0.0.1:8000/fs``. ``token`` is the bearer credential your server
    resolves to an actor; reads may or may not require it depending on the
    server's read gate. Transient failures (timeouts, 5xx) are retried here with
    exponential backoff, so the consumer only ever sees terminal outcomes.
    """

    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        *,
        timeout: float = 30.0,
        max_retries: int = 5,
        backoff_base: float = 0.5,
    ) -> None:
        import httpx  # lazy: keep the core dependency-free

        self._base = base_url.rstrip("/")
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._client = httpx.Client(headers=headers, timeout=timeout)
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._httpx = httpx

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HttpFsSource":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _enc(path: str) -> str:
        from urllib.parse import quote

        p = path if path.startswith("/") else "/" + path
        return quote(p, safe="/")  # the {*path} route keeps slashes

    def _get(self, url: str, *, params: Optional[dict] = None) -> "object":
        last: Exception = RuntimeError("unreachable")
        for attempt in range(self._max_retries + 1):
            try:
                r = self._client.get(self._base + url, params=params)
            except self._httpx.TransportError as e:  # timeout, conn reset, DNS…
                last = TransientError(str(e))
            else:
                if r.status_code == 404:
                    raise NotFound(url)
                if r.status_code >= 500:
                    last = TransientError(f"{r.status_code} {url}")
                elif r.status_code >= 400:
                    raise SourceError(f"{r.status_code} {url}: {r.text[:200]}")
                else:
                    return r
            if attempt < self._max_retries:
                time.sleep(self._backoff_base * (2**attempt))
        raise last

    def events(self, since: int, branch: Optional[str] = None) -> list[Event]:
        params: dict = {"since": since}
        if branch:
            params["branch"] = branch
        r = self._get("/events", params=params)
        return [Event.from_json(o) for o in r.json()]  # type: ignore[attr-defined]

    def read(self, path: str) -> bytes:
        return self._get("/files" + self._enc(path)).content  # type: ignore[attr-defined]

    def blame(self, path: str) -> list[BlameSpan]:
        rows = self._get("/blame" + self._enc(path)).json()  # type: ignore[attr-defined]
        return [
            BlameSpan(
                byte_start=b["byte_start"],
                byte_end=b["byte_end"],
                line_start=b["line_start"],
                line_end=b["line_end"],
                actor=b["actor"],
                session=b.get("session"),
                kind=b.get("kind", ""),
            )
            for b in rows
        ]

    def stat(self, path: str) -> Stat:
        o = self._get("/stat" + self._enc(path)).json()  # type: ignore[attr-defined]
        return Stat(kind=o["kind"], size=o["size"], mtime=o["mtime"], ctime=o["ctime"], ino=o["ino"])

    def list_dir(self, path: str) -> list[DirEntry]:
        url = "/dirs" if path in ("", "/") else "/dirs" + self._enc(path)
        rows = self._get(url).json()  # type: ignore[attr-defined]
        return [DirEntry(name=e["name"], kind=e["kind"]) for e in rows]


# --- the cursor (durable resume point) --------------------------------------


class CursorStore(abc.ABC):
    """Where the last durably-processed ``seq`` lives. ``None`` means "never run"
    and triggers the cold-start bootstrap."""

    @abc.abstractmethod
    def load(self) -> Optional[int]:
        ...

    @abc.abstractmethod
    def save(self, seq: int) -> None:
        ...


class MemoryCursorStore(CursorStore):
    """In-process cursor — for tests, or a run that should always cold-start."""

    def __init__(self, seq: Optional[int] = None) -> None:
        self._seq = seq

    def load(self) -> Optional[int]:
        return self._seq

    def save(self, seq: int) -> None:
        self._seq = seq


class FileCursorStore(CursorStore):
    """Cursor in a JSON file, written atomically (temp + ``os.replace``) so a
    crash mid-write cannot corrupt or lose it.

    A file cursor is separate from the sink, so the two can diverge if the process
    dies between the sink write and the cursor save — which is exactly why the
    consumer advances the cursor *after* the handler flushes, making that window
    a safe re-process rather than a lost or duplicated commit. For strict
    exactly-once, store the cursor *inside* the sink's transaction instead (see
    the README) and back it with a :class:`CursorStore` that reads/writes there.
    """

    def __init__(self, path: str) -> None:
        self._path = path

    def load(self) -> Optional[int]:
        try:
            with open(self._path) as f:
                return int(json.load(f)["seq"])
        except FileNotFoundError:
            return None

    def save(self, seq: int) -> None:
        d = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump({"seq": seq, "updated": int(time.time())}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path)
        except BaseException:
            try:
                os.unlink(tmp)
            finally:
                raise


# --- the write side (pluggable sink) ----------------------------------------


class ChangeHandler(abc.ABC):
    """React to changes. Buffer in :meth:`on_upsert` / :meth:`on_delete`, make it
    durable in :meth:`flush` — the consumer advances the cursor only after
    ``flush`` returns, which is what makes delivery at-least-once."""

    @abc.abstractmethod
    def on_upsert(self, change: Change) -> None:
        ...

    @abc.abstractmethod
    def on_delete(self, change: Change) -> None:
        ...

    def on_commit(self, event: Event) -> None:
        """A ``commit`` event — a version boundary. Default: ignore."""

    def on_other(self, event: Event) -> None:
        """A non-content event (``mkdir``/``symlink``/``lock``/``suggest``…).
        Default: ignore."""

    def on_snapshot_begin(self) -> None:
        """Cold-start tree walk is about to emit upserts for every current file."""

    def on_snapshot_end(self) -> None:
        ...

    def begin_batch(self) -> None:
        """Called before a batch is dispatched — and again before each retry of
        that batch. A buffering handler must reset its per-batch buffer here so a
        retry re-buffers from scratch rather than duplicating the first attempt."""

    def flush(self) -> None:
        """Make everything buffered since the last flush durable. Raise to abort
        the batch (the cursor is not advanced; the batch re-runs on restart)."""


class LoggingHandler(ChangeHandler):
    """A no-op sink that logs each change — a live smoke test of the feed."""

    def on_upsert(self, change: Change) -> None:
        body = change.try_read()
        n = "gone" if body is None else f"{len(body)}B"
        log.info("upsert %s (%s)%s", change.path, n, " [snapshot]" if change.snapshot else "")

    def on_delete(self, change: Change) -> None:
        log.info("delete %s", change.path)


class JsonlSink(ChangeHandler):
    """Append every change to a JSON-lines file — a complete, dependency-free
    reference sink. Buffers a batch and flushes with ``fsync`` so the cursor and
    the file advance together. Not a real search index, but the exact shape one
    plugs in: replace :meth:`flush`'s file append with a BigQuery load / an
    Elasticsearch bulk request / a Kafka produce."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._buf: list[dict] = []

    def begin_batch(self) -> None:
        self._buf.clear()  # a retry re-buffers from scratch

    def on_upsert(self, change: Change) -> None:
        body = change.try_read()
        if body is None:
            return  # superseded by a later remove/rename; its event handles it
        text = body.decode("utf-8", "replace")
        self._buf.append(
            {
                "op": "upsert",
                "seq": change.event.seq,
                "path": change.path,
                "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "text": text,
                "blame": [dataclasses.asdict(b) for b in change.blame()],
                "snapshot": change.snapshot,
            }
        )

    def on_delete(self, change: Change) -> None:
        self._buf.append({"op": "delete", "seq": change.event.seq, "path": change.path})

    def flush(self) -> None:
        if not self._buf:
            return
        with open(self._path, "a") as f:
            for row in self._buf:
                f.write(json.dumps(row) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self._buf.clear()


# --- the consumer (catch-up + tail; not pluggable) --------------------------


@dataclass
class _Op:
    type: str  # ChangeType.UPSERT | DELETE
    path: str
    event: Event


class Consumer:
    """Drives a :class:`ChangeHandler` from an :class:`FsSource`, resuming from a
    :class:`CursorStore`, with a no-gap cold-start bootstrap.

    ``backfill`` selects the cold-start (cursor is ``None``) behavior:

    * ``"snapshot"`` (default) — capture the feed head, walk the tree as upserts,
      then tail from the head. O(files) up front; the index is complete
      immediately.
    * ``"replay"`` — skip the walk and tail from seq 0. Correct because the feed
      is never pruned; simplest when the feed is short.
    * ``"none"`` — index only changes from *now* on (tail from the current head,
      no history).

    ``coalesce`` (default on) collapses repeated events for the same path within a
    drained batch to a single terminal op — fewer reads when catching up on a
    backlog. Turn it off if a handler needs every event in exact order.
    """

    def __init__(
        self,
        source: FsSource,
        handler: ChangeHandler,
        cursor: CursorStore,
        *,
        branch: Optional[str] = None,
        backfill: str = "snapshot",
        coalesce: bool = True,
        poll_interval: float = 2.0,
        snapshot_root: str = "/",
        on_error_backoff: float = 2.0,
        max_batch_retries: int = 6,
    ) -> None:
        if backfill not in ("snapshot", "replay", "none"):
            raise ValueError(f"backfill must be snapshot|replay|none, got {backfill!r}")
        self.source = source
        self.handler = handler
        self.cursor = cursor
        self.branch = branch
        self.backfill = backfill
        self.coalesce = coalesce
        self.poll_interval = poll_interval
        self.snapshot_root = snapshot_root
        self.on_error_backoff = on_error_backoff
        self.max_batch_retries = max_batch_retries
        self._stop = False
        self._booted = False

    def stop(self) -> None:
        """Ask :meth:`run_forever` to finish the current batch and return."""
        self._stop = True

    # -- public entry points --------------------------------------------------

    def run_once(self) -> bool:
        """Bootstrap if needed, then drain the feed to the current head exactly
        once. Returns True if any event was processed. Ideal for a cron/Lambda."""
        start = self._bootstrap_if_needed()
        return self._drain(start)

    def run_forever(self) -> None:
        """Bootstrap, then tail forever: drain to the head, sleep ``poll_interval``,
        repeat. Call :meth:`stop` (e.g. from a signal handler) to exit cleanly.

        For a Postgres-backed workspace you can replace the poll with the push
        feed (``LISTEN/NOTIFY`` via the ``origofs`` bindings' ``subscribe``); the
        bootstrap/drain logic here is unchanged — only the wait between drains is."""
        start = self._bootstrap_if_needed()
        cursor = start
        while not self._stop:
            advanced = self._drain(cursor, _return_cursor=True)
            cursor = advanced
            if self._stop:
                break
            time.sleep(self.poll_interval)

    # -- bootstrap ------------------------------------------------------------

    def _bootstrap_if_needed(self) -> int:
        if self._booted:
            loaded = self.cursor.load()
            return loaded if loaded is not None else 0
        self._booted = True
        loaded = self.cursor.load()
        if loaded is not None:
            log.info("resuming from cursor seq=%d", loaded)
            return loaded
        return self._bootstrap_cold()

    def _bootstrap_cold(self) -> int:
        if self.backfill == "replay":
            log.info("cold start: replaying the full feed from seq=0")
            self.cursor.save(0)
            return 0

        # Capture the head BEFORE any snapshot, so the tail replays whatever
        # changes concurrently with the walk (idempotently) — no gap.
        head = self._feed_head()
        if self.backfill == "none":
            log.info("cold start: skipping history, tailing from head seq=%d", head)
            self.cursor.save(head)
            return head

        log.info("cold start: snapshotting tree, then tailing from head seq=%d", head)
        self.handler.on_snapshot_begin()
        n = 0
        for path in self.source.walk_files(self.snapshot_root):
            ev = Event(seq=head, kind=KIND_WRITE, path=path, branch=self.branch)
            self.handler.on_upsert(Change(ChangeType.UPSERT, path, ev, self.source, snapshot=True))
            n += 1
        self.handler.on_snapshot_end()
        self.handler.flush()  # durable before we commit to tailing from `head`
        self.cursor.save(head)
        log.info("snapshot complete: %d files, cursor=%d", n, head)
        return head

    def _feed_head(self) -> int:
        """The current maximum ``seq`` in the feed (0 if empty), found by paging to
        the end. Cold-start only; keeps just a running max, so O(1) memory."""
        cur = 0
        while True:
            batch = self.source.events(cur, self.branch)
            if not batch:
                return cur
            cur = batch[-1].seq

    # -- drain / tail ---------------------------------------------------------

    def _drain(self, cursor: int, _return_cursor: bool = False) -> "bool | int":
        """Process batches until the feed is drained to the head. Advances the
        cursor after each batch is flushed. Returns whether anything was processed
        (run_once) or the final cursor (run_forever)."""
        processed = False
        while not self._stop:
            batch = self.source.events(cursor, self.branch)
            if not batch:
                break
            self._handle_batch_with_retry(batch)
            cursor = batch[-1].seq
            self.cursor.save(cursor)  # advance ONLY after a durable flush
            processed = True
        return cursor if _return_cursor else processed

    def _handle_batch_with_retry(self, batch: list[Event]) -> None:
        """Dispatch a batch and flush it, retrying transient failures with
        backoff. On persistent failure, raise — leaving the cursor un-advanced so
        a restart re-processes this batch (at-least-once)."""
        for attempt in range(self.max_batch_retries + 1):
            try:
                self.handler.begin_batch()
                self._dispatch(batch)
                self.handler.flush()
                return
            except (TransientError, ConnectionError, TimeoutError) as e:
                if attempt >= self.max_batch_retries:
                    raise
                wait = self.on_error_backoff * (2**attempt)
                log.warning("batch @seq<=%d failed (%s); retry in %.1fs", batch[-1].seq, e, wait)
                time.sleep(wait)

    def _dispatch(self, batch: list[Event]) -> None:
        if self.coalesce:
            self._dispatch_coalesced(batch)
        else:
            for ev in batch:
                self._dispatch_event(ev)

    def _dispatch_event(self, ev: Event) -> None:
        """Exact-order dispatch of a single event."""
        if ev.kind == KIND_WRITE:
            self.handler.on_upsert(Change(ChangeType.UPSERT, ev.path, ev, self.source))
        elif ev.kind == KIND_REMOVE:
            self.handler.on_delete(Change(ChangeType.DELETE, ev.path, ev, self.source))
        elif ev.kind == KIND_RENAME:
            self.handler.on_delete(Change(ChangeType.DELETE, ev.path, ev, self.source))
            if ev.detail:  # detail carries the rename target (the new path)
                self.handler.on_upsert(Change(ChangeType.UPSERT, ev.detail, ev, self.source))
        elif ev.kind == KIND_COMMIT:
            self.handler.on_commit(ev)
        else:
            self.handler.on_other(ev)

    def _dispatch_coalesced(self, batch: list[Event]) -> None:
        """Collapse content events per path to their terminal op, so a path
        written five times then deleted in one drained batch is handled once.
        ``commit``/other events still fire per-event, in order."""
        terminal: "dict[str, _Op]" = {}
        for ev in batch:
            if ev.kind == KIND_WRITE:
                terminal[ev.path] = _Op(ChangeType.UPSERT, ev.path, ev)
            elif ev.kind == KIND_REMOVE:
                terminal[ev.path] = _Op(ChangeType.DELETE, ev.path, ev)
            elif ev.kind == KIND_RENAME:
                terminal[ev.path] = _Op(ChangeType.DELETE, ev.path, ev)
                if ev.detail:
                    terminal[ev.detail] = _Op(ChangeType.UPSERT, ev.detail, ev)
            elif ev.kind == KIND_COMMIT:
                self.handler.on_commit(ev)
            else:
                self.handler.on_other(ev)
        for op in terminal.values():
            change = Change(op.type, op.path, op.event, self.source)
            if op.type == ChangeType.UPSERT:
                self.handler.on_upsert(change)
            else:
                self.handler.on_delete(change)


def build_http_consumer(
    base_url: str,
    handler: ChangeHandler,
    *,
    token: Optional[str] = None,
    cursor_path: str = ".origofs-cursor.json",
    **kwargs: object,
) -> Consumer:
    """Convenience wiring: an HTTP source + a file cursor + your handler."""
    return Consumer(
        HttpFsSource(base_url, token),
        handler,
        FileCursorStore(cursor_path),
        **kwargs,  # type: ignore[arg-type]
    )


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Tail an origofs workspace to a JSONL file.")
    p.add_argument("base_url", help="origofs API mount, e.g. http://127.0.0.1:8000/fs")
    p.add_argument("--token", help="bearer token (if the server gates reads)")
    p.add_argument("--out", default="changes.jsonl", help="JSONL sink path")
    p.add_argument("--cursor", default=".origofs-cursor.json")
    p.add_argument("--branch", help="restrict to one branch")
    p.add_argument("--backfill", default="snapshot", choices=["snapshot", "replay", "none"])
    p.add_argument("--once", action="store_true", help="drain to head and exit")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = build_http_consumer(
        args.base_url,
        JsonlSink(args.out),
        token=args.token,
        cursor_path=args.cursor,
        branch=args.branch,
        backfill=args.backfill,
    )
    if args.once:
        consumer.run_once()
    else:
        import signal

        signal.signal(signal.SIGINT, lambda *_: consumer.stop())
        signal.signal(signal.SIGTERM, lambda *_: consumer.stop())
        consumer.run_forever()
