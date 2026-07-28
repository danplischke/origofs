# origofs change consumer

A small, generic library for turning an origofs workspace into a reliable stream
of file changes — to feed a search index (BigQuery, Elasticsearch…), a mirror, a
webhook, or anything else. It is deliberately not tied to any one sink: you plug
in a handler and get **correct catch-up and resume** for free.

Why this is easy on origofs: the workspace is already a change-data-capture
source. Every user/agent action appends to an **append-only feed** with a
monotonic `seq` cursor (`GET /fs/events?since=`), and content is hash-addressed,
so re-applying an unchanged file is a no-op. You tail the feed; you don't crawl.

```
origofs feed ──> FsSource ──> Consumer ──> ChangeHandler (your sink)
                              ▲    │
                       CursorStore ┘   (durable resume point)
```

## Files

| File | What |
|---|---|
| `origofs_fs_consumer.py` | The library: `Consumer` + the three pluggable pieces. No third-party deps (the HTTP source imports `httpx` lazily). |
| `test_fs_consumer.py` | `unittest` proving the catch-up guarantees against an in-memory fake — no network, no origofs build. |
| `bigquery_sink.py` | A concrete sink: append-log + `file_current` view + hash-deduped content + `SEARCH()`, with the cursor stored *in* BigQuery for exactly-once. |

## Quickstart

```bash
# 1. Run a workspace server (the demo in examples/web/server mounts it at /fs):
#    uvicorn app:app  →  http://127.0.0.1:8000/fs

# 2. Tail it to a JSONL file (snapshot the tree, then follow live):
python origofs_fs_consumer.py http://127.0.0.1:8000/fs --token alice-dev --out changes.jsonl

# drain-and-exit (for cron/Lambda) instead of following:
python origofs_fs_consumer.py http://127.0.0.1:8000/fs --token alice-dev --once
```

```bash
# Run the tests:
python -m unittest test_fs_consumer -v
```

## Catch-up: the part that's easy to get wrong

Delivery is **at-least-once with a no-gap bootstrap**. Three cases:

1. **Cold start (`backfill="snapshot"`, default).** Capture the feed head
   *first*, then walk the current tree as synthetic upserts, then tail from the
   captured head. Anything written *during* the walk has `seq > head`, so the
   tail replays it — and because upserts are keyed by content, replaying an
   already-indexed file is idempotent. No gap, no double-count.

2. **Resume.** The cursor is persisted **only after** a batch is durably
   flushed. A crash between the sink write and the cursor save re-processes the
   last batch on restart (safe, because upserts/deletes are idempotent) rather
   than skipping it.

3. **Stale reads.** Reads reflect the *current* file, so an old `write` event
   whose path was since removed raises `NotFound` — treated as a no-op, since the
   removing event is in the feed too. The index is **eventually consistent** with
   the live tree, which is what a search index wants (it converges on freshest
   content).

Other bootstrap modes:

- `backfill="replay"` — tail from `seq 0`, no tree walk. Correct because the feed
  is never pruned; simplest when the feed is short.
- `backfill="none"` — index only changes from now on.

Batches are **coalesced** by default: a path written five times then deleted in
one drained catch-up is handled once (its terminal state), so a large backlog
doesn't read every intermediate version. Set `coalesce=False` if your sink needs
every event in exact order.

## Writing a sink

Subclass `ChangeHandler`. Buffer per batch, make it durable in `flush()` — the
consumer advances the cursor only after `flush()` returns.

```python
from origofs_fs_consumer import ChangeHandler, Change, Consumer, HttpFsSource, FileCursorStore

class MySink(ChangeHandler):
    def begin_batch(self):          # reset the buffer (also called on each retry)
        self.rows = []
    def on_upsert(self, ch: Change):
        body = ch.try_read()        # current bytes; None if superseded
        if body is None: return
        self.rows.append((ch.path, body, ch.blame()))   # blame() = who wrote each byte range
    def on_delete(self, ch: Change):
        self.rows.append((ch.path, None, None))
    def flush(self):                # persist self.rows atomically; raise to retry the batch
        my_index.bulk_write(self.rows)

Consumer(HttpFsSource("http://127.0.0.1:8000/fs", token="…"),
         MySink(), FileCursorStore(".cursor.json")).run_forever()
```

Handler contract:

| Method | When |
|---|---|
| `on_upsert(change)` / `on_delete(change)` | a file's content changed / was removed. `change.read()`, `.blame()`, `.stat()`, `.content_hash()` fetch lazily. |
| `on_commit(event)` | a `commit` — a version boundary. |
| `on_other(event)` | a non-content event (`mkdir`/`symlink`/`lock`/`suggest`…). |
| `begin_batch()` | before a batch (and before each retry) — reset your buffer here. |
| `flush()` | make the batch durable; the cursor advances only after this returns. |

`change.blame()` is the reason to source from origofs rather than a plain file
crawl: it gives per-byte authorship, so your index can answer "who wrote the
lines that match this query," not just "which files match."

## Exactly-once

The default `FileCursorStore` is separate from your sink, so a crash between the
sink write and the cursor save yields a safe *re-process* (at-least-once). To get
exactly-once, store the cursor **inside the sink's transaction** — write the data
and the new `seq` atomically. `bigquery_sink.py` shows this: the cursor lives in
an `ingest_state` table advanced in the same flush as the data, so the two can
never disagree.

## Swapping the transport

`HttpFsSource` is the only piece that knows about HTTP. Implement `FsSource`
(`events`, `read`, `blame`, `stat`, `list_dir`) over the in-process `origofs`
Python bindings instead, and the entire catch-up machinery is unchanged. On a
Postgres-backed workspace you can also replace the poll loop with the push feed
(`LISTEN/NOTIFY` via the bindings' `subscribe`) — same bootstrap, just a
different wait between drains.
```
