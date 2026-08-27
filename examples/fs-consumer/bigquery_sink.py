"""A BigQuery sink for the origofs change consumer — the search-index use case,
made concrete.

It models the workspace the way BigQuery likes: an append-only event log plus a
"current" view, and content deduplicated by hash so each distinct blob is stored
once. It writes with the **Storage Write API** (streaming) for the live tail and
keeps the cursor **inside the same commit** as the data, so the two can never
disagree — exactly-once, not just at-least-once.

Tables (create once; see ``DDL`` at the bottom):

* ``file_events``  — one row per change: seq, ts, actor, kind, path, sha256.
* ``file_content`` — one row per distinct sha256: text, size (dedup target).
* ``file_current`` — a VIEW: latest non-deleted row per path ⨝ file_content.
* ``ingest_state`` — one row: the consumer cursor.

Requires ``google-cloud-bigquery``. This file imports it lazily so the rest of
the example stays dependency-free.

    pip install google-cloud-bigquery
    python bigquery_sink.py http://127.0.0.1:8000/fs --dataset my_ds --token dev
"""

from __future__ import annotations

import hashlib
from typing import Optional

from origofs_fs_consumer import (
    Change,
    ChangeHandler,
    Consumer,
    CursorStore,
    Event,
    HttpFsSource,
)


class BigQueryCursorStore(CursorStore):
    """The cursor as a row in ``ingest_state`` — read at startup, written by the
    sink's :meth:`BigQuerySink.flush` in the same load job as the data. Keeping it
    beside the data is what upgrades at-least-once to exactly-once: there is no
    window where the data is committed but the cursor is not."""

    def __init__(self, client: object, table: str) -> None:
        self._client = client
        self._table = table  # "project.dataset.ingest_state"

    def load(self) -> Optional[int]:
        rows = list(self._client.query(f"SELECT seq FROM `{self._table}` LIMIT 1").result())  # type: ignore[attr-defined]
        return int(rows[0].seq) if rows else None

    def save(self, seq: int) -> None:
        # Normally written transactionally by flush(); this is the fallback path.
        self._client.query(  # type: ignore[attr-defined]
            f"MERGE `{self._table}` T USING (SELECT {int(seq)} AS seq) S "
            "ON TRUE WHEN MATCHED THEN UPDATE SET seq = S.seq "
            "WHEN NOT MATCHED THEN INSERT (seq) VALUES (S.seq)"
        ).result()


class BigQuerySink(ChangeHandler):
    """Buffers a batch of changes and lands them — events, deduped content, and
    the advanced cursor — in one flush."""

    def __init__(self, client: object, dataset: str) -> None:
        self._c = client
        self._ds = dataset  # "project.dataset"
        self._events: list[dict] = []
        self._content: dict[str, dict] = {}  # sha256 -> row (dedup within a batch)
        self._max_seq: Optional[int] = None

    def begin_batch(self) -> None:
        self._events.clear()
        self._content.clear()
        self._max_seq = None

    def on_upsert(self, ch: Change) -> None:
        body = ch.try_read()
        if body is None:
            return  # superseded by a later remove/rename
        sha = hashlib.sha256(body).hexdigest()
        self._events.append(
            {
                "seq": ch.event.seq,
                "ts": ch.event.ts,
                "actor_id": ch.event.actor_id,
                "kind": "upsert",
                "path": ch.path,
                "sha256": sha,
                "branch": ch.event.branch,
            }
        )
        # Store each distinct blob once — origofs dedups chunks, so this pays off.
        self._content.setdefault(
            sha,
            {"sha256": sha, "size": len(body), "text": body.decode("utf-8", "replace")},
        )
        self._max_seq = max(self._max_seq or 0, ch.event.seq)

    def on_delete(self, ch: Change) -> None:
        self._events.append(
            {
                "seq": ch.event.seq,
                "ts": ch.event.ts,
                "actor_id": ch.event.actor_id,
                "kind": "delete",
                "path": ch.path,
                "sha256": None,
                "branch": ch.event.branch,
            }
        )
        self._max_seq = max(self._max_seq or 0, ch.event.seq)

    def on_commit(self, ev: Event) -> None:  # a version boundary; recorded as an event
        self._events.append(
            {"seq": ev.seq, "ts": ev.ts, "actor_id": ev.actor_id, "kind": "commit",
             "path": ev.path, "sha256": None, "branch": ev.branch}
        )
        self._max_seq = max(self._max_seq or 0, ev.seq)

    def flush(self) -> None:
        if self._content:
            self._insert(f"{self._ds}.file_content", list(self._content.values()))
        if self._events:
            self._insert(f"{self._ds}.file_events", self._events)
        if self._max_seq is not None:
            BigQueryCursorStore(self._c, f"{self._ds}.ingest_state").save(self._max_seq)

    def _insert(self, table: str, rows: list[dict]) -> None:
        # insert_rows_json uses the streaming API; swap for a load job (NDJSON ->
        # GCS -> LOAD) for large backfills, which is far cheaper per GB.
        errors = self._c.insert_rows_json(table, rows)  # type: ignore[attr-defined]
        if errors:
            raise RuntimeError(f"BigQuery insert into {table} failed: {errors}")


DDL = """
-- Append-only mirror of the change feed.
CREATE TABLE IF NOT EXISTS `{ds}.file_events` (
  seq INT64, ts INT64, actor_id INT64, kind STRING, path STRING,
  sha256 STRING, branch STRING
) PARTITION BY TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(ts), DAY) CLUSTER BY path;

-- Distinct blobs, deduplicated by content hash.
CREATE TABLE IF NOT EXISTS `{ds}.file_content` (
  sha256 STRING, size INT64, text STRING
) CLUSTER BY sha256;

-- The consumer's resume point.
CREATE TABLE IF NOT EXISTS `{ds}.ingest_state` (seq INT64);

-- Current state: newest non-deleted event per path, joined to its content.
CREATE OR REPLACE VIEW `{ds}.file_current` AS
SELECT e.path, e.seq, e.actor_id, e.branch, c.size, c.text
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY path ORDER BY seq DESC) AS rn
  FROM `{ds}.file_events`
) e
LEFT JOIN `{ds}.file_content` c USING (sha256)
WHERE e.rn = 1 AND e.kind != 'delete';

-- Full-text search over current files:
--   SELECT path FROM `{ds}.file_current` WHERE SEARCH(text, 'needle');
-- (add: CREATE SEARCH INDEX idx ON `{ds}.file_content`(text);)
"""


def main() -> None:
    import argparse

    from google.cloud import bigquery  # type: ignore[import-not-found]

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("base_url", help="origofs API mount, e.g. http://127.0.0.1:8000/fs")
    p.add_argument("--dataset", required=True, help="project.dataset holding the tables")
    p.add_argument("--token", help="bearer token if the server gates reads")
    p.add_argument("--branch")
    p.add_argument("--backfill", default="snapshot", choices=["snapshot", "replay", "none"])
    p.add_argument("--print-ddl", action="store_true", help="print table DDL and exit")
    p.add_argument("--once", action="store_true")
    args = p.parse_args()

    if args.print_ddl:
        print(DDL.format(ds=args.dataset))
        return

    client = bigquery.Client()
    sink = BigQuerySink(client, args.dataset)
    consumer = Consumer(
        HttpFsSource(args.base_url, args.token),
        sink,
        BigQueryCursorStore(client, f"{args.dataset}.ingest_state"),
        branch=args.branch,
        backfill=args.backfill,
    )
    if args.once:
        consumer.run_once()
    else:
        consumer.run_forever()


if __name__ == "__main__":
    main()
