# Backup and recovery

Two stores, two very different jobs. Getting this distinction right is the
difference between an inconvenience and losing the thing origofs is for.

## What to back up

**The content store needs no backup from origofs.** It is immutable and
content-addressed, so whatever durability your bucket or filesystem already
provides is the whole story.

**The metadata database is the irreplaceable half.** `origofs fsck --rebuild`
reconstructs every committed file, directory, symlink and branch from the content
store alone — but **blame, the audit log, the actor registry and every
uncommitted edit exist only in the database.**

!!! danger "Back up the database"

    Losing the content store costs you bytes you can often re-derive. Losing the
    metadata database costs you attribution, which nothing can reconstruct.

## Taking a backup

```bash
origofs --workspace ./ws backup ./backups/meta-$(date +%F).db
```

SQLite is snapshotted with SQLite's own online backup API, so writers keep
running while it is taken. The command refuses to overwrite an existing file, so
a scheduled backup cannot quietly destroy the previous one.

!!! warning "Do not substitute `cp meta.db`"

    A live database has a `-wal` sidecar and may be mid-transaction. The copy
    *often* restores — which is exactly what makes it dangerous.

### Postgres

`origofs backup` deliberately **refuses** on Postgres rather than producing
something that merely resembles a backup. Use `pg_dump`, or continuous archiving
(PITR) — both give a consistent snapshot of a live database, and PITR
additionally bounds how much you can lose.

## Restoring

Put the snapshot back where the workspace expects it, alongside the **same
content store**. The snapshot is metadata only.

```bash
# stop anything serving the workspace first
rm -f ./ws/meta.db ./ws/meta.db-wal ./ws/meta.db-shm
cp ./backups/meta-2026-01-31.db ./ws/meta.db
origofs --workspace ./ws schema-version    # sanity-check, then restart
```

The Postgres procedure has the same shape: restore the database, keep the content
store as it is, then check `schema-version` before starting the new binaries.

## Moving between backends

`dump` and `load` write an engine-independent snapshot of the whole metadata
store as JSON Lines, which is how a workspace moves from SQLite to Postgres or
the other way:

```bash
origofs --workspace ./ws dump ./ws.jsonl
origofs --config pg.toml load ./ws.jsonl
```

`load` requires a **pristine** workspace — it will not merge into an existing
one. Both accept `-` for stdout/stdin.

## Recovering from a lost database

If the metadata database is gone and there is no backup, the content store can
still be walked:

```bash
origofs --workspace ./ws fsck               # report only — what could be recovered
origofs --workspace ./ws fsck --rebuild     # rebuild refs + working tree onto a fresh DB
```

This scans the object graph — commits, trees, chunks, the mirrored ref table —
and restores committed files, directories, symlinks and branches. It **does not**
recover blame, the audit log, actors, or uncommitted edits, because those never
lived in the content store.

`fsck` without `--rebuild` is read-only, and is worth running as a periodic
integrity check on its own.

## Reclaiming space

Content is immutable and never overwritten, so churn leaves orphaned chunks.

```bash
origofs --workspace "$WS" gc        # mark-and-sweep from live refs
origofs --workspace "$WS" repack    # packed stores: actually return the space
origofs --workspace "$WS" flush     # seal buffered writes to durable storage
```

`gc` is safe to run **alongside active writers**. It does not quiesce the store;
it works by an age gate, because content is written before the metadata that
references it, so every in-flight write has a window where its chunks look
unreferenced. Three parts make that hold, and all three matter: the sweep skips
anything younger than the grace period, a deduplicating write refreshes an object
that has gone stale, and the sweep re-checks an object's age at the moment it
deletes it — so a long pass cannot act on an age it read minutes earlier.

!!! note

    A content backend that cannot date its objects collects nothing, by design.

Packed stores need `repack` in addition to `gc`: `gc` marks objects dead, but the
space sits inside pack objects until they are compacted.

## Integrity

Content is verified against its hash on every read, so bit-rot or tampering
surfaces as an error rather than being served as authentic. That check happens at
the chunk-addressed boundary a caller reads by — see
[Storage backends](../reference/storage-backends.md#the-stack).

To probe that the backends are reachable at all, without doing any work:

```bash
curl http://host:8080/readyz     # distinct from liveness at /health
```

## Schema migrations

Migrations are forward-only. A normal `open` already applies any pending ones;
the explicit runner is there for a controlled rollout:

```bash
origofs --workspace "$WS" schema-version   # this workspace, and what the binary knows
origofs --workspace "$WS" migrate          # apply pending migrations
```
