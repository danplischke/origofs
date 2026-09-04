# Storage backends

A chunk's identity is its BLAKE3 hash, so deduplication, versioning and integrity
hold no matter where the bytes live. What changes between backends is cost,
latency and operational shape.

Configure them with [a TOML file](configuration.md#the-file), or directly with
the `Workspace::open_*` constructors.

## Content backends

### Local

A sharded content-addressed directory (`objects/aa/bbbb…`). `Workspace::open_local`.
The default, and the right choice for solo work.

### Object storage

Content-defined chunking keeps edits cheap — only changed chunks re-upload.

**S3 / R2 / MinIO** — `Workspace::open_s3`. Also covers GCS through its S3-interop
XML API with HMAC keys. Exercised in CI against a live MinIO on every push.

**Google Cloud Storage, natively** — `Workspace::open_gcs`, over the GCS JSON API
with OAuth2: a service-account key or file, Application Default Credentials, or
GKE workload identity. No HMAC keys needed.

!!! warning "Native GCS is not exercised against a live backend"

    Its builder — credential precedence, plaintext-endpoint handling — has unit
    coverage, and everything past construction is the same object-store code the
    MinIO leg runs end to end. But no CI job has ever pointed native GCS at a
    real bucket, because no GCS emulator can stand in for one: the object-store
    layer writes with a bare XML-API `PUT`, and the emulators do not serve that
    shape, so every write is rejected before it stores anything.

    The suite exists and passes against a real bucket; it needs credentials CI
    does not have. **Prefer the S3-interop flavour if you want the path with
    continuous coverage**, and validate `open_gcs` against your own bucket before
    relying on it.

    See [Deploying on GCS + Postgres](../DEPLOY-GCS-POSTGRES.md).

## The stack

Content backends compose as decorators. The `open_*` constructors are the
canonical recipes, and a real production stack looks like this:

```text
VerifyingStore( PackStore( ObjectContentStore::s3, index_dir ) )
```

| Layer | What it does |
|---|---|
| `PackStore` | Batches chunks into large pack objects — a few big PUTs instead of thousands of tiny ones — with a local per-chunk index for single ranged-GET reads. Needs `flush` and `repack`. |
| `VerifyingStore` | Re-hashes on read. A bit-rotted or tampered object surfaces as an error instead of being served as authentic. |
| `EncryptedStore` | XChaCha20-Poly1305 at rest. Addresses stay the *plaintext* hash, so dedup still works. |
| `TieredStore` | A local cache tier in front of a remote one. |
| `MemStore` | In-memory. Tests, and `open_object_memory`. |

!!! info "`VerifyingStore` goes on the outside"

    Integrity has to be checked at the chunk-addressed boundary a caller actually
    reads by. Underneath a pack or cache layer, it would be verifying something
    else.

## Packing

`packed = true` is the right default for a metered object store: it turns
thousands of small PUTs into a few large ones. Two consequences to plan for.

**`repack` is required to reclaim space.** `gc` marks objects dead, but the space
sits inside pack objects until they are compacted. See
[Reclaiming space](../guides/backup-and-recovery.md#reclaiming-space).

**The index is local, so it is single-writer-per-index.** With more than one
replica you must either give each its own index directory on a persistent volume,
and accept that they cannot share deduplication, or leave `packed = false`.

## Caching

Without a [read cache](configuration.md#cache), every read fetches every covering
chunk over the network, every time. With one, reads come off local disk, bounded
and LRU-evicted.

It is refused alongside a local content store, and alongside encryption at rest —
both for reasons that are about correctness, not policy. See
[Configuration](configuration.md#cache).

## Metadata backends

| | Use it for |
|---|---|
| **SQLite** | Solo and offline. One portable file, full speed, no server. |
| **Postgres** | Multi-writer and production. Serialized atomic-create, a transactional write path, and `LISTEN`/`NOTIFY` push for the change feed. |

Dialect differences are hidden behind one trait, so nothing above the store knows
which is in use — with one deliberate exception: the push feed (`subscribe`) is
Postgres-only and not on the object-safe trait. SQLite callers poll with `watch`.

Migrations are forward-only and authored once, with per-engine SQL where the two
diverge. A normal `open` applies pending migrations; `origofs migrate` is the
explicit runner.

Moving between them is [`dump` and
`load`](../guides/backup-and-recovery.md#moving-between-backends).

## Measuring your own

Published benchmark numbers depend on someone else's bucket and someone else's
latency. Two commands measure yours:

```bash
origofs --workspace "$WS" bench          # write N files, read them back twice
origofs --workspace "$WS" info /big.bin  # chunk count, size distribution, self-dedup
```
