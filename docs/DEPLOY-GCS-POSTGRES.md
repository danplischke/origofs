# Deploying on Postgres + native GCS

The Google Cloud pairing: `PostgresMetadataStore` for names and versions, a GCS
bucket for content. This is the composition `Workspace::open_pg_gcs` (and its
`_packed` / `_encrypted` variants) builds, and what
`crates/origofs-core/tests/pg_object_store.rs` exercises.

This page is the set of things that are *not* obvious from the constructor
signature and will otherwise be discovered in production.

```python
import origofs

cfg = origofs.GcsConfig(bucket="my-origofs-content", prefix="objects")
ws = await origofs.Workspace.open_pg_gcs(
    "host=10.0.0.5 user=origofs dbname=origofs sslmode=require", cfg
)
```

## Credentials

`GcsConfig` resolves in this order, and stops at the first that is set:

1. `service_account_key` (the JSON key file's *contents*) or `service_account_path`;
2. `application_credentials` (an ADC file), else `GOOGLE_APPLICATION_CREDENTIALS`
   or the well-known `gcloud` location;
3. the GKE/GCE metadata server.

On **workload identity, set none of them** — that is what makes (3) apply. Passing
an explicit key alongside an environment-provided one is rejected by the builder,
so the config starts clean when you set one explicitly and from the environment
when you do not.

`allow_http` exists only to point at a local emulator. Real GCS is always HTTPS;
leave it `false`.

## Cloud SQL requires TLS

Managed Postgres will not accept a plaintext connection. origofs ships a rustls
connector for this — it is not `NoTls` — but it needs to trust the server:

```
ORIGOFS_PG_CA_FILE=/var/secrets/server-ca.pem
```

Point it at the instance's server CA. Without it the handshake fails with a
certificate error rather than falling back to plaintext, which is the correct
behaviour but is not always obvious from the message. The TLS path has its own CI
leg (`pg-tls`), which asserts encryption via `pg_stat_ssl` rather than inferring
it from the connection succeeding.

If you connect through the Cloud SQL Auth Proxy over a Unix socket instead, note
that the TLS connector cannot parse a socket path as a DNS name — use the proxy's
TCP listener (`host=127.0.0.1`), which is what the proxy is for.

## `packed` is single-writer-per-index

`open_pg_gcs_packed` batches chunks into few large objects, which is the right
instinct against a per-request-billed store. But **the per-chunk index is a local
directory**, not part of the bucket. Two replicas with separate index directories
cannot see each other's chunks.

So, with more than one replica:

- either give each replica its own index on a persistent volume, and accept that
  they deduplicate independently;
- or use the unpacked `open_pg_gcs` and let GCS take the object count.

And note what packing actually buys: batching happens *within* a write, not across
writes, because each write's content must be durable before its metadata commits.
One large file becomes a handful of PUTs — the case it is for. Ten thousand small
files are still ten thousand writes and roughly ten thousand PUTs. If you are bulk
importing, fewer and larger writes (stream an archive through `write_reader`)
matter far more than `packed`.

## Garbage collection

`gc()` is safe to run alongside writers — the sweep skips anything younger than
the grace period, and a deduplicating write refreshes content that has gone stale.
On an object store that refresh is a re-PUT (there is no `utimes`), age-gated so it
only fires for objects old enough to actually be at risk; deduplicating onto recent
content costs nothing extra.

Two things to plan for:

- **Run it.** Nothing runs it for you. `origofs serve` reaps presence on a timer
  but does not collect; schedule `gc()` (or `origofs gc`) yourself.
- **On a packed store, `gc()` alone does not return space.** It drops index
  entries; `repack()` is what rewrites packs and actually shrinks the bucket.

`gc()` on a bucket with millions of objects lists the whole keyspace and builds an
in-memory reachability set. That is proportional to store size, so give the process
headroom or collect during a quiet window.

## Back up the database, not the bucket

The content store is a self-describing Merkle DAG: `rebuild()` (`origofs fsck
--rebuild`) recovers committed files, directories, symlinks, and branches from the
bucket alone. What it **cannot** recover is blame, the audit log, the actor
registry, and every uncommitted edit — those live only in Postgres.

So the backup that matters is the database. `backup_metadata()` uses SQLite's
online-backup API and therefore **refuses on Postgres**, pointing at `pg_dump` /
PITR rather than producing something that merely resembles a backup. Use Cloud
SQL's automated backups and point-in-time recovery.

## Encryption at rest

GCS encrypts at rest on its own. origofs's `open_pg_gcs_encrypted` is for when you
want the bucket operator not to be able to read your content either — it seals
objects with XChaCha20-Poly1305 under an Argon2id-derived key before they leave
the process.

Two things to know:

- Key derivation is deliberately slow and runs on the calling thread. Open the
  workspace at startup, not per request.
- Addresses stay the **plaintext** hash (convergent encryption), so deduplication
  still works — which makes a shared encrypted store an existence oracle: someone
  who can guess a plaintext can confirm whether it is present. Use per-tenant keys
  if that matters.

The same passphrase must be supplied on every open; a wrong one fails loudly
rather than returning garbage. The salt lives beside the content store and must
persist.

## Multiple workspaces

`ws.workspace("name")` opens another workspace in the same store: shared content
and identity (actors, blame, audit), separate root, refs, working tree, suggestion
queue, change feed, and presence.

There is **no actor→workspace mapping** in origofs. Which actor may reach which
workspace is for whatever resolves identity in your app to enforce. And note the
tenant layer in `docs/MULTI_TENANCY.md` (MT2+) is a concept, not an
implementation — do not rely on workspaces alone to isolate mutually distrusting
customers, because they share one content store and one actor registry.

## Local development

There is no usable local GCS emulator for this path: `fake-gcs-server` serves the
XML API virtual-hosted (by `Host` header) while `object_store` addresses GCS
path-style, so they do not interoperate. That is upstream, not something origofs
configures around.

Two options that do work:

- Develop against `open_pg_local` (Postgres + a local content directory). The
  engine, attribution, versioning, and GC behave identically; only the content
  backend differs, and that backend is the same `ObjectContentStore` code S3 and
  GCS both use.
- Point a **real** bucket at a dev prefix and run the env-gated suite:

  ```
  ORIGOFS_GCS_TEST_BUCKET=my-dev-bucket \
  ORIGOFS_GCS_TEST_SERVICE_ACCOUNT_PATH=/path/to/key.json \
    cargo test -p origofs-core --test content_backends gcs_backend -- --ignored
  ```

## Readiness

`ws.ready()` probes both stores and is what `GET /readyz` serves. It reports which
half is unhealthy but deliberately not *why* — the cause goes to the log, because
`/readyz` is unauthenticated by design and a Postgres probe error carries the DSN
host. Wire it to your liveness/readiness probes and read the logs for detail.
