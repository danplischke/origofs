# Configuration

Without a config file, origofs uses a local SQLite database and a local content
store under `--workspace`. To select Postgres or object storage instead, pass a
TOML file:

```bash
origofs --config team.toml serve --addr 0.0.0.0:8080
```

The shipped `deploy/config.example.toml` is the annotated reference; this page
covers what it selects and why.

## The file

```toml
[metadata]
backend = "postgres"
dsn = "host=postgres user=origofs password=origofs dbname=origofs sslmode=prefer"

[content]
backend = "s3"
bucket = "origofs-content"
region = "us-east-1"
packed = true
```

`--config` works with every daemon — `serve`, `nfs`, `mcp`, `mount` — so a
production backend needs no custom host program.

### `[metadata]`

| `backend` | Options |
|---|---|
| `sqlite` | `path` (default `<workspace>/meta.db`). Solo and offline. |
| `postgres` | `dsn`. Multi-writer and production. |

TLS is negotiated per the DSN's `sslmode`, libpq's own keyword, defaulting to
`prefer`.

!!! warning "`require` is stricter here than in libpq"

    origofs's `require` **refuses to connect unless the certificate verifies**.
    In libpq, `require` encrypts but verifies nothing. Certificates are checked
    against the platform root store; for a private CA or a self-signed server,
    point `ORIGOFS_PG_CA_FILE` at its PEM bundle.

### `[content]`

| `backend` | Options |
|---|---|
| `local` | `path` (default `<workspace>/cas`). A sharded directory. |
| `s3` | `bucket`, `region`, `endpoint`, `allow_http`, `access_key_id`, `secret_access_key`, `prefix`, `packed`, `index_dir`. |
| `gcs` | `bucket`, `prefix`, `packed`, `index_dir`, `service_account_path`, `allow_http`. |

Omit the S3 credentials to use the ambient AWS credential chain. Set `endpoint`
for MinIO, R2 or a custom host; `allow_http` is only accepted alongside one.

GCS credentials resolve in order: an explicit service-account key or key file,
then Application Default Credentials, then the GKE/GCE metadata server. On
workload identity, set none of them.

See [Storage backends](storage-backends.md) for the trade-offs, including when
`packed` is the wrong choice.

### `[cache]`

A bounded local LRU cache in front of an object store. Without it, every read
fetches every covering chunk over the network, every time.

```toml
[cache]
dir = "/var/cache/origofs"
max_bytes = 8589934592        # 8 GiB (default)
min_free_bytes = 2147483648   # yield below 2 GiB free (default; Unix only)
```

It is bounded and LRU-evicting, so it cannot fill the disk, and it yields when
the filesystem runs low — which matters when the cache shares a volume with
anything else.

!!! info "Two combinations are refused, deliberately"

    - With `backend = "local"`: it would copy every chunk twice and speed up
      nothing.
    - With `ORIGOFS_ENCRYPTION_KEY`: encrypted objects are addressed by their
      *plaintext* hash, so a cache below the encryption layer fails its own
      integrity check on every hit, and one above it writes plaintext to local
      disk.

## Encryption at rest

Set `ORIGOFS_ENCRYPTION_KEY` to a passphrase and it applies to whichever backends
the config selects. Content is encrypted with XChaCha20-Poly1305 before it
touches disk or the network, transparently to the engine.

The key is derived with Argon2id over a per-store random salt kept beside the
content — in the bucket, for an object store — so it survives losing the metadata
database, and garbage collection never touches it.

!!! danger "The same passphrase must be used on every open"

    A wrong one fails loudly rather than returning garbage. There is no recovery
    path if it is lost: the salt is not the secret.

Addresses stay the **plaintext** hash (convergent encryption), so dedup still
works.

## Workspace settings

These live in the workspace, not in a file, and are set with a command. All are
off or unlimited by default — see
[Operating a workspace](../guides/operating.md).

| Setting | Command | Default |
|---|---|---|
| Trash retention | `trash retention <7d\|off>` | off |
| ACL default-deny | `acl default-deny <on\|off>` | off |
| Read enforcement | `acl enforce-reads <on\|off>` | off |
| Byte / inode quota | `quota --bytes --inodes` | unlimited |
| Cross-mount POSIX locks | `posix-locks <on\|off>` | off |
| Require attribution | `require-attribution <on\|off>` | off |

## Environment variables

### Identity and secrets

| Variable | Effect |
|---|---|
| `ORIGOFS_ACTOR` | Default `--actor` for every attributed command. |
| `ORIGOFS_ENCRYPTION_KEY` | Passphrase for encryption at rest. Kept out of argv and shell history. |
| `ORIGOFS_AUTH_TOKENS` | `serve` bearer mappings, newline- or comma-separated, in the same `TOKEN=ACTOR[:SESSION]` form as `--auth-token`. |

### Observability

| Variable | Effect |
|---|---|
| `ORIGOFS_LOG`, `RUST_LOG` | Level filter for tracing output. Default `info`. |
| `ORIGOFS_METRICS` | `1` installs the Prometheus recorder, as `serve --metrics` does. |

### Postgres tuning

| Variable | Effect |
|---|---|
| `ORIGOFS_PG_POOL_SIZE` | Connection pool size. |
| `ORIGOFS_PG_WAIT_TIMEOUT_SECS` | Bound waiting for a free connection. |
| `ORIGOFS_PG_CONNECT_TIMEOUT_SECS` | Connect timeout. |
| `ORIGOFS_PG_RECYCLE_TIMEOUT_SECS` | Recycle timeout. |
| `ORIGOFS_PG_STATEMENT_TIMEOUT_MS` | `0` (off) by default. |
| `ORIGOFS_PG_CA_FILE` | PEM bundle for a private CA. |

Pool sizing is a deployment property: 16 is far too many for a dozen sidecars
sharing one small database, and far too few for a busy single writer.

!!! note "Why the statement timeout is off"

    origofs's statements are small, but a few legitimately run long — truncating
    a large working tree, a wide directory listing. A timeout that aborts a
    checkout is worse than one that never fires. Set the ceiling if you want one.

### Object store tuning

| Variable | Effect |
|---|---|
| `ORIGOFS_S3_TIMEOUT_SECS` | Abandon a request with no response. |
| `ORIGOFS_S3_CONNECT_TIMEOUT_SECS` | Fail fast on a mis-set endpoint. |
| `ORIGOFS_S3_MAX_RETRIES` | Retry count. |
| `ORIGOFS_S3_RETRY_TIMEOUT_SECS` | Total elapsed retry budget. |
| `ORIGOFS_UPLOAD_CONCURRENCY`, `ORIGOFS_FETCH_CONCURRENCY` | Parallelism for chunk transfer. |

These are explicit so a flaky bucket has stated behaviour rather than whatever the
client library defaults to. A request with no timeout is the one that turns a slow
S3 into a wedged server.

### Testing

| Variable | Effect |
|---|---|
| `ORIGOFS_PG_TEST_URL` | Postgres-backed tests self-skip unless this points at a reachable database. |
| `ORIGOFS_GCS_TEST_*` | Credentials for the native-GCS suite, which CI does not have. |
