# HTTP API

Every operation is available over HTTP/JSON — files as raw bytes, everything else
as JSON — so any client in any language can drive a workspace. Writes go through
the same engine path as every other surface, so they land on the change feed and
carry attribution.

```bash
origofs --workspace "$WS" serve --addr 127.0.0.1:8080 --auth-token "$TOKEN=$ACTOR"
```

The data surface is versioned under **`/v1`**. Liveness (`/health`), readiness
(`/readyz`) and `/metrics` stay at the root, so an orchestrator probes them
independently of the API version.

## Attribution never comes from the request

This is the central rule of the surface, and the reason it is the only place
identity is genuinely *verified*.

A write is attributed to the actor the **credential** resolves to. The request
body never names an actor, so a client cannot forge blame — and a
[propose-only](../guides/review.md#making-review-mandatory) actor's `PUT` is
routed into the review queue instead of landing.

`--auth-token TOKEN=ACTOR[:SESSION]` is the built-in bearer mapping, repeatable.
Set the same specs in **`ORIGOFS_AUTH_TOKENS`** (newline- or comma-separated) to
keep tokens out of `ps` and shell history.

!!! danger "`serve` refuses to bind a non-loopback address without authentication"

    That refusal is deliberate and is not a warning you can talk past. On
    loopback with no token given, all writes are attributed to an auto-created
    local actor — a development convenience, and nothing more.

## Reads are open unless you close them

Writes always need a credential. **Reads do not.** That is the right default for
a loopback development server and the wrong one for anything else — an open read
serves file bytes, blame, the audit log and the review queue.

```bash
origofs --workspace "$WS" serve --addr 0.0.0.0:8080 \
    --auth-token "$TOKEN=$ACTOR" --gate-reads --root /tenant-a
```

- `--gate-reads` requires the same credential on reads.
- `--root /tenant-a` restricts what the surface can address at all.

`serve` warns when it binds a non-loopback address without read gating.

There is a second, subtler interaction: once a workspace turns
[`acl enforce-reads`](../guides/operating.md#reads-are-a-separate-switch) on, an
anonymous read gets a **401** rather than being served. Turning that switch on
closes the anonymous door by itself, without also setting `--gate-reads`.

## Routes

All data routes are under `/v1`.

### Files and directories

| Method | Path |
|---|---|
| `GET` `PUT` `DELETE` | `/v1/files/{*path}` — raw bytes |
| `GET` `POST` | `/v1/dirs` — list or create at the root |
| `GET` `POST` | `/v1/dirs/{*path}` |
| `GET` | `/v1/stat/{*path}` |
| `GET` | `/v1/blame/{*path}` |
| `POST` | `/v1/rename` |

### Versioning

| Method | Path |
|---|---|
| `POST` | `/v1/commit` |
| `GET` | `/v1/log` — commit metadata; workspace-wide |
| `GET` | `/v1/log/{*path}` — the commits that changed one path |
| `GET` | `/v1/diff` — changed paths |
| `GET` | `/v1/diff/file` — one file's line diff |
| `GET` `POST` | `/v1/branches` — list or create |
| `POST` | `/v1/checkout` |

### Review queue

| Method | Path |
|---|---|
| `GET` `POST` | `/v1/suggestions` |
| `GET` | `/v1/suggestions/{id}` |
| `GET` | `/v1/suggestions/{id}/diff` |
| `POST` | `/v1/suggestions/{id}/accept` |
| `POST` | `/v1/suggestions/{id}/reject` |

### Identity, activity and trash

| Method | Path |
|---|---|
| `POST` | `/v1/actors` |
| `POST` | `/v1/sessions` |
| `GET` | `/v1/events?since=N` — the change feed |
| `GET` `POST` | `/v1/presence` — list, or heartbeat |
| `POST` | `/v1/revert-session` |
| `GET` | `/v1/trash` |
| `POST` | `/v1/trash/{id}/restore` |
| `DELETE` | `/v1/trash/{id}` |

### Co-editing (the `coedit` feature)

| Method | Path |
|---|---|
| `GET` | `/v1/coedit/{*path}` — WebSocket, flat `Y.Text` |
| `GET` | `/v1/coedit-tree/{*path}` — WebSocket, `Y.XmlFragment` |

These authenticate themselves — a browser cannot set headers on a WebSocket
upgrade — so they sit outside the read gate. See
[Live co-editing](../guides/teams.md#authenticating-a-browser).

### Unversioned

| Method | Path |
|---|---|
| `GET` | `/health` — liveness |
| `GET` | `/readyz` — probes the stores, so it can fail while `/health` passes |
| `GET` | `/metrics` — Prometheus text, `503` unless `--metrics` |

## A session

```bash
origofs --workspace "$WS" serve --addr 127.0.0.1:8080 --auth-token "$TOKEN=$ACTOR" &
AUTH=(-H "Authorization: Bearer $TOKEN")

curl "${AUTH[@]}" -X PUT --data-binary 'hello' \
     http://127.0.0.1:8080/v1/files/notes/a.txt
curl 'http://127.0.0.1:8080/v1/files/notes/a.txt'                 # → hello
curl "${AUTH[@]}" -X POST -d '{"message":"first"}' \
     http://127.0.0.1:8080/v1/commit
curl 'http://127.0.0.1:8080/v1/events?since=0'                    # the change feed
curl "${AUTH[@]}" -X POST -d '{"path":"/notes/a.txt"}' \
     http://127.0.0.1:8080/v1/presence                            # heartbeat
curl 'http://127.0.0.1:8080/readyz'
```

## Errors

Errors come back as a machine-readable envelope rather than a flat string:

```json
{"error": {"code": "...", "message": "...", "retryable": false}}
```

Every response carries an `x-request-id`. A permission refusal is a `403`; an
id-addressed resource outside your scope is a `404`, not a `403`, because a
refusal would confirm that the id exists.

## Limits

| Flag | Default | Why |
|---|---|---|
| `--max-body-bytes` | 64 MiB | `PUT` buffers the whole body, so this bounds per-request allocation. |
| `--request-timeout` | 60 s | `0` disables it. |
| `--max-concurrent-requests` | 512 | `0` disables the cap. |
| `--cors-origin` | same-origin only | Repeatable. |

## Metrics

```bash
origofs --workspace "$WS" serve --metrics    # or ORIGOFS_METRICS=1
curl http://127.0.0.1:8080/metrics
```

Off by default: without it nothing is exported and the route answers `503 metrics
not enabled`, so a scraper reports a failed scrape rather than a missing endpoint.
Like `/readyz`, the endpoint is unauthenticated.

That is safe only because **every label is a closed set** — an error code or
class, a fixed operation name, a *matched route template* like
`/v1/files/{*path}`. No path, actor, hash or file content ever reaches a scrape.
Keep it that way if you add a metric.

Series include `origofs_writes_total` and `origofs_write_bytes_total`,
`origofs_reads_total` and `origofs_read_bytes_total`, `origofs_chunks_put_total`
and `origofs_chunks_deduped_total` (the dedup hit rate), `origofs_commits_total`,
`origofs_gc_objects_deleted_total` and `origofs_gc_bytes_freed_total`,
`origofs_errors_total{code,class}`, and the `origofs_op_duration_seconds{op}` and
`origofs_http_request_duration_seconds{method,path}` histograms.

## Embedding it

The router is a normal axum `Router`, so it composes into an existing service.
Identity is resolved by a hook you provide rather than by origofs, which is the
point: your application already knows who is calling.

For Python, the same router ships as a FastAPI one — see
[Python](python.md#fastapi).
