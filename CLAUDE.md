# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What origofs is

origofs is a filesystem where humans and AI agents share the same files, and every
edit is recorded against the actor that made it. It is **not** a wrapper over
`git` or a VFS shim — it is a storage engine with four properties at its core:
content-addressed storage, a pluggable metadata database (Postgres or SQLite),
opt-in Git-style versioning, and per-actor, per-byte-range edit attribution
(`blame`). The engine is exposed through many surfaces: a CLI, a Rust SDK,
Python bindings, an HTTP/JSON API, an MCP server, FUSE/NFS mounts, a live
overlay mount, and a real-`git` interop bridge.

`docs/DESIGN.md` is the authoritative design doc and the milestone roadmap
(M0–M9). Doc comments throughout the code reference these milestones (e.g.
"M1", "§4d"). **Read `docs/DESIGN.md` before making any change that touches
architecture** — it explains *why* the metadata/content split, the object
model, attribution, and the failure-surface work are the way they are. The
`README.md` covers the same surface from the user's side with runnable examples.

## Build, test, lint

```bash
cargo build --release                 # ./target/release/origofs
cargo install --path crates/origofs-cli   # installs the `origofs` + `git-remote-origofs` binaries
cargo run -p origofs-cli -- --workspace ./ws init   # run the CLI without installing

cargo test --workspace                # all Rust tests
cargo test -p origofs-core                # one crate
cargo test -p origofs-core --test merge   # one integration-test file (tests/merge.rs)
cargo test -p origofs-core roundtrip      # filter by test-name substring (single test)
cargo test -p origofs-sdk --features full # exercise the access surfaces (api/mcp/fuse/nfs/sandbox/git)

cargo clippy --workspace --all-targets
cargo fmt                             # no rustfmt.toml — plain default style

cargo bench -p origofs-core               # Criterion micro-benchmarks (hot paths)
```

**Postgres-backed tests self-skip** unless `ORIGOFS_PG_TEST_URL` points at a
reachable database — so a plain `cargo test --workspace` silently exercises only
the SQLite path. To run the multi-writer / `LISTEN‑NOTIFY` / Postgres tests:

```bash
ORIGOFS_PG_TEST_URL="host=127.0.0.1 port=5432 user=postgres dbname=origofs" cargo test --workspace
```

**Python bindings** (`crates/origofs-py`) build with maturin, not cargo:

```bash
cd crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop        # builds the pyo3 extension + installs the `origofs` module
pytest tests/          # some tests also gate on ORIGOFS_PG_TEST_URL
```

### Toolchain note

There is no `rust-toolchain` file. **`origofs-core`, `origofs-sdk`, and `origofs-cli`
use `edition = "2024"`** (edition 2024 itself sets a Rust ≥ 1.85 *language* floor);
`origofs-py` inherits `edition = "2021"` from the workspace. The **effective MSRV is
1.88**, though — the code uses `let`-chains (stabilized in 1.88) and the dependency
graph (`icu`, via `url`/`object_store`) needs ≥ 1.86 — and the `msrv` CI job pins it
so an accidental newer-stdlib use or a dependency MSRV bump is caught. CI lives at
`.github/workflows/ci.yml` (fmt + clippy + tests, an explicit `coedit` pass, and the
`msrv` floor) and otherwise runs on stable. Use a recent stable toolchain.

## The one architectural idea everything hangs on

**The metadata store and the content store are split, and never mixed.**

- **`ContentStore`** holds the *bytes*: FastCDC content-defined chunks addressed
  by their BLAKE3 hash, plus the immutable git-style objects (`blob` = a chunk
  *manifest*, not raw bytes; `tree`; `commit`) that form a Merkle DAG. Immutable,
  deduplicated, integrity-verified on read.
- **`MetadataStore`** holds the *names and versions*: inodes, dentries, symlinks,
  refs/reflog, the attribution op-log and blame index, the audit log, the change
  feed, and presence. It stores content only as `manifest_hash` references —
  **it must never hold large file bytes.**

Both traits live in `origofs-core` (`content.rs`, `metadata.rs`) and both are used
as `Arc<dyn …>`, so a workspace picks its backends at runtime. The mutable POSIX
working tree (inode/dentry rows) is an **overlay whose base is a commit tree** —
exactly git's index idea. Reads fall through the working tree to the base tree
to content chunks; writes copy-up. Committing crystallizes the working tree into
new immutable tree/commit objects. This is the resolution of the
"mutable POSIX vs. immutable objects" tension, and understanding it is the key
to understanding the whole codebase (`docs/DESIGN.md` §3).

## How a call flows through the layers

Every surface funnels down to the same core, so a behavior change usually
belongs in `origofs-core`, not in each surface:

```
CLI · Python (own crates)  +  api · mcp · fuse · nfs · sandbox · git   (access
        │        surfaces — the latter six are feature-gated origofs-sdk modules)
        │  each resolves the caller → (actor, session)
        ▼
origofs-sdk::Workspace          ergonomic façade; `open_*` constructors wire backends
        ▼
origofs-core::Fs<M, C>          the working-tree engine — POSIX ops, chunking, commit,
        │                   merge, attribution, gc, recovery (engine.rs et al.)
        ├──► dyn MetadataStore   (SqliteMetadataStore | PostgresMetadataStore)
        └──► dyn ContentStore    (see "content backends compose" below)
```

`Workspace` (`crates/origofs-sdk/src/lib.rs`) is the front door you almost always
extend: it owns the `Fs`, exposes the public API, and is what the CLI, HTTP API,
sandbox, and Python bindings all call. It holds an `Option<Arc<PostgresMetadataStore>>`
on the side because a few Postgres-only features (the `subscribe` push feed via
`LISTEN/NOTIFY`) are **not** on the object-safe `MetadataStore` trait — SQLite
callers use `watch` (polling) instead.

## Content backends compose (decorator pattern)

Content backends wrap each other; the `open_*` constructors in `origofs-sdk` are the
canonical recipes for how they stack. Key point: **`VerifyingStore` goes on the
outside** so integrity is checked at the chunk-addressed boundary a caller reads
by.

- `LocalCasStore` — sharded `objects/aa/bbbb…` directory.
- `ObjectContentStore` — S3/R2/GCS/MinIO (`::s3`) or `::in_memory` (same adapter,
  no network — used for `open_object_memory` and tests).
- `PackStore` — batches chunks into large pack objects (few big PUTs instead of
  thousands of tiny ones) with a local per-chunk index; needs `flush`/`repack`.
- `VerifyingStore` — re-hashes on read; a bit-rotted/tampered object surfaces as
  `OrigoFSError::Corrupt` instead of being served as authentic.
- `EncryptedStore` — XChaCha20-Poly1305 at rest; addresses stay the *plaintext*
  hash (convergent encryption) so dedup still works.
- `TieredStore` / `MemStore` — local cache tier / in-memory store.

Example real stack (from `open_pg_s3_packed`):
`VerifyingStore(PackStore(ObjectContentStore::s3, index_dir))`.

## Attribution is the whole point — the write-path invariants

A blame trail is only trustworthy if the identity behind each write is, so the
write path enforces this and you must not weaken it:

- **Attributed writes carry a `WriteCtx` (actor + session).** `write_as` records
  an append-only `edit_op` (the ground-truth op-log) and updates the materialized
  interval `blame` index. Plain `write` is unattributed. `revert_session` walks
  every file an actor touched in a session and removes exactly the lines it
  authored, leaving others' edits intact.
- **The server never trusts a client-named actor.** Identity is resolved
  server-side. See `build_api_auth` in `crates/origofs-cli/src/main.rs`: it *refuses*
  to expose an unauthenticated API on a non-loopback address, and the HTTP body
  never names an actor. Preserve this when touching any surface.
- **Suggestions** (`suggest`/`accept`/`reject`) are the propose-and-review path:
  proposed bytes go into the content store immediately, the working tree changes
  only on `accept`, and `accept` lands the edit **attributed to the original
  author** while recording the approver (and refuses a stale base). Reviewer must
  differ from author.
- **The `Propose` write policy is enforced in the engine, not per surface.**
  Every *attributed* mutation on `Fs` — `write_or_propose`, `remove_or_propose`,
  `rename_as`, `mkdir_as`, `symlink_as`, `commit_as`, `accept_suggestion`,
  `reject_suggestion` — runs `ensure_may_write` (`suggest.rs`), which refuses a
  propose-only actor with `OrigoFSError::Denied` (`403` on the HTTP API). Ops with
  a propose-shaped equivalent queue instead of refusing. **A new mutating endpoint
  on any surface must call an attributed variant**, never the raw `remove`/
  `rename`/`mkdir_p`/`symlink`/`commit` — those take no actor, exist for internal
  machinery (checkout, merge materialization, applying an accepted suggestion),
  and are exempt by construction. `tests/mcp.rs` fails on an unclassified MCP tool
  so a new ungated one can't ship silently; the FUSE/NFS mounts remain a
  deliberate bypass (a mount has no actor context). Issue #78.

## Versioning

Opt-in, three modes (`VersioningMode` in `objectgraph.rs`): `off` (working tree +
attribution only), `native` (origofs's own chunked commit DAG — the default when a
workspace is initialized), and `git` (native DAG *plus* the `origofs-sdk` `git`
module's export/import + the `git-remote-origofs` bridge to genuine git objects,
behind the `git` feature). `native` and
`git` share one commit-DAG and merge engine (three-way / diff3, conflicts, LFS-style
`lock`s for binaries); they differ only in on-disk object encoding.

## Crate map

Four crates. The many *access surfaces* are no longer separate crates — they are
opt-in, feature-gated **modules of `origofs-sdk`** (default-off, so a plain
`origofs-sdk` build stays lean).

| Crate | Role |
|---|---|
| `origofs-core` | The engine. Both trait abstractions, all content backends, chunking, versioning, merge, attribution, gc, recovery, migrations. Everything else depends on it. (`edition 2024`) |
| `origofs-sdk` | `Workspace` — the ergonomic façade over `origofs-core::Fs`, **plus every access surface as a feature-gated module** (table below). The library every other surface calls. (`edition 2024`) |
| `origofs-cli` | The `origofs` binary **and** the `git-remote-origofs` helper (clap). A thin shell over `origofs-sdk` with all surfaces (`full`) enabled; the best index of what the system can do. (`edition 2024`) |
| `origofs-py` | pyo3/maturin bindings: async-native (`await` every I/O), a FastAPI router (`origofs.fastapi`), and overlay orchestration (`origofs.overlay`). Enables `origofs-sdk`'s `coedit` (always) + `fuse`/`nfs` (on Unix). |

### `origofs-sdk` access-surface features

Each is a module under `crates/origofs-sdk/src/`, gated by the matching feature
(default-off). `full` turns them all on (but not `coedit`); `origofs-cli` uses `full`.

| Feature | Module | Role |
|---|---|---|
| `api` | `origofs_sdk::api` | HTTP/JSON server (axum). `Authenticator`/`BearerAuth` resolve identity server-side. |
| `mcp` | `origofs_sdk::mcp` | MCP server — agents call filesystem tools over stdio, auto-attributed. |
| `sandbox` | `origofs_sdk::sandbox` | Overlay / sandbox edit-capture: run a process over a copy-on-write view, import its delta as attributed writes. Not a security boundary by default; opt-in bubblewrap *filesystem* isolation via `--isolate` (see below). |
| `fuse`, `nfs` | `origofs_sdk::fuse` / `::nfs` | POSIX mounts (FUSE on Linux; NFSv3 elsewhere). **Unix-only** (`cfg(unix)`). |
| `git` | `origofs_sdk::git` | Real-`git` interop: export/import genuine git objects. The `git-remote-origofs` binary (shipped by `origofs-cli`, `git clone origofs://…`) builds on it. |
| `coedit` | — | Opt-in CRDT co-editing (yrs); adds the y-sync WebSocket to the `api` surface. Kept separate from `full`. |
| `metrics` | — | Opt-in metrics recording (emit-only, no exporter); adds `GET /metrics` + per-request instrumentation to the `api` surface. Kept separate from `full`. |

## Conventions & gotchas that will bite you

- **Never put large bytes in the metadata DB.** The whole design rests on the
  metadata/content split; the DB references content by hash only.
- **Path traversal is rejected at every metadata boundary.** `validate_component`
  (`engine.rs`) refuses `.`/`..`/`/`/NUL in a single name so a poisoned name can
  never be *stored* — which is what stops it escaping during host materialization
  (e.g. the sandbox's `export_tree`). Any new inode-oriented op (FUSE/NFS handlers
  especially) must validate names too.
- **`origofs sandbox` / `origofs overlay` are edit-capture; a security boundary only with
  `--isolate`.** By **default** (`isolate: false`) the child runs with your
  privileges over a plain copy-on-write overlay — the whole host filesystem stays
  reachable (incl. this workspace's `meta.db`/`cas`), with no network namespace or
  seccomp, and origofs only strips `ORIGOFS_ENCRYPTION_KEY` from the env. **Not a security
  boundary; run only trusted code.** Passing **`--isolate`** (`RunOpts::isolate` /
  `LiveOpts::isolate`; needs `bwrap` ≥ 0.8.0, gated by `bwrap_available()`) runs
  the command under bubblewrap in a fresh tmpfs root that hides the host filesystem
  (`meta.db`/`cas`, home dir, credentials) — a real **filesystem** boundary for
  untrusted code. It is deliberately *only* filesystem isolation: the network
  namespace is left shared on purpose (agents need egress), so it does not by
  itself contain network-reachable resources. Either way the delta is captured and
  imported the same. Keep the default's "not-a-security-sandbox" caveat loud.
- **Content is immutable and never overwritten**, so churn leaves orphaned
  chunks. `gc` (mark-and-sweep from live refs) reclaims them and is **not** safe
  alongside active writers; packed stores additionally need `repack` to reclaim
  space. Content writes are idempotent (content-addressed), so retries are safe.
- **The content store can rebuild the DB, but not attribution.** It is a
  self-describing Merkle DAG with a mirrored ref table, so `origofs fsck --rebuild`
  (SDK `rebuild`/`scan`) restores committed files, dirs, symlinks, and branches
  from the bucket alone. Blame, the audit log, actors, and uncommitted edits live
  **only** in the DB — so the DB is the thing to back up.
- **SQLite = solo/offline; Postgres = multi-writer/production.** Dialect
  differences are hidden behind the `MetadataStore` trait; migrations
  (`migrations.rs`, `latest_schema_version`) are forward-only and authored once
  with per-engine SQL variants where they diverge. `Workspace::migrate` is the
  explicit runner (a normal `open` already migrates).
- **`ORIGOFS_ENCRYPTION_KEY`** opts a workspace into encryption at rest (kept out of
  argv/history); the *same* value must be used on every open or reads fail loudly.
- **Observability is emit-only in the library.** `origofs-core`/`origofs-sdk` emit
  `tracing` spans/events (the `Workspace` write-path methods are `#[instrument]`ed;
  `VerifyingStore` `warn!`s on a failed integrity check) but **install no
  subscriber** — a library that only emits is a no-op until a binary opts in, so a
  Rust embedder pays nothing and installs their own. The **CLI** installs one
  (`init_tracing`, `crates/origofs-cli/src/main.rs`): level filter from
  `ORIGOFS_LOG`/`RUST_LOG` (default `info`), `--log-format json|text`, always to
  **stderr** so `origofs mcp` keeps stdout for its JSON-RPC transport. Backend
  errors also carry a machine `code()` + `retryable()`/`class()` (`error.rs`) instead
  of a flat string, and `/readyz` (distinct from liveness `/health`) probes the
  stores via `MetadataStore::ping`/`ContentStore::ping`.
  **Metrics work the same way** (`metrics` feature, default-off and deliberately
  *not* in `full`): `origofs-core::metrics` records through the `metrics` facade —
  the recording bodies are `#[cfg]`-gated, so call sites are unconditional and
  compile to nothing when the feature is off — and links **no exporter**. The
  binary installs one (`init_metrics`, `origofs serve --metrics` /
  `ORIGOFS_METRICS=1`) and hands its renderer to `api::set_metrics_renderer`;
  `GET /metrics` then serves Prometheus text, and answers `503` when no exporter
  was installed (so a scraper reports a failed scrape rather than a missing
  endpoint). It sits outside `/v1`, ungated like `/readyz` — safe only because
  every metric label is a closed set (never a path, actor, or hash). **Keep it
  that way when adding a metric.**
- Integration tests live in each crate's `tests/` and are the clearest executable
  spec of behavior (e.g. `origofs-core/tests/{merge,attribution,recover,durability,
  integrity}.rs`). Mirror their style when adding coverage.

## License

MIT OR Apache-2.0
