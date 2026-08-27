# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What origo is

origo is a filesystem where humans and AI agents share the same files, and every
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
cargo build --release                 # ./target/release/origo
cargo install --path crates/origo-cli   # installs the `origo` binary
cargo run -p origo-cli -- --workspace ./ws init   # run the CLI without installing

cargo test --workspace                # all Rust tests
cargo test -p origo-core                # one crate
cargo test -p origo-core --test merge   # one integration-test file (tests/merge.rs)
cargo test -p origo-core roundtrip      # filter by test-name substring (single test)

cargo clippy --workspace --all-targets
cargo fmt                             # no rustfmt.toml — plain default style

cargo bench -p origo-core               # Criterion micro-benchmarks (hot paths)
```

**Postgres-backed tests self-skip** unless `ORIGO_PG_TEST_URL` points at a
reachable database — so a plain `cargo test --workspace` silently exercises only
the SQLite path. To run the multi-writer / `LISTEN‑NOTIFY` / Postgres tests:

```bash
ORIGO_PG_TEST_URL="host=127.0.0.1 port=5432 user=postgres dbname=origo" cargo test --workspace
```

**Python bindings** (`crates/origo-py`) build with maturin, not cargo:

```bash
cd crates/origo-py
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop        # builds the pyo3 extension + installs the `origo` module
pytest tests/          # some tests also gate on ORIGO_PG_TEST_URL
```

### Publishing the Python package

The PyPI **distribution is `origofs`**, not `origo` — that name belongs to an
unrelated project. The **import name is still `origo`**; only `[project].name` in
`crates/origo-py/pyproject.toml` differs. Don't "fix" the mismatch.

`crates/origo-py/Cargo.toml`'s `version` is the single source of truth for the
released version, and is deliberately **not** `version.workspace = true` — the
wheel releases on its own cadence, independent of the workspace's `0.0.0`.

Releasing is `git tag py-v<version> && git push origin <tag>`.
`.github/workflows/release-py.yml` builds wheels (Linux x86_64/aarch64 on
manylinux_2_28, macOS Intel/Apple Silicon, Windows x64) plus an sdist, imports
each artifact as a smoke test, and publishes via PyPI **Trusted Publishing
(OIDC)** — there is no API token in the repo, so don't add one. The workflow
refuses to publish when the tag and the manifest version disagree. It also runs
(without publishing) on PRs that touch the bindings or the manifests, which is
the only cross-platform build coverage the repo has — `ci.yml` is Linux-only.

### Toolchain note

There is no `rust-toolchain` file and no CI config in the repo. **`origo-core`
uses `edition = "2024"`** (needs Rust ≥ 1.85), while the other crates inherit
`edition = "2021"` from the workspace. Use a recent stable toolchain.

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

Both traits live in `origo-core` (`content.rs`, `metadata.rs`) and both are used
as `Arc<dyn …>`, so a workspace picks its backends at runtime. The mutable POSIX
working tree (inode/dentry rows) is an **overlay whose base is a commit tree** —
exactly git's index idea. Reads fall through the working tree to the base tree
to content chunks; writes copy-up. Committing crystallizes the working tree into
new immutable tree/commit objects. This is the resolution of the
"mutable POSIX vs. immutable objects" tension, and understanding it is the key
to understanding the whole codebase (`docs/DESIGN.md` §3).

## How a call flows through the layers

Every surface funnels down to the same core, so a behavior change usually
belongs in `origo-core`, not in each surface:

```
CLI / MCP / HTTP API / FUSE / NFS / Python   (access surfaces, one crate each)
        │  each resolves the caller → (actor, session)
        ▼
origo-sdk::Workspace          ergonomic façade; `open_*` constructors wire backends
        ▼
origo-core::Fs<M, C>          the working-tree engine — POSIX ops, chunking, commit,
        │                   merge, attribution, gc, recovery (engine.rs et al.)
        ├──► dyn MetadataStore   (SqliteMetadataStore | PostgresMetadataStore)
        └──► dyn ContentStore    (see "content backends compose" below)
```

`Workspace` (`crates/origo-sdk/src/lib.rs`) is the front door you almost always
extend: it owns the `Fs`, exposes the public API, and is what the CLI, HTTP API,
sandbox, and Python bindings all call. It holds an `Option<Arc<PostgresMetadataStore>>`
on the side because a few Postgres-only features (the `subscribe` push feed via
`LISTEN/NOTIFY`) are **not** on the object-safe `MetadataStore` trait — SQLite
callers use `watch` (polling) instead.

## Content backends compose (decorator pattern)

Content backends wrap each other; the `open_*` constructors in `origo-sdk` are the
canonical recipes for how they stack. Key point: **`VerifyingStore` goes on the
outside** so integrity is checked at the chunk-addressed boundary a caller reads
by.

- `LocalCasStore` — sharded `objects/aa/bbbb…` directory.
- `ObjectContentStore` — S3/R2/GCS/MinIO (`::s3`) or `::in_memory` (same adapter,
  no network — used for `open_object_memory` and tests).
- `PackStore` — batches chunks into large pack objects (few big PUTs instead of
  thousands of tiny ones) with a local per-chunk index; needs `flush`/`repack`.
- `VerifyingStore` — re-hashes on read; a bit-rotted/tampered object surfaces as
  `OrigoError::Corrupt` instead of being served as authentic.
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
  server-side. See `build_api_auth` in `crates/origo-cli/src/main.rs`: it *refuses*
  to expose an unauthenticated API on a non-loopback address, and the HTTP body
  never names an actor. Preserve this when touching any surface.
- **Suggestions** (`suggest`/`accept`/`reject`) are the propose-and-review path:
  proposed bytes go into the content store immediately, the working tree changes
  only on `accept`, and `accept` lands the edit **attributed to the original
  author** while recording the approver (and refuses a stale base). Reviewer must
  differ from author.

## Versioning

Opt-in, three modes (`VersioningMode` in `objectgraph.rs`): `off` (working tree +
attribution only), `native` (origo's own chunked commit DAG — the default when a
workspace is initialized), and `git` (native DAG *plus* the `origo-git`
export/import + `git-remote-origo` bridge to genuine git objects). `native` and
`git` share one commit-DAG and merge engine (three-way / diff3, conflicts, LFS-style
`lock`s for binaries); they differ only in on-disk object encoding.

## Crate map

| Crate | Role |
|---|---|
| `origo-core` | The engine. Both trait abstractions, all content backends, chunking, versioning, merge, attribution, gc, recovery, migrations. Everything else depends on it. (`edition 2024`) |
| `origo-sdk` | `Workspace` — the ergonomic façade over `origo-core::Fs`. The API every other surface calls. |
| `origo-cli` | The `origo` binary (clap). A thin shell over `origo-sdk`; the best index of what the system can do. |
| `origo-sandbox` | Overlay / sandbox edit-capture: run a process over a copy-on-write view, import its delta as attributed writes. Not a security boundary by default; opt-in bubblewrap *filesystem* isolation via `--isolate` (see below). |
| `origo-fuse`, `origo-nfs` | POSIX mounts (FUSE on Linux; NFSv3 elsewhere). |
| `origo-mcp` | MCP server — agents call filesystem tools over stdio, auto-attributed. |
| `origo-git` | Real-`git` interop: export/import genuine git objects; ships the `git-remote-origo` helper binary (`git clone origo://…`). |
| `origo-api` | HTTP/JSON server (axum). `Authenticator`/`BearerAuth` resolve identity server-side. |
| `origo-py` | pyo3/maturin bindings: async-native (`await` every I/O), a FastAPI router (`origo.fastapi`), and overlay orchestration (`origo.overlay`). |

## Conventions & gotchas that will bite you

- **Never put large bytes in the metadata DB.** The whole design rests on the
  metadata/content split; the DB references content by hash only.
- **Path traversal is rejected at every metadata boundary.** `validate_component`
  (`engine.rs`) refuses `.`/`..`/`/`/NUL in a single name so a poisoned name can
  never be *stored* — which is what stops it escaping during host materialization
  (e.g. the sandbox's `export_tree`). Any new inode-oriented op (FUSE/NFS handlers
  especially) must validate names too.
- **`origo sandbox` / `origo overlay` are edit-capture; a security boundary only with
  `--isolate`.** By **default** (`isolate: false`) the child runs with your
  privileges over a plain copy-on-write overlay — the whole host filesystem stays
  reachable (incl. this workspace's `meta.db`/`cas`), with no network namespace or
  seccomp, and origo only strips `ORIGO_ENCRYPTION_KEY` from the env. **Not a security
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
  self-describing Merkle DAG with a mirrored ref table, so `origo fsck --rebuild`
  (SDK `rebuild`/`scan`) restores committed files, dirs, symlinks, and branches
  from the bucket alone. Blame, the audit log, actors, and uncommitted edits live
  **only** in the DB — so the DB is the thing to back up.
- **SQLite = solo/offline; Postgres = multi-writer/production.** Dialect
  differences are hidden behind the `MetadataStore` trait; migrations
  (`migrations.rs`, `latest_schema_version`) are forward-only and authored once
  with per-engine SQL variants where they diverge. `Workspace::migrate` is the
  explicit runner (a normal `open` already migrates).
- **On macOS, `fuser` is built with its `macos-no-mount` feature** (see the
  `[target.'cfg(target_os = "macos")']` blocks in `origo-fuse`/`origo-py`).
  Building it normally there requires macFUSE headers via pkg-config, which no
  CI runner has — without this the *entire workspace* fails to compile on macOS,
  and no macOS wheel can be built. The cost is that `origo_fuse::mount`/`spawn`
  return a plain `io::Error` on macOS; that is the documented posture anyway
  (FUSE on Linux, NFSv3 elsewhere). Don't drop the feature to "clean up".
- **`ORIGO_ENCRYPTION_KEY`** opts a workspace into encryption at rest (kept out of
  argv/history); the *same* value must be used on every open or reads fail loudly.
- Integration tests live in each crate's `tests/` and are the clearest executable
  spec of behavior (e.g. `origo-core/tests/{merge,attribution,recover,durability,
  integrity}.rs`). Mirror their style when adding coverage.

## License

MIT OR Apache-2.0
