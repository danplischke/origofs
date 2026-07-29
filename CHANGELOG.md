# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it reaches
a first release.

## [Unreleased]

Nothing is released yet — there are no tags and no crates.io or PyPI packages, so
everything below describes the state of `main`. Pre-1.0: the HTTP surface is
versioned (`/v1`), but the Rust and Python APIs may still change without notice.

### Added

- **Content-addressed storage** — FastCDC chunking, BLAKE3 addressing, dedup, and
  range reads over local directories, S3/R2/MinIO, native GCS, an in-memory
  adapter, a pack layer that batches chunks into large objects, a verifying
  decorator that re-hashes on read, and XChaCha20-Poly1305 encryption at rest with
  convergent addressing so dedup survives.
- **Pluggable metadata store** — SQLite for solo/offline, Postgres for
  multi-writer, behind one trait with forward-only migrations.
- **Opt-in versioning** — a commit DAG with branches, checkout, log, status,
  three-way merge with conflicts, and locks; `off`, `native`, and `git` modes.
- **Real-`git` interop** — export and import genuine git objects (sha1 or sha256),
  a git-LFS pointer bridge, and the `git-remote-origofs` helper so the real `git`
  can clone, fetch, and push a workspace over `origofs://`.
- **Attribution** — per-actor, per-byte-range blame keyed by content, an
  append-only edit-op log, session revert, a propose-and-review suggestion queue,
  and per-actor write policies.
- **Access surfaces** — CLI, Rust SDK, async-native Python bindings, HTTP/JSON API,
  MCP server, FUSE and NFSv3 mounts, a live overlay mount for agents, and a
  copy-on-write sandbox with opt-in bubblewrap filesystem isolation.
- **Live collaboration** — an exactly-once, branch-scoped change feed, presence,
  Postgres `LISTEN/NOTIFY` push, and opt-in CRDT co-editing (`yrs`) speaking the
  Yjs y-sync protocol with per-character authorship.
- **Retrieval with provenance** — passage extraction carrying blame and a content
  hash for incremental embedding, plus a LlamaIndex reader on the Python side.
- **Python integrations** — a FastAPI router, an fsspec filesystem (with
  universal-pathlib support), and SQLAlchemy models with Alembic migrations.
- **Recovery** — `fsck --rebuild` reconstructs refs and the working tree from the
  content store alone after a metadata-DB loss.
- **Multi-workspace** — many workspaces in one store, sharing content and identity.
- **Operations** — mark-and-sweep GC, `repack`, emit-only `tracing` in the
  libraries with a subscriber in the CLI, machine-readable error codes with
  retryability, and `/health` plus a real `/readyz` probe.

[Unreleased]: https://github.com/danplischke/origofs/commits/main
