# Changelog

All notable changes to origofs are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) — see
[Stability](#stability) for what that means before 1.0.

## [Unreleased]

### Fixed — Postgres + object storage

- **Garbage collection could never run on Postgres.** The lease serializing
  collections was keyed `"\0gc-lease"`, chosen because path validation rejects
  NUL so no user lock could collide with it — true of paths, but the key is
  stored in a `text` column and Postgres cannot hold a NUL byte in one. Every
  `gc()` on the production metadata backend failed at `acquire_lock` with
  `invalid byte sequence for encoding "UTF8": 0x00`, so a Postgres deployment
  reclaimed nothing, ever. Every GC test ran on SQLite, which stores the byte
  happily.
- `lock`/`unlock` passed a caller-supplied string straight to the store, unlike
  every other path-taking operation. A NUL was therefore a hard Postgres error
  from user input, and nothing separated user paths from internal lease keys.
  Both now require an absolute path.
- `GcsConfig` had no `allow_http`, so the native GCS backend could not be pointed
  at a plaintext endpoint at all — `object_store` refused it with a `BadScheme`
  builder error before any request left the process. S3 has had the option from
  the start (it is how the MinIO CI leg works); GCS never got it, and with no GCS
  test leg nothing noticed. This is what a local emulator needs.
- `PostgresMetadataStore::schema_version` detected "table not created yet" by
  string-matching an error whose `Display` is a generic kind — the message lives
  in its source, so the match never fired. Now keyed on SQLSTATE `42P01`, which is
  also stable across server locales.

### Fixed — data loss

- `impl ContentStore for Arc<T>` did not forward `replace_keyed`. It is the one
  trait method with a default body, and that default is the delete-then-put the
  trait documents as unsafe, so the omission silently downgraded every backend
  reached through an `Arc`. `PackStore` holds its index as
  `Arc<dyn ContentStore>` and calls `replace_keyed` on every repack, so a crash
  in the window left a live chunk with no index entry — invisible to the next
  repack, which then deleted the pack holding it.
- Garbage collection's age gate only protected *newly written* content. `put`
  deduplicates and returns early, so an existing object got no fresh timestamp,
  and a writer that deduplicated onto old unreferenced content received `Ok` for
  bytes the sweep was about to reclaim. Adds `ContentStore::touch`, refreshed on
  the dedup path by every backend; `gc_with_grace` now refuses a grace inside the
  band where the race lives.
- `write_reader` discarded the chunking task's `JoinError`, so a panic in a
  caller-supplied `Read` committed a partial manifest as the whole file and
  returned `Ok(())`.
- `set_content` discarded its affected-row count, so a write whose path was
  unlinked mid-stream reported success while the bytes went nowhere.
- `vfs_write`/`vfs_truncate` (the FUSE/NFS path) were unguarded
  read-modify-write of the whole body, so two writes to different offsets of one
  file lost each other. Both are now conditional on the version they read, with a
  bounded retry.
- `blame()` indexed into file content using blame-run lengths without checking
  they described that content, panicking on an out-of-range slice. The map can
  come from a resync peer or a corrupt row.

### Fixed — correctness

- `Fs::init` now refuses a metadata database written by a newer origofs instead of
  applying unknown-version migrations against it. The content store has always
  had this guard; the metadata half did not, and migrations here have changed
  primary keys.
- Post-commit `mirror_refs` could make a retry wrapper re-run an operation whose
  metadata had already committed, producing a duplicate commit. The retry now
  wraps the mirror alone.
- `accept_suggestion`/`reject_suggestion` discarded the compare-and-set that
  decides whether they won the race, so an accept losing to a concurrent reject
  applied the proposed bytes and still reported success.
- `MetaTxn` gained an awaitable `rollback`. The Postgres `Drop` path can only
  spawn its ROLLBACK, so a caller that dropped a transaction and immediately
  re-read could be handed the same pooled connection with the transaction open.
- `Hash::from_hex` rejects uppercase, which previously produced a hash whose
  storage path pointed somewhere other than where the name came from.

### Fixed — write policy and surfaces

- `POST /v1/checkout` and `POST /v1/branches` authenticated and then discarded
  the actor, calling unattributed engine methods. Checkout rematerializes the
  whole working tree, so a propose-only token could destroy every uncommitted
  edit. Both now go through attributed, policy-gated variants.
- `POST /v1/sessions` read its actor from the request body — the one place the
  surface broke "the server never trusts a client-named actor".
- `POST /v1/actors` was ungated, letting a restricted actor mint unbounded rows in
  the table attribution resolves against.
- MCP `origofs_write`/`origofs_suggest` created the path's parent with an
  unattributed `mkdir_p` *before* the policy decision.
- The sandbox attributed file writes but not deletions, directories, or symlinks,
  so an imported `rm -rf` recorded nothing about who ran it.
- `origofs write --actor` used the policy-exempt `write_as`, so
  `origofs policy <actor> propose` had no effect on the CLI's own write command.

### Fixed — security posture

- `--isolate` stripped only `ORIGOFS_ENCRYPTION_KEY` from the child environment,
  inheriting `AWS_SECRET_ACCESS_KEY`, `DATABASE_URL`, and every API token — with
  egress deliberately open. The environment is now cleared.
- `--isolate` gained `--new-session`; without it the child shares the controlling
  terminal and can `TIOCSTI`-inject into the launching shell.
- `bwrap_available()` now checks the version its own documentation claimed it
  checked.
- Backend driver errors (SQL text, column and constraint names, connection paths)
  no longer reach HTTP response bodies or the unauthenticated `/readyz`.
- The "not a security boundary" caveat for `sandbox`/`overlay` without
  `--isolate` is printed at runtime, not only in `--help`.

### Added

- `Workspace::checkout_as`, `create_branch_as`, and `ensure_may_write`.
- `api::serve_with` / `serve_until_with`, so `ApiOptions` is reachable without
  giving up the graceful drain.
- Request budget on the HTTP surface: a 60s per-request timeout, a 512-request
  concurrency cap, and a 64 MiB body limit (was 1 GiB, unchangeable).
- `origofs serve` runs presence reaping on a timer; it previously had no caller.
- Python: `open_gcs_encrypted` and `open_pg_gcs_encrypted`, so encryption at rest
  is available on the GCS pairing as well as the S3 one.
- `allow_http` on `GcsConfig` across Rust, Python, and the `--config` TOML.

### Changed

- All four crates share one version (`0.1.0`) and edition (`2024`). The workspace
  declared `0.0.0`/`2021` while three crates hardcoded `0.1.0`/`2024`, so the
  Python wheel shipped as version 0.0.0.
- Crates now carry `description`, `repository`, `keywords`, `categories`,
  `rust-version = "1.88"`, and docs.rs metadata. `cargo publish` previously
  failed outright for want of a description, and docs.rs would have rendered
  `origofs-sdk` with every surface missing.
- `OrigoFSError`, `ErrorClass`, `BackendOrigin`, `WritePolicy`, `ActorKind`,
  `SuggestionKind`, `SuggestionStatus`, `VersioningMode`, `Segmentation`, and
  `ObjectFormat` are `#[non_exhaustive]`, as are the read-only report structs.
  Outcome enums (`WriteOutcome`, `MergeOutcome`, `ResyncOutcome`) are
  deliberately left exhaustive — see the note above each.
- Added `LICENSE-MIT` and `LICENSE-APACHE`. Every crate declared
  `MIT OR Apache-2.0` but neither text was in the tree.

### Performance

- `store_body` chunks off the async runtime. The FastCDC scan is a CPU-bound pass
  over the whole buffer behind `write`, `write_as`, `vfs_write`, and every merge,
  and it pinned a runtime worker for the duration.
- Reassembly buffers no longer pre-allocate from a manifest's declared size, which
  a crafted manifest controls.

---

## Stability

origofs is pre-1.0 and **not yet published to crates.io**. Until 1.0:

- **Minor versions (`0.x`) may break API compatibility.** Breaking changes are
  listed under `### Changed` with the reason and the migration.
- **The on-disk formats are versioned and checked at open.** The content store
  carries a format descriptor and refuses a store written by a newer origofs with
  `UnsupportedVersion`; the metadata schema does the same for its migration
  version. Migrations are forward-only and applied automatically on open.
  Downgrading a binary below the schema version of its database is not supported
  and is now refused rather than attempted.
- **MSRV is 1.88** and is treated as a breaking change to raise. It is pinned in
  CI so a dependency bump cannot raise it by accident.

The `#[non_exhaustive]` markings above exist so that the changes most likely
during hardening — a new error variant, a new counter on a report — are additive
rather than breaking.
