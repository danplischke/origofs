# origofs — pre-MVP review

> Status: **review snapshot**, taken against `1a0d5dd` (2026-07-30). Nothing here has
> been applied — this is a findings document, not a changelog.

## Context

Review taken before embedding origofs in a small MVP app that consumes it through the
**Python bindings**, using **multiple workspaces**. Findings are ordered by what will
actually bite that configuration, not by abstract severity.

Baseline health, measured rather than assumed:

- `cargo clippy --workspace --all-targets` — clean, zero warnings.
- `cargo clippy -p origofs-sdk --features full --all-targets` — clean.
- `cargo test --workspace` — green (exit 0). SQLite path only; the PG-gated suites
  self-skip locally but *do* run in CI, which spins up a real `postgres:16`.
- Zero `TODO`/`FIXME`/`todo!`/`unimplemented!` in `crates/*/src`.
- CI is unusually thorough: fmt, clippy, nextest, coverage, `cargo-deny`, MSRV 1.88,
  bounded libFuzzer smoke over all four object decoders, MinIO S3 leg, Postgres-over-TLS
  leg, macOS leg, Docker compose stack with a real write/read round-trip and a 401 assertion.

This is a mature codebase. The findings below are real, but they sit on a solid base.

Confidence is marked per finding: **[verified]** = I read the code and confirmed it
myself; **[reported]** = surfaced by a subagent sweep, consistent with the code I read
but not line-by-line confirmed by me.

---

## Tier 1 — Blockers for a Python + multi-workspace MVP

### 1. Multi-workspace is unreachable from Python **[verified]**

`Workspace::workspace(name)` and `Workspace::workspaces()`
(`crates/origofs-sdk/src/lib.rs:378`, `:425`) have **no pyo3 binding**. I enumerated
the full extension surface (`crates/origofs-py/src/lib.rs`) and the type stub
(`crates/origofs-py/python/origofs/__init__.pyi`) — neither name appears. The only
`workspace` in the pure-Python tree is an unrelated property at
`crates/origofs-py/python/origofs/fsspec.py:261`.

Your stated model is multiple workspaces. From Python today you get exactly one: the
`default` workspace each `open_*` constructor lands in. There is no HTTP workspace
routing either, so `origofs.fastapi.build_router` can't select one.

**Workarounds until it's bound:** one process (or one `Workspace` handle) per
workspace via separate `meta.db` files, or drive workspace creation through the
`origofs` CLI and accept that Python can't switch between them. Neither is good — the
whole point of the workspace layer is sharing one store.

### 2. The attributed mutation variants are missing from Python, so the propose-only gate doesn't hold **[verified]**

Exposed to Python: `write_as`, `write_or_propose`, `suggest`, `suggest_delete`,
`accept_suggestion`, `reject_suggestion`, `set_write_policy`.

**Not** exposed: `remove_or_propose`, `rename_as`, `mkdir_as`, `symlink_as`,
`commit_as`. Only the *unattributed* `remove`, `rename`, `mkdir_p`, `commit`,
`checkout`, `create_branch` are bound.

CLAUDE.md states the invariant plainly: the `Propose` policy is enforced in the engine
via `ensure_may_write`, and "a new mutating endpoint on any surface must call an
attributed variant." The Python surface ships `set_write_policy` — so you can mark an
agent propose-only — while giving that same agent unattributed `remove`, `rename`, and
`commit` that skip `ensure_may_write` entirely. The gate looks enforced and isn't.

Those operations also record no `edit_op` and no blame, so a Python-driven delete or
rename has no attribution trail — in a system whose whole premise is attribution.

### 3. You can open a packed store from Python but never reclaim its space **[verified]**

`open_local_packed`, `open_s3_packed`, `open_pg_s3_packed`, `open_gcs_packed`,
`open_pg_gcs_packed` are all bound. `flush()` and `repack()` are not. Neither is
`gc()` / `gc_with_grace()`.

Per CLAUDE.md, packed stores "need `flush`/`repack`" to reclaim space, and content is
immutable so churn *always* leaves orphans. A Python MVP on any packed backend has a
store that grows monotonically with no in-language way to collect it. You'd have to
shell out to the `origofs` CLI.

### 4. `backup_metadata` is not bound **[verified]**

CLAUDE.md: "The content store can rebuild the DB, but not attribution. Blame, the audit
log, actors, and uncommitted edits live **only** in the DB — so the DB is the thing to
back up." `Workspace::backup_metadata` (`crates/origofs-sdk/src/lib.rs:805`) wraps
SQLite's online-backup API, which is the *correct* way to snapshot a live DB (a file
copy is not). It has no Python binding. Your MVP cannot back up the one thing that is
unrecoverable.

### 5. You can branch from Python but not merge **[verified]**

`create_branch`, `checkout`, `current_branch`, `branches` are bound. `merge`,
`merge_branch`, `conflicts` are not. That's a one-way door — you can create divergent
history from Python and have no way to reconcile it.

### 6. Other confirmed Python gaps **[verified]**

Absent from both `lib.rs` and the `.pyi`: `revert_session` and `edit_ops` (the headline
"undo just the agent's work" feature — it exists **only** in the Rust SDK; no CLI
subcommand, no HTTP route, no MCP tool either), `open_local_encrypted` /
`open_encrypted` (no encryption at rest from Python at all), `symlink` / `readlink`,
`lock` / `unlock` / `locks`, `versioning_mode` / `set_versioning_mode`, `ready`
(no readiness probe), `resync`, `push_objects` / `fetch_objects`, `reap_presence`,
`supersede_stale_suggestions`.

README:162 says "Python is the same API with `await` on every call." That is an
overclaim and worth correcting regardless of what you fix.

---

## Tier 2 — Engine bugs that can lose data

### 7. `Arc<T>`'s `ContentStore` impl omits `replace_keyed`, silently reverting the pack index to the unsafe path **[verified]**

`crates/origofs-core/src/content.rs:526-573` forwards every trait method **except**
`replace_keyed`. So `Arc<dyn ContentStore>` falls through to the trait default at
`content.rs:52-55` — which is `delete(key)` then `put_keyed(key, …)`.

`PackStore.index` is `Arc<dyn ContentStore>` (`pack.rs:94`) and calls
`self.index.replace_keyed(...)` at `pack.rs:188`. The doc comment directly above the
default spells out the consequence: "a chunk with no index entry is invisible to
`repack`, which then reads the pack holding it as fully dead and deletes it." A crash
inside that window is unrecoverable data loss.

Every backend correctly overrides `replace_keyed`; the `Arc` forwarder that all of them
are reached through does not. One missing three-line method defeats the entire
atomic-replace design. `TieredStore` and `VerifyingStore` hold `Arc<dyn ContentStore>`
too and are affected identically.

This is the single highest-value fix in the report.

### 8. GC's grace period does not cover dedup onto an old, unreferenced chunk **[verified]**

`gc.rs`'s module docs (lines 14-33) argue the age gate makes GC "safe on a *live,
shared* workspace." The gate is `list_with_age()` vs `DEFAULT_GC_GRACE_SECS`
(`gc.rs:199-211`). But `LocalCasStore::put` early-returns on an existing object without
touching its mtime (`content.rs:344-346`), and `ObjectContentStore::put` does the same
via `head()`.

So: chunk X is old and currently unreferenced. A writer stores bytes that hash to X —
reverting a file, shared boilerplate, a checkout of old content — gets `Ok(hash)` with
no mtime bump, and commits metadata pointing at X. GC sees X unmarked *and* old, and
sweeps it. The committed file is permanently `ContentMissing`.

Note the docs disagree with each other about this: README:777 and
`crates/origofs-sdk/src/lib.rs:608` say GC is *not* safe with active writers;
`gc.rs:14-33` says it is. Given this hole, the conservative docs are right.

### 9. `write_reader` silently truncates the file if the chunker task panics **[verified]**

`crates/origofs-core/src/engine.rs:560-618`. `StreamCDC` runs on `spawn_blocking`;
fastcdc's own *errors* are correctly forwarded through the channel as `Err`. But on a
**panic** the sender drops, `rx.recv()` returns `None`, the loop exits normally, and
line 597 is `let _ = handle.await;` — the `JoinError` is discarded. The code then builds
a manifest from the partial chunk list and commits it as the complete file, returning
`Ok(())`.

The realistic trigger for an embedder is a panicking user-supplied `impl Read`. Result:
silent truncation reported as success.

### 10. No forward-compatibility guard on the metadata schema **[verified]**

`SqliteMetadataStore::init` (`sqlite.rs:176-220`) applies every migration not present in
`schema_meta` and never compares the DB's version against `latest_schema_version()`.
Postgres is the same shape. If the DB is at v16 and the binary knows v15, there is no
error — it proceeds against an unknown schema.

The content store gets a first-class guard for exactly this case
(`StoreDescriptor::min_reader_version` → `OrigoFSError::UnsupportedVersion`,
`format.rs:246`), and `error.rs` even documents `UnsupportedVersion` as "upgrade the
reader, not restore from a backup." The metadata half has no equivalent. Given V11/V13
changed primary keys, a future migration of that shape opened by an older binary is
silent-corruption territory.

Compounding it: every `open_*` constructor calls `fs.init()` unconditionally
(`crates/origofs-sdk/src/lib.rs:138`), so there is no read-only or no-auto-migrate mode.
A rolling deploy's first new pod migrates the DB out from under still-running old pods,
which — per the above — won't notice.

### 11. `blame()` slices without the length guard `revert_session` has **[verified]**

`attribution.rs:865-868`:

```rust
let end = pos + r.len;
let slice = &bytes[start as usize..end as usize];
```

`BlameMap::decode` (`attribution.rs:323-337`) is fully lenient — any `u64` length
parses. Sixty lines later, `revert_session` (`attribution.rs:934`) does
`if map.total() != bytes.len() as u64 { continue; }`. `blame()` does not. A blame map
whose runs over-cover the content panics the process on an out-of-range slice; `pos + r.len`
can also overflow.

The asymmetry between the two functions is strong evidence this is an oversight rather
than a deliberate contract. **[reported]** `resync::carry_blame` (`resync.rs:596-598`)
copies a remote peer's blame string verbatim, which would make it remotely reachable —
I did not confirm that path end to end.

---

## Tier 3 — Performance and behavior traps

### 12. `PackStore`'s entire reason for existing is defeated **[verified]**

`Fs::store_body` (`engine.rs:506`) and `write_reader` (`engine.rs:606`) call
`self.content.flush()` on **every** write as a durability barrier.
`PackStore::flush` is `self.seal()` (`pack.rs:450-452`).

So packing only ever batches within a single file write. Writing 10,000 small files
produces roughly 10,000 sealed pack objects — not the "few big PUTs instead of thousands
of tiny ones" that `open_s3_packed` is sold on
(`crates/origofs-sdk/src/lib.rs:216-220`). Durability is genuinely correct; the
performance property is not. Relevant to you because the packed constructors are the
ones bound in Python.

### 13. Chunking and hashing run inline on the async runtime **[verified]**

`Fs::store_body` (`engine.rs:484-508`) runs `fastcdc` over the whole buffer plus a
blake3 hash per chunk directly in the `async fn`, with no `spawn_blocking`. This is the
path behind `write`, `write_as`, `write_or_propose`, `vfs_write`, and every merge.
`write_reader` gets this right; `write` does not. A large `write()` occupies a runtime
worker for the duration — and in Python, that runtime is shared with everything else.

Also worth documenting: `EncryptedStore::from_passphrase` (`encrypt.rs:60-72`) runs
Argon2id in a plain sync fn, so the cost lands on the caller's thread.

### 14. Packed layouts are documented as the multi-writer recommendation, but the index is node-local **[reported]**

`open_pg_s3_packed` / `open_pg_gcs_packed` doc comments call these "the recommended
object-storage layout" for "many writers on one database", but `index_dir` is a
`LocalCasStore`. `docker-compose.yml:9-11` says the opposite — that a multi-container
deployment must share the index on a volume or use a single writer. Two processes with
separate index dirs will not see each other's chunks. The constraint belongs in the API
doc, not a compose comment.

### 15. Every `Workspace` mutation costs two extra un-transactional round trips **[verified]**

`Workspace::emit` (`crates/origofs-sdk/src/lib.rs:~440`) is called from 18 mutation
sites. Each call does `current_branch()` (a `get_ref` on HEAD) plus a `record_event`
insert, both outside the write's own transaction and both best-effort (`let _ = …`).

Two consequences: a fixed per-write overhead you can't opt out of, and — more
importantly — the change feed is **lossy by construction**. An `fs_event` can be dropped
while the write commits. If you build on `watch`/`subscribe`, treat the feed as a hint
and reconcile against actual state; don't treat it as a log.

### 16. Authorization is a single global flag **[verified]**

The only authorization primitive in the codebase is per-actor
`WritePolicy::{Direct, Propose}` (`attribution.rs:57`). I grepped for ACL/permission/
path-scoping machinery — there is none.

So within a workspace: any actor can read every file, and any `Direct` actor can write,
delete, or rename every file including other actors' work. Workspaces are a structural
boundary (`docs/MULTI_TENANCY.md` MT1, genuinely implemented and tested), but there is
no actor→workspace mapping — your app must enforce which actor may reach which
workspace, and per finding #1 you can't select a workspace from Python anyway.

`docs/MULTI_TENANCY.md` is explicit that the tenant layer (MT2+) "remains a concept." If
your multiple workspaces ever mean multiple *customers*, that gap is load-bearing.

---

## Tier 4 — HTTP surface (lower priority for you; matters if you expose `origofs.fastapi`)

You chose Python, so this section is mostly informational — but
`origofs.fastapi.build_router` mirrors the Rust API's route table, so the shape carries
over.

### 17. Two endpoints bypass attribution and the write policy entirely **[verified]**

`crates/origofs-sdk/src/api/mod.rs`:

```rust
async fn checkout(State(ws): State<Shared>, _auth: Auth, Json(req): Json<BranchReq>) …
    ws.checkout(&req.name).await?;      // raw, unattributed

async fn create_branch(State(ws): State<Shared>, _auth: Auth, Json(req): Json<BranchReq>) …
    ws.create_branch(&req.name).await?; // raw, unattributed
```

Both discard `_auth` after authenticating. `checkout` is destructive — it runs
`replace_working_tree_in`, an atomic truncate-and-rematerialize of the whole working
tree. A propose-only agent token — the actor you deliberately blocked from overwriting a
single file — can `POST /v1/checkout` and destroy every uncommitted edit, with no
`ensure_may_write`, no `edit_op`, and no change-feed event.

`origofs.fastapi` exposes `/checkout` and `/branches` too.

Note the MCP surface has a test that fails on an unclassified tool
(`crates/origofs-sdk/tests/mcp.rs:408`) — which is exactly why MCP is in better shape.
There is no equivalent guard on the HTTP route table, which is how these two got in.

### 18. `POST /v1/sessions` takes the actor from the request body **[verified]**

`api/mod.rs:1290-1296` — `req.actor` comes from JSON; `_auth` is discarded. This is the
one place the "server never trusts a client-named actor" rule is broken.

Impact is narrower than it first looks: writes attribute from the token's `Principal`,
not from a client-supplied session, so you can't forge a *write* this way. What you can
do is mint unbounded session rows attributed to arbitrary actor ids. `POST /v1/actors`
is likewise authenticated-but-unbounded.

### 19. No request budget of any kind **[verified]**

`max_body_bytes` defaults to 1 GiB (`api/mod.rs:222`) and `PUT /v1/files/*` buffers the
whole body in RAM (`body: Bytes`, `api/mod.rs:647`) — reads stream properly, writes
don't. `serve`/`serve_until` hardcode `router(ws, auth)` (`:383`), so neither an
embedder nor `origofs serve` can lower it. No `TimeoutLayer`, no `ConcurrencyLimitLayer`,
no rate limiting. `gate_reads` defaults to `false`, so by default every file, every
blame record, and every actor name is readable by anyone who can reach the port.

### 20. Raw backend errors reach HTTP response bodies **[reported]**

`api/mod.rs:450` puts `e.to_string()` into the JSON envelope. For
`OrigoFSError::Backend` the `Display` is `"{class} {origin} backend error: {source}"`
with an unmodified driver error — SQL text, column names, DB paths. `/readyz` echoes its
probe error verbatim and is unauthenticated by design (outside `/v1`). `/metrics` is
genuinely fine: label cardinality is a closed set, as CLAUDE.md requires.

---

## Tier 5 — Packaging and repo hygiene

### 21. No LICENSE files **[verified]**

Every crate declares `license = "MIT OR Apache-2.0"`; `pyproject.toml` repeats it;
README says it. There is no `LICENSE`, `LICENSE-MIT`, or `LICENSE-APACHE` anywhere in
the tree. For embedding in a product this is a legal blocker with a five-minute fix.

### 22. Versions are inconsistent; the Python wheel would ship as `0.0.0` **[verified]**

`Cargo.toml:6` sets `[workspace.package] version = "0.0.0"`. `origofs-core`, `-sdk`, and
`-cli` hardcode `0.1.0`. `origofs-py` uses `version.workspace = true` → **0.0.0**, and
`pyproject.toml` has `dynamic = ["version"]`, so `pip install` gets `origofs 0.0.0`.

Same split on edition: core/sdk/cli are `edition = "2024"`; `origofs-py` inherits
`edition = "2021"` from the workspace.

### 23. `cargo publish` would fail today **[verified]**

`origofs-core`, `-sdk`, and `-cli` set only `name`/`version`/`edition`/`license` — no
`description` (crates.io rejects that outright), no `repository`, no `readme`, no
`keywords`. Nothing is on crates.io, there are no git tags, no CHANGELOG, and no
stability/semver statement. An embedder must vendor a git dependency with no signal
about what may break.

### 24. No `rust-version`, no docs.rs metadata **[verified]**

CI pins MSRV 1.88 and the Dockerfile pins `rust:1.88-slim`, but no manifest carries
`rust-version = "1.88"` — so a consumer on 1.85–1.87 gets a wall of let-chain parse
errors instead of Cargo's clear MSRV message.

No `[package.metadata.docs.rs]` anywhere. `origofs-sdk`'s entire value (api, mcp,
sandbox, git, fuse, nfs, coedit, metrics) is behind default-off features, so docs.rs
would render the `Workspace` façade and nothing else. Needs `all-features = true` plus
`--cfg docsrs`.

### 25. No `#[non_exhaustive]` on any public enum **[verified]**

`OrigoFSError` (17 variants), `ErrorClass`, `BackendOrigin`, `MergeOutcome`,
`WriteOutcome`, `SuggestionStatus`, `WritePolicy`, `VersioningMode`, `FileKind`, and
others. Adding one error variant — near-certain during MVP hardening — is a
semver-major break for every downstream `match`. Cheapest possible fix, and it only
gets more expensive after you have consumers.

### 26. Missing hygiene **[verified]**

No `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`, `dependabot.yml`, `CODEOWNERS`, or
issue/PR templates. `deny.toml:14-18` waives two RUSTSEC advisories with sound reasoning
but no expiry or tracking issue — and with no Dependabot, the advisory gate will simply
start failing one day with no PR queued to fix it.

---

## Tier 6 — Test and CI gaps (CI is strong; these are the holes in it)

- **The deep-invariant suites are SQLite-only** **[reported]** — `simulation.rs`,
  `gc.rs`, `atomicity.rs`, `hardening.rs`, `attribution.rs`, `merge.rs`,
  `concurrency.rs` all instantiate `SqliteMetadataStore` exclusively. So crash
  simulation, GC-under-writers, transaction atomicity, hostile-input hardening, blame
  correctness, and merge resolution have never run against Postgres — the backend
  README calls the production pairing. (`ci.yml:22-24`'s comment claiming
  `concurrency.rs` is PG-gated is stale.)
- **FUSE has zero executed coverage** **[reported]** — all five tests in
  `fuse_mount.rs` gate on `mountable()` = `geteuid() == 0`; GitHub runners are non-root,
  so they always skip.
- **GCS has no CI backend** — `gcs_backend` is `#[ignore]`d with no `fake-gcs-server`
  job, unlike S3 which gets a dedicated MinIO leg. All four GCS constructors are public
  in Rust *and* Python.
- **`origofs-cli` has no unit tests** — 1,206 lines, zero `#[test]`, one integration
  test covering only the git-remote helper. Untested: `--config` TOML parsing (which
  selects the Postgres/S3/GCS backends), `--auth-token` parsing, the non-loopback bind
  guard. The `docker` job partially compensates.
- **Single-feature builds are never checked** **[verified]** — Cargo unifies features
  across `--workspace`, and `origofs-cli`/`-py` pull `full,metrics,coedit`. Every
  explicit invocation is `full` or a superset. There is no bare `cargo check -p
  origofs-sdk` and no single-surface build, so the repeated promise that "a default
  build pulls no axum/fuser/nfsserve" is never verified. `cargo hack
  --feature-powerset --depth 2` would close it.
- **`origofs-core` has no backend feature flags** **[verified]** — only `coedit` and
  `metrics` are optional; `pub mod postgres` is unconditional. A pure-SQLite MVP still
  compiles deadpool-postgres, tokio-postgres, rustls, rustls-native-certs, and
  `object_store` with `aws`+`gcp`. 418 crates in `Cargo.lock`, and
  `--no-default-features` is a no-op.
- **Example test suites never run** — `examples/fs-consumer/test_fs_consumer.py` and
  `examples/web/server/test_app.py` are outside the `pytest` scope of the `python` job,
  and `codecov.yml` ignores `examples/**`.

---

## Tier 7 — Smaller items worth a line

**[reported]** unless noted. Consistent with the code but not individually confirmed:

- `sandbox.rs:598-606` — sandbox import attributes file *writes* only. Deletions,
  directory creation, and symlinks import with no blame and no edit-op, so
  `origofs sandbox --actor 7 -- rm -rf src/` records nothing about who deleted it.
- `sandbox.rs:612` — import calls raw `write_as`, which deliberately skips
  `ensure_may_write`, so the sandbox is a side door around the propose gate.
- `sandbox.rs:314-317` **[verified]** — `--isolate` only does
  `.env_remove("ORIGOFS_ENCRYPTION_KEY")`, no `--clearenv`. The module doc claims
  credentials "simply not present", which is true of the *filesystem* but not of
  `AWS_SECRET_ACCESS_KEY` / `DATABASE_URL` / API keys sitting in the child's `environ`.
  No `--new-session` either (bubblewrap flags TIOCSTI injection without it).
- `sandbox.rs:285-291` **[verified]** — `bwrap_available()` is just `bwrap --version`
  exit status, with no version check, despite its own doc, the CLI messages, and
  CLAUDE.md all saying `>= 0.8.0` is gated by it. It fails closed (an old bwrap dies on
  `--overlay`), so this is a doc/gating accuracy issue rather than a hole.
- `crates/origofs-cli/src/main.rs:851-915` **[verified]** — the "not a security
  boundary" caveat is never printed at runtime when `isolate == false`. It lives only in
  `--help` and doc comments, while strictly less dangerous cases (non-loopback NFS bind,
  non-loopback metrics bind) both warn. CLAUDE.md asks for this caveat to be kept loud.
- `mcp.rs:126-130`, `:215-219` — `origofs_write` / `origofs_suggest` call raw
  `ws.mkdir_p(parent)` *before* the policy decision, so a propose-only agent's queued
  suggestion still creates unattributed parent directories. The engine fixed exactly
  this class internally (`suggest.rs:274-283`); the MCP surface still does it. The test
  misses it because every test path is at the root.
- `mcp.rs:412` — an unknown tool returns `Ok(...)` with `isError: false`, so a typo'd
  tool name looks like a successful call to the agent.
- `mcp.rs:401` — commit author is hardcoded `"mcp-agent"` while the HTTP surface
  correctly resolves the actor's display name.
- `vfs.rs:143-194` — `vfs_write` / `vfs_truncate` are unguarded read-modify-write with
  no transaction and no `set_content_if`; two concurrent writes to different offsets of
  one file lose the first. This is the FUSE/NFS path, where concurrent writers are the
  norm.
- `sqlite.rs:1450-1458` — `MetaTxn::set_content` discards the affected-row count, so a
  write to a path unlinked mid-stream updates 0 rows, commits, and returns `Ok`. The
  checked primitive (`set_content_if`) already exists and is used elsewhere.
- `version.rs:257`, `:358`, `merge.rs:288` — `commit_attempt` / `checkout_attempt` /
  `merge_live` do `txn.commit()` then `mirror_refs()`, all inside `retrying(...)`.
  `mirror_refs` can return a retryable error, which re-runs the whole operation against
  an already-advanced HEAD. `retry.rs:22-24` states the rule this violates.
- `suggest.rs:500-552` — `accept_suggestion` discards the `bool` from
  `resolve_suggestion`'s CAS, so two concurrent accepts both apply the write and nobody
  notices.
- `version.rs:224` — `build_tree` walks the working tree outside any transaction, so a
  commit can mix pre- and post-write state of different files: a snapshot that never
  existed.
- `postgres.rs:2118-2134` — `PostgresTxn::Drop` spawns ROLLBACK into a detached task and
  never awaits it; deadpool can recycle a connection with an open transaction.
- `version.rs:425-448` — `plan_materialize` builds the entire tree in RAM before the
  transaction opens, and its nested `Drop` is synchronous recursion (stack overflow on a
  deep tree from a bucket, `git import`, or a resync peer).
- `objectstore.rs:349-425`, `gc.rs:132` — `ContentStore::list` / `list_with_age` return
  unbounded `Vec`s and GC builds a `HashSet` of everything reachable. GC on a
  multi-million-object bucket is an OOM, not a slow operation.
- `content.rs:596-737` — `MemStore` uses `std::sync::Mutex` with 18
  `.expect("mem store poisoned")` sites. `SqliteMetadataStore` moved to `parking_lot`
  for exactly this reason and has a regression test; `MemStore` didn't, and it's a
  `pub` export and an obvious MVP choice.
- `types.rs:37-41` — `Hash::from_hex` accepts uppercase while `to_hex` emits lowercase,
  so a `list()` result can point at a different path than `path_for` — a GC delete
  would then no-op forever.
- `engine.rs:779`, `merge.rs:167`, `interop.rs:32` — `content_bytes` caps its allocation
  hint at 8 MiB with a comment explaining why a crafted manifest demands it; these three
  reassembly paths pre-allocate from the manifest's declared size uncapped.
- `migrations.rs:577-607` — `dentry` has no foreign key to `inode` despite
  `PRAGMA foreign_keys=ON`, so orphan dentries aren't structurally prevented.
- `crates/origofs-sdk/src/lib.rs:789-808` **[verified]** — `schema_version`'s doc
  comment is glued onto `backup_metadata`'s block, so `backup_metadata`'s rustdoc opens
  with a sentence about migrations and `schema_version` is undocumented.
- `crates/origofs-cli/src/main.rs:505` — `origofs write --actor` calls `write_as`, not
  `write_or_propose`, so `origofs policy <actor> propose` has no effect on the CLI's own
  write command.
- `crates/origofs-sdk/src/lib.rs:1290`, `:1297` — `reap_presence` and
  `supersede_stale_suggestions` have no callers anywhere, so a long-running `origofs
  serve` grows the presence table forever.

---

## Verification notes

What I ran and read directly, so you can weigh the report:

- `cargo clippy --workspace --all-targets` and
  `cargo clippy -p origofs-sdk --features full --all-targets` — both clean.
- `cargo test --workspace` — exit 0.
- Read in full or in the relevant region: `content.rs` (Arc forwarder, trait defaults,
  `LocalCasStore::put`), `pack.rs` (index type, `flush`, `replace_keyed` call site),
  `gc.rs` (module docs, sweep loop, grace constant), `engine.rs` (`store_body`,
  `write_reader`, `content_bytes`, `read_range`), `attribution.rs` (`blame`,
  `revert_session`, `WritePolicy`), `sqlite.rs` (`init`, `schema_version`),
  `error.rs`, `api/mod.rs` (route table, `checkout`, `create_branch`, `create_actor`,
  `create_session`, `Auth`/`Principal`), `sandbox.rs` (`bwrap_available`, env handling),
  `crates/origofs-sdk/src/lib.rs` (`Workspace`, `workspace()`, `emit`, full method list),
  `crates/origofs-py/src/lib.rs` + `__init__.pyi` (full exported surface),
  `python/origofs/fastapi.py` (route table), all four `Cargo.toml`s, `pyproject.toml`,
  `.github/workflows/ci.yml`, `docs/DESIGN.md` §9-§10, `docs/MULTI_TENANCY.md`.

Items marked **[reported]** came from a subagent sweep. They're consistent with the code
I read and cite specific lines, but I did not confirm each one myself — verify before
acting on any of them.

---

## Suggested order, for a Python + multi-workspace consumer

1. **#7** (`Arc` `replace_keyed`) — three lines, prevents unrecoverable pack loss.
2. **#1, #2** (Python `workspace`/`workspaces` and the attributed mutation variants) —
   without these your stated architecture isn't expressible and the propose gate is
   decorative.
3. **#3, #4** (Python `gc`/`flush`/`repack`/`backup_metadata`) — operability; you
   currently cannot collect or back up from Python.
4. **#21, #22, #24, #25** (LICENSE, versions, `rust-version`, `non_exhaustive`) — an
   afternoon, and `non_exhaustive` gets strictly more expensive once you have consumers.
5. **#9, #10, #11** (silent truncation, schema guard, blame slice) — small, contained.
6. **#8, #12** (GC dedup race, pack flush) — real design work; until #8 is fixed, run GC
   only when the workspace is quiet and make the docs agree with each other.
