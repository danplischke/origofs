# Changelog

All notable changes to origofs are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) — see
[Stability](#stability) for what that means before 1.0.

## [Unreleased]

### Changed

- **The tree co-editing room is confirmed compatible with PlateJS/Slate, and the
  claim is now pinned by a test against the real client** (#152). It was reported
  as wire-*incompatible*: `@platejs/yjs` binds through `@slate-yjs/core`, which
  roots its document at a `Y.XmlText`, where origofs binds a `Y.XmlFragment` — so
  "the two peers never converge".

  The root-type observation is right; the conclusion is not. Yjs keys root types
  by **name**, and `doc.get(name, T)` binds a view of whatever branch is already
  there rather than asserting a type the peer must match, so both sides address
  the same branch. `origofs-core/tests/coedit_tree_slate.rs` drives bytes captured
  from a real `yjs@13` + `@slate-yjs/core@1.0.2` client through the real
  `CoeditTreeDoc`: the document lands, the Slate value round-trips through
  `yTextToSlateElement`, and two connections editing it are still blamed per run.
  `tests/fixtures/slate_yjs_client.mjs` regenerates the fixtures.

  What the docs never said, and what most likely produced the report, is that
  origofs's `a` (author) and `n` (node id) stamps are ordinary Yjs *formatting
  attributes* — and on the Slate binding a formatting attribute is a **mark**, so
  every text node arrives with two marks it did not author. That is deliberate:
  `n` is the token a host cites in its span map at checkpoint time, so it has to
  be readable from the client. A schema must **ignore** them rather than strip
  them, because the server re-asserts them on every apply. Documented on the
  module, the socket, the README and the teams guide.

  Also corrected: `y-slate` is not a package and is no longer named anywhere —
  the Slate binding is `@slate-yjs/core`, which is what `@platejs/yjs` wraps. And
  `api_coedit_tree_ws.rs` no longer calls its raw `yrs::Doc` client "what PlateJS
  runs": that is the `y-prosemirror`/TipTap shape, and modelling the client rather
  than using one is how the compatibility claim went unchecked in the first place.

  Not done, and not doable: an `XmlText`-**rooted** room. `XmlTextRef` does not
  implement yrs's `RootRef` ("not bound to be used as root-level types"), so the
  pinned yrs cannot create one, and #144 leaves no upgrade path.

- **Encrypted objects are versioned, and the Argon2id cost belongs to the store
  rather than to a dependency.** Everything else in a bucket could be evolved
  without rewriting it; the ciphertext could not. `EncryptedStore` wrote the bare
  AEAD output, so nothing recorded which cipher, nonce derivation or KDF had
  produced an object — and the failure that implies is the least legible one in
  the system: a changed scheme decrypts to nothing and reports
  `Corrupt("wrong key or corrupt data")`, which is exactly what a mistyped
  passphrase looks like, on data that is perfectly intact.

  Objects are now framed `ORGE | version | AEAD(…)` under `format.rs`'s existing
  rules. It costs five bytes and changes no address — the address is the
  *plaintext* hash (convergent encryption), never the stored bytes. **Objects
  written by earlier origofs carry no header and are read forever.** Ciphertext is
  indistinguishable from random, so roughly one legacy object in 2^32 begins with
  `ORGE` by chance; the header therefore only chooses which interpretation to try
  first and the **AEAD tag decides**, making a coincidence cost one extra open
  instead of a false corruption report. A ciphertext from a newer origofs is
  `UnsupportedVersion` ("upgrade"), not `Corrupt` ("restore a backup").

  The key-derivation parameters were `argon2::Params::default()` — a constant the
  dependency owns, and one it has already moved once (0.4 → 0.5 raised `m_cost`
  from 4096 to 19456). A second such change would have re-derived a different key
  for every existing store on a routine `cargo update`, reported as a wrong
  passphrase, with nothing in the store recording what the real cost had been.
  They are now pinned as `KdfParams::LEGACY` and **recorded per store**, in a `kdf`
  sidecar beside the salt (same namespace, same reasoning: outside the
  content-addressed space so GC cannot sweep it, beside the content so it survives
  a metadata-DB loss). A store holding a salt but no descriptor predates this and
  is legacy by definition — the distinction matters the day `KdfParams::current()`
  is raised, which is now a safe thing to do. `tests/encryption.rs` pins the
  ciphertext of a fixed `(passphrase, salt, plaintext)`, which pins the whole chain
  — Argon2id, BLAKE3's keyed nonce derivation, XChaCha20-Poly1305 and the framing —
  so a dependency bump that moves any of it fails in CI rather than in a bucket.

- **The store descriptor's two fields move independently, so a fleet survives its
  own rollout.** `min_reader_version` is the field that gates: raising it locks
  every older build out of the *whole* store at open. It was stamped equal to
  `format_version`, and stamped on **open** — so the day `write_version` went to 2,
  the first node to upgrade would have taken the bucket away from every node that
  had not, before a single v2 object existed anywhere. That is the exact inversion
  of rule 3 ("ship the reader before the writer"), in the mechanism meant to
  enforce it. `format_version` is advisory now and still rises on open;
  `min_reader_version` comes from its own `MIN_READER_VERSION` constant, raised
  deliberately in the release that starts writing genuinely non-additive objects.
  Neither field is ever lowered, so an older build re-opening a store cannot erase
  a newer one's warning.

- **The co-edit CRDT sidecars are versioned, so their framing can be evolved
  without reading as "this document has no history".** Both shapes treated a
  sidecar they could not parse as an *absent* one — the flat shape rebuilds from
  the file, the tree shape opens an **empty** document, which a host that ignores
  `CoeditTreeDoc::resumed()` then checkpoints over the real file. The framings
  carried a single magic byte (`1` flat, `2` tree) that named the shape and not
  the layout, so any future change to either would have taken that silent
  fallback: no file corrupted, but every live document's editing history and
  per-node author stamps dropped, quietly.

  They now carry the same 5-byte header every structured object has —
  `ORGY | version | …` (flat) and `ORGX | version | …` (tree), added to
  `format.rs` under its existing rules (never re-encode a shipped version, add a
  decoder arm, ship the reader before the writer). **Sidecars written by earlier
  origofs still resume**, history intact: the old single-byte framings are read
  forever and are unambiguous against the new ones, since neither `1` nor `2` is
  `O`. A sidecar from a *newer* origofs is now `UnsupportedVersion` — an upgrade
  problem the reader reports — instead of the "absent, so rebuild" fallback;
  genuinely unrecognizable bytes still take that fallback, which is what it is
  for.

  The two kinds stay out of `GRAPH_KINDS`, and so out of the store descriptor's
  `format_version`. That is a judgement, not an oversight: a store-level bump
  locks an older build out of the *whole* store at open — every ordinary file
  read included — and co-editing is an opt-in feature (`coedit`) most workspaces
  never write a byte of. The loud per-document error is the proportionate
  replacement, and it is what the descriptor was covering for.

### Added

- **`Workspace.aclose()` and `async with`, so a long-lived host can release the
  backends on purpose** (#154). A server opens a workspace in its startup hook and
  had nothing to await at shutdown: the Postgres pool is reclaimed when the last
  handle drops, and an embedder cannot make a drop happen on demand — so a reload,
  or a test app entering a second lifespan, left the old pool alive holding its
  connections. `close` is on `Workspace` in Rust and bound as `aclose` in Python,
  where a deterministic async teardown is spelled that way.

  It reaches the real backend the way `ping` does: `close` is on both store traits
  with a no-op default, `PostgresMetadataStore` closes its pool, and every
  decorator forwards, so a close on `VerifyingStore(PackStore(…))` is not one that
  stops at the outside. It is **one-way and idempotent** — a call afterwards fails
  `Unavailable` rather than hanging or silently reconnecting, because a call after
  shutdown is a lifecycle bug and a store that quietly comes back hides it, while
  a teardown hook that runs twice is ordinary. There is deliberately no
  synchronous `close()` in Python: every I/O method there is async, so one would
  have to block on the runtime, and doing that from inside the event loop where a
  shutdown hook lives deadlocks.

  An S3/GCS backend has no override and says so at the call site — `object_store`
  exposes no shutdown hook, so it is released by dropping it and the deterministic
  half of a shutdown is the metadata pool.

- **`build_router(include=…/exclude=…)` and `build_coedit_router`** (#153). The
  FastAPI router mounted origofs's entire REST surface or nothing, behind one
  `authn`. For a host that stores bodies in origofs but owns its own access model
  that is the wrong shape — it wants the co-editing socket and routes every
  mutation through its own authorized endpoints — and with a single gate, a caller
  who satisfies `authn` could reach *every* mutating route. In an app whose own
  endpoints enforce more than `authn` does (a role, a lock), that is an auth
  bypass: a viewer `PUT`ting over a body their own endpoint would have refused.
  The workaround was to reach into `router.routes` and re-register the one route
  wanted, coupling to a path string and a route class.

  Routes are now grouped by capability — `files`, `blame`, `history`,
  `suggestions`, `revert`, `actors`, `presence`, `coedit`, `trash`, `health`
  (`ROUTE_GROUPS`) — and either list selects them. Prefer `include`: an allowlist
  keeps meaning what it said when origofs adds a group, where a denylist quietly
  starts mounting it. An unknown group name **raises**, because the quiet version
  of that bug is the dangerous one — a typo in `include` mounts nothing and a typo
  in `exclude` mounts everything, and both look like a working call. Omitting both
  is unfiltered and byte-identical to today, so no existing deployment moves.

  A new route must join a group: `test_every_route_belongs_to_exactly_one_group`
  fails otherwise, the same discipline the MCP tool and CLI subcommand
  classifications already carry. Note that read and write routes *already* carry
  different gates — `authn` is applied only to mutating routes, `reader` only to
  reads — so gating them separately needs no new parameter; what this adds is
  which routes exist at all.

- **`origofs migrate --check` and `--backup <path>`, and a schema report that
  precedes the migration it reports on.** Opening a workspace *is* the migration
  runner, and both of these subcommands ran after the open — so `origofs migrate`,
  documented as the deliberate deploy step, could only ever answer "already
  current", and `schema-version` reported the version the same process had just
  created. A pending migration was therefore unobservable, which made "should I
  take a backup first?" unanswerable in practice.

  Both now read the metadata store *unmigrated*, ahead of the open. `--check`
  reports what would be applied and exits; `--backup` snapshots the database
  before the first step and applies nothing if the snapshot fails, because a
  backup taken after the step protects against nothing. Migrations are forward-only
  and there are deliberately no down-migrations — one that dropped a column a newer
  build had been filling would destroy every row written since the upgrade — so
  that snapshot is the whole rollback plan, and the command says so when you
  migrate without one.

  The guard on the other side already existed and is now covered: an older binary
  meeting a newer database refuses it as `UnsupportedVersion` **before** touching
  it, leaving the database byte-for-byte as found, so rolling the binaries forward
  again is a recovery rather than a repair
  (`origofs-core/tests/schema_rollback.rs`).

## [0.0.3] — 2026-08-30

The 0.1.0 section below announced a tagged release, but the `v0.1.0` tag was
never pushed, so the release workflow never ran and no 0.1.0 wheels shipped.
`v0.0.3` is therefore the first release actually cut through `release.yml`,
and it carries everything since `v0.0.2`.

### Changed

- **License consolidated to MIT only.** The project was dual-licensed
  `MIT OR Apache-2.0`; it is now MIT alone. `LICENSE-MIT` became `LICENSE`,
  `LICENSE-APACHE` is gone, and every declaration (`[workspace.package]`,
  `pyproject.toml`, README, CONTRIBUTING) says `MIT`. Under the old `OR` terms
  every user already had the right to take the code as MIT, so nothing anyone
  holds is narrowed.

### Added

- **A structured (`Y.XmlFragment`) co-editing shape, so rich-text editors bind
  natively (#92).** `CoeditDoc` models a document as one flat `Y.Text`, but every
  mainstream rich-text CRDT binding — `@platejs/yjs`/`@slate-yjs/core`, `y-prosemirror`,
  TipTap — binds to a structured tree, so none of them could attach to an origofs
  room. Hosts had to mirror: serialize the editor to text on every change and diff
  it against the shared `Y.Text`. That converges, but the caret is lost on every
  remote edit, serializer round-trip noise reads as authored bytes, and attribution
  is only ever as sharp as the host's whole-file text diff — which is the part that
  undercuts the project's headline promise, since byte-level authorship is only as
  good as the edits a client can express.

  `CoeditTreeDoc` is the structured shape, attributed by the same server-side rule
  as the flat path: the content an update *introduces* is stamped with the
  connection's actor, never with an author the client names. It is a *content* diff
  per text node rather than "which runs lack an author", because Yjs makes an insert
  inherit the formatting attributes at its position — text typed against the end of
  someone else's run arrives already wearing their stamp.

  **origofs does not adopt a document model to do it.** A tree has no canonical byte
  serialization, and picking one would make the engine responsible for a dialect. The
  host — which owns the schema, because its editor defines it — supplies the
  serialized body plus a span map (`[(byte_start, byte_end, node)]`), and origofs
  resolves each node id to the author it stamped itself, landing the result through
  the existing `write_as_blamed`. The host names byte ranges and nodes, never an
  actor. Bytes no span covers — a bullet marker, a fence, a paragraph break — fall
  back to the checkpointing actor, the same rule the flat path already uses for an
  unattributed run.

  Two behaviours follow from not owning the schema, and both are deliberate: a
  document whose sidecar is missing or stale opens **empty** (a tree cannot be
  rebuilt from flat bytes), so `resumed()` must be checked before an editor is bound;
  and an out-of-band write is refused with `Conflict` rather than reconciled, because
  clobbering it silently is the one outcome worse than an error.

  Served at `GET /v1/coedit-tree/{path}` (same authentication, frame cap and
  per-connection session as the flat socket) with the host-driven
  `POST /v1/coedit-tree-checkpoint/{path}`, on both the Rust API and the FastAPI
  router; `open_coedit_tree` / `checkpoint_coedit_tree` / `persist_coedit_tree` on
  the SDK and the Python bindings.

- **Server-side durability for a shape the server cannot serialize.** Because only
  the host can produce a tree's bytes, `persist_coedit_tree` writes the ydoc sidecar
  *alone*, with no body, and the room sweeper calls it on the same tick a flat room
  checkpoints on. A worker crash then costs no editing history even though the file
  has not moved — and because it has not moved, this deliberately does not stamp
  `checkpointed_at`, which would tell a reader its bytes are fresher than they are.

### Fixed

- **`sandbox --isolate` did not work on any current LTS distribution.** The
  bubblewrap gate accepted `>= 0.8.0`, but the `--overlay` options it depends on
  landed in **0.11.0**. Ubuntu 24.04 ships 0.9.0 and Debian 12 ships 0.8.0, so on
  both the check passed and the run then died on `bwrap: Unknown option
  --overlay-src` — failing closed, but telling the operator nothing, which is
  exactly what adding a version check was meant to prevent. The floor is now
  0.11.0, and **capability is probed rather than inferred from the version**,
  because no version implies it: upstream omits the overlay options from setuid
  installs. A new `sandbox::bwrap_gap()` reports which of the three cases applies
  (absent / too old / built without overlays) and both the SDK error and the CLI's
  `--isolate` preflight now say so instead of a blanket "not available on PATH".
  Found because `--isolate` had never been executed by a test (#103).

### Added — testing

- **The isolated sandbox is exercised end-to-end.** Every case in
  `crates/origofs-sdk/tests/sandbox.rs` passed `isolate: false`, so the one thing
  origofs presents as a real security boundary was covered only by a unit test over
  the argv handed to bubblewrap. Four tests now run under bubblewrap and assert the
  boundary from *inside*: the workspace's `meta.db` and content store are
  unreachable, home directories are absent, the parent environment does not leak,
  the workspace content is still visible (so the assertions can't pass by the
  sandbox simply being empty), and an isolated run refuses rather than silently
  downgrading where bubblewrap can't provide isolation. A `sandbox-isolate` CI job
  builds bubblewrap 0.11.0 — `apt-get install bubblewrap` is *not* enough on
  `ubuntu-latest` — and sets `ORIGOFS_REQUIRE_BWRAP=1` so a missing bubblewrap
  fails the job instead of skipping it. (#103)
- **The `llamaindex` and `markitdown` integrations are actually run.** Their tests
  guard on `pytest.importorskip`, and neither package was in the `python` CI job's
  install line — so four tests in `test_rag.py` skipped on every run and
  `origofs/llamaindex.py` (201 lines) had never been imported by a test that
  executed. Both are installed now, and `tests/test_optional_extras.py` keeps it
  that way: it fails if the extras declared in `pyproject.toml` drift from what CI
  installs, so a new integration cannot arrive already unexercised. (#104)

## [0.1.0] — 2026-08-10

The first tagged release, and the first one you can install rather than build:
`origofs` now ships as abi3 wheels (see *Added* below). Everything that follows
was previously reachable only by cloning at a commit SHA.

### Added — object storage

- **`S3Config` accepts a `session_token`.** The S3 builder set only the access
  key and secret, so temporary credentials — the kind AWS SSO / SAML federation
  and any `AssumeRole` flow hand out — could not be used: their key pair is only
  valid when the STS session token travels with every request, and there was
  nowhere to put it. Added an optional `session_token` (forwarded to the object
  store via `with_token`, redacted in `Debug`), plumbed through the Python
  binding (`S3Config(..., session_token=...)`) and the CLI content config
  (`session_token`, or `ORIGOFS_S3_TEST_SESSION_TOKEN` in the gated tests). Omit
  it for long-lived keys or anonymous access — behaviour is unchanged.

### Added — media

- **Chunk uploads run concurrently.** They were stored one at a time —
  `put().await` in a loop — so a write cost one round trip per chunk. Invisible on
  a local store and dominant on object storage: content-defined chunking turns
  1 GiB of incompressible data into ~13,700 chunks, so at a 30 ms RTT a single
  gigabyte was about seven minutes of pure latency with the link nearly idle.
  Bounded window (`ORIGOFS_UPLOAD_CONCURRENCY`, default 16), ordered, so the
  manifest stays in byte order.
- **`GET /v1/files/*` supports `Range`.** The Rust HTTP API had no range handling,
  no `Accept-Ranges`, and no `Content-Length`, and served everything as
  `application/octet-stream` — so a `<video>` could not seek, a download could not
  resume, and a browser downloaded media instead of playing it. The Python router
  had honoured `Range` from the start; the surface it mirrors did not. Single-range
  `206`/`416` per RFC 9110, streamed rather than buffered (a player may legally ask
  for `bytes=0-`).
- `Content-Type` is guessed from the extension on both HTTP surfaces — a small
  closed table in Rust, `mimetypes` in Python — defaulting to
  `application/octet-stream`.
- The chunker's hand-off queue is as deep as the upload window. At the old fixed
  depth of 8 a 16-wide window could never fill, silently halving the concurrency
  it advertised.
- `Fs::read_range_stream` / `read_range_stream_owned`: a ranged read that streams
  and trims the boundary chunks, rather than materializing the range like
  `read_range`.

### Added — arbitrary file sizes

- **`write_reader_as`: attributed streaming writes.** Streaming and attribution
  were mutually exclusive: `write_reader` was the only streaming write and it is
  unattributed, so supplying an actor — the entire premise of origofs — forced the
  whole body resident. `origofs write --from big.bin` streamed; adding `--actor 7`
  did not. Subject to the write policy, and blame covers the whole file rather
  than being diffed against a previous body that is deliberately not resident.
- Python `write_path_as` / `write_path` / `read_to_path`. Neither `write_reader`
  nor `read_stream` had ever been bound, and `write`/`write_as` take a `bytes`
  object that pyo3 copies into Rust — about 3x the file transiently — so the
  effective ceiling from Python was available memory. Measured: a 287 MiB
  attributed write went from 312 MB peak RSS to 27 MB.
- `origofs write --from FILE --actor N` streams. A propose-only actor still
  buffers, because a queued suggestion holds the proposed bytes; deciding that up
  front keeps `origofs policy <actor> propose` behaving identically with and
  without `--from`.
- `origofs.fastapi`'s `PUT /files/{path}` streams the request body, spilling past
  8 MiB to a temp file handed to `write_path_as`. It took `body: bytes`, so the
  whole upload sat in memory — asymmetric with the `GET` beside it, which has
  always streamed and honoured `Range`.
- `docs/LIMITS.md`: where the ceilings actually are, and which paths stream.

### Fixed — silent overflow at the encode boundary

Three `as u32` casts wrapped silently past their range, corrupting an object at
*write* time and surfacing only on the next read. Each had a careful *decode*-side
guard — the format layer reasoned hard about hostile input coming in and not at all
about honest data going out. All three now return `TooLarge`.

- `Manifest::encode`'s chunk count (wraps at 64 TiB).
- `PackLoc`'s pack offset, staged-blob length, and trailer length (wrap at 4 GiB —
  reachable, because `stage` seals *after* inserting, so any single `put` larger
  than the pack target gets a pack of its own, and a ~7.5 TiB file's manifest is
  such a put).
- `ObjectContentStore` has no multipart upload by design (chunks are ≤ 256 KiB;
  packing is the answer to per-request cost). Only a manifest can approach the
  S3/GCS 5 GiB single-request ceiling, and it now fails locally with a message
  naming the cause rather than as a raw provider error partway through.

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
- **`origofs.fastapi` was the third surface, and no one had audited it.** Every
  mutating route authenticated the caller and then discarded the principal —
  the handlers named it `_ctx` — calling the unattributed engine ops. Those skip
  `ensure_may_write` and record no `edit_op`, so a propose-only actor could not
  overwrite a file through `PUT` but could delete it and commit the deletion.
  `DELETE`, `POST /dirs`, `/rename`, `/commit`, `/branches`, `/checkout` and
  `/actors` now call the attributed variants: a propose-only delete is queued for
  review, the rest are refused, and namespace mutations carry an actor.
- The router mapped a policy refusal to `409`. `PermissionError` subclasses
  `OSError`, which it maps to conflict for a non-empty directory; an
  authorization outcome is `403`, and `409` already carries stale-base semantics
  for suggestion accepts.
- The router's `POST /sessions` read its actor from the request body, the same
  break the Rust surface had two bullets above.
- Both anti-regression guards now cover Python: a structural test that parses the
  router and fails on a handler that binds its principal and drops it, and a fake
  workspace defining only the attributed methods, so an unattributed call is an
  `AttributeError` rather than a silent success.

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
- The co-editing WebSocket now caps frames at 16 MiB. `DefaultBodyLimit` does not
  apply to WebSocket frames, so `ApiOptions::max_body_bytes` silently did not
  govern `/v1/coedit/*` — the one hole in the request budget.
- `413` responses name the limit and the setting that changes it, instead of an
  empty body.
- Python's `io_err` maps `NotFound`/`PermissionDenied`/etc. to the matching
  builtin exception rather than flattening everything to `OSError`, and is no
  longer `#[cfg(unix)]` — the streaming bindings touch the filesystem on every
  platform.
- **Python wheels are built and published on tag** (`.github/workflows/release.yml`).
  origofs was published nowhere, so every consumer built the extension: container
  builds carried a Rust toolchain in a dedicated maturin stage, `uv sync` could
  not install origofs at all — so a host had to import it lazily and degrade
  everywhere — and pinning meant a commit SHA. abi3-py39, so one wheel per
  platform covers CPython 3.9+: manylinux x86_64/aarch64, macOS arm64/x86_64,
  Windows x64, plus an sdist. Each leg smoke-tests its own wheel by writing
  attributed bytes and reading blame back. Tagged builds attach everything to the
  GitHub Release; PyPI publishing uses Trusted Publishing and is off until the
  `PUBLISH_TO_PYPI` repository variable is set.
- PyPI-facing package metadata: the README as the long description, project URLs,
  and classifiers.
- **`revert_session` takes a `path_prefix`, and returns the paths it changed**
  rather than a count. In a multi-tenant workspace — one workspace, tenant-scoped
  paths — an "undo this agent's work" button lives in *one* tenant's UI, but the
  session it reverts may have written anywhere, and an unscoped revert followed
  it there silently. The prefix matches on directory boundaries, so `/tenant-a`
  covers `/tenant-a/notes.txt` and never `/tenant-abc/notes.txt`. Filtering
  inside the call is the point: the documented workaround — pre-flight with
  `edit_ops`, check the paths, then revert — reads the session's reach and acts
  on it in two calls, so a write landing in between is reverted without ever
  having been checked. Across the engine, SDK, HTTP API (`path_prefix` in the
  body, `paths` in the response), CLI (`--path-prefix`), and Python.
- `POST /v1/revert-session` had no test at all, on a route that deletes other
  people's work. It has three now.
- **A co-editing credential can ride `Sec-WebSocket-Protocol`** —
  `new WebSocket(url, ["origofs", token])`, the one header a browser can set on
  an upgrade — on both the Rust HTTP API and the FastAPI router. The server
  echoes back only the `origofs` marker, which the handshake requires and which
  a client that offered no subprotocol must not receive. `?token=` was the
  documented answer and keeps working, but a URL is the worst place for a
  credential: it lands in access logs, proxy logs and `Referer`-adjacent tooling
  by default, where a subprotocol value does not.
- **`origofs.fastapi.build_router` takes a `root=`, so a multi-tenant host can
  actually use it** (#93). A host putting many tenants in one workspace could
  authorise the path-carrying routes — its dependency reads
  `request.path_params["path"]` — but had nothing to authorise the
  workspace-global ones against: `/log`, `/status`, `/diff`, `/events`,
  `/presence`, `/branches`, `/suggestions`, and the id-addressed suggestion
  routes, where a workspace-global id was itself enough to read, accept or reject
  somebody else's proposal. The only safe move was to refuse all of them and
  re-implement blame and suggestion review in front of the SDK.

  `root` is a fixed path (mount one router per tenant) or a dependency resolving
  one from the request (one router that scopes itself). Every caller-supplied
  path resolves *under* it, so there is no representable request for another
  tenant's file; listing routes are filtered to it; and the id-addressed
  suggestion routes answer `404` outside it — `404` rather than `403` so a caller
  cannot walk the id space to learn what other tenants have open. Operations no
  filter can narrow — commit, branches, checkout, and the commit log, a shared
  history whose messages and authors belong to everybody — are refused with
  `403`. Actors and sessions stay workspace-wide, because identity is store-wide
  in origofs by design and a tenant-scoped actor would be a fiction. Without
  `root` nothing changes.

  **The Rust HTTP API has the same workspace-global routes and the same gap.**
  This fixes the surface the issue was filed against; the shape is unsolved
  there.
- **Live co-editing rooms are checkpointed on a cadence, not only on last
  leave.** A room's CRDT lives in process memory, and its only path to durable
  storage was the last socket disconnecting — but a browser tab left open on a
  document *is* an open room, so that could be hours. Until then `read` served
  the last checkpoint and blame carried only the runs folded in at that point,
  and a worker dying in between lost the rest of the session from the durable
  side (bounded by the relay's replay window on Postgres, unbounded on SQLite,
  where the relay is off). A new `CheckpointPolicy` — on `ApiOptions` in Rust and
  `build_router(checkpoint=…)` in Python — checkpoints a room 5 seconds after it
  goes quiet and at least every 60 seconds while it stays busy. Two triggers
  because they answer different questions: idle bounds a *finished* burst of
  typing, interval bounds a *continuous* session, which idle alone never would
  since every keystroke resets it. Driven inside the SDK rather than left to each
  host, which has no signal about room activity — "call `checkpoint_all` on a
  timer" writes idle rooms and misses busy ones. Disable both triggers for the
  previous behaviour.
- **`live_doc` reports `checkpointed_at`** (schema V16), so a reader learns not
  just *that* the bytes may lag an open editor but by how much — "last saved 3
  minutes ago" instead of "this may be stale". Distinct from `since`, which is
  when the path first went live and deliberately never moves; `None` for a path
  that is live but has never been checkpointed.
- **A co-editing connection is bound to a session**, opened for it when the
  credential names only an actor. Such a connection stamped its edits
  `(actor, session=None)`, which `revert_session` can never undo — the feature
  the op-log exists for, missing on the surface that produces the most edits,
  since every keystroke is one. One session per connection is the natural unit:
  exactly what one person typed in one sitting.
- **The Python stubs type what they return.** 31 methods returned
  `dict[str, Any]`/`list[dict[str, Any]]`, with the keys described only in a
  neighbouring comment — so a caller had to guess whether `span["actor"]` was an
  id or an inline record (it is a record), and whether a timestamp was
  `created_at` or `created_ts` (both exist, on different records). 24 `TypedDict`s
  now describe every record, with `Literal` unions for the closed string sets.
  A runtime test drives a real workspace and compares each record's keys to its
  declaration, so the stub cannot drift from the extension the way a comment
  can — and a new record must be exercised or explicitly excused.

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
- **FUSE is Linux-only in the Python extension**, narrower than the SDK's own
  `cfg(unix)` reach. It was `cfg(unix)`, which made a macOS wheel unbuildable:
  `fuser`'s build script unconditionally probes pkg-config for macFUSE there, and
  a kernel extension is not something a wheel can carry — the same reason the
  `macos` CI job already excludes FUSE. macOS keeps `serve_nfs`, the mount path
  `docs/DESIGN.md` specifies for it, and `Workspace.mount()` raises a clear error
  as it already did on Windows. Linux is untouched: `fuser`'s `libfuse` feature is
  off by default, so it takes the pure-Rust mount path and needs no system
  library. Building from source on a Mac with macFUSE installed still works by
  enabling the `fuse` feature and the `fuser` dependency by hand.

### Performance

- `store_body` chunks off the async runtime. The FastCDC scan is a CPU-bound pass
  over the whole buffer behind `write`, `write_as`, `vfs_write`, and every merge,
  and it pinned a runtime worker for the duration.
- Reassembly buffers no longer pre-allocate from a manifest's declared size, which
  a crafted manifest controls.

---

## Stability

origofs is pre-1.0. The Python package is published to PyPI as `origofs`; the
Rust crates are **not yet on crates.io**. Until 1.0:

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

[Unreleased]: https://github.com/danplischke/origofs/compare/v0.0.3...HEAD
[0.0.3]: https://github.com/danplischke/origofs/compare/v0.0.2...v0.0.3
[0.1.0]: https://github.com/danplischke/origofs/releases/tag/v0.1.0
