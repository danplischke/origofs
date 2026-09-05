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

There is no `rust-toolchain` file. **All four crates use `edition = "2024"`**,
inherited from `[workspace.package]` (edition 2024 itself sets a Rust ≥ 1.85
*language* floor). The **effective MSRV is 1.88**, though — the code uses `let`-chains (stabilized in 1.88) and the dependency
graph (`icu`, via `url`/`object_store`) needs ≥ 1.86 — and the `msrv` CI job pins it
so an accidental newer-stdlib use or a dependency MSRV bump is caught. CI lives at
`.github/workflows/ci.yml` (fmt + clippy + tests, an explicit `coedit` pass, and the
`msrv` floor) and otherwise runs on stable. Use a recent stable toolchain.

**All three platforms are built and tested**: Linux is the primary leg, plus a
`macos` job (SQLite + the NFS surface, no FUSE) and a `windows` job (SQLite + the
portable surfaces, plus the `x86_64-pc-windows-msvc` wheel `release.yml` ships).
Cross-checking Windows locally without a Windows box:
`rustup target add x86_64-pc-windows-gnu`, install `gcc-mingw-w64-x86-64`, then
`cargo clippy --target x86_64-pc-windows-gnu -p origofs-sdk --features full
--all-targets`. No `cfg(target_env)` appears anywhere in the tree, so the
`gnu`/`msvc` split changes nothing about which code is selected — the `gnu`
cross-check catches every `cfg`-shaped break, and only linking differs.

## The one architectural idea everything hangs on

**The metadata store and the content store are split, and never mixed.**

- **`ContentStore`** holds the *bytes*: FastCDC content-defined chunks addressed
  by their BLAKE3 hash, plus the immutable git-style objects (`blob` = a chunk
  *manifest*, not raw bytes; `tree`; `commit`) that form a Merkle DAG. Immutable,
  deduplicated, integrity-verified on read.
- **`MetadataStore`** holds the *names and versions*: inodes, dentries, symlinks,
  refs, the attribution op-log and blame index, the audit log, the change
  feed, and presence. It stores content only as `manifest_hash` references —
  **it must never hold large file bytes.**

Both traits live in `origofs-core` (`content.rs`, `metadata.rs`) and both are used
as `Arc<dyn …>`, so a workspace picks its backends at runtime.

**`MetadataStore` is twelve traits, not one.** It was a single trait with 93
methods covering the POSIX namespace, refs, config, workspaces, ACLs, trash,
three unrelated kinds of lock, attribution, the change feed, the suggestion queue
and the portable dump format — so "pluggable metadata database" cost 93 methods
and a third hand-written SQL dialect to make good on. The concerns were already
known (the trait carried section comments for exactly these groups); they are
types now: `StoreLifecycle`, `NamespaceStore`, `RefStore`, `ConfigStore`,
`WorkspaceRegistry`, `AclStore`, `TrashStore`, `LockStore`, `AttributionStore`,
`CollabStore`, `SuggestionStore`, `PortableStore`.

`MetadataStore` remains as the sum, a marker with a blanket impl, so a backend
gets it by implementing the parts and no method lives on it to drift. They are
supertraits, so a `dyn MetadataStore` still resolves all 93 methods directly.
**Depend on `MetadataStore` only where you genuinely need the whole store** (the
engine, the `Workspace` façade); name the part everywhere else — that is the
point of the split. The cost, which is real: a caller has to import each part
whose methods it calls, because Rust resolves a method only through a trait in
scope. The mutable POSIX
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
  - **A revision is not a second proposal (#164).** `write_or_propose` had no
    update-in-place, so an actor told "revise your proposal" could only propose
    again — stacking a *sibling* on the same base. Accept handled that correctly by
    accident (landing either moves the file, so the other goes stale); **reject did
    not**, leaving the abandoned draft `pending` with a base that still matched the
    file, so it accepted cleanly and landed text the author had replaced and the
    reviewer never chose. `replaces: Option<i64>` on `suggest`/`suggest_delete`/
    `write_or_propose`/`remove_or_propose` retires that draft as the new one is
    created; `supersede_suggestion` is the standalone form.
  - **Opt-in, not a default.** Two drafts a reviewer chooses between is a real
    workflow and origofs cannot tell it from a revision, so retiring the earlier
    one automatically would silently discard the alternative — the same class of
    unasked-for outcome as #158, in the other direction.
  - **"Stale" is not "obsolete".** `supersede_stale_byte_suggestions` retires
    proposals whose *base moved on*, and an author revising a draft has changed no
    bytes, so it returns `0` for siblings. That is a different relation, not a gap
    in it — which is why #164 needed a new operation rather than a fix there.
  - **Retiring is ordered before creating**, so the guarantee is the one a caller
    relies on: if the propose returns `Ok`, `replaces` is superseded. The opposite
    order leaves both pending when the second half fails, which is the bug. The
    cost is the reverse window (retired, no replacement), and that one is
    survivable — the row and its bytes are still there. The propose check runs
    first so a refusal cannot retire a draft for nothing.
  - **Disposing of a draft is authorized like rejecting one**: the author may
    always retire their own, anyone else needs `WRITE` at its path — otherwise one
    propose-only agent clears another's work out of the queue. **Every** propose
    call carries `replaces`, the CRDT pair included: stacked CRDT drafts are less
    dangerous (a CRDT proposal never goes stale, and applying an author's earlier
    state after their later one merges a subset), but "the proposal I meant is no
    longer this one" is the same relation on either shape.
  - **"Already accepted" is a conflict, not a bad request (#164).**
    `OrigoFSError::AlreadyResolved` (`already_resolved`, `409`,
    `AlreadyResolvedError` in Python) replaced the `InvalidArgument` every
    resolve path returned for a settled row — which said the *request* was
    malformed when it was well-formed and merely out of date. It is the third thing
    a reviewing caller handles beside `StaleBase` and the raced-CAS `Conflict`, and
    unlike either it is terminal: read the row's status, there is nothing to retry.
- **The write policy and the ACLs are enforced in the engine, not per surface.**
  Every *attributed* mutation on `Fs` — `write_or_propose`, `remove_or_propose`,
  `rename_as`, `mkdir_as`, `symlink_as`, `commit_as`, `checkout_as`,
  `create_branch_as`, `revert_session_as`, `accept_suggestion`,
  `reject_suggestion`, `open_coedit`, `open_coedit_tree`, `checkpoint_coedit`,
  `checkpoint_coedit_tree`, `load_coedit_tree_as`, and `record_suggestion` (every suggestion funnels
  through it) — runs one of four checks, all refusing with
  `OrigoFSError::Denied` (`403` on the HTTP API). Ops with a propose-shaped
  equivalent queue instead of refusing.
  - `ensure_may_write_at` (`acl.rs`) — the default. Takes the path and consults
    the grant covering it, falling back to the actor's write policy where there
    is none.
  - `ensure_may_write_workspace` (`acl.rs`) — for an op with no single path that
    reaches every one of them (`commit`, `checkout`, `create_branch`, an
    unbounded `revert_session`); checks the grant at `/`. **Having no path is not
    the same as touching none** — these used to take the path-less check below,
    so no ACL could contain them.
  - `ensure_may_propose_at` (`acl.rs`) — the suggestion queue's counterpart to
    the first, satisfied by `WRITE` or `PROPOSE`. On `record_suggestion`, so it
    covers byte, delete and CRDT suggestions at once — calling `suggest*`
    directly used to queue a proposal for an actor denied both rights, since the
    check lived only inside `write_or_propose`.
  - `ensure_may_write` (`suggest.rs`) — policy only, no grant. Now *only* for
    path-free administration (registering an actor, setting a policy). Reach for
    it last, and never for something that touches the working tree. **A new mutating endpoint
  on any surface must call an attributed variant**, never the raw `remove`/
  `rename`/`mkdir_p`/`symlink`/`commit` — those take no actor, exist for internal
  machinery (checkout, merge materialization, applying an accepted suggestion),
  and are exempt by construction. `tests/mcp.rs` fails on an unclassified MCP tool
  so a new ungated one can't ship silently, and
  `origofs-cli/tests/cli.rs::every_mutating_subcommand_is_classified_and_attributable`
  does the same for the CLI — every subcommand must be classified, every exemption
  must carry a *reason*, and every attributed one must actually offer `--actor`.
  Issues #78, #128.
- **A mount is bound to one actor, or to none — and `none` is the only bypass
  left.** The FUSE/NFS surfaces address everything by inode through the `vfs_*`
  layer, which took no actor at all, so path-scoped ACLs did not reach them: an
  agent refused `WRITE` under `/src` over MCP took the identical action through a
  mount. Since #141 every mutating inode op has an ACL-checked `vfs_*_as`
  counterpart taking `Option<WriteCtx>`, and both mounts hold one for their
  lifetime (`fuse::spawn_as`/`mount_as`, `nfs::serve_as`, `origofs mount --actor`,
  `origofs nfs --actor`, and `ws.mount(..., ctx=)` / `ws.serve_nfs(..., ctx=)` from
  Python). `None` is the historical anonymous mount and still bypasses, which is
  why it is a *visible argument* rather than an absent one.
  - **Checked in the engine, as always.** The guard is inside the `_as` method, so
    a caller cannot forget it; what a surface *can* do is call the unchecked op
    instead, and that is source text rather than behaviour. Two structural tests
    close it: `origofs-core/tests/vfs_acl.rs::every_inode_op_has_a_checked_counterpart`
    (a new `vfs_thing` must gain a `vfs_thing_as` or an exemption with a reason)
    and `origofs-sdk/tests/mount_acl.rs` (no mount may call an unchecked op). The
    latter is deliberately not feature-gated, so it runs on the Windows leg too.
  - **It authorizes; it does not attribute.** A write through a mount still
    records no `edit_op` and no blame. The mount's actor bounds what the mount can
    reach — do not read it as attribution, and do not read `--actor` as
    authentication: the kernel never says which process issued a request, and
    NFSv3 authenticates nobody, so one actor covers everything on that mountpoint
    or socket.
  - **Reads follow the same opt-in as everywhere else.** Gated only under
    `acl_enforce_reads`, and `readdir` filters per entry like `ls_as` — a listing
    that names what a `stat` would refuse is the existence oracle the refusal
    exists to prevent. The filtered listing pages internally, because a page that
    filters to empty would otherwise read as end-of-directory and truncate.
- **A checkpoint never overwrites a file that changed underneath it.** The tree
  shape refuses (`ensure_tree_coherent`) — origofs cannot parse bytes back into
  nodes. The flat shape folds the foreign write in (`reconcile_out_of_band`,
  replaying the CRDT sidecar, comparing against the live marker's coherence hash),
  and **refuses when it cannot** — a missing or unreadable sidecar,
  a removed file, bytes that are no longer UTF-8. Those arms used to
  `return Ok(())`, which the caller read as "nothing to reconcile" before
  overwriting. **A branch checkout is the case that makes it bite:** `checkout`
  rematerializes the file *and* swaps away the sidecar (it lives in the working
  tree, under `/.origofs/ydoc/`), while the live marker is metadata and survives —
  so reconciliation's one input is gone exactly when it is needed, and a room
  opened on the old branch wrote its content onto the new branch. `commit` and
  `checkout` are otherwise blind to live rooms by design; the checkpoint is where
  that is caught, and the caller recovers by re-opening the document.
- **A socket-less checkpoint must not claim the path.** A host landing tree bytes
  with no editor attached (a "Save" button) goes through `load_coedit_tree_as` —
  the write check, no live marker. `open_coedit_tree` there leaked a permanent
  marker, because the matching `end_coedit` lives on the socket disconnect path
  that flow never reaches. Same rule as `load_coedit_as` on the flat side. Note
  `_as` here is **not** the propose form (that is `load_coedit_tree_to_propose`),
  which is why both docstrings spell the distinction out rather than leaving it to
  the name.
- **The tree checkpoint's guard is on the document, not on the live marker
  (#158, #161).** `CoeditTreeDoc::base_hash` is the BLAKE3 of the body the document
  is *coherent with* — what it resumed from, was told about via `seeded_from`, or
  last crystallized — and `ensure_tree_coherent` compares that against the file,
  with the persisted sidecar as a cross-worker second opinion (that is what keeps a
  second worker's replica entitled to checkpoint after the first landed a body).
  The marker was the wrong home for it in two ways, both measured: **opening** a
  room refreshes the marker's `content_hash` from the file, so a stateless handler
  that re-opened before each checkpoint reset the guard to whatever a foreign write
  had just produced; and a socket-less checkpoint leaves no marker at all, so it was
  never guarded. Both look identical to the working case from the call site.
  - **An unseeded document over a non-empty file is refused too.** It is *empty*, so
    landing it is the data loss rather than a step towards it. `seeded_from(body)` is
    how a host that parsed the file into the tree says so — and, from the file's
    current bytes, is also the deliberate-overwrite escape hatch, written down
    rather than assumed. Over HTTP that is `"seeded_from_file": true` on the
    checkpoint route.
  - **`persist_coedit_tree` frames the sidecar against the document's base, never
    the file's current bytes.** Framing against the file laundered a foreign write
    into "coherent" (the next open resumed and checkpointed straight over it) and
    handed an unseeded document a claim it had not earned, re-arming the overwrite
    through the sweeper's timer instead of through the caller.
- **A co-editing socket is a write channel, and takes the write check to open.**
  `open_coedit`/`open_coedit_tree` require `WRITE` at the path, not merely a
  valid credential — the WebSocket upgrade authenticates but does not authorize,
  so without this any authenticated caller edited any path: `write_or_propose`
  refused them and the identical bytes landed through `checkpoint_coedit`, whose
  `write_as_blamed` is exempt by construction (it is the CRDT coordinator's own
  write). Both checkpoints re-check as a backstop for a caller holding a
  `CoeditDoc` from elsewhere. To build a *proposal* against a co-edited document
  without a session, use `load_coedit_as` — propose right, no live marker; that
  is what the `origofs_suggest_coedit` MCP tool does, and gating it on write
  would have broken the propose-only agents the tool exists for.
- **Both document shapes have a proposal path, and they accept differently.**
  The tree shape had no counterpart to `suggest_coedit`, so on the shape a
  rich-text editor actually binds to (`Y.XmlFragment`), a propose-only actor
  could not reach the review queue at all — its options were a byte suggestion,
  whose base goes stale on every keystroke elsewhere in the file and whose
  acceptance discards concurrent work, or nothing. `suggest_coedit_tree` (and
  `suggest_coedit_tree_update` for a browser holding the blobs) records one, and
  `load_coedit_tree_to_propose` resumes the replica to build it against —
  *propose* right, no live marker. Note the asymmetry with `load_coedit_tree_as`
  beside it, which serves a host's socket-less **checkpoint** and so takes the
  write check: on this shape `_as` is not the propose form.

  It is a separate `SuggestionKind` (`crdt-tree`) rather than a flag on `Crdt`
  because **acceptance differs**. Landing a flat proposal is `applyUpdate` then
  serialize the `Y.Text`, which origofs can do; landing a tree one needs the
  document written back out as bytes, and only the host knows the schema — the
  same reason `checkpoint_coedit_tree` takes a body. So `accept_suggestion`
  *refuses* a tree proposal and names `accept_coedit_tree_suggestion`, which
  takes the host's `body` and `spans` and resolves the row in one call. One kind
  for both would have applied a tree update to a flat document and produced a
  file nobody can read.

  Acceptance carries the review rules itself — approver holds `WRITE` at the
  path, approver ≠ author — because it does not route through
  `accept_suggestion`. It lands the bytes as the **author** through
  `checkpoint_coedit_tree_unchecked`, and that split is load-bearing on both
  shapes: `apply_coedit_suggestion` used to call the *checked* `checkpoint_coedit`
  as the author, so a propose-only actor's CRDT proposal was recorded happily and
  then refused at acceptance with a `Denied` naming the author — the review queue
  was unusable for exactly the population it exists for. The approver's right is
  established before that point; re-checking as the author asks the wrong actor.
- **Usage accounting reaches the CLI too.** `#116` built recursive usage,
  `statfs` and quotas, and the mounts answer `df` from them — but nothing on the
  CLI did, so a workspace could not be measured or capped without writing code.
  `origofs du [path]` and `origofs quota [--bytes|--inodes]`. Both count an inode
  with several names once and sum **logical** size, never physical: a quota in
  deduplicated bytes would move under a user who changed nothing, because
  someone else's write can dedup against theirs.
- **The trash is a recovery path, so it has to be reachable.** A committed file
  can be read back out of history; an **uncommitted** one could not be recovered
  at all, which matters more here than for an ordinary filesystem because the
  users are agents and `rm -rf` on a bad path is a routine failure mode. `#115`
  built the engine half — retention config, GC root 5, an entry carrying *the
  actor and session that deleted it*, so a restore is attributed and the deletion
  is already in the op-log beside it — and nothing exposed it: no subcommand, no
  route, no tool. `origofs trash list/restore/purge/retention`, `origofs_trash` +
  `origofs_restore`, `GET /v1/trash` + `POST /v1/trash/{id}/restore` +
  `DELETE /v1/trash/{id}`, and the matching FastAPI routes.
  Retention stays **off by default**: turning it on silently would change when
  space is reclaimed for every existing deployment, and the first anyone would
  learn of it is a storage bill. An empty listing therefore distinguishes
  "nothing deleted" from "not collecting" — only one of those is a configuration
  answer. Three rules the surfaces share: a **restore is a write**, so it takes
  the attributed `restore_trash`; a **purge takes `WRITE` at the entry's path**,
  because it destroys the only remaining copy of an uncommitted file; and a
  **trash id is a workspace-global integer**, so every id-addressed route
  resolves it to a path and checks the scope first, answering *not found* rather
  than *denied* for the same reason the suggestion routes do.
- **Changing an ACL is itself a gated operation — use the `_as` form.** `grant`,
  `revoke`, `set_acl_default_deny`, `set_acl_enforce_reads` and `set_write_policy`
  take **no** authorization: `granted_by` is an audit field the caller fills in,
  not a claim anything verifies, so an actor reaching them hands itself `WRITE` at
  `/` (measured: a propose-only agent self-granted and went from `Proposed` to
  `Wrote` in two calls). That is survivable only because no network surface exposes
  them — no ACL route on the HTTP API, no MCP tool, no CLI subcommand — and safety
  by absence of a route is not safety. `grant_as`/`revoke_as`/
  `set_acl_default_deny_as`/`set_acl_enforce_reads_as`/`set_write_policy_as` are
  what a surface must call. Two conditions on a grant, both load-bearing (a test
  fails for each alone): **`WRITE` at the prefix**, because delegation is
  administrative — being able to read a subtree does not make you the one who
  decides who else reads it; and **no amplification** — every bit granted must be
  one the granter holds there, or a write-only actor mints itself `READ`. `WRITE`
  implies `PROPOSE` for delegation (as in `ensure_may_propose_at`) so a holder of
  `READ|WRITE` can hand on `READ|PROPOSE`; it deliberately does **not** imply
  `READ`. The workspace switches take `ensure_may_write_workspace`: ungated, an
  actor denied a read would simply turn enforcement off. The raw forms stay for
  provisioning, which by construction has no actor — the first grant in a fresh
  workspace precedes anyone holding rights in it.
- **Reads are checked only where a workspace opts in.** `Perms::READ` went from a
  bit nothing consulted to one `ensure_may_read_at` enforces, behind
  `acl_enforce_reads` (workspace setting, **default off**) — reads have never been
  checked, so no existing workspace holds read grants and enforcing on upgrade
  would stop every actor at once. The attributed reads (`read_as`,
  `read_range_as`, `stat_as`, `ls_as`, `readlink_as`, `blame_as`) run it; the
  unattributed ones stay open by construction like `remove`/`rename`/`mkdir_p`,
  because checkout, merge, gc and the CRDT coordinator are built from them. Like
  the write checks it runs **before** any lookup, so a denial cannot leak
  existence — which matters more here, since probing for existence is the point of
  an unauthorized read.

  **`ls_as` filters per entry, and had to.** It checks the directory (a refusal —
  an empty listing would say "this directory is here and holds nothing") and then
  drops the entries the actor may not read (an absence — an entry it may not read
  looks exactly like one that is not there). The pair `ls_as`/`stat_as` has to
  agree, because a listing that hides what a stat serves is the oracle; both ask
  the same resolver whether the actor holds `READ` at the entry's own full path,
  so neither can drift.

  **The collection reads filter the same way** — `diff_as`, `presence_as`,
  `list_suggestions_as`, `live_paths_as` — and the id-addressed ones
  (`get_suggestion_as`, `suggestion_diff_as`, `live_doc_as`) answer *not found*
  rather than *denied*: a suggestion id is a guessable, workspace-global handle,
  so a refusal would confirm one exists at it. A row with **no** path is dropped
  too (an idle `Presence`), matching the ruling `Scope::contains(None)` already
  makes for tenancy. Two collections stay ungated with reasons in the exempt
  lists: `log` (commit metadata, no paths) and the **change feed** — filtering
  `watch(after_seq)` would leave a client that can see none of the rows polling
  the same cursor forever, so it needs a high-water mark in the response shape
  first.

  **Every surface threads the actor, and each has its own answer for "no actor".**
  The engine half shipped first and no surface called it, which made the switch
  decoration on the only place it is for. Now: the HTTP API's `ReadAuth`
  extractor resolves a principal when there is one and, when there is not, serves
  the read anonymously while enforcement is off and answers **401** once it is on
  — so turning the switch on closes the anonymous door by itself, without also
  setting `gate_reads`. MCP always has an agent, so its read tools simply pass
  `self.ctx()`. The CLI takes `--actor`/`ORIGOFS_ACTOR` on every read that
  reveals a path, which is not an identity check (nothing on the CLI is) but is
  how you see what an actor would actually be served. `build_router`'s `reader`
  dependency may now return a `WriteCtx`, and reads run as it; returning `None`
  keeps the older gate-only shape.

  Three structural guards keep it that way, one per surface, because a new route
  or tool is invisible to behavioural tests:
  `api_read_acl.rs::every_read_route_binds_its_read_auth`,
  `mcp.rs::no_mcp_tool_reads_through_an_unattributed_method`, and rule 5 of the
  CLI's `every_mutating_subcommand_is_classified_and_attributable`. Each takes an
  exempt list where every entry carries a reason — the same discipline the write
  side already had, and for the same reason: "exempt" with no reason is how #78
  and #128 both happened.

  **`origofs acl` is the surface the ACLs never had.** No HTTP route, no MCP
  tool, and until now no subcommand — so a workspace could not be configured
  without writing Rust or Python, and "no route exists" was the only thing
  standing between a propose-only agent and a self-granted `WRITE` at `/`.
  `acl grant/revoke/default-deny/enforce-reads` take `--by` and go through the
  gated `_as` forms; omitting `--by` uses the ungated provisioning form and says
  so in its output, because the raw forms exist for the first grant in a fresh
  workspace and not for a caller who forgot the flag. `acl show` and `acl check`
  answer the question an ACL bug is actually asking. Issue #124.
- **A FastAPI route group is a capability, and it is split where a host needs to
  draw the line (#153, #160).** `origofs.fastapi._ROUTE_GROUPS` names every route
  by what it lets a caller *do*, and `include=`/`exclude=` mount by those names —
  for the host that stores bodies in origofs but owns its access model, and would
  otherwise have a route a caller satisfying `authn` can reach past its own checks.
  `coedit` was one group covering both sockets *and* the mutating tree-checkpoint
  route; `coedit-ws` and `coedit-checkpoint` split them, with `coedit` kept as an
  **alias** for both so every existing `include`/`exclude` still means what it
  meant. The bundling rationale ("a tree socket cannot work without a checkpoint")
  was true of the capability, not of *this route*: a host with its own save path
  needs origofs not to mount a second write path to the same bytes, gated
  differently. Note `authn` cannot express this — sockets and checkpoint are all
  mutating, so no strictness ordering separates them; route selection is the only
  lever. Aliases are expanded before the set arithmetic, so an `exclude` drops
  every half. `test_every_route_belongs_to_exactly_one_group` keeps the table
  honest: an unclassified route ships on every filtered router by accident.
- **`effective_perms` is cached, and the cache is exact rather than fresh-ish.**
  It was up to three round trips — `list_acl` returns *every* grant the actor
  holds — with a linear prefix match over the result: 16% of a read at one grant,
  **228%** at 201, growing with exactly the per-project grants a multi-tenant
  deployment accumulates. Affordable on the write path, not on the read path. The
  cache is keyed on `acl.generation`, a counter in the store bumped by `grant`,
  `revoke`, `set_write_policy` and both ACL switches, so a revoke on one worker is
  seen by the next check on every other — a TTL would have traded the exactness
  every write check has today for speed. Two things had to be true for it to be
  *constant* in grant count rather than merely cheaper: prefixes are parsed once at
  load and indexed by prefix (a grant covers a path exactly when it is one of that
  path's ancestors, so a match is a few map lookups), and a cache hit resolves
  **under the read lock** — handing the entry back meant cloning the grant map per
  check, which put the linear cost straight back.
- **CLI identity is asserted, not verified, and the CLI is not a boundary.**
  Mutating subcommands take `--actor`, falling back to `ORIGOFS_ACTOR` so a shell
  or an agent harness sets identity once (issue #128). None of that is an identity
  *check*: whoever writes the argv writes the environment, and a local process
  holding the workspace directory has `meta.db` and the CAS on disk anyway. The
  boundary is the HTTP surface, where `build_api_auth` resolves identity
  server-side and refuses an unauthenticated API off-loopback. What the CLI flags
  buy is that attribution **gets recorded** — previously `rm`/`mv`/`mkdir` could
  not be attributed at all. `origofs require-attribution on` (workspace setting,
  default off) turns an unattributed mutation into an error; treat it as
  attribution completeness, never as access control.

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
| `origofs-py` | pyo3/maturin bindings: async-native (`await` every I/O), a FastAPI router (`origofs.fastapi`), and overlay orchestration (`origofs.overlay`). Enables `origofs-sdk`'s `coedit` (always), `nfs` (on Unix), and `fuse` (Linux only — narrower than Unix, since macFUSE is a kernel extension a wheel can't carry; macOS mounts over NFSv3 instead). |

### `origofs-sdk` access-surface features

Each is a module under `crates/origofs-sdk/src/`, gated by the matching feature
(default-off). `full` turns them all on (but not `coedit`); `origofs-cli` uses `full`.

| Feature | Module | Role |
|---|---|---|
| `api` | `origofs_sdk::api` | HTTP/JSON server (axum). `Authenticator`/`BearerAuth` resolve identity server-side. |
| `mcp` | `origofs_sdk::mcp` | MCP server — agents call filesystem tools over stdio, auto-attributed. |
| `sandbox` | `origofs_sdk::sandbox` | Overlay / sandbox edit-capture: run a process over a copy-on-write view, import its delta as attributed writes. Not a security boundary by default; opt-in bubblewrap *filesystem* isolation via `--isolate` (see below). **Unix-only** (`cfg(unix)`) — it is built on overlayfs whiteouts, which are character devices. |
| `fuse`, `nfs` | `origofs_sdk::fuse` / `::nfs` | POSIX mounts (FUSE on Linux; NFSv3 elsewhere). **Unix-only** (`cfg(unix)`). |

**`full` is platform-dependent, and that is deliberate.** Three of its six
surfaces (`fuse`, `nfs`, `sandbox`) are `#[cfg(all(unix, feature = "…"))]`
modules, so on Windows `--features full` is still a valid, buildable set that
yields `api` + `mcp` + `git` and omits the rest. `origofs-cli` consumes `full` and
must keep compiling there: its `mount`/`nfs`/`sandbox`/`overlay` subcommands keep
their clap definitions on every platform and `#[cfg]`-split only their bodies,
returning `unix_only(…)` on Windows so the user gets an explanation rather than
clap's "unrecognized subcommand". **When adding a surface that touches a kernel
interface, gate the module on `unix` (not just the feature) and split the CLI arm
the same way** — gating on the feature alone is what kept the Windows target from
compiling at all until #107.
| `git` | `origofs_sdk::git` | Real-`git` interop: export/import genuine git objects. The `git-remote-origofs` binary (shipped by `origofs-cli`, `git clone origofs://…`) builds on it. |
| `coedit` | — | Opt-in CRDT co-editing (yrs); adds the y-sync WebSocket to the `api` surface. Kept separate from `full`. |
| `metrics` | — | Opt-in metrics recording (emit-only, no exporter); adds `GET /metrics` + per-request instrumentation to the `api` surface. Kept separate from `full`. |

## Conventions & gotchas that will bite you

- **Never put large bytes in the metadata DB.** The whole design rests on the
  metadata/content split; the DB references content by hash only.
- **`/.origofs` is origofs's own state, and machinery for user files must not
  treat it as user content.** The co-edit CRDT sidecars live in the working tree
  (`/.origofs/ydoc`) so they are versioned, collected, deduplicated and encrypted
  like any other file and ride with their branch — a deliberate trade whose price
  is that code written for user files reaches them. `merge` was where that showed
  (#142): two branches that had both checkpointed one document produced an
  unresolvable binary conflict on a hidden file, plus a `.theirs` sibling nobody
  would find, and a sidecar that happened to be valid UTF-8 would have been
  *diff3-merged* into a structurally invalid CRDT state. Use `INTERNAL_DIR` /
  `is_internal_path` (`engine.rs`, deliberately **not** in `coedit.rs` — a
  workspace written by a co-editing build gets merged and exported by builds
  without the feature). Match on the **directory boundary**, never a bare
  `starts_with`: `/.origofs-bench` is a real path. **`git export` had the same gap
  (#143)** and now skips `/.origofs` at the root of every commit tree, so a
  published repo carries no sidecars and none of the `(actor, session)` stamps and
  node ids inside them. It filters by *position*, not by tree identity: the same
  origofs tree can be a filtered root in one commit and an ordinary `/sub`
  directory in another, so the exporter's memo is keyed on `(hash, is_root)` — the
  hash alone hands the second encoding to the first commit. **Import is
  deliberately asymmetric** and keeps a `.origofs` it finds: an incoming repo's
  directory of that name is somebody else's content, and the export filter exists
  to keep origofs's state out of what it publishes, not to reserve the name.
- **`handle_sync` speaks the y-websocket envelope, and says so when a frame does
  not (#162).** Frames carry an outer message tag (`messageSync` = 0) wrapping the
  y-sync payload. A client written against y-sync *directly* sends a bare update,
  which starts with `messageYjsUpdate` = 2 — `messageAuth` in the envelope — so it
  decodes cleanly and does nothing. The socket then connects, handshakes, reports
  the right peer count and never converges, with nothing anywhere to attribute it
  to; "no error" reads as "my frames are fine, the problem is elsewhere".
  `drive_sync` now counts every outer tag it has no handler for into
  `SyncReply::unhandled` and logs it at `warn`. Deliberately **not** an error:
  `messageAuth` is a real y-protocol message, and refusing a whole payload for one
  would break a conforming client to diagnose a non-conforming one. Awareness is
  *handled* (relayed) so it is not counted — every Yjs client emits it constantly,
  and a warning per heartbeat would bury the signal.
- **`yrs` is pinned at 0.23.5 on purpose, and the co-editing socket is exposed
  because of it (#144).** A malformed y-sync update reaches
  `from_utf8_unchecked` (`encoding/read.rs`, `updates/decoder.rs`) and is then
  iterated in `block::utf16_len` — undefined behaviour, aborting under the debug
  UB checks and **silent in release**. 51 bytes are enough, through the public
  `CoeditDoc::load`, and `handle_sync` feeds it bytes from clients `coedit.rs`
  explicitly does not trust. **Do not "just upgrade"**: measured, 0.24/0.25/0.26
  reproduce it identically, and 0.27.4 (the latest) has the same two unsafe sites
  *and* does not compile on stable Rust, since it uses `if let` guards. Nothing
  local contains it either — the abort is non-unwinding, so `catch_unwind` is no
  help, and validating the bytes first would mean reimplementing the decoder.
  `tests/coedit_malformed_update.rs` holds the reproducer, `#[ignore]`d because it
  would take the suite down rather than fail it; run it with `--ignored` when
  trying a candidate yrs, and drop the `#[ignore]`s in the change that moves the
  pin. `fuzz_targets/coedit_state_decode.rs` drives the same path and is expected
  to abort.
- **There are two things called a lock, and they are unrelated (#119).**
  `Fs::lock` is the durable, named, git-LFS-style claim on a **path**, taken by a
  person so nobody else edits a binary; it outlives every process involved.
  `vfs_setlk_as` is a POSIX **advisory byte-range** lock on an *inode*, owned by an
  open file description and dead with the process. Neither can be expressed in the
  other's table, which is why `posix_lock` is its own.
  - **Why it is stored at all.** A FUSE filesystem that does not implement `setlk`
    still has working advisory locks — the kernel serves them locally, per mount —
    so an in-process table would reimplement what already works. The only thing
    missing, and the only reason to answer `setlk`, is coordination *between*
    mounts. That is why the table is durable rather than in-memory.
  - **Off by default** (`origofs posix-locks on`), because answering `setlk` takes
    locking *away* from the kernel's local handling: a bug here breaks what works
    today. Same reasoning and same default as `acl_enforce_reads` and trash
    retention. Mounts read it once, at mount time.
  - **A durable table cannot be tidied by a process that has died**, so every row
    carries a `holder` (the mount instance — a clean unmount deletes its rows) and
    an `expires_at` lease that a live mount renews on a timer. Renewing only on
    lock operations would not do: a process that takes a lock and then works for
    five minutes would lose it. Expired rows are dropped inside the same
    transaction that next touches the inode, so nothing needs a background reaper.
  - **The semantics live in `posixlock.rs` and touch no database.** Splitting a
    range somebody re-locks the middle of, downgrading half of an exclusive lock,
    an unlock that punches a hole — that is where this gets subtle, and it is
    unit-tested directly rather than through two backends. Each backend only runs
    `resolve` inside a transaction. **That transaction is the correctness
    boundary**: SQLite takes `BEGIN IMMEDIATE` and Postgres a per-inode
    `pg_advisory_xact_lock`, because read-decide-write must be serialized or two
    mounts both find no conflict and both insert. `SELECT … FOR UPDATE` is *not*
    enough on Postgres — there are no gap locks, and the rows in question do not
    exist yet.
  - **`F_SETLKW` replies from off the session thread.** `fuser`'s dispatch loop is
    single-threaded (`Config::n_threads` defaults to 1 and this mount does not
    raise it), so waiting inline would freeze every operation on the mountpoint
    for the duration. The waiter moves the `ReplyEmpty` onto the mount's runtime
    and answers later. It is bounded (30s) because `fuser` exposes no
    `FUSE_INTERRUPT` hook here, so an unbounded wait could never be cancelled.
  - **Authorization follows what the lock claims**: exclusive takes the write
    check, shared takes the read guard (inert unless `acl_enforce_reads`), and
    **unlocking is never refused** — an actor whose grant was revoked mid-flight
    must still be able to let go, or the range stays stuck until its lease runs
    out.
  - **NFS has none of this**, and not by choice: `nfsserve` exposes no NLM hooks,
    and NFSv3 locking is a separate protocol. `fallocate`/`copy_file_range` from
    the same issue are answered on FUSE (`vfs_copy_range_as`/`vfs_allocate_as`)
    and remain absent on NFS for the same reason — `nfsserve` never surfaces the
    operations.
  - **Four test tiers, because each one misses what the next catches.**
    `property.rs` asserts the resolver's invariants over arbitrary op sequences —
    states are built by *folding* random requests, never generated directly, since
    a hand-made lock set is mostly states the resolver cannot emit.
    `posix_lock_sim.rs` is the seeded DST: it drives the **real store** and
    compares the rows against the reference model (`posixlock::apply`) after every
    step, because `resolve` can be perfect while the SQL translation is not — a
    `DELETE` that misses its row, an expiry filter off by one. It carries a
    negative control that deliberately breaks the model and requires the
    comparison to notice. `concurrency.rs` covers contention. The
    `posix_lock_resolve` fuzz target is the deeper search of the same invariants;
    note CI only `cargo check`s the fuzz crate, which is exactly why the
    properties also run in-crate.
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
  `LiveOpts::isolate`; needs a non-setuid `bwrap` ≥ 0.11.0 — where `--overlay` landed; capability is probed by `bwrap_gap()`/`bwrap_available()`, not inferred from the version) runs
  the command under bubblewrap in a fresh tmpfs root that hides the host filesystem
  (`meta.db`/`cas`, home dir, credentials) — a real **filesystem** boundary for
  untrusted code. It is deliberately *only* filesystem isolation: the network
  namespace is left shared on purpose (agents need egress), so it does not by
  itself contain network-reachable resources. Either way the delta is captured and
  imported the same. Keep the default's "not-a-security-sandbox" caveat loud.
- **Content is immutable and never overwritten**, so churn leaves orphaned
  chunks. `gc` (mark-and-sweep from live refs) reclaims them; packed stores
  additionally need `repack` to reclaim space. Content writes are idempotent
  (content-addressed), so retries are safe.
  **GC is safe alongside active writers, by an age gate rather than by quiescing
  the store** — content is written before the metadata referencing it, so every
  write has a window where its chunks are unreferenced, and reachability alone
  would sweep exactly those. Three parts make it hold, and all three are load-
  bearing: the sweep skips anything younger than the grace period
  (`list_with_age`), a deduplicating `put` refreshes an object that has gone stale
  (`touch`), and the sweep re-checks an object's age at the moment it deletes it
  (`delete_if_older_than`) so a long pass cannot act on an age it read minutes
  earlier. A backend that cannot date its objects collects nothing.
- **Upgrading origofs must never require rewriting a bucket, and the two stores
  get opposite treatments because they fail in opposite directions.** Content is
  immutable and hash-addressed, so a format change mints *new* objects and leaves
  every old one valid — the rules live in `format.rs` (never re-encode a shipped
  version; add a decoder arm; raise `max_read_version` a release before
  `write_version`), and `tests/format.rs` pins the v1 bytes *and* their hashes so a
  silent re-address fails in CI rather than in a bucket. It also pins the FastCDC
  boundaries, because re-chunking is not a correctness break but is a total dedup
  break. The metadata DB is the opposite: rewritten in place, forward-only, no
  down-migrations by design.
  - **Anything a dependency owns is pinned in *data*, not left to the crate.** The
    encrypted-at-rest layout is the sharp case: `EncryptedStore` frames objects
    `ORGE | version | AEAD(…)` (bare pre-envelope objects read forever — the AEAD
    tag, not the header, is what finally decides between the two shapes), and the
    Argon2id cost is `KdfParams::LEGACY` recorded per store in a `kdf` sidecar
    rather than `argon2::Params::default()`, a constant the crate has already moved
    once. Both failures would have surfaced as `Corrupt("wrong key or corrupt
    data")` — the wrong-passphrase message — on intact data.
    `tests/encryption.rs` pins the ciphertext of a fixed `(passphrase, salt,
    plaintext)`, which pins Argon2id, the BLAKE3 nonce derivation, the cipher and
    the framing at once.
  - **`min_reader_version` is the field that locks a fleet out; `format_version` is
    advisory.** They are separate constants deliberately: stamping them together on
    *open* meant the first node to upgrade took the store away from every node that
    had not, before writing a single new-version object. Raise `MIN_READER_VERSION`
    only in the release that starts writing objects older readers genuinely cannot
    use.
  - **The rollback path is a backup, not a down-migration.** An older binary meeting
    a newer DB refuses it (`UnsupportedVersion`) *before* touching it, so the DB is
    left exactly as found; `origofs migrate --check` shows a pending step and
    `--backup` snapshots before applying. Both read the store **unmigrated, ahead of
    the workspace open** — opening is the migration runner, so anything asking after
    the open can only describe what it just did.
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
  **A conflict that needs its own recovery gets its own variant** (#159).
  `Conflict` covered both "your proposal's base moved" and "somebody wrote around
  your live document", which ask for *opposite* recoveries, so callers were reduced
  to substring-matching the message — and then breaking on any rewording.
  `StaleBase` and `ForeignWrite` split those out (`stale_base`/`foreign_write`
  codes; `StaleBaseError`/`ForeignWriteError` subclassing `ConflictError` in
  Python, so `except ConflictError` and the 409 mapping are unchanged). The
  consequence for the tree: **use `is_conflict()`, never a bare `Conflict(_)`
  pattern** — every site that meant "any conflict" was a 409 or a "the caller must
  re-read" decision that still applies to all three, and a narrowed match is a
  silent behaviour change.
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
- **`origofs/__init__.pyi` is hand-maintained, and it fails open** — so it gets
  structural tests of its own, in `tests/test_stub_records.py`. Nothing checks a
  stub against reality by default: mypy silences errors inside site-packages, so a
  host running plain `mypy` sees neither the error nor the types. Two undefined
  names (`Inode`, `BlameRange` — the records are `StatResult` and `BlameSpan`)
  quietly degraded `stat_as`/`blame_as` to `Any`, i.e. exactly the attributed,
  ACL-checked variants a multi-tenant host is told to prefer; and
  `SuggestionContent` declared `bytes` where the extension returns `str`, so
  anything typed against it was wrong at runtime (#163). Three checks now hold the
  line: the record-key comparison against a real workspace, an `ast` pass that
  every name used in an annotation is declared, and — for `WorkspaceProtocol`
  (`python/origofs/protocol.py`, the structural type a host or test double depends
  on, since `Workspace` is concrete) — that it names only methods `Workspace`
  actually has.
- Integration tests live in each crate's `tests/` and are the clearest executable
  spec of behavior (e.g. `origofs-core/tests/{merge,attribution,recover,durability,
  integrity}.rs`). Mirror their style when adding coverage.

## License

MIT
