# Improvement plan — open issues, triaged against `main`

Written 2026-08-10 against `e479591`. Every claim below was verified by reading
the code at the cited location, not inferred from an issue's checkboxes. The
housekeeping it recommends has since been carried out — #78 is closed, #75 is
rewritten, and the P0 finding is filed as #99.

## Summary

Nine issues are open. The picture they paint is out of date in both directions:

- **Two trackers are substantially stale.** #75 lists twelve remaining items;
  eleven have shipped. #78's proposal has landed essentially in full on the Rust
  surfaces, and `CLAUDE.md` already documents it as engine-enforced.
- **One real trust gap was not filed at all.** The Python FastAPI router
  (`crates/origofs-py/python/origofs/fastapi.py`) still mutates through the
  *unattributed* engine ops, so it is exactly the bypass #78 closed — on the one
  surface #78 never audited. Now #99; see P0 below.
- **The seven issues filed on 2026-08-10 (#92–#98) are all genuinely open**, and
  they are notably coherent: they are one integrator's report from wrapping the
  Python bindings for a multi-tenant, Plate-based host. They should be read as a
  single adoption story, not seven unrelated tickets.

## Triage

| # | Title | Verified status |
|---|---|---|
| #78 | WritePolicy per-call-site, fails open | **Done on Rust surfaces** — closed as completed |
| #99 | Python router bypasses the write policy | Filed off the P0 finding below |
| #75 | Remaining open work, consolidated | **11 of 12 shipped** — rewritten to its 2 real items |
| #96 | abi3 wheels to PyPI | **Done** — `release.yml`; PyPI publish awaits setup |
| #95 | TypedDict stubs | **Done** — 24 `TypedDict`s + a runtime parity test |
| #94 | `revert_session` path scope | **Done** — `path_prefix` at all four layers |
| #93 | fastapi multi-tenant authorisation | Open. 14 workspace-global routes (the `409`→`403` half is done) |
| #98 | Co-edit WS credential + session | **Done** — subprotocol auth + session per connection |
| #97 | Co-edit interval checkpointing | Open. `_Rooms.leave` is the only checkpoint |
| #92 | Structured (XmlFragment) co-edit doc | Open. `coedit.rs:75` is a single flat `TextRef` |

### #78 — closed, with one carve-out

Shipped and verified: `OrigoFSError::Denied` (`error.rs`), `ensure_may_write`
(`suggest.rs`), `remove_as`/`rename_as`/`mkdir_as`/`symlink_as`,
`remove_or_propose`, the approver-policy gate on `accept_suggestion`, the commit
gate, and the surfaces wired through. Both anti-regression tests exist and do the
job the issue asked for: `origofs-sdk/tests/mcp.rs::every_mutating_mcp_tool_is_policy_classified`
fails on an unclassified MCP tool, and
`api_write_policy.rs::every_mutating_route_binds_its_principal` parses the router
source and fails on a mutating route that doesn't bind its principal.
`origofs-core/tests/write_policy.rs` covers each refusal and each exemption.

The carve-out is P0 below, now tracked as #99: the Python router is a third
surface, and neither guard test covers it.

### #75 — rewritten down to two items

Verified **shipped** since the issue was written: offline→reconnect resync
(`origofs-core/src/resync.rs`, `origofs-sdk/tests/resync.rs`), metrics
(`origofs-core/src/metrics.rs`, the `metrics` feature, `GET /metrics`),
`POST /v1/presence` (`api/mod.rs:283`), CRDT suggestions with `SuggestionKind::Crdt`
+ `apply_coedit_suggestion` (`suggest.rs:553`), `Superseded` now actually written
(`suggest.rs:652`, `tests/suggest_superseded.rs`), the ydoc sidecar slot
(`COEDIT_SIDECAR_DIR = "/.origofs/ydoc"`, `tests/coedit_sidecar_gc.rs`), the
live/dirty marker (`live_doc`), M14 NFS shutdown (`serve_nfs(addr, shutdown=…)`,
`tests/shutdown.rs`), M16 readdir pagination (`list_dir_page` +
`vfs_readdir_page_with_attrs`, used by both mounts, `tests/readdir_paging.rs`),
M17 opaque-dir xattrs (`sandbox.rs:472-606`), and per-version migration coverage
(`tests/migration_paths.rs`, both engines, both hand-seeded and runner-built).

FUSE cache invalidation is shipped too, but *deliberately partial*: `invalidate`
(`fuse.rs:426`) issues `inval_inode` for data and parent attrs, and drops
`inval_entry` because it deadlocked the mount roughly one run in eight. The
residual gap — a resolved dentry stays resolvable for up to `TTL` after a remote
create/delete/rename — is documented in place and bounded at one second. The
follow-up it names (a mount guard that stops the watcher *before* unmount, an API
change to `spawn`/`origofs-py`'s `Mount`) is the only piece left.

**Genuinely still open:** the agentfs `.db` importer. `agentfs` appears nowhere
in the tree, not even in prose.

## The plan

### P0 — Close the Python router's write-policy bypass (#99)

`crates/origofs-py/python/origofs/fastapi.py` authenticates every mutating route
and then discards the principal — the handlers literally name it `_ctx`:

| Route | Line | Calls | Should call |
|---|---|---|---|
| `DELETE /files/{path}` | 586 | `ws.remove` | `ws.remove_or_propose` |
| `POST /dirs/{path}` | 597 | `ws.mkdir_p` | `ws.mkdir_as` |
| `POST /rename` | 606 | `ws.rename` | `ws.rename_as` |
| `POST /commit` | 619 | `ws.commit` | `ws.commit_as` |
| `PUT /files/{path}` (parent mkdir) | 525 | `ws.mkdir_p` | `ws.mkdir_as` |

All four attributed variants are already exported by the bindings
(`origofs-py/src/lib.rs:2483,2515,2532,2567`), so this is a call-site change, not
new engine work. Consequences today:

1. A propose-only actor cannot overwrite a file through `PUT` (that route does
   probe `ensure_may_write`) but **can delete it and commit the deletion** — the
   precise failure #78 was opened about.
2. Namespace mutations through this router are unattributed, so "who deleted
   this" has no answer on the Python surface.
3. `POST /commit` takes `author` from the **request body** (`_Commit.author`,
   line 86). The docstring frames it as a git-level author distinct from blame,
   which is defensible, but it is still a client-named identity string on a
   mutating route — `commit_as` takes the authenticated ctx *and* an author, so
   binding the ctx costs nothing and closes the question.

Also fold in #93's minor: `_run` maps `PermissionError` → **409** (lines 151-165).
Once these routes are attributed, that mapping starts firing on real policy
refusals, where **403** is the truthful code. Fix it in the same change.

**Guard:** port `every_mutating_route_binds_its_principal` to `pytest` — walk the
router's `@router.post/put/delete` registrations and assert each handler both
binds `authn` and passes it to an attributed call. Without it this regresses the
same way it arrived.

*Cost: small. Risk: low. Do it first — it is the only correctness item on the
list.*

### P1 — Ship wheels (#96)

The reporter says it plainly: "far and away the biggest adoption cost I hit —
everything else on my list is an API nicety by comparison." Believe them. Today
`uv sync` cannot install origofs, so every Python host carries lazy imports and
degradation paths purely because the package is unobtainable, and pins by commit
SHA because there is nothing else to pin.

There is no `release.yml` and no git tags. The work is a workflow, not a code
change:

1. `maturin-action` building manylinux/macOS/Windows abi3 wheels on tag, plus an
   sdist as a companion.
2. Tag `v0.1.0` and cut the first release. Attach wheels to the GitHub Release
   even before PyPI — that alone lets a host pin a URL + hash instead of running
   a Rust toolchain in a build stage.
3. A `CHANGELOG.md`, so "which version are we on" stops being git archaeology.
4. Decide the default feature set for the published wheel. `origofs-py` enables
   `coedit` always and `fuse`/`nfs` on Unix; `libfuse-dev` in the build is part
   of why the reporter's image build doubled. Consider a wheel without the mount
   features and a documented extra for hosts that want them.

*Cost: medium, mostly CI plumbing. Value: unblocks every downstream Python host.*

### P2 — The cheap API wins (#95, #94, half of #98)

Three small, independent changes that remove real guesswork from integrators:

- **#95 TypedDicts.** 31 `dict[str, Any]` returns in `__init__.pyi`, with shapes
  described only in prose. The reporter's wrapper defensively handles *both* an
  actor id and an inline actor record for `span["actor"]` because the stub does
  not say which — and depends on an undocumented `path` key in `get_suggestion()`
  for a security check. Add `BlameSpan`, `ActorRecord`, `SuggestionRecord`,
  `PassageRecord`, `LiveMarker`, `EditOp`, `StatResult`, `DirEntry`. Pure stub
  change, no runtime cost. `test_parity.py` already exists and is the natural
  place to pin key names against the real bindings so the stub can't rot.
- **#94 `revert_session(…, path_prefix=…)`.** Add the scope in
  `attribution.rs:1030` inside the existing transaction, thread it through
  `origofs-sdk`, `origofs-py`, and the HTTP API. Return the changed **paths**
  rather than a count. The pre-flight-with-`edit_ops` workaround hosts use today
  is racy by construction — a write can land between the check and the revert —
  so this is a small correctness win, not only ergonomics.
- **#98 part 2 — a session per connection.** `apply_update_as` with a
  session-less `WriteCtx` produces edits `revert_session` can never undo, on the
  surface that generates the most edits. Have the room open a session when the
  supplied ctx has none. "One session per connection" is the only sensible
  answer, so the SDK should own it rather than documenting it as homework.

### P3 — Co-edit durability (#97, #98 part 1)

- **Interval/idle checkpointing.** Today `checkpoint_coedit` runs only on last
  leave (`_Rooms.leave`, line 370). A browser tab left open on a document is an
  open room, so "last leave" can be hours; a worker dying in between loses the
  un-checkpointed session (bounded by the Postgres relay window, unbounded on
  SQLite where the relay is off). Drive an interval + idle timer inside the
  SDK/coordinator — the host has no signal about room activity, so leaving it to
  each host is the wrong layer. Ship the documentation half regardless: state the
  durability window, and make `live_doc`'s `since` explicitly mean
  "checkpointed at" so a UI can render "last saved 3 minutes ago".
- **`Sec-WebSocket-Protocol` auth.** Both WS surfaces (`api/coedit.rs:279`, and
  the Python router's docstring) accept the credential only as `?token=`, which
  is the one place a secret reliably lands in access and proxy logs. Accept the
  subprotocol form (`["origofs", "<token>"]`, echo back `origofs`) — the one
  header a browser *can* set on an upgrade — and document the same-origin cookie
  path. Keep `?token=` working; this is an addition.

### P4 — Multi-tenant authorisation (#93)

14 workspace-global routes (`/log`, `/status`, `/diff`, `/events`, `/presence`,
`/branches`, `/checkout`, `/commit`, `/suggestions` and the id-addressed
suggestion routes) have nothing for a host's `authn`/`reader` to authorise
against, so a multi-tenant host must refuse them wholesale and rebuild blame and
suggestion review in front of the SDK. Do it in two steps:

1. **Pass the request scope to the hooks** — hand `authn`/`reader` the
   `HTTPConnection` (or the resolved path + query). Smallest change; unblocks
   hosts immediately and lets them authorise uniformly instead of reaching into
   `request.path_params` on some routes and giving up on others.
2. **A router-level root** — `build_router(ws, root="/tenants/{x}", …)`,
   resolving every path under the root and filtering the global listers to it.
   This is what makes the router genuinely multi-tenant, and it needs path/prefix
   filters exposed on `log`, `diff`, `events`, and `presence` (which the store
   layer already supports).

Worth noting the same question applies to the Rust HTTP API, which has the same
global routes. Solving it only in the Python router leaves the shape unsolved.

### P5 — Structured co-editing (#92)

The largest and least certain item; sequence it last, but recognise what it
costs to defer. A flat `TextRef` means no mainstream rich-text binding
(`@platejs/yjs`, `y-prosemirror`, `y-slate`, TipTap) can attach, so hosts mirror
via serialize-diff-write. That degrades in three ways, and the third undercuts
the project's headline claim: **per-run attribution is only as sharp as the
host's text diff**, and a common-prefix/suffix diff collapses two concurrent
edits in different paragraphs into one replaced span. Byte-level authorship is
only as good as the granularity the client can express.

Before building, settle the two questions that decide the design:

- **What are the durable bytes?** `checkpoint_coedit` needs a deterministic
  serialization for `read`, three-way merge, and git export to stay meaningful.
- **How does blame project onto them?** A tree gives node/mark ranges; mapping
  those to byte ranges probably wants the serializer to emit a span map.

A "bring your own schema — we attribute the runs, you own serialization" cut is
enough to unblock a native binding and defers both questions. The reporter has
offered to test a prototype against Plate/Slate; take them up on it.

### Housekeeping — done

- **#78 closed as completed**, with each of its eight proposal items mapped to
  where it landed and both anti-regression guards confirmed
  (`write_policy.rs`, `mcp.rs`, `api_write_policy.rs`).
- **#99 filed** off the P0 finding, referencing #78 as the scope it completes.
- **#75 rewritten** down to the agentfs importer plus the FUSE `inval_entry`
  follow-up; the item-by-item verification of the eleven shipped items is kept as
  a comment on the issue. It had become 92% stale, claiming metrics, resync,
  presence writes, and migration coverage were missing when all four are tested
  on `main`.

**#75's two remaining items stay low priority.** The agentfs importer has no
evidence of demand; the FUSE dentry gap is documented, bounded at one second, and
its fix is an API break. Neither should outrank #92–#99, which come from an actual
integrator.

## Suggested order

```
P0  Python router write-policy bypass + 403 (#99)  ← DONE
P1  PyPI wheels + changelog (#96)                  ← DONE (tag + PyPI setup left to the owner)
P2  TypedDict stubs (#95) · revert_session scope (#94) · WS session (#98)  ← DONE
P3  Interval checkpointing (#97)                   ← next
P4  Multi-tenant authorisation (#93)
P5  Structured co-editing (#92)
```

**Where P1 stands.** The workflow is in place and every leg builds; what remains
is not code. Tag `v0.1.0` to cut the first release (wheels attach to it with no
account setup), and — to publish to PyPI — register the trusted publisher, create
the `pypi` environment, and set the `PUBLISH_TO_PYPI` repository variable. Those
are the owner's to do.

**#98's subprotocol half shipped early**, with the session half, since both are
about the `WriteCtx` a socket carries and touching that file twice made no sense.
P3 is therefore just #97's interval checkpointing.

P0 and P1 are independent and can run in parallel. Everything in P2 is
independent of everything else. P4 is worth doing before P5, because a host that
cannot authorise the router will not get far enough to care about the editor
binding.
