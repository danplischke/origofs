# Contributing to origofs

## Before you change architecture

`docs/DESIGN.md` is the authoritative design document and the milestone roadmap
(M0–M9). Doc comments across the code reference it by milestone and section
("M1", "§4d"). **Read it before any change that touches the object model,
attribution, or the metadata/content split** — it explains *why* those are the way
they are, and most surprising-looking code is deliberate.

`CLAUDE.md` is the working orientation: crate map, how a call flows through the
layers, and the invariants that will bite you.

## Build, test, lint

```bash
cargo build --release
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all                          # no rustfmt.toml — plain default style
```

Two things a plain `cargo test --workspace` does **not** cover, and CI does:

```bash
# The default-off access surfaces (api/mcp/sandbox/git/fuse/nfs) are features of
# origofs-sdk, so their code and their tests are invisible to --workspace.
cargo test -p origofs-sdk --features full

# Postgres-backed tests self-skip without this, so the multi-writer,
# LISTEN/NOTIFY, and transaction-isolation paths silently do not run.
ORIGOFS_PG_TEST_URL="host=127.0.0.1 port=5432 user=postgres dbname=origofs" \
  cargo test --workspace
```

Python bindings build with maturin, not cargo:

```bash
cd crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin pytest && maturin develop
pytest tests/
```

MSRV is **1.88** (let-chains, plus the dependency graph's own floor) and is pinned
in CI. Raising it is a breaking change.

## Documentation

The user-facing docs live in `docs/` and build with
[Zensical](https://zensical.org), the static site generator from the Material for
MkDocs team:

```bash
pip install -r requirements-docs.txt
zensical serve                 # http://localhost:8000, live reload
zensical build --strict        # what CI runs — a broken link fails the build
```

`--strict` is the point: a page linking to a heading someone renamed fails
rather than shipping as a dead link. CI runs it on every PR.

**Zensical is alpha (0.0.x), so `requirements-docs.txt` pins it exactly.** Its
config format is not stable yet; bump the pin deliberately and re-run the strict
build. It reads `zensical.toml` — and, as a compatibility path, a `mkdocs.yml`
too, though this repo uses the native config.

Everything under `docs/` is published, so a new page needs a `nav` entry in
`zensical.toml`. Zensical has no `exclude_docs`, which is why the working
documents that used to live in `docs/` — `IMPROVEMENT_PLAN.md` and `REVIEW.md` —
sit in `notes/` instead. Put working notes there, not in `docs/`.

Prose only — there is no rustdoc in the site. If you change a command's flags,
the page describing it is part of the change.

## Invariants that will bite you

These are enforced, sometimes by a test that fails structurally rather than
behaviourally. If one of those fails, it is telling you something real.

- **Never put large bytes in the metadata database.** The whole design rests on the
  split; the DB references content by hash only.
- **Every mutating endpoint on every surface must call an attributed variant** —
  `write_or_propose`, `remove_or_propose`, `rename_as`, `mkdir_as`, `symlink_as`,
  `commit_as`, `checkout_as`, `create_branch_as`. The raw `write`/`remove`/
  `rename`/`mkdir_p`/`symlink`/`commit`/`checkout` take no actor, exist for
  internal machinery (checkout, merge materialization, applying an accepted
  suggestion), and skip the §6 write policy by construction.
  `tests/mcp.rs::every_mutating_mcp_tool_is_policy_classified` and
  `tests/api_write_policy.rs::every_mutating_route_binds_its_principal` fail on an
  unclassified tool or a route that drops its principal. FUSE/NFS are a documented
  exception — a mount has no actor context.
- **The server never trusts a client-named actor.** Identity is resolved
  server-side, always. No request body or query parameter names an actor.
- **Validate path components at every metadata boundary.** `validate_component`
  refuses `.`/`..`/`/`/NUL so a poisoned name can never be *stored*, which is what
  stops it escaping later during host materialization.
- **A `ContentStore` backend that reports object ages must be able to refresh
  them.** `list_with_age` and `touch` are two halves of one mechanism; overriding
  one without the other re-opens a garbage-collection race.
  `tests/gc.rs::every_dateable_backend_can_refresh_recency` enforces the pairing.
- **Every metric label must be a closed set** — never a path, actor, hash, or
  workspace name. `GET /metrics` is unauthenticated by design, and that is only
  safe because of this.
- **Do not retry an operation whose metadata has already committed.** See
  `crate::retry`; `mirror_refs_post_commit` exists because this rule was broken.

## Tests

Integration tests live in each crate's `tests/` and are the clearest executable
spec of behaviour — `origofs-core/tests/{merge,attribution,recover,durability,
integrity,simulation}.rs` especially. Mirror their style.

Two habits worth keeping:

- **Prove a regression test fails without its fix.** Revert the fix, watch the test
  fail, restore it. A test written after the fix that never saw red is a test that
  might be asserting nothing.
- **Assert the invariant, not the timing.** Where a bug is a race, find the
  deterministic property that closes it and assert *that* — a test that tries to
  hit the window is flaky, and a sequential test usually cannot reach it at all.

## Commits and pull requests

Explain **why**, not just what. The code says what changed; a reader six months
out needs the failure mode, the reasoning, and what was rejected. Reference the
design doc section or issue where relevant.

## License

By contributing you agree that your contributions are licensed under the same
terms as the project: MIT.
