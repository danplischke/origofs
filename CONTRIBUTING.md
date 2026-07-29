# Contributing to origofs

Bug reports, questions, and pull requests are all welcome. This file covers how to
build the thing, what CI expects, and the handful of invariants that are easy to
break by accident.

Before a change that touches architecture, read [`docs/DESIGN.md`](docs/DESIGN.md).
It explains *why* the metadata/content split, the object model, attribution, and
the failure-surface work are the way they are, and a change that fights those
choices is usually a change in the wrong place.

## Build and test

Rust **1.88+** — edition 2024 sets a 1.85 language floor, but the code uses
let-chains and the dependency graph raises the real minimum. There is no
`rust-toolchain` file; use a recent stable.

```bash
cargo build --release
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt                                    # no rustfmt.toml — default style

cargo test -p origofs-core                   # one crate
cargo test -p origofs-core --test merge      # one integration-test file
cargo test -p origofs-sdk --features full    # the access surfaces
```

**Postgres-backed tests self-skip** unless `ORIGOFS_PG_TEST_URL` points at a
reachable database, so a plain `cargo test --workspace` exercises only the SQLite
path. If your change goes anywhere near the metadata store, run both:

```bash
ORIGOFS_PG_TEST_URL="host=127.0.0.1 port=5432 user=postgres dbname=origofs" cargo test --workspace
```

The Python bindings build with maturin, not cargo:

```bash
cd crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin && maturin develop
pytest tests/
```

## Where things go

Four crates, and the access surfaces are feature-gated modules rather than
separate crates:

| Crate | Role |
|---|---|
| `origofs-core` | The engine: both store traits, all content backends, chunking, versioning, merge, attribution, gc, recovery, migrations. |
| `origofs-sdk` | `Workspace`, the façade over `origofs-core::Fs` — plus `api`, `mcp`, `sandbox`, `git`, `fuse`, `nfs`, `coedit` as opt-in modules. |
| `origofs-cli` | The `origofs` and `git-remote-origofs` binaries. |
| `origofs-py` | pyo3/maturin bindings and the Python integrations. |

Because every surface funnels down to the same core, **a behaviour change almost
always belongs in `origofs-core`, not in each surface.** If you find yourself making
the same fix in the FUSE handler and the HTTP handler, it belongs one layer down.

## Invariants not to break

These are the ones that cost real debugging when they slip:

- **Never put large bytes in the metadata DB.** It references content by hash
  only. The whole design rests on that split.
- **Validate names at every metadata boundary.** `validate_component` rejects
  `.`, `..`, `/`, and NUL in a single name, which is what stops a poisoned name
  from ever being *stored* — and therefore from escaping during host
  materialization. Any new inode-oriented operation needs the same check.
- **The server never trusts a client-named actor.** Identity is resolved
  server-side, on every surface. The HTTP body must never name an actor.
- **Attributed writes carry a `WriteCtx`** and record an append-only edit-op plus
  the materialized blame index. Plain `write` is unattributed and invalidates
  blame rather than inventing an author.
- **Content is immutable and never overwritten.** Writes are idempotent, so
  retries are safe; churn leaves orphans for `gc` to reclaim.
- **Migrations are forward-only**, authored once, with per-engine SQL where SQLite
  and Postgres diverge.

## Pull requests

- Keep the diff to one concern. Separate mechanical churn from behaviour changes
  so a reviewer can read them independently.
- Add a test that fails without your change. The integration tests in each
  crate's `tests/` are the clearest executable spec of behaviour — match their
  style. Regression tests for security findings live in
  `crates/origofs-core/tests/hardening.rs` and each pins a specific fix.
- Match the surrounding code's comment density and naming. Comments here explain
  *why* rather than restating the code; several carry milestone or design
  references (`M1`, `§4d`) that are worth continuing.
- CI must be green: fmt, clippy, tests on both engines, the MSRV leg, `cargo-deny`,
  the fuzz smoke run, coverage, MinIO, and macOS. Patch coverage is gated.

## AI-assisted contributions

This project is largely vibe-coded, so AI-assisted PRs are welcome rather than
frowned on — with one condition: **read what you're submitting.** A PR you can't
explain isn't reviewable, and the point of this project is knowing who wrote what.
