# Install

## The CLI

origofs is a Rust workspace. Build the `origofs` binary with a recent stable
toolchain:

```bash
cargo install --path crates/origofs-cli
```

That installs two binaries: `origofs`, and `git-remote-origofs`, the helper that
lets the real `git` clone from a workspace (see
[Versioning](../guides/versioning.md#real-git-interop)).

To build without installing:

```bash
cargo build --release        # ./target/release/origofs
```

!!! note "Toolchain"

    There is no `rust-toolchain` file. Every crate is edition 2024, and the
    effective minimum is **Rust 1.88** — the code uses `let` chains, and the
    dependency graph needs ≥ 1.86. Use a recent stable toolchain; CI pins the
    floor so an accidental bump is caught.

Linux, macOS and Windows are all built and tested. Two surfaces are Unix-only
because they sit on kernel interfaces — FUSE mounts and the overlay sandbox — so
on Windows those subcommands explain themselves rather than existing and failing.
macOS has no FUSE (macFUSE is a kernel extension) and mounts over
[NFSv3](../guides/mounts.md#nfs) instead.

## Python

```bash
pip install origofs
```

Wheels are **abi3**, so one per platform covers CPython ≥ 3.9 and there is no
Rust toolchain needed at install time. They are built for manylinux
(x86_64/aarch64), macOS (arm64/x86_64) and Windows x64, published to PyPI and
attached to every [release](https://github.com/danplischke/origofs/releases).

Integrations ship as extras:

```bash
pip install "origofs[fastapi]"      # also: fsspec, upath, llamaindex, markitdown, db
```

To build the bindings yourself — a platform without a wheel, or working on the
bindings themselves — they use maturin, not cargo:

```bash
cd crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop
pytest tests/
```

See the [Python reference](../reference/python.md) for the API.

## Docker

The repository ships a `Dockerfile` and a `docker-compose.yml` that brings up
origofs with Postgres and MinIO, which is the quickest way to see the
[team setup](../guides/teams.md) without provisioning anything:

```bash
docker compose up
```

## What you get

A workspace is a directory origofs manages: a metadata database (`meta.db`) next
to a content store (`cas/`). Nothing else is required to start — point the CLI at
a directory and [initialize it](quickstart.md).

For a team deployment, point it at Postgres and object storage instead. See
[Storage backends](../reference/storage-backends.md) and
[Configuration](../reference/configuration.md).
