# Changelog

All notable changes to origofs are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims at
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The Rust crates (`origofs-core`, `origofs-sdk`, `origofs-cli`) and the Python
package (`origofs`, built from `origofs-py`) share one version, set once in the
workspace manifest — so a tag names the same code on crates.io, PyPI, and here.

## [Unreleased]

### Added

- **Python wheels are built and published on tag** (`.github/workflows/release.yml`,
  #96). abi3-py39, so one wheel per platform covers CPython 3.9+: manylinux
  x86_64/aarch64, macOS arm64/x86_64, Windows x64, plus an sdist. Every tagged
  build attaches its artifacts to the GitHub Release; PyPI publishing rides
  Trusted Publishing and stays off until the repository variable
  `PUBLISH_TO_PYPI` is set (see the workflow header for the one-time setup).
  Consumers no longer need a Rust toolchain in a container build stage or a
  commit SHA to pin.

### Changed

- **FUSE is scoped to Linux in the Python extension** (`origofs-py`). It was
  `cfg(unix)`, which made a macOS wheel unbuildable: `fuser`'s build script
  probes pkg-config for macFUSE there, and a kernel extension is not something a
  wheel can carry. macOS keeps `serve_nfs` — the mount path `docs/DESIGN.md`
  already specifies for it — and `Workspace.mount()` now raises a clear error
  there as it already did on Windows. Linux is unaffected (`fuser`'s `libfuse`
  feature is off by default, so it uses the pure-Rust mount path and needs no
  system library). Building from source on a Mac with macFUSE installed still
  works by enabling the `fuse` feature and the `fuser` dependency by hand.

### Fixed

- **`origofs.fastapi` enforces the write policy** (#99). Every mutating route
  authenticated the caller and then discarded the principal, calling the
  unattributed engine ops — which skip `ensure_may_write` and record no
  `edit_op`. A propose-only actor could not overwrite a file through `PUT` but
  could delete it and commit the deletion. `DELETE`, `POST /dirs`, `/rename`,
  `/commit`, `/branches`, `/checkout` and `/actors` now go through the
  attributed, policy-gated variants, so a propose-only actor's delete is queued
  for review and the rest are refused. Namespace mutations carry an actor, so
  "who deleted this file" has an answer on this surface too.
- **A write-policy refusal is `403`, not `409`**, in `origofs.fastapi` (#93).
  `PermissionError` was collapsing into the `OSError` arm, which carries
  stale-base conflict semantics.
- **`POST /sessions` binds the session to the credential** in `origofs.fastapi`,
  instead of taking `actor` from the request body — the same fix the Rust HTTP
  API already carries (`docs/REVIEW.md` item 18). An authenticated caller could
  previously mint a session belonging to another actor.

[Unreleased]: https://github.com/danplischke/origofs/compare/main...HEAD
