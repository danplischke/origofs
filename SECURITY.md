# Security policy

## Reporting a vulnerability

Please report security issues **privately**, not as a public issue: use GitHub's
[private vulnerability reporting](https://github.com/danplischke/origofs/security/advisories/new)
on this repository (Security → Report a vulnerability).

Include what you'd want to receive: the affected surface, a reproduction, and what
an attacker gets. There is no bounty and no formal response SLA — this is a
pre-1.0 side project — but reports will be read and credited.

## Supported versions

None, formally. origofs is pre-1.0 with no published releases; only `main` is
maintained, and fixes land there.

## What is *not* a vulnerability here

Some sharp edges are documented, deliberate behaviour rather than bugs. Reporting
these is welcome as a docs or design discussion, but they aren't security issues:

- **`origofs sandbox` / `origofs overlay` are not a security boundary by default.**
  The child process runs with your privileges over a plain copy-on-write overlay:
  the whole host filesystem is reachable, there is no network namespace and no
  seccomp, and origofs only strips `ORIGOFS_ENCRYPTION_KEY` from its environment.
  This is edit *capture*, and the README says so. Passing `--isolate` runs the
  command under bubblewrap in a fresh tmpfs root — a real **filesystem** boundary
  — but the network namespace is still shared on purpose, because agents need
  egress. Escaping something that was never a sandbox isn't a finding; a way to
  escape `--isolate`'s filesystem boundary very much is.
- **origofs ships no authentication.** Identity resolution belongs to the embedder,
  because a blame trail is only as trustworthy as the identity behind each write.
  The CLI's `--auth-token` bearer mapping is a convenience for the shipped daemon,
  not an auth system.
- **`gc` is not safe alongside active writers.** It's documented as an
  offline/quiescent operation.
- **A workspace's metadata DB is as sensitive as its contents.** Blame, the audit
  log, and actors live only there.

## What is in scope

The things the design actively promises, and where a break is a real finding:

- **Forged attribution** — any path by which a client can make a write land
  credited to an actor other than the one its credential resolves to, on any
  surface, or bypass a propose-only actor's write policy.
- **Path traversal** — a name that escapes the virtual filesystem and reaches the
  host during materialization (sandbox export, mounts, git export).
- **Integrity** — content that fails to be caught by the read-time re-hash, or a
  way to make a tampered object be served as authentic.
- **Untrusted-input handling** — panics, aborts, or unbounded allocation from a
  hostile or corrupt object, manifest, pack, git object, or request. The object
  decoders are fuzzed in CI precisely because they parse bytes from a bucket
  nobody has to be trusted to own.
- **Encryption at rest** — key handling, nonce reuse, or anything that puts
  plaintext where the ciphertext was supposed to go.
- **Isolation** — an escape from `--isolate`'s bubblewrap filesystem boundary.

Prior work in this area, including the full failure-surface audit and the
regression tests that pin each fix, is in `crates/origofs-core/tests/hardening.rs`
and [`docs/DESIGN.md`](docs/DESIGN.md) §7.
