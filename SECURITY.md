# Security

## Reporting a vulnerability

Please report security issues privately, via
[GitHub's private vulnerability reporting](https://github.com/danplischke/origofs/security/advisories/new)
rather than a public issue. Include what you were running (version or commit,
backends, which surface), what you observed, and a reproduction if you have one.

origofs is pre-1.0 and maintained on a best-effort basis; there is no guaranteed
response time and no bug bounty.

## What is and is not a security boundary

Being explicit about this matters more than a policy statement, because the
project deliberately ships things that *look* like boundaries and are not.

### `origofs sandbox` / `origofs overlay` — by default, not a boundary

Without `--isolate`, the child process runs **with your privileges** over a plain
copy-on-write overlay. There is no `pivot_root`, no network namespace, and no
seccomp: the entire host filesystem stays reachable by absolute path, including
the workspace's own `meta.db` and `cas/`. The command inherits your environment
apart from `ORIGOFS_ENCRYPTION_KEY`. **Run only code you trust.**

With `--isolate` (needs `bwrap` >= 0.11.0, non-setuid — that is where the `--overlay` options it relies on were added) the command runs under bubblewrap in a
fresh tmpfs root: the host filesystem is absent, the environment is cleared to
`PATH`/`HOME`/`TMPDIR`, and `--new-session` detaches the controlling terminal.
This is a **filesystem** boundary. The network namespace is left shared on
purpose, because agents generally need egress — so it does not contain
network-reachable resources, and a secret you pass in explicitly is a secret the
sandboxed code can exfiltrate.

### FUSE and NFS mounts — no actor context

A mount has no way to know which actor is behind a `write(2)`, so the mount
surfaces bypass the per-actor write policy by construction. This is a deliberate
gap, not an oversight. Do not expose a mount as a trust boundary between actors.

NFSv3 is unauthenticated; the CLI warns on a non-loopback bind.

### The HTTP API

- Identity is resolved **server-side** by the `Authenticator` you supply. The
  request body never names an actor, so attribution cannot be forged by a client.
- `origofs serve` **refuses** to expose an unauthenticated API on a non-loopback
  address.
- **Reads are open by default** (`gate_reads: false`). Every file, every blame
  record, and every actor display name is readable by anyone who can reach the
  port unless you set `gate_reads` or gate at your proxy.
- `/health`, `/readyz`, and `/metrics` sit outside `/v1` and are unauthenticated
  by design so a probe or scraper needs no credential. They are built not to
  leak: `/readyz` reports only which store is unhealthy, and every metric label is
  a closed set (never a path, actor, or hash). **Keep it that way when adding a
  metric.**
- `BearerAuth` is a static token map with no expiry or revocation. It is a
  reasonable default for tokens minted out of band; implement `Authenticator` for
  anything that needs rotation.

### Authorization is coarse

The only authorization primitive is a per-actor write policy: `Direct` (writes
land immediately) or `Propose` (writes are queued for review by a *different*
actor). There is **no path-scoped or per-file authorization**. Within a workspace,
any actor can read every file, and any `Direct` actor can modify or delete every
file including other actors' work.

Workspaces are a structural boundary — separate roots, refs, working trees,
suggestion queues, change feeds, and blame — but there is no built-in actor →
workspace mapping. If your deployment needs one, enforce it in the layer that
resolves identity. The **tenant** layer described in `docs/MULTI_TENANCY.md`
(MT2+) is a concept, not an implementation; do not rely on workspaces alone to
isolate mutually distrusting customers.

### At-rest encryption

`ORIGOFS_ENCRYPTION_KEY` (or `open_local_encrypted`) enables XChaCha20-Poly1305
at rest with an Argon2id-derived key. Note that addresses stay the **plaintext**
hash — convergent encryption, so deduplication still works. That means a shared
encrypted store is an existence oracle: someone who can guess a plaintext can
confirm whether it is present. Use per-tenant keys if that matters.

The same key must be supplied on every open; a wrong one fails loudly rather than
returning garbage. The salt lives beside the content store and must persist.

## Supply chain

`cargo deny` runs in CI over RUSTSEC advisories, a license allow-list, and source
pinning (`deny.toml`). Dependabot keeps the dependency tree and the CI actions
current so the advisory gate does not fail with nothing queued to fix it.
