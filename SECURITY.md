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

### FUSE and NFS mounts — one actor, or none

A mount is **bound to a single actor for its lifetime** (`origofs mount --actor`,
`origofs nfs --actor`, `fuse::spawn_as`/`mount_as`, `nfs::serve_as`, or `ctx=`
from Python). Every mutating inode operation the mount issues is ACL-checked
against that actor, so a path-scoped grant reaches a mount the same way it
reaches MCP or the HTTP API.

Three things that follows *not* being:

- **It authorizes; it does not attribute.** A write through a mount records no
  `edit_op` and no blame. The bound actor bounds what the mount can reach. Do not
  read a mount's blame silence as "nobody wrote this".
- **`--actor` is not authentication.** The kernel never tells a FUSE server which
  process issued a request, and NFSv3 authenticates nobody, so one actor covers
  every process touching that mountpoint or socket. A mount is not a trust
  boundary *between* the users of that mount.
- **An actor-less mount still bypasses.** `None` is the historical anonymous
  mount and is kept as a *visible argument* rather than an absent one, precisely
  so that choosing it is a choice.

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

### Authorization: path-scoped, but writes-first and opt-in

Two primitives, and the difference between them matters:

- A per-actor **write policy** — `Direct` (writes land immediately) or `Propose`
  (writes are queued for review by a *different* actor).
- Path-scoped **ACL grants** — `(actor, path prefix) -> READ | WRITE | PROPOSE`,
  matched on directory boundaries, longest prefix wins.

Every *attributed* mutation is checked in the engine, not per surface, so a new
route or tool cannot forget the check. Delegation (`grant_as`/`revoke_as`) needs
`WRITE` at the prefix and cannot amplify: every bit granted must be one the
granter already holds there.

The parts to plan around:

- **Read enforcement is off by default** (`acl_enforce_reads`). Reads have never
  been checked, so no existing workspace holds read grants and enforcing on
  upgrade would stop every actor at once. Until you turn it on, **any actor can
  read every file, every blame record, and every actor display name** in the
  workspace.
- **Unattributed operations are open by construction.** `write`, `remove`,
  `rename`, `mkdir_p`, `commit` and the unattributed reads take no actor and run
  no check. They exist for internal machinery — checkout, merge materialization,
  gc, applying an accepted suggestion — and are what a local process holding the
  workspace directory can call directly anyway.
- **`Fs` is not a boundary against a Rust embedder.** Anything linking the crate
  can reach the metadata and content stores. The boundary is the HTTP surface.
- **The raw ACL setters take no authorization at all.** `grant`, `revoke`,
  `set_acl_default_deny`, `set_acl_enforce_reads` and `set_write_policy` exist
  for provisioning, which by construction precedes anyone holding rights. They
  are not exposed on any network surface; do not add one. Surfaces call the
  `_as` forms.

Workspaces are a structural boundary — separate roots, refs, working trees,
suggestion queues, change feeds, and blame — but there is no built-in actor →
workspace mapping. If your deployment needs one, enforce it in the layer that
resolves identity. The **tenant** layer described in `docs/MULTI_TENANCY.md`
(MT2+) is a concept, not an implementation; do not rely on workspaces alone to
isolate mutually distrusting customers.

### Live co-editing (`coedit`) — a known memory-safety hazard on untrusted input

**If you enable the `coedit` feature and expose its WebSocket to clients you do
not control, read this first.**

origofs pins `yrs` at 0.23 deliberately. A **malformed y-sync update reaches
unvalidated UTF-8 handling inside `yrs`** (`from_utf8_unchecked` in
`encoding/read.rs` and `updates/decoder.rs`) and is then iterated in
`block::utf16_len`. That is undefined behaviour: it aborts under the debug UB
checks and is **silent in release**. 51 bytes are enough, and the path is
reachable through `CoeditDoc::load` — which is what `handle_sync` calls on bytes
from a connected client.

What has been tried, so this is not re-litigated:

- **Upgrading does not fix it.** 0.24, 0.25 and 0.26 reproduce it identically.
  0.27.4 (the latest) has the same two `unsafe` sites *and* does not build on
  stable Rust.
- **Nothing local contains it.** The abort is non-unwinding, so `catch_unwind`
  does not help, and validating the bytes beforehand would mean reimplementing
  the decoder.

The reproducer is `crates/origofs-core/tests/coedit_malformed_update.rs`,
`#[ignore]`d because it takes the suite down rather than failing it; run it with
`--ignored` when evaluating a candidate `yrs`. `fuzz_targets/coedit_state_decode.rs`
drives the same path and is expected to abort.

**What this means for a deployment.** `coedit` is off by default and is
deliberately not part of `full`, so you have to ask for it. If you have asked for
it, treat the co-editing WebSocket as reachable only by clients you trust: put it
behind authentication that you actually enforce, and do not expose it to the open
internet. Opening a co-editing socket already requires `WRITE` at the path — but
authorization runs *after* the transport is up, and the decoder sees the frame
either way.

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
