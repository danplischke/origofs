# Permissions in origofs — what exists, and how to add them

> Status: **concept / RFC.** Taken against `adb6ec8` (2026-08-18). Companion to
> `docs/MULTI_TENANCY.md`, which specifies the *isolation* boundary (tenants and
> workspaces); this document covers *authorization within* one — who may read and
> write which paths. The baseline in §1 is what the code does today; §3 onward is
> a proposal, not a description.

## Summary

**origofs has no file or folder permissions.** `mode` is stored, committed, and
reported, and nothing anywhere consults it to allow or deny an operation. The
only authorization in the engine is the per-actor **write policy**
(`Direct | Propose`) — global, binary, and write-only.

That is a defensible place to have started: the write policy plus the workspace
wall covers "an untrusted agent can't land edits without review" and "project A
can't see project B," which were the first two things this system needed. It does
not cover "this agent may write under `/src/parser` and nowhere else", which is
the next one.

---

## 1. The honest baseline

### 1a. `mode` is carried faithfully and never enforced

`Inode.mode: u32` (`crates/origofs-core/src/types.rs:100`) is persisted
(`migrations.rs:590` SQLite, `:614` Postgres), set at creation
(`vfs.rs:273`/`:291`, masked into `S_IFREG`/`S_IFDIR`), encoded into committed
tree objects (`TreeEntry.mode`, `objectgraph.rs:45`) so it survives commit and
checkout, reported by FUSE (`fuse.rs:587`) and NFS (`nfs.rs:111`), and read by
git export to decide the exec bit (`git/export.rs:210`).

That is the complete list of uses. A grep for permission-check machinery across
`crates/*/src` returns one hit — `format.rs:check_readable` — which is about
object format versions, not access.

### 1b. There is no ownership at all

No `uid`, no `gid`: not on `Inode`, not on `InodeInit`, not in any migration
through V16. **Both mounts hardcode `uid: 0, gid: 0`** (`fuse.rs`, `nfs.rs:113`).

### 1c. The kernel is asked to enforce mode, vacuously

FUSE mounts with `MountOption::DefaultPermissions` (`fuse.rs:518`), which tells
the kernel to run real POSIX checks against the attributes origofs reports. Since
every inode reports as root-owned and `fuse_mountable()` requires `is_root`, every
check passes and the mount is root-only by construction.

This is coherent rather than broken, but it is exactly why `allow_other` or a
non-root mount is not currently viable: a non-root caller would be evaluated
against uid 0 in the *other* class and lose write access to the entire tree. Fixing
that is §3a, not a mount-option change.

### 1d. `chmod` silently does nothing ([#121](https://github.com/danplischke/origofs/issues/121))

Both mounts accept a mode change and discard it.

- FUSE `setattr` (`fuse.rs:623`) binds `_mode`, `_uid`, `_gid` with leading
  underscores and honours only `size`. It then replies with the **unchanged**
  attributes, so the caller sees success.
- NFS `setattr` (`nfs.rs:161`) says so in a comment: *"origofs's minimal inode set
  doesn't persist uid/gid/atime/mtime; mode changes aren't yet surfaced by `vfs_*`,
  so those set-attrs are accepted but no-op."*

There is no `chmod` or `set_mode` anywhere in the engine, so **mode is write-once
at creation and immutable thereafter through every surface**. `chmod +x build.sh`
on a mount reports success and changes nothing — and because the exec bit *is*
exported to git (§1a), the mode a file is created with is the mode it carries into
history forever. NFS at least creates with sensible defaults (`0o644` for files,
`0o755` for dirs, `nfs.rs:246`/`:260`); FUSE passes the caller's mode through at
create and that is the only chance to set it.

The silence is the problem more than the missing feature: an accepted-and-ignored
`chmod` is worse than `EPERM`, because a script that checks its return code
proceeds on a false premise.

### 1e. The actual authorization model is the write policy

`WritePolicy::{Direct, Propose}` (`attribution.rs:59`), a column on `actor`
(migration V10), enforced by `ensure_may_write(ctx, op)` (`suggest.rs:222`). It is
the only source of `OrigoFSError::Denied` in the tree (`suggest.rs:225`, plus the
SDK wrapper at `origofs-sdk/src/lib.rs:1099`).

Its shape matters for everything below:

| Property | Today |
|---|---|
| Granularity | per **actor**, whole workspace |
| Values | binary — direct, or propose-and-review |
| Direction | **writes only** — reads are never gated |
| Argument | `(ctx, op)` — **no path** |

`CLAUDE.md` records the one architectural rule that makes it trustworthy: the
policy is *"enforced in the engine, not per surface"*, and `tests/mcp.rs` fails on
an unclassified MCP tool so a new ungated one cannot ship silently. Any permission
system must inherit that rule or it will be re-holed by the next surface.

### 1f. Isolation is workspaces; authorization inside one is absent

MT1 is implemented — `workspace_id` across the working-tree, namespace, activity
and attribution tables, per-workspace root inodes, per-workspace blame.
`docs/MULTI_TENANCY.md` §7 then states the gap in its own words:

> *"All of a tenant's actors may reach all of its workspaces by default; a
> deployment that wants per-workspace scoping (project A's agent can't touch
> project B) enforces it in the resolver/router as a policy check after the tenant
> is resolved."*

Nothing in the engine does that, and no surface offers a hook for it.

### 1g. One real per-path control exists, and it is Python-only ([#125](https://github.com/danplischke/origofs/issues/125))

`origofs.fastapi`'s root-scoping (`fastapi.py:215`–`:268`) is the only working
path-level access control in the repository, and it is well built:

- `_under` does **directory-boundary** matching, not `startswith`, so `/tenant-a`
  does not cover `/tenant-abc`;
- `_scoped` **prepends** the root rather than comparing against it, so an
  out-of-scope path is not representable in a request;
- a `None` path is excluded, because a record naming no path (an idle presence
  row) still tells a scoped reader that a neighbour exists;
- `_require_in_scope` refuses with **404, not 403**, so a caller cannot distinguish
  "exists but is not yours" from "does not exist".

The Rust `api` module has no equivalent: `Principal` is `{actor, session}`
(`api/mod.rs:85`) and `gate_reads` defaults to `false` (`:254`), so reads are open
unless the embedder opts in.

---

## 2. The ruling: two systems, not one

The trap is to make POSIX ownership the authorization model. Resist it.

origofs's principals are **actors** — humans and agents, already the unit of
identity, attribution, the audit log, and the write policy. Unix uids are a
different namespace with a different lifecycle, and mapping actors onto them is
the impedance mismatch that makes multi-user NFS miserable. An agent is not a
login.

So there are two independent pieces of work that are easy to confuse:

| | Purpose | Principal | Enforced by |
|---|---|---|---|
| **(a) POSIX ownership** | be a correct filesystem: `chown`, `chmod`, honest `stat`, non-root mounts | uid/gid | the kernel, via `default_permissions` |
| **(b) Actor ACLs** | decide which actor may touch which subtree | `actor_id` | the engine, at one chokepoint |

They meet in exactly one place — a mount has no actor context, so enforcing (b)
through a mount requires either mounting per-actor or mapping uid → actor. That
decision is §5, and it is the only hard coupling between them.

---

## 3. Proposal

### 3a. Ownership (`uid`/`gid`) — migration V17 ([#122](https://github.com/danplischke/origofs/issues/122))

Add `uid`/`gid` to `Inode`, `InodeInit`, and the schema, defaulting to 0 so
existing workspaces are unchanged by the migration. Surface `chmod`/`chown` as
`vfs_*` operations, wire both mounts' `setattr` to them, and report the real
values instead of 0.

This is small, and it is what unblocks `allow_other`, non-root mounts, and the
`link`/`statfs` items in the JuiceFS review (#119). It buys **no** authorization by
itself — it makes the mount stop lying.

One decision: whether mode/uid/gid changes are *attributed*. A `chmod` is a
metadata mutation an actor performed, and the audit log arguably wants it. Cheapest
consistent answer: `chmod_as`/`chown_as` are attributed ops that append an
`edit_op` with no byte range, and the unattributed forms stay internal like the
rest of the exempt machinery (§4).

### 3b. Write ACLs — the high-leverage change ([#123](https://github.com/danplischke/origofs/issues/123))

Every attributed mutation already funnels through one function, so give it a path:

```rust
ensure_may_write(ctx, op)  →  ensure_may_write(ctx, op, path)
```

backed by a prefix-grant table (migration V18), longest-prefix-match wins:

```sql
CREATE TABLE acl (
  workspace_id INTEGER NOT NULL,
  actor_id     INTEGER NOT NULL,
  path_prefix  TEXT    NOT NULL,  -- '/' = the whole workspace
  perms        INTEGER NOT NULL,  -- bitset: read | write | propose
  PRIMARY KEY (workspace_id, actor_id, path_prefix)
);
```

Notes on the shape:

- **Default-allow at `/`.** Backfill every existing actor with a root grant
  carrying its current write policy, so V18 is behaviour-preserving. A deployment
  tightens by adding narrower grants; a deployment that wants deny-by-default flips
  a per-workspace config key.
- **`Propose` becomes a perm, not an actor mode.** Strictly more expressive: "may
  write `/docs`, may only propose under `/src`" is currently unrepresentable. The
  existing column becomes the root grant's perms, so nothing is lost.
- **Prefix matching must use the Python router's semantics** (§1g) —
  directory-boundary, not `startswith`. Getting this wrong is the classic
  `/tenant-a` vs `/tenant-abc` bug, and there is already a correct implementation
  in the tree to port.
- **Rename is two checks, not one.** `rename_as(from, to)` needs write on both
  sides; checking only the source lets an actor move a file it controls into a tree
  it does not.

### 3c. Read ACLs — the expensive half, staged separately ([#124](https://github.com/danplischke/origofs/issues/124))

Writes are cheap because `ensure_may_write` already exists. Reads have no
equivalent: `read`, `read_range`, `ls`, `stat`, `blame` take **no actor context at
all**. Gating them means introducing a read context and threading it through every
read path, every surface, and every binding — a breaking API change across the
whole project.

Worse, the front door is not the only door. A read ACL that gates `read` but not
these leaks the same information:

| Side door | Leaks |
|---|---|
| `blame` | who wrote which lines of a file you cannot read |
| `list_suggestions` | pending content and paths |
| `events_since` (change feed) | paths, sizes, timing |
| `active_presence` | that a path exists and is being edited |
| `log` / `diff` | committed paths and contents |
| `list_locks`, `list_conflicts` | paths |

The Python router already learned this and filters presence rows explicitly. Any
Rust-side read ACL must cover this whole set on day one, or it is decoration.

**Recommendation: do not build this until there is a concrete requirement.** Write
ACLs (§3b) plus the workspace wall cover the realistic multi-agent threat — a
misbehaving agent damaging things — while read confidentiality between actors
inside one workspace is a much stronger claim that the architecture does not
currently support anywhere.

---

## 4. Invariants any implementation must hold

1. **Enforce in the engine, never per surface.** The rule `CLAUDE.md` already
   states for the write policy. Per-surface enforcement is re-holed by the next
   surface; the MCP classification test is the pattern for making that structural.
2. **The unattributed ops stay exempt, deliberately.** `remove`/`rename`/`mkdir_p`/
   `symlink`/`commit` take no actor because they are internal machinery — checkout,
   merge materialization, applying an accepted suggestion. Same carve-out as the
   write policy, for the same reason, and it must be documented as such rather than
   discovered later.
3. **Maintenance paths ignore ACLs entirely.** `gc` already marks across *all*
   workspaces precisely because a scoped sweep deletes live data; the same applies
   here. `rebuild`, `repack`, and `backup` likewise. An ACL-aware `gc` is a
   data-loss bug, not a security feature.
4. **Denied must not leak existence.** Follow the Python router: 404 where a 403
   would confirm a path exists. `OrigoFSError::Denied` maps to 403 today, which is
   right for a path the caller can see and wrong for one it cannot.
5. **Every grant change is auditable.** Granting yourself write is the interesting
   event in any breach; it belongs in the audit log with the actor that made it.

---

## 5. The mount problem

FUSE and NFS have **no actor context** — a deliberate bypass recorded in
`CLAUDE.md`, because a mount has no way to know which actor is behind a syscall.
Actor ACLs are therefore unenforceable through a mount, and shipping §3b without
saying so would create a false sense of containment: an agent denied write access
over MCP could take the same action through the mount.

Three options, in increasing cost:

1. **Document the bypass and leave it.** Consistent with today's attribution story.
   Acceptable only while mounts are root-only and operator-driven.
2. **Mount per actor.** The mount handle carries a `WriteCtx`; one mount serves one
   actor. Cheap, composes with §3a's ownership work, and turns the mount into an
   enforceable surface. Probably the right answer.
3. **Map uid → actor.** A real multi-user mount. Requires §3a plus an actor↔uid
   table, and is the only option that lets several actors share one mount.

Whichever is chosen must be decided *before* §3b ships, because "ACLs exist" and
"ACLs are bypassable by design on two surfaces" need to be stated in the same
breath.

---

## 6. Staging

| Phase | Scope | Issue | Migration | Cost |
|---|---|---|---|---|
| 0 | `chmod`/`chown` stop silently no-oping (§1d) — at minimum, fail loudly | #121 | — | tiny |
| 1 | `uid`/`gid` + working `chmod`/`chown` + honest mount attrs (§3a) | #122 | V17 | small |
| 2 | Path-scoped write ACLs (§3b) + the mount ruling (§5) | #123 | V18 | medium |
| 3 | Surface parity: port the Python router's scoping to the Rust API, add bindings, extend the MCP classification test | #125 | — | medium |
| 4 | Read ACLs (§3c) — only against a real requirement | #124 | V19 | large |

Phase 2 is the one that changes what the product can claim: it turns a global
binary trust flag into "this agent may write under `/src/parser` and nowhere
else", which is the thing anyone pointing several agents at one workspace will ask
for first.

Phase 0 is worth doing immediately and independently of all of it — a `chmod` that
returns success without doing anything is a bug regardless of what the permission
model eventually becomes.

---

## 7. Open questions

1. **Deny-by-default or allow-by-default**, per workspace or globally? Allow-by
   default is the compatible choice; deny-by-default is the one a security review
   will ask for.
2. **Do groups exist?** Per-actor grants are simple and get repetitive fast with
   many agents. An `actor_group` indirection is the obvious extension and the
   obvious thing to defer — but the table shape should not preclude it.
3. **Does an ACL travel?** `resync` remaps actor identities across workspaces
   (`resync.rs`). Do grants move with them, or is authorization strictly local to a
   deployment? Local is safer and probably correct, but it must be explicit or
   resync will silently drop them.
4. **What does `commit` require?** A commit touches every dirty path. Does it
   demand write on all of them, or is committing a distinct permission? The former
   is more correct and makes partial-permission commits fail confusingly.
5. **Suggestions vs. read access.** `accept_suggestion` lands an edit attributed to
   the original author. If that author has since lost write access to the path,
   does the accept still land? (It should — the reviewer is the one acting — but
   the check must be written deliberately rather than falling out of whichever
   actor happens to be in the `WriteCtx`.)
