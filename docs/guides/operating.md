# Operating a workspace

Four things you configure rather than call: what may be recovered, who may reach
what, how much space is used, and whether file locks coordinate between mounts.

All four are **off or unlimited by default**. Turning one on changes behaviour for
everyone using that workspace, so none of them arrives with an upgrade.

## Undo a delete

A committed file can always be read back out of history. An **uncommitted** one
could not be recovered at all — which matters more here than on an ordinary
filesystem, because the users are agents and `rm -rf` on a bad path is a routine
failure mode.

```bash
origofs --workspace "$WS" trash retention        # "trash is disabled" — the default
origofs --workspace "$WS" trash retention 7d     # start collecting, keep a week

origofs --workspace "$WS" rm /draft.md --actor "$AGENT"
origofs --workspace "$WS" trash list             # #1  file  5  actor=2  /draft.md
origofs --workspace "$WS" trash restore 1 --actor "$DAN"
origofs --workspace "$WS" trash purge 1          # or --all
```

Retention takes a duration — `7d`, `48h`, `3600s`, or bare seconds — and `off`
disables it.

Entries record **who deleted them**, so a restore is attributed and the deletion
is already in the op-log beside it. Three rules the surfaces share:

- A **restore is a write**, so it is attributed like any other.
- A **purge takes `WRITE` at the entry's path**, because it destroys the only
  remaining copy of an uncommitted file.
- A **trash id is workspace-global**, so an id-addressed call resolves it to a
  path and checks scope first, answering *not found* rather than *denied*.

!!! info "Why it is off by default"

    Turning retention on silently would change when space is reclaimed for every
    existing deployment, and the first anyone would learn of it is a storage
    bill. Because it is explicit, an empty listing tells you *which* empty it is:
    nothing deleted, or not collecting. Only one of those is a configuration
    answer.

The trash is also reachable over [HTTP](../reference/http-api.md) and
[MCP](../reference/mcp.md).

## Scope what an agent can reach

Grants are `(actor, path prefix) → permissions`. Longest prefix wins, matched on
directory boundaries, so `/tenant-a` never covers `/tenant-abc`.

```bash
origofs --workspace "$WS" acl grant 1 /src read+write
origofs --workspace "$WS" acl check 1 /src/main.rs   # actor 1 at /src/main.rs: read+write
origofs --workspace "$WS" acl show
origofs --workspace "$WS" acl revoke 1 /src
```

Permissions are `read`, `write`, `propose`, `none`, or a combination written with
`+`.

### A grant on its own restricts nothing

This is the part that surprises people. With `default-deny` **off** — the default
— an actor with no matching grant falls back to its write policy, which for an
ordinary actor is full access:

```bash
origofs --workspace "$WS" acl check 1 /secrets.txt   # read+write+propose
origofs --workspace "$WS" acl default-deny on
origofs --workspace "$WS" acl check 1 /secrets.txt   # none
```

`acl check` exists for exactly this: it answers the question an ACL bug is
actually asking, after prefix matching *and* the fallback.

### Reads are a separate switch

```bash
origofs --workspace "$WS" acl enforce-reads on
```

Reads have never been checked, so no existing workspace holds read grants, and
turning this on without writing them first stops every actor at once. Once on,
listings filter per entry — a listing that names what a `stat` would refuse is
the existence oracle the refusal exists to prevent — and id-addressed reads
answer *not found* rather than *denied*, because an id is a guessable handle and
a refusal would confirm one exists.

### Granting as someone

```bash
origofs --workspace "$WS" acl grant 3 /docs read --by "$DAN"
```

`--by` grants *as* that actor, and two conditions apply:

- **`WRITE` at the prefix.** Delegation is administrative — being able to read a
  subtree does not make you the one who decides who else reads it.
- **No amplification.** Every bit granted must be one the granter holds there, or
  a write-only actor could mint itself `READ`.

`WRITE` implies `PROPOSE` for delegation. It deliberately does **not** imply
`READ`.

Omitting `--by` provisions as the workspace owner and says so in its output. That
form exists for the first grant in a fresh workspace, which by construction
precedes anyone holding rights in it — not for a caller who forgot the flag.

Grants are enforced in the engine, so they apply identically over MCP, the HTTP
API, the CLI and a [mount](mounts.md#a-mount-is-bound-to-one-actor).

## Measure and cap

```bash
origofs --workspace "$WS" du /              # /   2 inodes   6 bytes
origofs --workspace "$WS" du /src           # a subtree

origofs --workspace "$WS" quota             # bytes:  6 / unlimited
                                            # inodes: 2 / unlimited
origofs --workspace "$WS" quota --bytes 10G --inodes 100000
origofs --workspace "$WS" quota --bytes off
```

Both count an inode with several names **once**, and sum **logical** size — never
deduplicated bytes. A quota measured in physical bytes would move under a user
who changed nothing, because someone else's write can dedup against theirs.

Mounts answer `df` from the same numbers.

## Share file locks between mounts

```bash
origofs --workspace "$WS" posix-locks              # off, the default
origofs --workspace "$WS" posix-locks on
origofs --workspace "$WS" posix-locks --path /notes.md
```

This is **not** "locking on/off" — see
[POSIX advisory locks](mounts.md#posix-advisory-locks) for what it actually
changes. Locks are taken by mounts, not by the CLI, and carry a lease so a mount
that dies does not hold a byte range forever.

NFS exports do not support this: NFSv3 locking is a separate protocol (NLM) that
origofs does not speak.

## Require attribution

```bash
origofs --workspace "$WS" require-attribution on
```

Every mutating CLI command must then name an actor. It is an
[attribution-completeness switch, not access
control](attribution.md#requiring-attribution).

## Changing ACLs is itself gated

The raw `grant`/`revoke`/`set_write_policy` SDK calls take **no** authorization —
`granted_by` is an audit field the caller fills in, not a claim anything verifies.
An actor reaching them hands itself `WRITE` at `/`.

That is survivable only because no network surface exposes them: there is no ACL
route on the HTTP API and no MCP tool. **Safety by absence of a route is not
safety**, so a surface must call the `_as` forms, which are gated. The CLI's `acl`
subcommand does exactly that whenever `--by` is present.
