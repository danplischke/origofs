# POSIX mounts

A workspace is not a directory on your disk, so to use ordinary tools on it you
mount it. Two surfaces do that, and both are Unix-only.

| | Where | Needs |
|---|---|---|
| [FUSE](#fuse) | Linux | root and `/dev/fuse` |
| [NFSv3](#nfs) | macOS, and Linux | nothing privileged to serve |

Both address everything by inode through the same engine layer, so a write
through a mount lands on the change feed like any other.

## FUSE

```bash
sudo origofs --workspace "$WS" mount /mnt/ws --actor "$DAN"
```

The command blocks until the mountpoint is unmounted. Everything under
`/mnt/ws` is now a real file to every program on the machine.

## NFS

macOS has no FUSE — macFUSE is a kernel extension a wheel cannot carry — so
mounts there go over NFSv3:

```bash
origofs --workspace "$WS" nfs --addr 127.0.0.1:11111 --actor "$DAN"
```

Then, from another terminal:

```bash
sudo mount -o vers=3,tcp,port=11111,mountport=11111 127.0.0.1:/ /mnt/ws
```

## A mount is bound to one actor

`--actor` binds the mount to that actor for its lifetime, and every operation
through it is checked against that actor's path grants. Omit it and the mount is
**anonymous** and bypasses ACLs entirely — which is the historical behaviour, and
why the actor is a visible argument rather than an absent one.

!!! warning "It authorizes; it does not attribute"

    A write through a mount records **no blame and no edit-op**. The mount's
    actor bounds what the mount can *reach* — do not read it as attribution.

    Nor is `--actor` authentication. The kernel never tells a FUSE server which
    process issued a request, and NFSv3 authenticates nobody, so one actor
    covers everything on that mountpoint or socket. If you need per-write
    attribution, use a surface that has an identity per call: the
    [HTTP API](../reference/http-api.md), [MCP](../reference/mcp.md), or the
    [overlay mount](agents.md#a-live-overlay-mount), which captures changes and
    attributes them as it goes.

Reads through a mount follow the same opt-in as everywhere else: checked only
where the workspace sets `acl enforce-reads on`. When it is on, `readdir`
filters per entry the same way `ls` does, because a listing that names what a
`stat` would refuse is the existence oracle the refusal exists to prevent.

## Copy and allocate are served from the manifest

Two syscalls are answered without moving bytes, which is worth knowing because it
changes what is cheap.

**`copy_file_range`** — a server-side copy. Content is chunked and
hash-addressed, so a range copy repoints the destination at chunks that already
exist. Only a chunk *straddling* the start or end of the range is ever read, so
copying a gigabyte costs at most two chunks of I/O. `cp --reflink` and anything
built on `copy_file_range` gets this for free.

**`fallocate`** — answered in terms of observable results rather than block
reservation, because there are no blocks to reserve:

| Mode | What happens |
|---|---|
| `KEEP_SIZE` alone | A genuine no-op. |
| Plain (extend) | A size change. Not a promise about later writes. |
| `PUNCH_HOLE`, `ZERO_RANGE` | Deduplicated zero chunks. Punching a hole *releases* space once [`gc`](backup-and-recovery.md#reclaiming-space) runs. |
| `COLLAPSE_RANGE`, `INSERT_RANGE` | `EOPNOTSUPP`. They shift every subsequent byte; a near-miss would be worse than a refusal. |

## POSIX advisory locks

`fcntl` byte-range locks (`F_GETLK`/`F_SETLK`/`F_SETLKW`) — what SQLite, editors
and lockfile-based tools use. **Off by default:**

```bash
origofs --workspace "$WS" posix-locks on
origofs --workspace "$WS" posix-locks              # print the current setting
origofs --workspace "$WS" posix-locks --path /db.sqlite   # who holds what
```

!!! info "What the switch actually changes"

    A FUSE mount that does not answer `fcntl` locks still has **working advisory
    locks** — the kernel serves them locally, per mount. So this does not add
    locking to a workspace that had none.

    What it adds is coordination *between* mounts: two processes, on two
    machines, against one workspace. It also takes locking over from the kernel
    for that mount, which is why it is a deliberate switch rather than a default.

Mounts read the setting once, at mount time. Remount to pick up a change.

Locks are held in the metadata store with a lease, so a mount that dies does not
leave a file locked forever.

!!! warning "Not the same as `origofs lock`"

    [`origofs lock`](versioning.md#merging) is an LFS-style **path lock**: a
    workflow convention for binaries that cannot be merged. Unrelated objects,
    sharing a word.

## Limits

Mounts are a POSIX veneer over a versioned, attributed store, and a few things
behave differently from a local disk. Read [Limits](../LIMITS.md) before pointing
a demanding workload at one.
