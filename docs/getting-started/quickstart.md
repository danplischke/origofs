# Quickstart

Every command below is run against a throwaway workspace. Nothing touches your
home directory except the workspace folder itself.

## Create a workspace

```bash
export WS=./ws
origofs --workspace "$WS" init
```

`--workspace` points at the directory holding `meta.db` and `cas/`. It defaults
to `.origofs`, and every command takes it. Export it once and the examples below
read more cleanly.

## Read and write

`write` takes its bytes from stdin, or from a file with `--from`. Paths are
absolute *within the workspace* — `/notes/a.txt` is not a path on your disk.
Parent directories are created as needed.

```bash
echo 'hello from origofs' | origofs --workspace "$WS" write /notes/a.txt

origofs --workspace "$WS" ls   /notes         # file	a.txt
origofs --workspace "$WS" read /notes/a.txt   # hello from origofs
origofs --workspace "$WS" stat /notes/a.txt   # size, kind, timestamps
```

That write is **unattributed**. It works, and it records nothing about who made
it — which is the one thing origofs exists to do.

## Write on the record

An actor is a registered identity: a human or an agent. Registering one prints
its id.

```bash
DAN=$(origofs --workspace "$WS" actor dan)          # a human
CLAUDE=$(origofs --workspace "$WS" actor claude --agent --model claude-opus-4)
```

Pass `--actor` and the write records blame and an append-only edit-op:

```bash
printf 'hello from dan\nsecond line\n' \
  | origofs --workspace "$WS" write /notes/a.txt --actor "$DAN"

origofs --workspace "$WS" blame /notes/a.txt
#    1-2     human:dan
```

Now let the agent rewrite the file:

```bash
printf 'hello from dan\nsecond line\nand a third, by the agent\n' \
  | origofs --workspace "$WS" write /notes/a.txt --actor "$CLAUDE"

origofs --workspace "$WS" blame /notes/a.txt
#    1-2     human:dan
#    3       agent:claude
```

Blame is per line, and the two writers' work is separable — which is what makes
[undoing one agent's session](../guides/attribution.md#undo-one-session)
possible.

!!! tip "Set the actor once"

    Every attributed command falls back to the **`ORIGOFS_ACTOR`** environment
    variable, so a shell or an agent harness can set identity once instead of
    threading `--actor` through every call:

    ```bash
    export ORIGOFS_ACTOR="$DAN"
    ```

    This is *not* an identity check — whoever writes the environment writes the
    identity. See [Core concepts](concepts.md#identity-is-asserted-here-verified-at-the-boundary).

## Commit

A commit snapshots the working tree, the same idea as git's index:

```bash
origofs --workspace "$WS" commit -m 'first notes' --actor "$DAN"
origofs --workspace "$WS" log
origofs --workspace "$WS" status          # working-tree changes since HEAD
```

`--actor` is the identity the commit is checked against and attributed to.
`--author` is the free-form name recorded *inside* the commit object, the way
git records one; it defaults to `origofs` and is not an identity.

## See it as ordinary files

To use normal tools on a workspace, mount it (Linux; needs `/dev/fuse`):

```bash
sudo origofs --workspace "$WS" mount /mnt/ws --actor "$DAN"
```

Now `/mnt/ws/notes/a.txt` is a real file to every program on the machine, and
writes through it are checked against `dan`'s grants. See
[POSIX mounts](../guides/mounts.md).

## From Rust

`write` is unattributed and `write_as` carries an identity; everything else is
the same call.

```rust
use origofs_sdk::{Workspace, WriteCtx};

let ws = Workspace::open_local("meta.db", "cas").await?;   // or open_pg(dsn, cas)
ws.mkdir_p("/notes").await?;

let dan = ws.create_human("dan", None).await?;
let ctx = WriteCtx::session(dan, ws.create_session(dan, None).await?);
ws.write_as(ctx, "/notes/a.txt", b"hello").await?;         // attributed

let bytes = ws.read("/notes/a.txt").await?;
let spans = ws.blame("/notes/a.txt").await?;               // who wrote which bytes
```

## From Python

The same API, with `await` on every I/O method:

```python
import asyncio, origofs

async def main():
    ws  = await origofs.Workspace.open_local("meta.db", "cas")
    dan = await ws.create_human("dan")
    ctx = origofs.WriteCtx.session(dan, await ws.create_session(dan))

    await ws.write_as(ctx, "/notes/a.txt", b"hello")
    print(await ws.read("/notes/a.txt"))
    print(await ws.blame("/notes/a.txt"))   # [{'byte_start': 0, ..., 'actor': {...}}]

asyncio.run(main())
```

See the [Python reference](../reference/python.md).

## Next

- [Core concepts](concepts.md) — the model underneath all of this.
- [Working with agents](../guides/agents.md) — mounts, MCP and sandboxes.
- [Propose and review](../guides/review.md) — make an agent's writes land in a
  queue instead of on the file.
