# Working with agents

There are three ways to put an agent to work in a workspace. They differ in what
the agent sees and how much it has to know about origofs.

| | The agent sees | Attribution | Use it when |
|---|---|---|---|
| [Overlay mount](#a-live-overlay-mount) | An ordinary directory | Streamed as it works | The agent is an existing tool that just edits files |
| [MCP](#mcp) | Filesystem tools | Server-side, per call | You control the agent and want the review loop |
| [Sandbox](#run-and-import-a-sandbox) | A copy-on-write directory | Imported at exit | You want an all-or-nothing run you can discard |

## A live overlay mount

The fastest path. origofs sets up an unprivileged kernel overlay over the
workspace, runs your agent inside it, and streams the agent's changes back into
origofs — attributed, *as they happen*, not only when the process exits:

```bash
origofs --workspace "$WS" overlay --actor "$AGENT" --sync-ms 500 -- \
    some-agent --do-the-thing
```

The agent sees a normal directory and reads and writes at native speed. origofs
captures each create, modify and delete into the content store and records it
against `--actor`. By the time the command exits, `blame` and the
[change feed](teams.md#the-change-feed) already reflect everything it did.

`--sync-ms` is how often changes are folded in while the agent runs (default
500 ms). Lower it to see the feed move sooner; raise it for a chattier agent.

!!! warning "Not a security boundary by default"

    By default the agent runs **with your privileges** over a plain copy-on-write
    overlay. The whole host filesystem stays reachable, including this
    workspace's own `meta.db` and content store, your home directory and your
    credentials. There is no network namespace and no seccomp filter; origofs
    only strips `ORIGOFS_ENCRYPTION_KEY` from the environment.

    Run only agents you trust, or pass **`--isolate`**.

`--isolate` runs the command under [bubblewrap](https://github.com/containers/bubblewrap)
in a fresh tmpfs root that hides the host filesystem — a real boundary for
untrusted code. It needs a non-setuid `bwrap` ≥ 0.11.0 on `PATH`. It is
deliberately *only* filesystem isolation: the network namespace is left shared on
purpose, because agents need egress, so it does not by itself contain anything
network-reachable.

Either way the delta is captured and imported identically.

## MCP

origofs speaks the Model Context Protocol over stdio, so an agent calls
filesystem tools directly and every write is attributed server-side:

```bash
origofs --workspace "$WS" mcp --agent-name claude --model claude-opus-4
```

The agent gets the whole loop as tools — reads, writes, an exact
search-and-replace edit, the [review queue](review.md), the trash. It cannot name
an actor: the server resolves identity from the session it was started with, so
an agent cannot forge blame, and it cannot accept its own proposals.

See the [MCP reference](../reference/mcp.md) for the tool list.

## Run and import: a sandbox

Where the overlay streams changes continuously, `sandbox` is a single
transaction: run a command over a copy-on-write view, then import everything it
changed as one attributed commit — or throw it away.

```bash
origofs --workspace "$WS" sandbox --actor "$AGENT" -- pytest -x
origofs --workspace "$WS" sandbox --actor "$AGENT" --discard -- ./risky-script
```

`--discard` drops the changes instead of importing them, which makes this the
right shape for "let it try, and only keep the result if it worked". The same
`--isolate` flag and the same default-is-not-a-security-boundary caveat apply.

Both `sandbox` and `overlay` are Unix-only — they are built on overlayfs
whiteouts, which are character devices.

## Bounding what an agent can do

Two independent gates, and it is worth being clear that they answer different
questions.

**Where can it write?** — [path ACLs](operating.md#scope-what-an-agent-can-reach).
A grant scopes an actor to a subtree.

```bash
origofs --workspace "$WS" acl grant "$AGENT" /src read+write --by "$DAN"
```

**Must a human see it first?** — the [write policy](review.md). An actor set to
`propose` has every write, on every surface, routed into the review queue instead
of landing.

```bash
origofs --workspace "$WS" write-policy "$AGENT" propose
```

The policy is a property of the *actor*, not of its kind: a trusted agent stays
`direct`, and an untrusted human contributor can be `propose`-only. Neither gate
depends on the agent cooperating — both are enforced in the engine, below every
surface.

## After the run

```bash
origofs --workspace "$WS" blame /src/main.rs        # who wrote each line
origofs --workspace "$WS" watch --follow            # the change feed, live
origofs --workspace "$WS" diff main agent-branch    # what moved
```

And if it went wrong, [undo exactly that agent's
session](attribution.md#undo-one-session) without touching anyone else's work.
