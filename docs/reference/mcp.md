# MCP server

origofs speaks the Model Context Protocol over stdio, so an agent calls
filesystem tools directly and every write is attributed server-side.

```bash
origofs --workspace "$WS" mcp --agent-name claude --model claude-opus-4
```

| Flag | Default |
|---|---|
| `--agent-name` | `mcp-agent` |
| `--model` | `unknown` |

The named agent is registered on start, and every call is attributed to it.
Logging goes to **stderr**, so stdout stays clean for the JSON-RPC transport.

## What the agent cannot do

Three things are structurally out of reach, and they are the reason MCP is a
better fit than handing an agent a shell:

- **It cannot name an actor.** Identity comes from the server's own session, so
  an agent cannot forge blame or act as someone else.
- **It cannot accept its own proposals.** Review requires a different actor, and
  that is checked in the engine.
- **It cannot escape its grants.** [Path ACLs](../guides/operating.md#scope-what-an-agent-can-reach)
  and the [write policy](../guides/review.md#making-review-mandatory) are enforced
  below every surface, so a `propose`-only agent's `origofs_write` is *routed* into
  the review queue rather than refused — the result says which happened.

There is deliberately **no ACL tool**. Changing an ACL is an administrative
operation, and exposing it here would let an agent grant itself whatever it
wanted.

## Tools

### Reading and writing

- **`origofs_read`** — Read a file's contents.
- **`origofs_ls`** — List a directory.
- **`origofs_write`** — Write a file, attributed to this agent. If the agent is
  propose-only, the edit is queued as a suggestion instead of landing, and the
  result says which happened.
- **`origofs_edit`** — Edit by exact string replacement: replace `old` with
  `new`. `old` must appear exactly once unless `replace_all` is set. **Prefer
  this over `origofs_write` for a small change** — it sends only the changed text
  and credits only the changed lines.
- **`origofs_mkdir`** — Create a directory and parents.
- **`origofs_rm`** — Remove a file or empty directory. Governed by the write
  policy the same way: a propose-only agent queues a deletion for review.

### Review

- **`origofs_suggest`** — Propose an edit for review. The bytes are stored now;
  the file changes only when a different actor accepts.
- **`origofs_suggestions`** — List pending suggestions, optionally filtered to a
  path.
- **`origofs_suggestion_diff`** — A suggestion's unified diff, base to proposed.
- **`origofs_accept`** — Accept one, landing it attributed to its author.
  Refused if this agent is the author.
- **`origofs_reject`** — Reject one without applying it.

### History and authorship

- **`origofs_blame`** — Per-line authorship, human versus agent.
- **`origofs_commit`** — Snapshot the working tree.
- **`origofs_log`** — Commit history.

### Recovery

- **`origofs_trash`** — List deleted files that can still be restored, newest
  first, with who deleted each. When the trash is off it *says so* rather than
  reporting an empty list, because "nothing was deleted" and "nothing is being
  kept" are different answers.
- **`origofs_restore`** — Put a deleted file back. The restore is credited to the
  agent and the original deletion stays in the record.

### Live documents

- **`origofs_live`** — Whether a path has a live co-editing document open — that
  is, whether its stored bytes are the whole truth or a checkpoint that may lag
  what people are typing. Omit `path` to list every live document. Reading a live
  path always works; this only tells the agent how fresh the answer is.
- **`origofs_suggest_coedit`** — Propose a change to a live document as a CRDT
  merge instead of a file body. Requires the `coedit` feature.

!!! tip "Why `origofs_suggest_coedit` exists"

    Prefer it over `origofs_suggest` whenever `origofs_live` says a path is live.
    A byte proposal there goes stale on somebody else's keystroke, and accepting
    it replaces the whole body. A CRDT proposal merges, so a concurrent disjoint
    edit survives — and it is never stale, because a merge is defined for any pair
    of states.

## Keeping the surface honest

A structural test fails on an unclassified MCP tool, so a new ungated one cannot
ship silently, and a second one fails if a tool reads through an unattributed
method. Both take an exempt list where **every entry carries a reason** — because
"exempt" with no reason is how gaps get introduced.

If you add a tool, expect to classify it.
