# Attribution and blame

Attribution is the point of origofs. This page covers what gets recorded, how to
read it back, and how to undo one actor's work without touching anyone else's.

## What a write records

An attributed write does two things beyond storing bytes:

1. It appends an **edit-op** to a log. That log is the ground truth — append-only,
   never rewritten.
2. It updates the **blame index**, a materialized map of byte ranges to the actor
   and session that wrote them.

Blame is recorded per **byte range**. Line numbers are derived from it for
line-oriented views, which is why blame survives a reformat that shifts every
line number in a file.

## Reading blame

```bash
origofs --workspace "$WS" blame /notes/a.txt
```

```text
   1-40    human:dan
  41-58    agent:claude
  59-72    human:dan
```

Each span names the actor and its kind, so "a person wrote this, an agent wrote
that" is answerable at a glance. From Rust or Python you get the byte offsets and
the session id too, and the actor record comes back inlined — there is no second
lookup to do:

```python
for span in await ws.blame("/notes/a.txt"):
    print(span["byte_start"], span["byte_end"], span["actor"]["name"], span["session"])
```

If the workspace [enforces reads](operating.md#scope-what-an-agent-can-reach),
pass `--actor` so the answer is the one that actor is entitled to.

## Sessions

A session groups one actor's work into an episode — one agent run, one editing
sitting. Every attributed write records the session it happened in.

```bash
DAN=$(origofs --workspace "$WS" actor dan)
```

The CLI creates a session for you per invocation; the SDKs let you create one
explicitly and reuse it across many writes, which is what a long-running agent
should do:

```python
ctx = origofs.WriteCtx.session(agent, await ws.create_session(agent, "nightly-run"))
```

## Undo one session

This is the operation a version-control system cannot give you. `revert-session`
walks every file one actor touched in one session and removes exactly the lines
that actor authored — leaving concurrent edits by everyone else in place.

```bash
origofs --workspace "$WS" revert-session \
    --actor "$AGENT" --session "$SESSION" --by "$DAN"
```

- `--actor` is whose work is being undone.
- `--session` is which episode.
- `--by` is who is performing the revert, and it is checked against the write
  policy — an undo is itself a write.

!!! tip "Finding a session id"

    The CLI does not print session ids — every attributed CLI command opens its
    own session labelled `cli`, so there is rarely one long-lived id to reach
    for. Where you need one, read it off the blame spans, which carry
    `session` alongside each range:

    ```python
    {span["session"] for span in await ws.blame("/src/main.rs")}
    ```

    A long-running agent should create one session up front and reuse it, which
    makes its whole run revertible in a single call.

Bound it to a subtree with `--path-prefix`:

```bash
origofs --workspace "$WS" revert-session \
    --actor "$AGENT" --session "$SESSION" --by "$DAN" --path-prefix /src
```

The prefix matches on **directory boundaries**: `/tenant-a` covers
`/tenant-a/notes.txt` and never `/tenant-abc/...`. Omit it and the revert reaches
everywhere that session wrote — which is why an unbounded revert is checked at
the workspace root rather than at any one path.

## Requiring attribution

An unattributed write is a real write that records nothing. To make that an error
instead of a silent gap:

```bash
origofs --workspace "$WS" require-attribution on
origofs --workspace "$WS" require-attribution        # print the current setting
```

Every mutating command must then name an actor, via `--actor` or
`ORIGOFS_ACTOR`. The setting is **off** by default.

!!! note "This is completeness, not access control"

    An actor id on a command line is asserted by whoever writes the command line,
    and a process that can reach the workspace directory can reach `meta.db`
    directly. Identity is *verified* only where something resolves it
    server-side — see [the HTTP API](../reference/http-api.md).

## The change feed

Every change lands on an ordered feed, so "what happened while I was away" is a
cursor query rather than a diff:

```bash
origofs --workspace "$WS" watch --since 0     # replay from the beginning
origofs --workspace "$WS" watch --follow      # tail it
```

```text
1	mkdir	-	/notes
2	write	-	/notes/a.txt
3	write	actor:1	/notes/a.txt
4	commit	actor:1	/  (first)
```

The seq in the first column is the cursor: pass it back as `--since` to resume
exactly where you stopped. A `-` in the actor column is an unattributed change.

Over HTTP that is `GET /v1/events?since=N`. On Postgres, clients can be pushed to
over `LISTEN`/`NOTIFY` instead of polling — see
[Running for a team](teams.md#the-change-feed).

## The audit log

Separately from blame, origofs records an audit log of operations. Blame answers
"who wrote this byte"; the audit log answers "what was attempted". Both live in
the metadata database, and neither can be rebuilt from the content store — see
[Backup and recovery](backup-and-recovery.md).
