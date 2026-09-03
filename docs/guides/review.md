# Propose and review

An actor can submit an edit for review instead of applying it. The proposed bytes
go straight into the content store — deduplicated and diffable — but the working
tree does not change until someone accepts.

The queue is **actor-agnostic**. It is a change-request workflow between people
just as much as an agent-proposal one.

## The loop

```bash
echo "patched" | origofs --workspace "$WS" suggest /main.rs \
    --actor "$AGENT" --summary "fix the off-by-one"

origofs --workspace "$WS" suggestions --status pending
origofs --workspace "$WS" suggestion-diff 1        # base → proposed, unified
origofs --workspace "$WS" accept 1 --actor "$DAN"  # applies it
origofs --workspace "$WS" reject 2 --actor "$DAN"  # discards it
```

Proposing a deletion instead of a body:

```bash
origofs --workspace "$WS" suggest /obsolete.rs --actor "$AGENT" --delete
```

## What acceptance guarantees

Three rules, all enforced in the engine rather than per surface:

- **The edit lands attributed to its original author**, and the approver is
  recorded separately. Blame stays honest — accepting an agent's work does not
  turn it into yours.
- **Nobody accepts their own proposal.** The approver must differ from the
  author.
- **A stale base is refused.** If the file moved since the proposal was made,
  `accept` refuses rather than clobbering the change it never saw, and retires
  that proposal as `superseded` instead of leaving it pending forever. A
  *successful* accept does the same to the other pending proposals on that path,
  because it just moved their base too.

## Two kinds of proposal

"Stale" means different things depending on what is being proposed.

| Kind | Base | The proposal | Accepting it |
|---|---|---|---|
| `bytes` (default) | the file's content hash | a whole file body | a conditional whole-file write — refused, and superseded, if the file moved |
| `crdt` | the document's CRDT state vector | an opaque update blob | a merge |

A CRDT merge is defined for *any* pair of states, so a `crdt` proposal against a
[live co-edited document](teams.md#live-co-editing) is never stale: a colleague's
concurrent edit elsewhere in the file neither invalidates it nor gets clobbered by
it. Those proposals are therefore never swept as superseded.

```rust
let doc = ws.open_coedit(ctx, "/notes.md").await?;
doc.insert(ctx, 0, "a suggestion");
let id = ws.suggest_coedit(ctx, "/notes.md", &doc, Some("reword the intro")).await?;
```

Rich-text documents (the structured `XmlFragment` shape a browser editor binds to)
have their own pair — `suggest_coedit_tree` to propose and
`accept_coedit_tree_suggestion` to land one. They are a separate kind because
acceptance genuinely differs: landing a flat proposal is a merge plus a
serialization origofs can do itself, while a structured document has to be written
back out as bytes and only the host application knows the schema. `accept`
therefore *refuses* a tree proposal and names the call that handles it, rather
than applying a tree update to a flat document and producing a file nobody can
read.

## Making review mandatory

Whether an actor *must* propose is its **write policy** — a bounded trust gate
that is a property of the actor, not of its kind.

```bash
origofs --workspace "$WS" write-policy "$AGENT" propose   # writes go to the queue
origofs --workspace "$WS" write-policy "$AGENT" direct    # writes land (the default)
```

A `propose`-only actor's writes are routed into the queue **on every surface** —
CLI, MCP, the HTTP API, a mount. There is no surface that bypasses it, because
the routing happens in the engine.

The gate applies to any mutation that names an actor. `write`, `rm`, `mv`,
`mkdir` and `commit` all take `--actor` (or read `ORIGOFS_ACTOR`) and route
through the same check. Operations with a propose-shaped equivalent queue;
operations without one refuse.

## Policy and ACLs are different questions

They compose, and confusing them is the usual source of a surprising refusal.

- The **write policy** asks *must this land through review?* It is a property of
  the actor and applies everywhere.
- An **[ACL grant](operating.md#scope-what-an-agent-can-reach)** asks *may this
  actor touch this path at all?* `PROPOSE` is its own permission bit, so an actor
  can hold the right to propose under `/src` without the right to write there.

A `WRITE` grant implies `PROPOSE`. An actor with neither cannot reach the queue —
which is deliberate: without that check, calling `suggest` directly would queue a
proposal for an actor denied both rights.

To see what an actor may actually do somewhere:

```bash
origofs --workspace "$WS" acl check "$AGENT" /src/main.rs
```

## Over MCP

An agent gets the whole loop as tools — `origofs_suggest`,
`origofs_suggestion_diff`, `origofs_suggestions`, `origofs_accept`,
`origofs_reject` — under the same server-side attribution and policy enforcement,
and it cannot accept its own proposals. See the [MCP
reference](../reference/mcp.md).
