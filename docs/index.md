# origofs

A filesystem where humans and AI agents share the same files, and **every edit is
recorded against the actor that made it**.

origofs is not a wrapper over `git` and not a VFS shim. It is a storage engine
with four properties at its core — content-addressed storage, a pluggable
metadata database, opt-in Git-style versioning, and per-actor, per-byte-range
edit attribution — reachable through a CLI, a Rust SDK, Python bindings, an
HTTP/JSON API, an MCP server, and POSIX mounts.

## The problem it solves

An agent that edits your files leaves you with a directory and no way to answer
basic questions about it. A `git diff` tells you a line changed; it does not tell
you that the agent wrote it, in which session, under whose instruction, or how to
take back just that agent's work without touching anyone else's.

Six questions a directory of files can't answer:

| Question | origofs's answer |
|---|---|
| Who wrote this line — a person or an agent? | [`origofs blame`](guides/attribution.md) reports it per line, along with the session behind the write. |
| Can I undo just the agent's work? | [`revert-session`](guides/attribution.md#undo-one-session) removes exactly the lines one actor authored in one session. Everyone else's edits stay. |
| Can I review before it lands? | Agents can [propose](guides/review.md) edits into a review queue; a human accepts (credited to the agent) or rejects. |
| Can people and agents edit together, live? | Opt-in [CRDT co-editing](guides/teams.md#live-co-editing): humans, agents and browser editors converge on one document, still attributed. |
| Will it hold up for a team? | [Postgres](guides/teams.md) backs many concurrent writers on one workspace, with a live change feed and presence. |
| Can I trust what I read back? | Content is verified against its hash on every read, so bit-rot or tampering is an error — never silently served. |

## Where to start

<div class="grid cards" markdown>

-   :material-download: **[Install](getting-started/install.md)**

    Build the CLI, or `pip install origofs` for the Python bindings.

-   :material-rocket-launch: **[Quickstart](getting-started/quickstart.md)**

    A workspace, an attributed write, and a blame report — in about ten commands.

-   :material-lightbulb: **[Core concepts](getting-started/concepts.md)**

    Actors, sessions, the working tree, and why metadata and content are split.

-   :material-robot: **[Working with agents](guides/agents.md)**

    Mounts, MCP, and sandboxes — the three ways to put an agent to work.

</div>

## A workspace is not a directory

The paths you use (`/notes/a.txt`) live *inside* a workspace, not on your disk. A
workspace is a metadata database next to a content store — locally, a `meta.db`
file and a `cas/` directory; for a team, Postgres and an object store. You never
edit either by hand.

That indirection is what buys you the table above. It is also the main thing to
get used to: to see a workspace as ordinary files, [mount
it](guides/mounts.md).

## Status

origofs is under active development and the design doc's milestone roadmap
(M0–M9) is the authoritative statement of what is built. Read
[Limits](LIMITS.md) before putting real data in it, and
[Design](DESIGN.md) for why the system is shaped the way it is.
