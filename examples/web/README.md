# origofs web — a React + PlateJS editor with lineage & attribution

A full-stack example: a **PlateJS** rich-text editor in **React**, backed by an
origofs workspace through the Python **FastAPI** bindings, where every edit is
attributed to the human or agent that made it — and you can *see* it, per block
and per line, alongside the version history and an agent review queue.

It's the "humans and agents co-write a document, fully attributed" story, wired
end to end:

| | |
|---|---|
| **Attribution** | origofs records per-line **blame** on every attributed write. The editor shows it three ways, all **native Plate**: authored text gets an author-colored underline (a Plate **decoration**), the caret's line names its author (from `useEditorSelection`), and an exact **Blame tab** lists every source line with its author (like `git blame`). |
| **Inline suggestions** | When an agent proposes an edit, review it **in the editor** — VSCode agent-edit style. The proposal is a read-only **Plate** document: unchanged text reads normally, and each change appears in place with word-level **`<ins>` / `<del>`** marks (new green, old struck red) and a **✓ Keep / ✗ Discard** control, plus **Keep all / Discard**. Attribution is preserved (see below): keeping credits the *agent*, never the reviewer. |
| **Lineage** | The **History tab** is origofs's commit DAG — pick a commit to see the unified diff it introduced. The **Suggestions tab** is the full propose-and-review queue across documents. |
| **Live** | Presence (who's here now) and an SSE **activity feed** of every attributed change, straight off origofs's change feed. |
| **Trust** | Identity is resolved **server-side**. The browser sends a bearer token; the server maps it to an origofs actor and attributes the write. The request body never names an actor, so **attribution can't be forged**. |

```
┌─────────────── React + PlateJS (app/) ───────────────┐
│  Edit · Blame · History · Suggestions · Activity      │
└───────────────┬───────────────────────────────────────┘
                │  /fs/*  (origofs router)   /api/*  (app layer)
┌───────────────▼───────────────────────────────────────┐
│  FastAPI doc-server (server/)                          │
│    origofs.fastapi.build_router  +  bearer auth → actor    │
└───────────────┬───────────────────────────────────────┘
                │  origofs Python bindings (write_as, blame, …)
┌───────────────▼───────────────────────────────────────┐
│  origofs workspace  —  content store + metadata (blame)    │
└────────────────────────────────────────────────────────┘
```

## Run it

You need **two** processes: the Python doc-server and the Vite dev server.

### 1. The backend (`server/`)

Build the origofs Python bindings once (they're a compiled pyo3 module), then run the
server:

```bash
cd ../../crates/origofs-py
python -m venv .venv && . .venv/bin/activate
pip install maturin && maturin develop          # builds + installs the `origofs` module
pip install fastapi "uvicorn[standard]"

cd ../../examples/web/server
uvicorn app:app --reload                         # http://127.0.0.1:8000
```

By default it opens a throwaway temp workspace. Point it at a durable one with
`ORIGOFS_WORKSPACE=/srv/ws` (local) or `ORIGOFS_DSN=postgres://…` (multi-writer).

### 2. The frontend (`app/`)

```bash
cd ../app          # examples/web/app
npm install
npm run dev        # http://localhost:5173  (proxies /fs and /api to :8000)
```

Open http://localhost:5173, **sign in** as Ada, Grace, or the `claude` agent
(the picker is seeded from the server's demo tokens), and start writing. Save is
an attributed write; the inline attribution and Blame tab update to credit you.

> The demo tokens (`tok-ada`, …) are **hardcoded for the demo only**. In a real
> app you'd resolve the bearer token to an actor with your own auth (JWT /
> session / verified agent token) — see `resolve_principal` in `server/app.py`.

### Try the whole loop

1. Sign in as **Ada**, write a few paragraphs, **Save**. Her text gets her
   author color inline.
2. Sign in as **claude** (agent), edit, and **Suggest…** instead of Save.
3. Sign in as **Grace**; a banner offers to **Review inline**. Keep/discard the
   agent's changes in the editor, then **Apply**. The Blame tab mixes Ada and
   claude per line — the agent's kept lines are credited to the agent, not the
   reviewer.
4. **Commit snapshot**, then open **History** to see the diff.

## How attribution maps onto a rich-text editor

origofs stores each document as **Markdown** and attributes it **by source line**.
PlateJS edits **blocks**. The example bridges the two honestly:

- **Storage** — the editor serializes to Markdown (`serializeMd`) on save and
  deserializes on load (`deserializeMd`), so origofs keeps human-readable text, a
  meaningful line-based blame, real diffs, and 3-way merge. (Trade-off: content
  is whatever round-trips through Markdown.)
- **Blame tab** — renders the exact source lines with their authors. This is 1:1
  with what origofs stored, so it's the ground truth (`src/editor/BlameView.tsx`).
- **Inline attribution** — a Plate **decoration** underlines authored text in the
  author's color, rendered through Plate's own leaf pipeline (no DOM overlays,
  `src/editor/attributionPlugin.tsx`). Each top-level block's source-line span is
  computed by serializing that block alone, then resolved to its dominant author
  (`src/lib/blame.ts`) — a best-effort projection of the exact line blame onto
  rendered blocks.

Both the attribution and the suggestion review are built entirely through Plate
APIs (decorations, mark/element plugins, `usePluginOption`, transforms) — no DOM
measurement, no separate diff view, no hand-rolled Markdown rendering.

Unattributed content (a plain `write`, not `write_as`) has **no** blame — origofs
returns an empty list, and the UI says so rather than crediting anyone.

## Inline review — keep/discard without losing attribution

The inline suggestion review (VSCode's "review agent edits") has a granularity
mismatch with origofs to solve: origofs's `accept` applies a *whole* proposal atomically
and credits the original author, but VSCode lets you keep/discard **per hunk**.
The example keeps both the per-hunk UX *and* origofs's credit-the-author guarantee:

- **Keep all** → origofs's native `accept_suggestion` — atomic, refuses a stale base
  (409), credits the agent. The fast path.
- **Partial keep** → the server reconstructs just the kept hunks (`base` + chosen
  changes) and writes the result **as the agent** (`write_as` with the agent's
  actor). The server is the trusted identity boundary, so the agent stays
  credited for its lines; the reviewer only ever *chooses* hunks, never authors
  them. The original proposal is then resolved.
- **Discard all** → origofs's `reject_suggestion`.

So a reviewer can accept half of an agent's proposal and blame still shows those
lines as the agent's — which is the whole point of origofs. (Partial keep bypasses
origofs's atomic accept CAS, so it's a deliberate demo trade-off; keep-all is the
CAS-safe path.) See `server/app.py` (`/api/suggestion/{id}/apply`) and
`app/src/review/ReviewOverlay.tsx`.

## Layout

```
server/
  app.py            FastAPI: build_router(/fs) + bearer auth → actor, plus
                    /api/{config,me,actors,doc,feed} + the inline-review endpoints
  test_app.py       end-to-end tests (real workspace): attribution, forge-
                    prevention, the suggestion flow (incl. partial keep), commit/log
  requirements.txt
app/
  src/
    lib/            origofsClient.ts (typed HTTP), types.ts (exact API shapes),
                    blame.ts (line↔block mapping), colors.ts, time.ts
    session.tsx     token → actor, the actor directory, per-actor color
    doc/            useDocument — load text + blame, stale-while-revalidate
    editor/         EditPane (editor + inline review), Editor (Plate),
                    attributionPlugin (native decoration), BlameView, plugins
    review/         ReviewOverlay (read-only Plate review), suggestionPlugins
                    (ins/del marks + change element), buildSuggestionValue
    panels/         HistoryPanel, SuggestionsPanel, ActivityFeed, DiffText
    App.tsx, main.tsx, styles.css
```

## Verifying

```bash
# backend (needs the built `origofs` module + fastapi/httpx/pytest)
cd server && python test_app.py          # or: pytest

# frontend types + production build
cd app && npm run build
```

## What origofs provides vs. what the app provides

origofs is a storage-and-attribution engine, not an auth server — that's deliberately
yours. So the split is:

- **origofs** (`/fs`, via `build_router`, and its bindings): files, per-line blame,
  versioning, diff, the suggestion queue (incl. `suggestion_content` to read a
  proposal's base/proposed text), the change feed, presence, and the actor
  directory (`list_actors` / `actor(id)` resolve the `actor_id` in the id-only
  feeds to a name + kind) — all attribution resolved server-side.
- **the app** (`/api`): just the glue origofs leaves to you — mapping *your*
  users/agents onto origofs actors (bearer token → `find_or_create_*`), a combined
  document load (text + blame in one call), and the SSE feed. No app-side actor
  table and no proposed-text stash: both come straight from origofs.
