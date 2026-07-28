"""A complete, runnable origofs sample app: a human and an AI agent co-writing a
document, with every change attributed to whoever (or whatever) made it.

This is the example to start from. It shows the whole flow wired together the
way a real service would:

  * **Bring-your-own auth.** A bearer token is resolved to an origofs *actor* — the
    request body never names one, so attribution can't be forged. The token
    store here stands in for your JWT/session backend.
  * **Identity mapping with no side table.** `find_or_create_human` /
    `find_or_create_agent` map your external user/agent ids onto origofs actors
    idempotently, so a token always resolves to the same actor.
  * **The full workspace API** (files, blame, versioning, diff, the agent
    suggestion review queue, presence) mounted in one line via
    `origofs.fastapi.build_router`.
  * **A live SSE feed** built on the change feed, for a collaborative UI.

Two ways to run it
------------------
1. As a real server::

       pip install "origofs[fastapi]" uvicorn      # or: maturin develop && pip install fastapi uvicorn httpx
       uvicorn collab_app:app --reload
       # then drive it over HTTP (see the curl hints printed at "/")

2. As a self-contained demo — no server, no curl, nothing to set up::

       python collab_app.py

   It spins the app up in-process, then plays out the story end to end
   (human writes → agent suggests → reviewer accepts → blame shows both),
   narrating each step. Great for seeing what origofs actually does in ~5 seconds.

By default the workspace is a throwaway temp directory. Point it at a real
store with env vars: ``ORIGOFS_WORKSPACE=/srv/ws`` (local), or ``ORIGOFS_DSN=postgres://…``
for the multi-writer Postgres backend.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

import origofs
from origofs.fastapi import build_router


# --- your identity backend --------------------------------------------------
# Stand-in for whatever you already have: a bearer token -> the principal it
# authenticates. In a real app you'd decode a JWT / look up a session / verify
# an agent token here instead. Each principal carries a stable external id; origofs
# maps that id to an actor the first time we see it and reuses it forever after.
#
# !!! DEMO ONLY — these are hardcoded sample tokens. Do NOT ship them, and do not
# copy this static dict into a real service: replace `_ctx_for_token` with your
# real auth (JWT/session/verified agent token). Anyone who knows one of these
# strings can act as that principal.

PRINCIPALS: dict[str, dict] = {
    "tok-dan":      {"kind": "human", "external_id": "user:dan",     "name": "Dan"},
    "tok-reviewer": {"kind": "human", "external_id": "user:reviewer", "name": "Reviewer"},
    "tok-claude":   {"kind": "agent", "external_id": "agent:claude",  "name": "claude", "model": "opus"},
}

# Cache: token -> the WriteCtx we resolved for it (actor id + a session). Keeps
# us from re-onboarding / re-creating a session on every request.
_ctx_cache: dict[str, origofs.WriteCtx] = {}


async def _ctx_for_token(ws: origofs.Workspace, token: str) -> origofs.WriteCtx:
    """Map a token to the origofs actor its writes are attributed to, creating the
    actor (idempotently) and a session on first use."""
    cached = _ctx_cache.get(token)
    if cached is not None:
        return cached

    principal = PRINCIPALS.get(token)
    if principal is None:
        raise HTTPException(status_code=401, detail="unknown token")

    if principal["kind"] == "agent":
        actor_id = await ws.find_or_create_agent(
            principal["external_id"], principal["name"], principal["model"]
        )
    else:
        actor_id = await ws.find_or_create_human(principal["external_id"], principal["name"])

    session_id = await ws.create_session(actor_id, client="collab_app")
    ctx = origofs.WriteCtx.session(actor_id, session_id)
    _ctx_cache[token] = ctx
    return ctx


async def authn(authorization: Optional[str] = Header(default=None)) -> origofs.WriteCtx:
    """The auth dependency handed to ``build_router``: resolve
    ``Authorization: Bearer <token>`` to the origofs actor its writes are
    attributed to. Every mutating route depends on this, so a client cannot
    forge attribution by naming an actor in the request body.

    It reads the workspace from module state (set on startup) so the same
    dependency works whether we're serving for real or running the in-process
    demo."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    return await _ctx_for_token(_WS, token)


# --- the app ----------------------------------------------------------------

_WS: origofs.Workspace  # set on startup, read by `authn`

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _WS
    dsn = os.environ.get("ORIGOFS_DSN")
    tmp: Optional[tempfile.TemporaryDirectory] = None
    if dsn:
        ws = await origofs.Workspace.open_pg(dsn, os.environ.get("ORIGOFS_CAS", "cas"))
    else:
        ws_dir = os.environ.get("ORIGOFS_WORKSPACE")
        if ws_dir is None:
            tmp = tempfile.TemporaryDirectory(prefix="origofs-collab-")
            ws_dir = tmp.name
        os.makedirs(ws_dir, exist_ok=True)
        ws = await origofs.Workspace.open_local(f"{ws_dir}/meta.db", f"{ws_dir}/cas")

    _WS = ws
    app.state.ws = ws
    _ctx_cache.clear()  # actors are per-workspace; don't reuse across restarts

    # Mount the full workspace API under /fs, attributed via our bearer auth.
    app.include_router(build_router(ws, authn=authn), prefix="/fs")
    try:
        yield
    finally:
        if tmp is not None:
            tmp.cleanup()


app = FastAPI(title="origofs collab", lifespan=lifespan)


@app.get("/")
async def index():
    """A tiny service index — and a reminder that identity is bearer-driven."""
    return {
        "service": "origofs collab",
        "how": "send Authorization: Bearer <token>; the write is attributed to that principal",
        "tokens": {t: p["external_id"] for t, p in PRINCIPALS.items()},
        "try": [
            "curl -H 'Authorization: Bearer tok-dan' -X PUT --data-binary 'hi' localhost:8000/fs/files/notes.md",
            "curl localhost:8000/fs/blame/notes.md",
            "curl -N localhost:8000/feed        # live SSE stream of attributed edits",
        ],
    }


@app.get("/feed")
async def feed(request: Request, since: int = 0):
    """A live Server-Sent Events stream of attributed changes — what a
    collaborative UI subscribes to. Backed by the change feed: this polls
    ``watch`` (works on any backend); on Postgres swap in ``ws.subscribe`` for a
    true LISTEN/NOTIFY push with no polling."""
    ws: origofs.Workspace = request.app.state.ws

    async def gen():
        cursor = since
        while not await request.is_disconnected():
            events = await ws.watch(cursor)
            for e in events:
                cursor = max(cursor, e["seq"])
                yield f"data: {json.dumps(e)}\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Self-contained demo: `python collab_app.py`.
# Spins the app up in-process with a temp workspace and plays out the story.
# ---------------------------------------------------------------------------

def _demo() -> None:
    from fastapi.testclient import TestClient

    def auth(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    def show_blame(blame: list[dict]) -> None:
        for r in blame:
            a = r["actor"]
            span = f"L{r['line_start']}" if r["line_start"] == r["line_end"] else f"L{r['line_start']}-{r['line_end']}"
            print(f"      {span:<8} {a['kind']}:{a['display_name']}")

    path = "/README.md"
    v1 = b"# origofs\nA filesystem that remembers who wrote each line.\n"
    v2 = b"# origofs\nA filesystem that remembers who wrote each line.\nContent-addressed, versioned, attributed.\n"

    with TestClient(app) as client:
        print("\n=== origofs collab — end-to-end demo ===\n")

        print("1. Dan (a human) writes README.md")
        r = client.put(f"/fs/files{path}", content=v1, headers=auth("tok-dan"))
        r.raise_for_status()
        print(f"   -> wrote {r.json()['written']} bytes")
        print("   blame:")
        show_blame(client.get(f"/fs/blame{path}").json())

        print("\n2. An unauthenticated write is refused (attribution can't be forged)")
        r = client.put(f"/fs/files{path}", content=b"sneaky")
        print(f"   -> HTTP {r.status_code} ({r.json()['detail']})")
        assert r.status_code == 401

        print("\n3. claude (an AI agent) *suggests* an edit — the file is untouched")
        r = client.post(
            "/fs/suggestions",
            params={"path": path, "summary": "add a one-line tagline"},
            content=v2,
            headers=auth("tok-claude"),
        )
        r.raise_for_status()
        sid = r.json()["id"]
        print(f"   -> suggestion #{sid} queued")
        pending = client.get("/fs/suggestions", params={"status": "pending"}).json()
        print(f"   pending suggestions: {[s['id'] for s in pending]}")
        current = client.get(f"/fs/files{path}").content
        print(f"   file still says: {current.decode().splitlines()[-1]!r}  (suggestion not applied)")
        assert current == v1

        print("\n   the reviewer inspects the proposed diff:")
        for line in client.get(f"/fs/suggestions/{sid}/diff").text.splitlines():
            print(f"      {line}")

        print("\n4. The reviewer accepts it — now it's applied, credited to the agent")
        r = client.post(f"/fs/suggestions/{sid}/accept", headers=auth("tok-reviewer"))
        r.raise_for_status()
        applied = client.get(f"/fs/files{path}").content
        assert applied == v2
        print("   blame now mixes human and agent, per line:")
        show_blame(client.get(f"/fs/blame{path}").json())

        print("\n5. Snapshot the version and read the history")
        r = client.post("/fs/commit", json={"message": "docs: tagline", "author": "dan"},
                        headers=auth("tok-dan"))
        r.raise_for_status()
        print(f"   -> commit {r.json()['hash'][:12]}")
        for c in client.get("/fs/log").json():
            print(f"      {c['hash'][:12]}  {c['message']}")

        print("\n6. The change feed — every attributed operation, in order")
        for e in client.get("/fs/events").json():
            who = f"actor {e['actor_id']}" if e["actor_id"] is not None else "git-level"
            print(f"      seq {e['seq']:>2}  {who:<11} {e['kind']:<8} {e['path']}")

    print("\n=== demo complete — a human and an agent co-wrote a file, fully attributed ===\n")


if __name__ == "__main__":
    _demo()
