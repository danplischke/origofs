"""origofs document server — the backend for the React + PlateJS attribution demo.

This is the companion to ``examples/web/app`` (a Vite + React + PlateJS editor).
It shows the *proper* shape of an origofs integration:

  * **origofs owns attribution, the app owns identity.** origofs never trusts a
    client-named actor — every mutating route resolves the caller to an origofs actor
    server-side, via the ``authn`` dependency, and attributes the write to it.
    The React app only ever sends a bearer token; it cannot forge who wrote what.
  * **The whole workspace API in one line.** ``origofs.fastapi.build_router`` mounts
    files, blame, versioning, diff, the agent-suggestion review queue, the change
    feed, and presence under ``/fs``.
  * **A thin app layer on top** (``/api/*``) for the things a UI wants that origofs
    deliberately leaves to you: *who am I* (``/api/me``), an *actor directory*
    that resolves the ``actor_id`` in events/suggestions to a name+kind
    (``/api/actors``), a *combined document load* (text + line blame in one round
    trip, ``/api/doc/{path}``), and a *live SSE feed* of attributed edits
    (``/api/feed``).

Why the app owns the actor directory: origofs embeds the full actor in every *blame*
range, but events, suggestions, and ``resolved_by`` carry only an ``actor_id``.
The app is the component that *created* those actors (it maps your users/agents
onto origofs actors), so it is the natural place to resolve an id back to a name —
origofs stays a storage-and-attribution engine, not a user directory.

Run it
------
    pip install -r requirements.txt          # origofs[fastapi] + uvicorn
    uvicorn app:app --reload                 # http://127.0.0.1:8000

Point it at a real store with env vars: ``ORIGOFS_DSN=postgres://…`` (multi-writer),
or ``ORIGOFS_WORKSPACE=/srv/ws`` (local dir). Default is a throwaway temp workspace.

    !!! DEMO AUTH ONLY. The bearer tokens below are hardcoded so you can try the
    demo with `curl`/the React token picker. Do NOT ship them — replace
    ``resolve_principal`` with your real auth (JWT / session / verified agent
    token). Anyone who knows one of these strings can act as that principal.
"""
from __future__ import annotations

import asyncio
import difflib
import json
import os
import tempfile
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import origofs
from origofs.fastapi import build_router


# --- your identity backend (DEMO) -------------------------------------------
# A bearer token -> the principal it authenticates. In a real app you would
# decode a JWT / look up a session / verify an agent token here. Each principal
# carries a stable ``external_id``; origofs maps that onto an actor the first time we
# see it and reuses it forever after (idempotent, no side table).

PRINCIPALS: dict[str, dict[str, Any]] = {
    "tok-ada":      {"kind": "human", "external_id": "user:ada",      "name": "Ada Lovelace"},
    "tok-grace":    {"kind": "human", "external_id": "user:grace",    "name": "Grace Hopper"},
    "tok-claude":   {"kind": "agent", "external_id": "agent:claude",  "name": "claude", "model": "opus-4"},
}


class Identity:
    """Resolves bearer tokens to origofs actors (idempotently) and creates a session
    on first use. origofs owns the actor *records* — the UI resolves the id-only feeds
    with ``ws.list_actors()`` / ``ws.actor(id)``, so this keeps no directory of its
    own. A real deployment would resolve tokens against your user store here."""

    def __init__(self, ws: "origofs.Workspace") -> None:
        self._ws = ws
        self._ctx: dict[str, origofs.WriteCtx] = {}  # token -> WriteCtx (cached)

    async def onboard_all(self) -> None:
        """Pre-create every demo principal's actor so ``ws.list_actors()`` is
        complete from the first request (before anyone has authenticated)."""
        for token in PRINCIPALS:
            await self._actor_id_for(token)

    async def _actor_id_for(self, token: str) -> int:
        principal = PRINCIPALS.get(token)
        if principal is None:
            raise HTTPException(status_code=401, detail="unknown token")
        if principal["kind"] == "agent":
            return await self._ws.find_or_create_agent(
                principal["external_id"], principal["name"], principal["model"]
            )
        return await self._ws.find_or_create_human(
            principal["external_id"], principal["name"]
        )

    async def ctx_for_token(self, token: str) -> "origofs.WriteCtx":
        """Resolve a token to the :class:`origofs.WriteCtx` its writes attribute to,
        creating the actor (idempotent) and a session on first use."""
        cached = self._ctx.get(token)
        if cached is not None:
            return cached
        actor_id = await self._actor_id_for(token)
        session_id = await self._ws.create_session(actor_id, client="web")
        ctx = origofs.WriteCtx.session(actor_id, session_id)
        self._ctx[token] = ctx
        return ctx


def bearer_token(authorization: Optional[str] = Header(default=None)) -> str:
    """Extract the bearer token from the ``Authorization`` header, or 401."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    return authorization.split(" ", 1)[1].strip()


# --- app wiring -------------------------------------------------------------


async def _open_workspace() -> "origofs.Workspace":
    dsn = os.environ.get("ORIGOFS_DSN")
    if dsn:
        return await origofs.Workspace.open_pg(dsn, os.environ.get("ORIGOFS_CAS", "cas"))
    ws_dir = os.environ.get("ORIGOFS_WORKSPACE")
    if ws_dir is None:
        # Throwaway temp workspace; kept alive on app.state so it isn't GC'd.
        tmp = tempfile.TemporaryDirectory(prefix="origofs-web-")
        _TMP.append(tmp)
        ws_dir = tmp.name
    os.makedirs(ws_dir, exist_ok=True)
    return await origofs.Workspace.open_local(f"{ws_dir}/meta.db", f"{ws_dir}/cas")


_TMP: list[tempfile.TemporaryDirectory] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    ws = await _open_workspace()
    identity = Identity(ws)
    await identity.onboard_all()
    # Only per-run state goes here. The /fs router is wired once at import time
    # (below) against a proxy that reads this — so re-running the lifespan (e.g. a
    # fresh TestClient per test) swaps the workspace without stacking routers.
    app.state.ws = ws
    app.state.identity = identity
    try:
        yield
    finally:
        for tmp in _TMP:
            tmp.cleanup()
        _TMP.clear()


app = FastAPI(title="origofs web — attribution & lineage", lifespan=lifespan)

# The React dev server runs on a different origin (Vite: 5173). For a *dev*
# example we allow it broadly; tighten `allow_origins` for anything real.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- app-level convenience endpoints (/api/*) -------------------------------
# Everything origofs-native lives under /fs (the router). These add the few things a
# UI wants on top: identity, an actor directory, a combined doc load, and a feed.


def _ws(request: Request) -> "origofs.Workspace":
    return request.app.state.ws


async def _authn(request: Request, token: str = Depends(bearer_token)) -> "origofs.WriteCtx":
    """The single auth dependency, used by both the /fs router and /api routes:
    resolve the bearer token to the actor the write is attributed to. Reads the
    workspace/identity from app.state, so a client can't forge attribution and a
    test can swap the workspace per run."""
    return await request.app.state.identity.ctx_for_token(token)


class _WsProxy:
    """Forwards every attribute to the workspace on ``app.state``.

    ``build_router`` binds to a concrete workspace, but ours is opened in the
    lifespan (async). Wiring the router against this proxy lets us mount it once,
    at import time, while the real workspace is created per run — so re-running
    the lifespan swaps the workspace under the same routes instead of stacking a
    second router."""

    def __getattr__(self, name: str) -> Any:
        ws = getattr(app.state, "ws", None)
        if ws is None:
            raise RuntimeError("workspace not initialized (is the app lifespan running?)")
        return getattr(ws, name)


# The full origofs workspace API under /fs, attribution driven by `_authn`. Mounted
# once, at import time, against the proxy — never inside the lifespan.
app.include_router(build_router(_WsProxy(), authn=_authn), prefix="/fs")


@app.get("/api/config")
async def config() -> dict[str, Any]:
    """Non-secret dev config: the demo tokens, so the React token-picker can
    offer a 'sign in as' list. DEMO ONLY — a real app would never ship tokens."""
    return {
        "demo": True,
        "tokens": [
            {"token": t, "name": p["name"], "kind": p["kind"], "external_id": p["external_id"]}
            for t, p in PRINCIPALS.items()
        ],
    }


def _actor_view(a: Optional[dict[str, Any]]) -> dict[str, Any]:
    """The compact actor shape the UI wants, from an origofs actor dict."""
    if a is None:
        return {}
    return {
        "id": a["id"],
        "display_name": a["display_name"],
        "kind": a["kind"],
        "model": a.get("agent_model"),
    }


@app.get("/api/me")
async def me(request: Request, ctx: "origofs.WriteCtx" = Depends(_authn)) -> dict[str, Any]:
    """Who the presented token authenticates as — the actor resolved from origofs by
    id (``ws.actor``), no app-side directory."""
    info = _actor_view(await _ws(request).actor(ctx.actor_id))
    return {
        "actor_id": ctx.actor_id,
        "session_id": ctx.session_id,
        "display_name": info.get("display_name"),
        "kind": info.get("kind"),
        "model": info.get("model"),
    }


@app.get("/api/actors")
async def actors(request: Request) -> list[dict[str, Any]]:
    """Resolve the ``actor_id`` carried by events, suggestions, and ``resolved_by``
    to a name + kind — straight from origofs (``ws.list_actors``), no app-side table.
    (Blame already embeds the full actor; this is for the id-only feeds.)"""
    return [_actor_view(a) for a in await _ws(request).list_actors()]


@app.get("/api/doc/{path:path}")
async def doc(request: Request, path: str) -> dict[str, Any]:
    """Load a document in one round trip: its UTF-8 text **and** its per-line
    blame. Missing file -> ``exists: false`` with empty text/blame, so the editor
    can open a fresh document without a 404 dance."""
    ws = _ws(request)
    p = path if path.startswith("/") else "/" + path
    try:
        raw = await ws.read(p)
    except FileNotFoundError:
        return {"path": p, "exists": False, "text": "", "blame": []}
    # blame() is empty for an unattributed/plain write; that's fine — the UI shows
    # those lines as "unattributed" rather than crediting anyone.
    blame = await ws.blame(p)
    return {
        "path": p,
        "exists": True,
        "text": bytes(raw).decode("utf-8", errors="replace"),
        "blame": blame,
    }


@app.get("/api/feed")
async def feed(request: Request, since: int = 0):
    """A live Server-Sent Events stream of attributed changes — what the UI's
    activity ticker subscribes to. Backed by the change feed: polls ``watch``
    (works on any backend). On Postgres, swap in ``ws.subscribe`` for a true
    LISTEN/NOTIFY push with no polling."""
    ws = _ws(request)

    async def gen():
        cursor = since
        # Prime the stream so a just-connected client learns the current cursor.
        yield f": connected at {cursor}\n\n"
        while not await request.is_disconnected():
            events = await ws.watch(cursor)
            for e in events:
                cursor = max(cursor, e["seq"])
                yield f"data: {json.dumps(e)}\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- inline suggestion review (VSCode-style agent edits) --------------------
# origofs's suggestion queue is propose-then-accept. These endpoints let the UI show
# a pending suggestion *inline* as a line diff and Keep/Discard it per hunk.
#
# Attribution is preserved: "keep all" uses origofs's native accept (atomic, credits
# the agent). A *partial* keep can't go through origofs accept (that applies the whole
# proposal), so the server reconstructs the kept hunks and writes them **as the
# agent** — the server is the trusted identity boundary, so the agent stays
# credited for its lines and the reviewer never is.


def _segments(base: str, proposed: str) -> tuple[list[dict[str, Any]], int]:
    """Line-diff base→proposed as a list of segments. Each changed segment is a
    'hunk' with an index; equal segments carry the shared lines. The frontend
    renders these inline and sends back which hunk indices to keep."""
    b, p = base.splitlines(), proposed.splitlines()
    segs: list[dict[str, Any]] = []
    hunk = 0
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(a=b, b=p, autojunk=False).get_opcodes():
        if tag == "equal":
            segs.append({"tag": "equal", "del": b[i1:i2], "add": b[i1:i2], "hunk": None})
        else:
            segs.append({"tag": tag, "del": b[i1:i2], "add": p[j1:j2], "hunk": hunk})
            hunk += 1
    return segs, hunk


def _reconstruct(segs: list[dict[str, Any]], keep: set[int], base: str, proposed: str) -> str:
    out: list[str] = []
    for s in segs:
        if s["hunk"] is None:
            out.extend(s["del"])            # equal (del == add)
        elif s["hunk"] in keep:
            out.extend(s["add"])            # keep the agent's change
        else:
            out.extend(s["del"])            # discard: fall back to the base
    merged = "\n".join(out)
    if merged and (proposed.endswith("\n") or base.endswith("\n")):
        merged += "\n"
    return merged


class _ApplyReq(BaseModel):
    keep: list[int]  # hunk indices to keep


@app.get("/api/suggestion/{sid}")
async def suggestion_detail(request: Request, sid: int) -> dict[str, Any]:
    """A pending suggestion as an inline line diff. Base + proposed content come
    straight from origofs (``ws.suggestion_content``) — no app-side stash — so this
    works for *any* suggestion, including ones proposed via the raw /fs route."""
    ws = _ws(request)
    sug = await ws.get_suggestion(sid)
    if sug is None:
        raise HTTPException(status_code=404, detail=f"no suggestion #{sid}")
    actor = await ws.actor(sug["actor_id"])
    content = await ws.suggestion_content(sid)
    base, proposed = content["base"], content["proposed"] or ""
    segs, total = _segments(base, proposed)
    return {
        **sug,
        "actor_name": actor["display_name"] if actor else f"actor #{sug['actor_id']}",
        "actor_kind": actor["kind"] if actor else "agent",
        "base_text": base,
        "segments": segs,
        "hunks": total,
    }


@app.post("/api/suggestion/{sid}/apply")
async def apply_suggestion(
    request: Request,
    sid: int,
    req: _ApplyReq,
    ctx: "origofs.WriteCtx" = Depends(_authn),
) -> dict[str, Any]:
    """Keep the chosen hunks. Keep-all → origofs accept (credits the agent, refuses a
    stale base). Partial → the server writes the kept hunks as the agent, then
    resolves the original proposal. Discard-all → reject."""
    ws = _ws(request)
    sug = await ws.get_suggestion(sid)
    if sug is None:
        raise HTTPException(status_code=404, detail=f"no suggestion #{sid}")

    content = await ws.suggestion_content(sid)
    base, proposed = content["base"], content["proposed"] or ""
    segs, total = _segments(base, proposed)
    keep = {i for i in req.keep if 0 <= i < total}

    if total > 0 and len(keep) == total:
        await ws.accept_suggestion(sid, ctx)  # native accept: atomic, credits the agent
        return {"applied": True, "mode": "accept", "kept": len(keep), "total": total}

    merged = _reconstruct(segs, keep, base, proposed)
    if merged != base:
        # Write the kept hunks AS THE AGENT so its lines stay credited to it.
        agent_id = sug["actor_id"]
        agent_session = await ws.create_session(agent_id, client="review-apply")
        agent_ctx = origofs.WriteCtx.session(agent_id, agent_session)
        await ws.write_as(agent_ctx, sug["path"], merged.encode("utf-8"))
    await ws.reject_suggestion(sid, ctx)  # original proposal resolved (superseded)
    return {"applied": True, "mode": "partial", "kept": len(keep), "total": total}


@app.get("/")
async def index() -> dict[str, Any]:
    return {
        "service": "origofs web — attribution & lineage",
        "origofs_api": "/fs (files, blame, commit, log, diff, suggestions, events, presence)",
        "app_api": [
            "/api/config", "/api/me", "/api/actors", "/api/doc/{path}", "/api/feed",
            "/api/suggestion/{id}", "/api/suggestion/{id}/apply",
        ],
        "how": "send Authorization: Bearer <token>; writes are attributed to that principal",
        "frontend": "run the Vite app in ../app and open http://localhost:5173",
    }
