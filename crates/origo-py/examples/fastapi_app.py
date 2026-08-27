"""Example: driving an origo workspace from FastAPI.

The point of the bindings is that YOU own the endpoints and resolve identity
however you like (JWT, session cookie, an agent token, ...), then attribute the
write to that user/agent via `WriteCtx`. origo records blame + an edit-op audit
trail for every attributed write.

Run:
    pip install "origofs[fastapi]"        # or: maturin develop && pip install fastapi uvicorn
    uvicorn fastapi_app:app --reload
"""
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

import origo

WS: origo.Workspace  # set on startup


@asynccontextmanager
async def lifespan(app: FastAPI):
    global WS
    # Local dev; swap for origo.Workspace.open_pg(dsn, cas_dir) for multi-writer.
    WS = await origo.Workspace.open_local("meta.db", "cas")
    yield


app = FastAPI(lifespan=lifespan)


# --- identity injection -----------------------------------------------------
# Replace this with your real auth. Resolve the request's principal to an origo
# actor id (+ optional session), then hand back a WriteCtx.

async def current_ctx(
    x_actor_id: int,          # e.g. decoded from a JWT / session
    x_session_id: Optional[int] = None,
) -> origo.WriteCtx:
    if x_session_id is not None:
        return origo.WriteCtx.session(x_actor_id, x_session_id)
    return origo.WriteCtx.actor(x_actor_id)


# --- file endpoints ---------------------------------------------------------

@app.get("/files/{path:path}")
async def read_file(path: str):
    try:
        data = await WS.read("/" + path)
    except FileNotFoundError:
        raise HTTPException(404, "not found")
    return Response(content=bytes(data), media_type="application/octet-stream")


@app.put("/files/{path:path}")
async def write_file(path: str, body: bytes, ctx: origo.WriteCtx = Depends(current_ctx)):
    # The write is attributed to the authenticated actor/session -> blame + audit.
    await WS.write_as(ctx, "/" + path, body)
    return {"path": "/" + path, "written": len(body)}


@app.get("/blame/{path:path}")
async def blame(path: str):
    return await WS.blame("/" + path)


# --- versioning + diff (for a review UI) ------------------------------------

class CommitReq(BaseModel):
    author: str
    message: str


@app.post("/commit")
async def commit(req: CommitReq):
    return {"hash": await WS.commit(req.author, req.message)}


@app.get("/diff")
async def diff(from_: str, to: str):
    return await WS.diff(from_, to)


@app.get("/diff/file", response_class=PlainTextResponse)
async def diff_file(from_: str, to: str, path: str):
    return await WS.diff_file(from_, to, path)


# --- agent-suggestion review queue ------------------------------------------

@app.post("/suggestions")
async def suggest(path: str, body: bytes, summary: Optional[str] = None,
                  ctx: origo.WriteCtx = Depends(current_ctx)):
    # An agent proposes an edit; the working tree is untouched until accepted.
    return {"id": await WS.suggest(ctx, path, body, summary)}


@app.get("/suggestions")
async def list_suggestions(status: Optional[str] = None):
    return await WS.list_suggestions(status)


@app.get("/suggestions/{sid}/diff", response_class=PlainTextResponse)
async def suggestion_diff(sid: int):
    return await WS.suggestion_diff(sid)


@app.post("/suggestions/{sid}/accept")
async def accept(sid: int, ctx: origo.WriteCtx = Depends(current_ctx)):
    try:
        await WS.accept_suggestion(sid, ctx)  # applied, credited to the agent
    except origo.ConflictError as e:
        raise HTTPException(409, str(e))       # stale base -> re-diff / re-suggest
    return {"accepted": sid}


# --- live feed + presence ---------------------------------------------------

@app.get("/events")
async def events(since: int = 0):
    return await WS.watch(since)


@app.get("/presence")
async def presence(window: int = 60):
    return await WS.presence(window)


# --- mount orchestration (optional) -----------------------------------------
# You can expose a workspace as a real filesystem or over NFS, controlled from
# Python. Example: mount on startup, unmount on shutdown.
#
#   mount = WS.mount("/mnt/origo")          # FUSE, returns a handle
#   ...
#   mount.unmount()
#
#   import asyncio
#   nfs = asyncio.create_task(WS.serve_nfs("127.0.0.1:11111"))   # runs until cancelled
#   ...
#   nfs.cancel()
