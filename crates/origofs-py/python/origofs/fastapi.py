"""A ready-made FastAPI :class:`~fastapi.APIRouter` over an origofs workspace.

origofs deliberately has no built-in authentication: an attributed write is only as
trustworthy as the identity behind it, and *you* own that. This module gives you
every workspace endpoint (files, blame, versioning, diff, suggestions, the change
feed, presence) wired up — plus a live co-editing WebSocket (``/coedit/{path}``)
that speaks the Yjs y-sync protocol, so an unmodified editor (PlateJS,
``y-websocket``) collaborates in real time — and lets you plug in your own auth as
an ordinary FastAPI dependency that resolves a request to the actor it should be
attributed to.

    from fastapi import FastAPI, Header, HTTPException
    import origofs
    from origofs.fastapi import build_router

    async def authn(authorization: str = Header(...)) -> origofs.WriteCtx:
        actor_id, session_id = await my_auth.resolve(authorization)   # your logic
        if actor_id is None:
            raise HTTPException(401, "unauthenticated")
        return origofs.WriteCtx.session(actor_id, session_id)

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn), prefix="/fs")

Every mutating route depends on ``authn`` and attributes the change to the
:class:`~origofs.WriteCtx` it returns — so blame and the audit log reflect the
authenticated principal, and a client cannot forge attribution by naming an
actor id in the request. Read routes are open by default; pass ``reader`` (any
dependency) to gate them too.

Requires FastAPI: ``pip install "origofs[fastapi]"``.
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, Union

try:
    from fastapi import (
        APIRouter,
        Body,
        Depends,
        HTTPException,
        Query,
        Response,
        WebSocket,
        WebSocketDisconnect,
    )
    from fastapi.responses import PlainTextResponse
    from pydantic import BaseModel, Field
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "origofs.fastapi requires FastAPI. Install it with: pip install \"origofs[fastapi]\""
    ) from exc

if TYPE_CHECKING:  # import only for type checkers; the module loads without the ext
    import origofs

# A dependency that resolves a request to the WriteCtx a change is attributed to.
# It may be sync or async and may declare its own FastAPI dependencies/params.
AuthnDep = Callable[..., Union["Any", Awaitable["Any"]]]

__all__ = ["build_router"]


# --- request bodies ---------------------------------------------------------


class _Rename(BaseModel):
    from_: str = Field(..., alias="from")
    to: str

    model_config = {"populate_by_name": True}


class _Commit(BaseModel):
    message: str
    # Git-level commit author (free text). Distinct from per-line blame, which is
    # driven by the authenticated actor on each write.
    author: str = "origofs"


class _Name(BaseModel):
    name: str


class _Touch(BaseModel):
    path: Optional[str] = None


# --- error translation ------------------------------------------------------

_ORIGOFS_EXC: Optional[tuple] = None


def _origofs_exc() -> tuple:
    """(ConflictError, OrigoFSError), resolved lazily so this module imports without
    the compiled extension present (e.g. for unit tests with a fake workspace)."""
    global _ORIGOFS_EXC
    if _ORIGOFS_EXC is None:
        try:
            import origofs

            _ORIGOFS_EXC = (origofs.ConflictError, origofs.OrigoFSError)
        except Exception:  # pragma: no cover - only if the native module is absent

            class _Never(Exception):
                pass

            _ORIGOFS_EXC = (_Never, _Never)
    return _ORIGOFS_EXC


async def _run(awaitable: Awaitable[Any]) -> Any:
    """Await a workspace call, mapping origofs errors to HTTP status codes."""
    conflict_error, origofs_error = _origofs_exc()
    try:
        return await awaitable
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e) or "not found")
    except FileExistsError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except IsADirectoryError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except NotADirectoryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except conflict_error as e:  # stale base on a suggestion accept
        raise HTTPException(status_code=409, detail=str(e))
    except origofs_error as e:
        raise HTTPException(status_code=500, detail=str(e))


def _abs(path: str) -> str:
    return path if path.startswith("/") else "/" + path


# --- live co-editing rooms --------------------------------------------------


class _Conn:
    """One connected socket and its **own** outbound queue. Every byte written to
    a socket goes through its queue and single writer task, so a peer's fan-out
    never races the socket's own replies."""

    __slots__ = ("socket", "out")

    def __init__(self, socket: WebSocket) -> None:
        self.socket: WebSocket = socket
        self.out: "asyncio.Queue[bytes]" = asyncio.Queue()


class _Room:
    """One live document shared by every socket editing it: the attributed CRDT
    plus the set of connected sockets to fan edits out to."""

    __slots__ = ("doc", "conns")

    def __init__(self, doc: "origofs.CoeditDoc") -> None:
        self.doc: "origofs.CoeditDoc" = doc
        self.conns: set[_Conn] = set()

    def fanout(self, sender: _Conn, frame: bytes) -> None:
        """Queue `frame` for every connection except the one it came from."""
        for conn in self.conns:
            if conn is not sender:
                conn.out.put_nowait(frame)


async def _finalize(writer_task: "asyncio.Task[None]", rooms: "_Rooms", path: str, ctx: Any, conn: "_Conn") -> None:
    """Tear a connection down: stop its writer and leave the room (which
    checkpoints on the last leave). Run this under :func:`asyncio.shield` — the
    socket closing tends to cancel the endpoint task, and the checkpoint must still
    complete."""
    writer_task.cancel()
    try:
        await writer_task
    except BaseException:  # noqa: BLE001 - already-cancelled writer; nothing to do
        pass
    await rooms.leave(path, ctx, conn)


class _Rooms:
    """Per-process registry of live co-editing rooms, keyed by path.

    A room is created on the first join (opening the document) and, when the last
    socket leaves, checkpointed into the byte-range blame index and evicted. This
    is the shared, long-lived state that makes co-editing efficient: all sockets on
    one path share one CRDT, instead of each request opening its own view.

    The registry is per **process**. Under a single worker that is the whole story.
    Across multiple workers it is not shared — pin a document to one worker (sticky
    routing by path) or run the co-editing endpoint as its own single-process
    service, exactly as an in-memory ``y-websocket`` server would. State stays
    durable and consistent regardless, because every checkpoint lands through the
    shared workspace.
    """

    def __init__(self, ws: Any) -> None:
        self._ws = ws
        self._rooms: dict[str, _Room] = {}
        self._lock = asyncio.Lock()

    async def join(self, path: str, ctx: Any, conn: _Conn) -> _Room:
        async with self._lock:
            room = self._rooms.get(path)
            if room is None:
                doc = await self._ws.open_coedit(ctx, path)
                room = _Room(doc)
                self._rooms[path] = room
            room.conns.add(conn)
            return room

    async def leave(self, path: str, ctx: Any, conn: _Conn) -> None:
        async with self._lock:
            room = self._rooms.get(path)
            if room is None:
                return
            room.conns.discard(conn)
            if not room.conns:
                # Final checkpoint under the registry lock so a concurrent join
                # can't fork a fresh room off a half-written sidecar.
                await self._ws.checkpoint_coedit(ctx, path, room.doc)
                del self._rooms[path]


# --- router factory ---------------------------------------------------------


def build_router(
    ws: Any,
    *,
    authn: AuthnDep,
    reader: Optional[AuthnDep] = None,
    **router_kwargs: Any,
) -> "APIRouter":
    """Build an :class:`~fastapi.APIRouter` serving ``ws``.

    Parameters
    ----------
    ws:
        An open :class:`origofs.Workspace` (or any object with the same async
        methods — handy for testing).
    authn:
        Your authentication dependency. It must resolve the request to the
        :class:`origofs.WriteCtx` the change should be attributed to (raise
        ``fastapi.HTTPException`` to reject). It is applied to every mutating
        route, and its return value is passed straight to the attributed
        workspace call — the request body never names the actor, so attribution
        can't be forged. May be sync or async and may declare its own
        dependencies (headers, cookies, a JWT-decode dependency, …).
    reader:
        Optional dependency gating read-only routes. Its return value is
        ignored; raise to reject. Omit to leave reads open.
    **router_kwargs:
        Forwarded to :class:`~fastapi.APIRouter` (``prefix``, ``tags``,
        router-wide ``dependencies=[...]``, …).
    """
    router = APIRouter(**router_kwargs)

    # Shared, long-lived co-editing rooms — created once here, not per request.
    rooms = _Rooms(ws)

    # Read-route gate: a dependency whose value we don't use. When no `reader`
    # is given, a no-op keeps the signature uniform.
    if reader is None:
        async def _read_gate() -> None:
            return None
    else:
        _read_gate = reader  # type: ignore[assignment]

    # --- files --------------------------------------------------------------

    @router.get("/files/{path:path}", dependencies=[Depends(_read_gate)])
    async def read_file(path: str) -> Response:
        data = await _run(ws.read(_abs(path)))
        return Response(content=bytes(data), media_type="application/octet-stream")

    @router.put("/files/{path:path}")
    async def write_file(path: str, body: bytes = Body(default=b""), ctx: Any = Depends(authn)):
        p = _abs(path)
        parent, _, _ = p.rpartition("/")
        if parent:  # create intermediate dirs, like the Rust HTTP API does
            await _run(ws.mkdir_p(parent))
        await _run(ws.write_as(ctx, p, body))
        return {"path": p, "written": len(body)}

    @router.delete("/files/{path:path}")
    async def remove_file(path: str, _ctx: Any = Depends(authn)):
        await _run(ws.remove(_abs(path)))
        return {"removed": _abs(path)}

    # --- directories --------------------------------------------------------

    @router.get("/dirs/{path:path}", dependencies=[Depends(_read_gate)])
    async def list_dir(path: str):
        return await _run(ws.ls(_abs(path)))

    @router.post("/dirs/{path:path}")
    async def make_dir(path: str, _ctx: Any = Depends(authn)):
        await _run(ws.mkdir_p(_abs(path)))
        return {"created": _abs(path)}

    @router.get("/stat/{path:path}", dependencies=[Depends(_read_gate)])
    async def stat(path: str):
        return await _run(ws.stat(_abs(path)))

    @router.post("/rename")
    async def rename(req: _Rename, _ctx: Any = Depends(authn)):
        await _run(ws.rename(_abs(req.from_), _abs(req.to)))
        return {"from": _abs(req.from_), "to": _abs(req.to)}

    # --- attribution --------------------------------------------------------

    @router.get("/blame/{path:path}", dependencies=[Depends(_read_gate)])
    async def blame(path: str):
        return await _run(ws.blame(_abs(path)))

    # --- versioning ---------------------------------------------------------

    @router.post("/commit")
    async def commit(req: _Commit, _ctx: Any = Depends(authn)):
        return {"hash": await _run(ws.commit(req.author, req.message))}

    @router.get("/log", dependencies=[Depends(_read_gate)])
    async def log():
        return await _run(ws.log())

    @router.get("/status", dependencies=[Depends(_read_gate)])
    async def status():
        return await _run(ws.status())

    @router.get("/diff", dependencies=[Depends(_read_gate)])
    async def diff(from_: str = Query(..., alias="from"), to: str = Query(...)):
        return await _run(ws.diff(from_, to))

    @router.get("/diff/file", response_class=PlainTextResponse, dependencies=[Depends(_read_gate)])
    async def diff_file(
        path: str = Query(...),
        from_: str = Query(..., alias="from"),
        to: str = Query(...),
    ):
        return await _run(ws.diff_file(from_, to, _abs(path)))

    @router.get("/branches", dependencies=[Depends(_read_gate)])
    async def branches():
        return await _run(ws.branches())

    @router.post("/branches")
    async def create_branch(req: _Name, _ctx: Any = Depends(authn)):
        await _run(ws.create_branch(req.name))
        return {"branch": req.name}

    @router.post("/checkout")
    async def checkout(req: _Name, _ctx: Any = Depends(authn)):
        await _run(ws.checkout(req.name))
        return {"branch": req.name}

    # --- agent-suggestion review queue --------------------------------------

    @router.post("/suggestions")
    async def suggest(
        path: str = Query(...),
        body: bytes = Body(default=b""),
        summary: Optional[str] = Query(default=None),
        ctx: Any = Depends(authn),
    ):
        return {"id": await _run(ws.suggest(ctx, _abs(path), body, summary))}

    @router.get("/suggestions", dependencies=[Depends(_read_gate)])
    async def list_suggestions(
        status: Optional[str] = Query(default=None),
        path: Optional[str] = Query(default=None),
    ):
        return await _run(ws.list_suggestions(status, path))

    @router.get("/suggestions/{sid}/diff", response_class=PlainTextResponse,
                dependencies=[Depends(_read_gate)])
    async def suggestion_diff(sid: int):
        return await _run(ws.suggestion_diff(sid))

    @router.post("/suggestions/{sid}/accept")
    async def accept(sid: int, ctx: Any = Depends(authn)):
        await _run(ws.accept_suggestion(sid, ctx))
        return {"accepted": sid}

    @router.post("/suggestions/{sid}/reject")
    async def reject(sid: int, ctx: Any = Depends(authn)):
        await _run(ws.reject_suggestion(sid, ctx))
        return {"rejected": sid}

    # --- live collaboration -------------------------------------------------

    @router.get("/events", dependencies=[Depends(_read_gate)])
    async def events(since: int = Query(default=0)):
        return await _run(ws.watch(since))

    @router.get("/presence", dependencies=[Depends(_read_gate)])
    async def presence(window: int = Query(default=60)):
        return await _run(ws.presence(window))

    @router.post("/presence/touch")
    async def touch(req: _Touch, ctx: Any = Depends(authn)):
        if ctx.session_id is None:
            raise HTTPException(status_code=400, detail="presence requires a session (WriteCtx.session)")
        await _run(ws.touch(ctx.actor_id, ctx.session_id, _abs(req.path) if req.path else None))
        return {"ok": True}

    # --- live co-editing (Yjs y-sync) ---------------------------------------

    @router.websocket("/coedit/{path:path}")
    async def coedit(websocket: WebSocket, path: str, ctx: Any = Depends(authn)) -> None:
        """Live co-editing over the Yjs y-sync binary protocol.

        Authentication reuses ``authn`` — the same dependency as every mutating
        route — so it resolves the socket to the actor its edits are attributed to.
        Since browsers can't set headers on a WebSocket, have ``authn`` read a
        ``?token=`` query param (it can, like any FastAPI dependency); content is
        attributed server-side regardless of what the client's bytes claim.
        """
        p = _abs(path)
        await websocket.accept()
        conn = _Conn(websocket)
        room = await rooms.join(p, ctx, conn)

        async def writer() -> None:
            while True:
                await websocket.send_bytes(await conn.out.get())

        # Greet the client with SyncStep1, then let the writer own all sends.
        conn.out.put_nowait(await room.doc.sync_start())
        writer_task = asyncio.create_task(writer())
        try:
            while True:
                data = await websocket.receive_bytes()
                reply = await room.doc.handle_sync(ctx, data)
                if reply.reply:
                    conn.out.put_nowait(reply.reply)
                if reply.broadcast:
                    room.fanout(conn, reply.broadcast)
        except WebSocketDisconnect:
            pass
        finally:
            # Shielded so the last-leave checkpoint completes even though closing
            # the socket cancels this endpoint task.
            try:
                await asyncio.shield(_finalize(writer_task, rooms, p, ctx, conn))
            except asyncio.CancelledError:
                pass

    return router
