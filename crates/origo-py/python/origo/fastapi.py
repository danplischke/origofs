"""A ready-made FastAPI :class:`~fastapi.APIRouter` over an origo workspace.

origo deliberately has no built-in authentication: an attributed write is only as
trustworthy as the identity behind it, and *you* own that. This module gives you
every workspace endpoint (files, blame, versioning, diff, suggestions, the change
feed, presence) wired up, and lets you plug in your own auth as an ordinary
FastAPI dependency that resolves a request to the actor it should be attributed
to.

    from fastapi import FastAPI, Header, HTTPException
    import origo
    from origo.fastapi import build_router

    async def authn(authorization: str = Header(...)) -> origo.WriteCtx:
        actor_id, session_id = await my_auth.resolve(authorization)   # your logic
        if actor_id is None:
            raise HTTPException(401, "unauthenticated")
        return origo.WriteCtx.session(actor_id, session_id)

    app = FastAPI()
    app.include_router(build_router(ws, authn=authn), prefix="/fs")

Every mutating route depends on ``authn`` and attributes the change to the
:class:`~origo.WriteCtx` it returns — so blame and the audit log reflect the
authenticated principal, and a client cannot forge attribution by naming an
actor id in the request. Read routes are open by default; pass ``reader`` (any
dependency) to gate them too.

Requires FastAPI: ``pip install "origofs[fastapi]"``.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional, Union

try:
    from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response
    from fastapi.responses import PlainTextResponse
    from pydantic import BaseModel, Field
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "origo.fastapi requires FastAPI. Install it with: pip install \"origofs[fastapi]\""
    ) from exc

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
    author: str = "origo"


class _Name(BaseModel):
    name: str


class _Touch(BaseModel):
    path: Optional[str] = None


# --- error translation ------------------------------------------------------

_ORIGO_EXC: Optional[tuple] = None


def _origo_exc() -> tuple:
    """(ConflictError, OrigoError), resolved lazily so this module imports without
    the compiled extension present (e.g. for unit tests with a fake workspace)."""
    global _ORIGO_EXC
    if _ORIGO_EXC is None:
        try:
            import origo

            _ORIGO_EXC = (origo.ConflictError, origo.OrigoError)
        except Exception:  # pragma: no cover - only if the native module is absent

            class _Never(Exception):
                pass

            _ORIGO_EXC = (_Never, _Never)
    return _ORIGO_EXC


async def _run(awaitable: Awaitable[Any]) -> Any:
    """Await a workspace call, mapping origo errors to HTTP status codes."""
    conflict_error, origo_error = _origo_exc()
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
    except origo_error as e:
        raise HTTPException(status_code=500, detail=str(e))


def _abs(path: str) -> str:
    return path if path.startswith("/") else "/" + path


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
        An open :class:`origo.Workspace` (or any object with the same async
        methods — handy for testing).
    authn:
        Your authentication dependency. It must resolve the request to the
        :class:`origo.WriteCtx` the change should be attributed to (raise
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

    return router
