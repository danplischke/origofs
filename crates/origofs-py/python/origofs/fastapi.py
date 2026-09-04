"""A ready-made FastAPI :class:`~fastapi.APIRouter` over an origofs workspace.

origofs deliberately has no built-in authentication: an attributed write is only as
trustworthy as the identity behind it, and *you* own that. This module gives you
every workspace endpoint (files, blame, versioning, diff, suggestions incl.
propose-a-deletion, actors/sessions, the change feed, presence) wired up — plus a
live co-editing WebSocket (``/coedit/{path}``) that speaks the Yjs y-sync
protocol, so an unmodified editor (PlateJS, ``y-websocket``) collaborates in real
time — and lets you plug in your own auth as an ordinary FastAPI dependency that
resolves a request to the actor it should be attributed to. File reads stream
(never buffering a whole file in memory) and honor a single-range ``Range``
request header (``206``/``416``), so large files and partial fetches (seeking,
resumable downloads) work the same as any static file server.

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
dependency) to gate them too — and if it returns a ``WriteCtx``, reads run *as*
that actor, so a workspace with ``acl_enforce_reads`` on applies ``Perms.READ``
to what comes back.

Attribution is not decoration: every mutating route calls an **attributed**
workspace method, so the caller's :class:`~origofs.WritePolicy` governs it in the
engine. A propose-only actor's write or delete is queued as a suggestion for
review (``{"path": …, "proposed": <id>}``); everything else it may not do
directly — rename, mkdir, commit, branch, checkout, registering actors — is
refused with ``403``. Namespace mutations carry an actor too, so "who deleted
this file" has an answer.

For **many tenants in one workspace**, pass ``root=`` — a fixed path, or a
dependency that resolves one from the request. Every caller-supplied path then
resolves under it, the listing routes are filtered to it, and the operations no
filter can narrow (commit, branches, checkout, the commit log) are refused
rather than acting workspace-wide. See :func:`build_router`.

Requires FastAPI: ``pip install "origofs[fastapi]"``.
"""
from __future__ import annotations

import asyncio
import contextlib
import mimetypes
import os
import tempfile
import time
import uuid
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, Optional, Union

try:
    from fastapi import (
        APIRouter,
        Body,
        Depends,
        Header,
        HTTPException,
        Query,
        Response,
        WebSocket,
        WebSocketDisconnect,
    )
    from fastapi import Request
    from fastapi.responses import PlainTextResponse, StreamingResponse
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

__all__ = ["build_router", "build_coedit_router", "CheckpointPolicy", "ROUTE_GROUPS"]


# --- route groups (issue #153) ----------------------------------------------
#
# `build_router` mounted origofs's entire REST surface or nothing. For a host that
# stores bodies in origofs but owns the access model, that is the wrong shape: it
# wants the co-editing socket and routes every mutation through its own authorized
# endpoints. With one router there was no supported way to say so, so integrators
# reached into `router.routes` and re-registered the one route they wanted --
# coupling to a path string and a route class that a version bump can change.
#
# Each route belongs to exactly one group, named for the capability rather than
# the URL, because a caller is choosing what the surface can *do*.
#
# A websocket is keyed with `None` for its method: it has none, and pairing the
# path with a method that does not exist would make the two kinds of route look
# comparable when they are not.
_ROUTE_GROUPS: "dict[str, tuple[tuple[Optional[str], str], ...]]" = {
    # Reading and changing files and directories.
    "files": (
        ("GET", "/files/{path:path}"),
        ("PUT", "/files/{path:path}"),
        ("DELETE", "/files/{path:path}"),
        ("GET", "/dirs/{path:path}"),
        ("POST", "/dirs/{path:path}"),
        ("GET", "/stat/{path:path}"),
        ("POST", "/rename"),
    ),
    # Per-byte attribution. Its own group because it is the read a host is most
    # likely to want without granting the rest of `files`.
    "blame": (("GET", "/blame/{path:path}"),),
    # Commits, branches, checkout, and the views over them. Workspace-wide by
    # nature -- a scoped router already refuses most of these with 403.
    "history": (
        ("POST", "/commit"),
        ("GET", "/log"),
        ("GET", "/status"),
        ("GET", "/diff"),
        ("GET", "/diff/file"),
        ("GET", "/branches"),
        ("POST", "/branches"),
        ("POST", "/checkout"),
    ),
    # The propose-and-review queue, both document shapes. The tree-suggestion
    # routes sit here rather than in `coedit` because proposing and accepting is
    # the review path; the socket is not.
    "suggestions": (
        ("POST", "/suggestions"),
        ("GET", "/suggestions"),
        ("GET", "/suggestions/{sid}"),
        ("GET", "/suggestions/{sid}/diff"),
        ("POST", "/suggestions/{sid}/accept"),
        ("POST", "/suggestions/{sid}/reject"),
        ("POST", "/coedit-tree-suggest/{path:path}"),
        ("GET", "/coedit-tree-suggestions/{sid}/update"),
        ("POST", "/coedit-tree-suggestions/{sid}/accept"),
    ),
    # Undo everything one actor did in one session. Its own group: it is the most
    # destructive route here and the one most likely to be withheld.
    "revert": (("POST", "/revert-session"),),
    # Registering actors and opening sessions. Workspace-wide by design --
    # identity is store-wide in origofs, so these are never tenant-scoped.
    "actors": (("POST", "/actors"), ("POST", "/sessions")),
    # The change feed and who is currently where.
    "presence": (
        ("GET", "/events"),
        ("GET", "/presence"),
        ("POST", "/presence"),
        ("POST", "/presence/touch"),
    ),
    # Live co-editing: both sockets, plus the checkpoint route the tree socket
    # cannot work without -- origofs cannot serialize a tree, so the host lands
    # the bytes. Mounting the socket without it would give a room nobody can save.
    "coedit": (
        (None, "/coedit/{path:path}"),
        (None, "/coedit-tree/{path:path}"),
        ("POST", "/coedit-tree-checkpoint/{path:path}"),
    ),
    # Recovering uncommitted deletions.
    "trash": (
        ("GET", "/trash"),
        ("POST", "/trash/{sid}/restore"),
        ("DELETE", "/trash/{sid}"),
    ),
    # Liveness and readiness. Ungated and unscoped, as they are on the Rust API:
    # a probe that needs a credential reads as an unhealthy backend.
    "health": (("GET", "/health"), ("GET", "/readyz")),
}

#: The route groups :func:`build_router` understands, for ``include``/``exclude``.
#: Read it rather than hard-coding names -- it is the list the validation checks
#: against, so a name from here is a name that works.
ROUTE_GROUPS = frozenset(_ROUTE_GROUPS)

# (method, path) -> group. Built once; the reverse of the table above.
_GROUP_OF = {key: group for group, keys in _ROUTE_GROUPS.items() for key in keys}


def _route_keys(route: Any) -> "list[tuple[Optional[str], str]]":
    """The ``(method, path)`` keys identifying ``route``, or ``[]`` if it has none.

    A websocket route has no ``methods``; an HTTP route can in principle carry
    several, so every one is looked up and they must agree.
    """
    path = getattr(route, "path", None)
    if path is None:
        return []
    methods = getattr(route, "methods", None)
    if not methods:
        return [(None, path)]
    # HEAD is synthesized by Starlette alongside GET and is not a route anyone
    # declared, so it is not something the table should have to name.
    return [(m, path) for m in sorted(methods) if m not in ("HEAD", "OPTIONS")]


def _resolve_groups(
    include: "Optional[Iterable[str]]", exclude: "Optional[Iterable[str]]"
) -> "Optional[frozenset[str]]":
    """The groups to mount, or ``None`` for "everything, unfiltered"."""
    if include is not None and exclude is not None:
        raise ValueError(
            "pass `include` or `exclude`, not both -- one names what to mount and "
            "the other what to drop, and together they only say the same thing twice"
        )
    if include is None and exclude is None:
        return None
    names = frozenset(include if include is not None else exclude)  # type: ignore[arg-type]
    unknown = sorted(names - ROUTE_GROUPS)
    if unknown:
        # Loud, because the quiet version of this bug is the dangerous one: a
        # typo in `include` mounts nothing and a typo in `exclude` mounts
        # everything, and both look like a working call.
        raise ValueError(
            f"unknown route group(s): {unknown}. Known groups: "
            f"{sorted(ROUTE_GROUPS)} (see `origofs.fastapi.ROUTE_GROUPS`)"
        )
    return names if include is not None else ROUTE_GROUPS - names


def _filter_routes(router: "APIRouter", keep: "frozenset[str]") -> None:
    """Drop every route whose group is not in ``keep``, in place.

    Filtering after the fact rather than skipping registration is deliberate: the
    router's closures -- the co-editing room registry, its sweeper, the relay --
    are built once for the whole router and the surviving routes still need them.
    It is also what an integrator was already doing by hand, minus the coupling.

    Every route must be claimed by exactly one group. An unclaimed one raises
    rather than being silently kept or silently dropped -- both of those are ways
    to ship a surface nobody chose. This only runs when filtering was asked for,
    so a mapping gap can never change the default router's behaviour.
    """
    kept = []
    for route in router.routes:
        keys = _route_keys(route)
        if not keys:
            kept.append(route)  # not a path route (a Mount, say); leave it alone
            continue
        groups = {_GROUP_OF.get(k) for k in keys}
        if None in groups:
            missing = sorted(k for k in keys if k not in _GROUP_OF)
            raise RuntimeError(
                f"origofs.fastapi: route(s) {missing} belong to no group, so "
                f"`include`/`exclude` cannot say whether to mount them. This is a "
                f"bug in origofs -- please report it."
            )
        if groups & keep:
            kept.append(route)
    router.routes[:] = kept


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


class _RevertSession(BaseModel):
    """The body of a session revert.

    It names the actor and session *to undo*, which is not the caller: reverting
    an agent's sitting is a thing a reviewer does to somebody else's work. The
    caller's own identity still comes from ``authn`` and is never in the body, so
    the authorization question ("may this reviewer revert here?") is answered
    server-side while the target stays a parameter.
    """

    actor: int
    session: int
    path_prefix: Optional[str] = None


class _Touch(BaseModel):
    """The body of a presence heartbeat. It carries **no** actor and **no**
    session on purpose: like every other mutating route, identity is resolved
    server-side from ``authn``, so a client can only ever heartbeat *itself* —
    naming someone else is not expressible, let alone honoured."""

    path: Optional[str] = None


class _CreateActor(BaseModel):
    name: str
    agent: bool = False
    model: Optional[str] = None
    controller: Optional[int] = None


class _CreateSession(BaseModel):
    """The body of ``POST /sessions``. It carries **no** actor: the session
    belongs to whoever the credential resolves to, resolved server-side."""

    client: Optional[str] = None


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


def _as_bytes(v: Any) -> bytes:
    """A Yjs blob as it survives JSON: raw bytes, a list of ints, or base64.

    A browser sends a `Uint8Array`, which `JSON.stringify` turns into an object
    of index→byte or an array depending on how the client serializes it; a
    Python client sends bytes directly. Accepting all three keeps the route
    usable from either side without a second, binary-only endpoint.
    """
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v)
    if isinstance(v, list):
        return bytes(int(b) & 0xFF for b in v)
    if isinstance(v, dict):  # {"0": 1, "1": 2, ...} from JSON.stringify
        return bytes(int(v[k]) & 0xFF for k in sorted(v, key=int))
    if isinstance(v, str):
        import base64

        return base64.b64decode(v)
    raise HTTPException(
        status_code=400, detail="expected bytes, a list of byte values, or base64"
    )


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
    except PermissionError as e:
        # A write-policy refusal: a propose-only actor reaching for an operation
        # it may not land directly. An authorization outcome, so `403` -- not the
        # `409` it used to collapse into via the OSError arm below, which is
        # already carrying stale-base semantics for suggestion accepts.
        #
        # Must precede OSError: PermissionError is a subclass of it.
        raise HTTPException(status_code=403, detail=str(e))
    except OSError as e:
        # A non-empty directory (rmdir or a rename onto one) is the one origofs
        # error the Rust binding maps to a plain OSError rather than one of the
        # specific subclasses above (see `to_pyerr` in origofs-py/src/lib.rs) --
        # catch it here so it doesn't fall through as an unhandled 500.
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except conflict_error as e:  # stale base on a suggestion accept
        raise HTTPException(status_code=409, detail=str(e))
    except origofs_error as e:
        raise HTTPException(status_code=500, detail=str(e))


def _abs(path: str) -> str:
    return path if path.startswith("/") else "/" + path


# --- tenant scoping (issue #93) ---------------------------------------------
#
# A host that puts many tenants in one workspace — the documented "one workspace,
# scoped paths" shape — could authorise the path-carrying routes (its dependency
# reads `request.path_params["path"]`) but had nothing to authorise the
# workspace-global ones against: `GET /log`, `/status`, `/diff`, `/events`,
# `/presence`, `/branches`, `/suggestions`, and the id-addressed suggestion
# routes. Suggestion ids are workspace-global, so knowing an id was enough. The
# only safe move was to refuse all of them and re-implement blame and suggestion
# review in front of the SDK.
#
# `root` fixes that at the router: every path resolves *under* the root, and the
# global routes are filtered to it or refused. A host mounts one router per
# tenant, or passes a dependency that resolves the root from the request.


def _norm_root(root: str) -> str:
    """A root as the scope helpers want it: absolute, no trailing slash."""
    r = _abs(root.strip()).rstrip("/")
    return r


def _under(root: str, path: Optional[str]) -> bool:
    """Whether `path` is the root itself or sits beneath it.

    Directory-boundary matching, not `startswith`: `/tenant-a` must not cover
    `/tenant-abc`, which is precisely the neighbour a scope exists to exclude.
    A `None` path is *not* under any root — a record that names no path (an
    idle presence row) tells a scoped reader something about a tenant it cannot
    see, so it is filtered out rather than let through.
    """
    if path is None:
        return False
    if not root:
        return True  # the empty root is the whole workspace
    return path == root or path.startswith(root + "/")


def _scoped(root: str, path: str) -> str:
    """Resolve a caller-supplied path inside `root`.

    The caller's path is always relative to the root, so a client cannot address
    anything outside its tenant by asking for one — there is no representable
    request for `/other-tenant/secrets`, because the root is prepended rather
    than compared against.

    A `..` component is refused outright. `validate_component` in the engine
    already refuses to *store* one, but that is a different guarantee: it stops a
    poisoned name being persisted, not a path being resolved out of its scope
    here.
    """
    p = _abs(path)
    if any(part == ".." for part in p.split("/")):
        raise HTTPException(status_code=400, detail="path may not contain '..'")
    if not root:
        return p
    return root if p == "/" else root + p


def _require_in_scope(root: str, path: Optional[str]) -> None:
    """Refuse a record that is outside the scope, as a **404**.

    Not a 403: a scoped caller must not be able to tell "this suggestion exists
    but belongs to someone else" from "no such suggestion". The status is the
    same one it would get for an id that never existed.
    """
    if not _under(root, path):
        raise HTTPException(status_code=404, detail="not found")


def _unscopable(what: str) -> "HTTPException":
    """The refusal for an operation that has no per-tenant meaning.

    Commits, branches and checkout act on the whole working tree — a checkout
    rematerializes *every* tenant's files — and the commit log is a shared
    history whose messages and authors belong to everybody. There is no filter
    that makes them tenant-scoped, so a scoped router refuses them rather than
    pretending. Mount an unscoped router (no `root`) for the operator surface
    that legitimately needs them.
    """
    return HTTPException(
        status_code=403,
        detail=f"{what} acts on the whole workspace and is unavailable on a "
               f"path-scoped router (built with root=…)",
    )


_STREAM_CHUNK = 1 << 20  # 1 MiB per read_range() call when streaming a full file


# A single-range `Range: bytes=start-end` (also `bytes=start-` and the suffix
# form `bytes=-N`). Multi-range (`bytes=0-10,20-30`) and non-byte units fall
# back to `None` -- RFC 7233 permits a server to just ignore a Range header it
# doesn't support and return the whole entity, which is what the caller does.
def _parse_range(range_header: Optional[str], size: int) -> Optional[tuple]:
    if not range_header or "," in range_header or not range_header.startswith("bytes="):
        return None
    spec = range_header[len("bytes="):].strip()
    start_s, sep, end_s = spec.partition("-")
    if not sep:
        return None
    if start_s == "":
        # Suffix range: the last `end_s` bytes.
        if not end_s.isdigit():
            return None
        suffix = int(end_s)
        if suffix == 0:
            raise HTTPException(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        start, end = max(0, size - suffix), size - 1
    else:
        if not start_s.isdigit():
            return None
        start = int(start_s)
        end = int(end_s) if end_s.isdigit() else size - 1
    if size == 0 or start >= size or start > end:
        raise HTTPException(status_code=416, headers={"Content-Range": f"bytes */{size}"})
    return start, min(end, size - 1)


# The subprotocol a browser client offers to carry its credential:
# `new WebSocket(url, ["origofs", token])`. The server echoes back this marker
# (never the token) as the selected protocol.
_COEDIT_SUBPROTOCOL = "origofs"

# The `XmlFragment` root a tree room binds to when the client names none. Editors
# differ (`y-prosemirror` defaults to "prosemirror", `@platejs/yjs` is
# configurable), so it is always explicit on the wire and this is only the default.
_DEFAULT_TREE_ROOT = "content"


async def _session_bound(ws: Any, ctx: Any) -> Any:
    """`ctx` with a session guaranteed, opening one if it has none.

    A ``WriteCtx.actor(...)`` stamps edits ``(actor, session=None)``, and
    ``revert_session`` needs a session — so a live-editing connection built that
    way produces edits that can never be undone as a unit, on the surface that
    produces the most edits (issue #98). A credential that *does* name a session
    is left alone: the host has already said what unit of work this is.

    Falls back to the original ctx if the workspace-like object has no
    ``create_session`` (this router is deliberately duck-typed for testing).
    """
    if getattr(ctx, "session_id", None) is not None:
        return ctx
    factory = getattr(ws, "create_session", None)
    if factory is None:
        return ctx
    import origofs  # deferred: the router imports without the compiled extension

    session = await _run(factory(ctx.actor_id, "coedit"))
    return origofs.WriteCtx.session(ctx.actor_id, session)


async def _safe_close(websocket: WebSocket, code: int, reason: str) -> None:
    """Close a websocket, tolerating one that's already gone (the peer
    disconnected concurrently, racing this same close) instead of letting
    that failure become another uncaught exception."""
    try:
        await websocket.close(code=code, reason=reason[:123])
    except Exception:
        pass


# --- live co-editing rooms --------------------------------------------------


class _Conn:
    """One connected socket and its **own** outbound queue. Every byte written to
    a socket goes through its queue and single writer task, so a peer's fan-out
    never races the socket's own replies."""

    __slots__ = ("socket", "out")

    def __init__(self, socket: WebSocket) -> None:
        self.socket: WebSocket = socket
        self.out: "asyncio.Queue[bytes]" = asyncio.Queue()


class CheckpointPolicy:
    """When a live co-editing room's document is written back to durable storage.

    A room's CRDT lives in process memory. Without a policy it reaches durable
    storage only when the **last socket leaves** — and a browser tab left open on
    a document is an open room, so "last leave" can be hours away. Until then
    ``read``/``read_range`` serve the last checkpoint and blame carries only the
    runs folded in at that point; the live marker says the bytes may lag, which is
    the right primitive, but over a long session "may lag" stops being useful. And
    if the worker dies in between — a deploy, an OOM — the un-checkpointed part of
    the session is gone from the durable side. On Postgres the relay table bounds
    that exposure to its replay window; on SQLite the relay is off entirely, so the
    exposure is the whole session (issue #97).

    The two triggers answer different questions, so both exist:

    * ``idle_after`` bounds how long a *finished* burst of typing sits un-durable;
    * ``max_interval`` bounds a *continuous* session, which idle alone never would
      because every keystroke resets it.

    Set either to ``None`` to disable that trigger; disable both for the old
    checkpoint-on-last-leave-only behaviour.
    """

    __slots__ = ("idle_after", "max_interval", "tick")

    def __init__(
        self,
        idle_after: Optional[float] = 5.0,
        max_interval: Optional[float] = 60.0,
        tick: float = 1.0,
    ) -> None:
        self.idle_after = idle_after
        self.max_interval = max_interval
        # How often to look for due rooms, and therefore the granularity of both
        # triggers -- no point setting it finer than the smaller of them.
        self.tick = tick

    @property
    def armed(self) -> bool:
        return self.idle_after is not None or self.max_interval is not None


class _Room:
    """One live document shared by every socket editing it: the attributed CRDT
    plus the set of connected sockets to fan edits out to."""

    __slots__ = (
        "doc", "conns", "ctx", "path", "xml_root",
        "last_edit", "last_checkpoint", "dirty",
    )

    def __init__(self, doc: Any, ctx: Any, path: str, xml_root: Optional[str] = None) -> None:
        self.doc = doc
        self.conns: set[_Conn] = set()
        # The workspace path this room edits, and -- for a tree room (#92) -- the
        # `XmlFragment` root it is bound to. `xml_root` is `None` for the flat
        # shape, and is what decides how the room reaches durable storage: the
        # server can materialize a `Y.Text`, but only the host can serialize a tree.
        self.path = path
        self.xml_root = xml_root
        # The newest joiner's context: a periodic checkpoint has no connection of
        # its own to borrow an identity from, and this is the same one the final
        # checkpoint on last leave uses. It only names the op-log entry and
        # backstops a span the CRDT left unattributed -- every real run keeps the
        # author stamped on it when it was typed.
        self.ctx = ctx
        now = time.monotonic()
        self.last_edit = now
        # A fresh room counts as just-checkpointed: its content came *from* the
        # durable blob, so the interval trigger measures from now rather than
        # firing immediately on a room nobody has typed into.
        self.last_checkpoint = now
        self.dirty = False

    def touch_edit(self) -> None:
        """Record that an edit landed, so the sweeper knows there is something to
        checkpoint and when the room last went quiet."""
        self.last_edit = time.monotonic()
        self.dirty = True

    def is_due(self, policy: CheckpointPolicy, now: float) -> bool:
        if not self.dirty:
            return False
        idle_due = policy.idle_after is not None and now - self.last_edit >= policy.idle_after
        interval_due = (
            policy.max_interval is not None
            and now - self.last_checkpoint >= policy.max_interval
        )
        return idle_due or interval_due

    def fanout(self, sender: Optional[_Conn], frame: bytes) -> None:
        """Queue `frame` for every connection except `sender` (pass ``None`` to
        reach all, e.g. for a frame relayed from another worker)."""
        for conn in self.conns:
            if conn is not sender:
                conn.out.put_nowait(frame)


def _flat_key(path: str) -> tuple:
    """Registry key for a flat (``Y.Text``) room."""
    return ("flat", path)


def _tree_key(path: str, xml_root: str) -> tuple:
    """Registry key for a tree (``Y.XmlFragment``) room (#92).

    The shape is part of the key because one path may legitimately be open in both
    at once — a terminal editor on the flat shape, a browser on the tree — and the
    two must never share a document.
    """
    return ("tree", xml_root, path)


def _relay_key(key: tuple) -> str:
    """The routing key peers publish and subscribe under, so two shapes of one path
    stay separate across workers. ``\0`` cannot occur in a path."""
    return key[1] if key[0] == "flat" else "tree\0{}\0{}".format(key[1], key[2])


async def _finalize(writer_task: "asyncio.Task[None]", rooms: "_Rooms", key: tuple, ctx: Any, conn: "_Conn") -> None:
    """Tear a connection down: stop its writer and leave the room (which
    checkpoints on the last leave). Run this under :func:`asyncio.shield` — the
    socket closing tends to cancel the endpoint task, and the checkpoint must still
    complete."""
    writer_task.cancel()
    try:
        await writer_task
    except BaseException:  # noqa: BLE001 - already-cancelled writer; nothing to do
        pass
    await rooms.leave(key, ctx, conn)


class _Rooms:
    """Per-process registry of live co-editing rooms, keyed by path.

    A room is created on the first join (opening the document) and, when the last
    socket leaves, checkpointed into the byte-range blame index and evicted. This
    is the shared, long-lived state that makes co-editing efficient: all sockets on
    one path share one CRDT, instead of each request opening its own view.

    The registry is per **process**. Under a single worker that is the whole story.
    Across multiple workers, when the workspace is **Postgres**-backed, rooms are
    bridged over the cross-worker relay: every attributed delta is published, and a
    background task applies peers' deltas to this worker's rooms and fans them out
    to its sockets, so all replicas converge. A joining room replays recent ops to
    catch up. On SQLite (single-writer) the relay is simply off. Either way state
    stays durable through the shared workspace's checkpoints.
    """

    def __init__(self, ws: Any, policy: Optional[CheckpointPolicy] = None) -> None:
        self._ws = ws
        # Keyed by shape as well as path -- see `_flat_key` / `_tree_key`.
        self._rooms: dict[tuple, _Room] = {}
        self._lock = asyncio.Lock()
        self._policy = policy if policy is not None else CheckpointPolicy()
        self._sweeper_task: Optional["asyncio.Task[None]"] = None
        # This worker's id, tagged on every published op to skip our own echo.
        self._origin = uuid.uuid4().hex
        # Resolved lazily, not here. `build_router` is legitimately called before a
        # workspace exists — the documented pattern for an app that opens its
        # workspace in an async lifespan is to wire the router once at import time
        # against a proxy, so the routes are stable while the workspace is swapped
        # underneath. Probing the backend in `__init__` broke exactly that: it
        # forced the proxy to resolve before the lifespan had run.
        # (`examples/web/server/app.py` does this, and its test suite could not even
        # be collected — which nothing noticed, because that suite was never run.)
        self._relay: Optional[bool] = None
        self._drain_task: Optional["asyncio.Task[None]"] = None

    def ensure_relay(self) -> None:
        """Start the cross-worker drain task once (a no-op without Postgres, or
        after the first call). Called on the first socket, in async context —
        which is also the first point at which the workspace is guaranteed live,
        so the backend probe happens here."""
        if self._relay is None:
            self._relay = bool(getattr(self._ws, "is_postgres", lambda: False)())
        if self._relay and self._drain_task is None:
            self._drain_task = asyncio.create_task(self._drain())

    def ensure_sweeper(self) -> None:
        """Start periodic checkpointing once (a no-op when no trigger is armed, or
        after the first call). Called on the first socket, in async context.

        Driven here rather than left to the host on purpose: a host has no signal
        about room activity -- it cannot see when a document went quiet -- so
        "call checkpoint_coedit on a timer" is both more work and strictly worse,
        since it writes idle rooms and misses busy ones."""
        if self._policy.armed and self._sweeper_task is None:
            self._sweeper_task = asyncio.create_task(self._sweep())

    async def _sweep(self) -> None:
        """Checkpoint due rooms forever, on the policy's tick."""
        while True:
            await asyncio.sleep(self._policy.tick)
            try:
                await self.checkpoint_due()
            except asyncio.CancelledError:
                raise
            except Exception:
                # A sweeper that dies takes durability with it and says nothing,
                # so it survives anything one round can raise.
                pass

    async def checkpoint_due(self) -> None:
        """Checkpoint every room the policy says is due, leaving them live.

        The registry lock is taken only to pick the due rooms and released before
        any I/O, so a checkpoint on a slow store never blocks a join or a leave.
        """
        now = time.monotonic()
        async with self._lock:
            due = [room for room in self._rooms.values() if room.is_due(self._policy, now)]
        for room in due:
            # Clear `dirty` *before* the write, so an edit landing during it marks
            # the room dirty again and gets its own checkpoint. The other order
            # would swallow that edit until the next one arrived.
            room.dirty = False
            room.last_checkpoint = now
            try:
                await self._write_back(room)
            except Exception:
                # Put it back in the queue rather than waiting for the next edit:
                # a failed checkpoint means these bytes are still not durable.
                room.dirty = True

    async def _write_back(self, room: _Room) -> None:
        """Write a room back to durable storage, as far as the server is able to.

        A flat room checkpoints in full -- text and blame -- because the server can
        materialize its bytes. A tree room only gets its **sidecar** persisted: the
        body is the host's serialization and the server has no serializer, so
        producing one here would mean inventing a document model. The file and its
        blame move when the host calls :meth:`checkpoint_tree`; this is what keeps a
        crash from costing the editing history in between.
        """
        if room.xml_root is None:
            await self._ws.checkpoint_coedit(room.ctx, room.path, room.doc)
        else:
            await self._ws.persist_coedit_tree(room.path, room.doc)

    async def _drain(self) -> None:
        """Apply peers' published deltas to the rooms this worker hosts and fan
        them out to its sockets, until the relay connection closes."""
        try:
            sub = await self._ws.coedit_subscribe()
        except Exception:
            return  # not Postgres (or setup failed): single-worker mode
        while True:
            try:
                notes = await sub.recv()
            except Exception:
                break
            if not notes:
                break  # connection closed
            for note in notes:
                if note.origin == self._origin:
                    continue  # our own op — already applied + fanned out locally
                room = next(
                    (r for k, r in self._rooms.items() if _relay_key(k) == note.path),
                    None,
                )
                if room is None:
                    continue  # not hosting this document here
                try:
                    await room.doc.apply_relayed(note.delta)
                except Exception:
                    continue
                room.fanout(None, note.delta)  # to every local socket

    async def publish(self, key: tuple, frame: bytes) -> None:
        """Publish a local edit's delta to peer workers (a no-op without the relay)."""
        if not self._relay:
            return
        try:
            await self._ws.coedit_publish(_relay_key(key), self._origin, frame)
        except Exception:
            pass  # relay is best-effort; local editing continues regardless

    async def join(self, key: tuple, ctx: Any, conn: _Conn) -> _Room:
        async with self._lock:
            room = self._rooms.get(key)
            if room is None:
                path = key[1] if key[0] == "flat" else key[2]
                xml_root = None if key[0] == "flat" else key[1]
                if xml_root is None:
                    doc = await self._ws.open_coedit(ctx, path)
                else:
                    doc = await self._ws.open_coedit_tree(ctx, path, xml_root)
                if self._relay:
                    # Ensure the relay table exists, then replay recent ops so this
                    # room catches up to peers before its first socket syncs.
                    try:
                        await self._ws.coedit_relay_init()
                        for note in await self._ws.coedit_replay(_relay_key(key)):
                            await doc.apply_relayed(note.delta)
                    except Exception:
                        pass
                room = _Room(doc, ctx, path, xml_root)
                self._rooms[key] = room
            else:
                # The newest joiner is who a background checkpoint runs as.
                room.ctx = ctx
            room.conns.add(conn)
            return room

    async def leave(self, key: tuple, ctx: Any, conn: _Conn) -> None:
        async with self._lock:
            room = self._rooms.get(key)
            if room is None:
                return
            room.conns.discard(conn)
            if not room.conns:
                # Final write-back under the registry lock so a concurrent join
                # can't fork a fresh room off a half-written sidecar -- or clear
                # the live marker out from under a room still taking edits.
                room.ctx = ctx
                await self._write_back(room)
                # Only after that lands: until it does, the durable blob really
                # does lag the document, and the marker is what says so.
                # (`open_coedit` set it; this is the matching clear, exactly as
                # the Rust api::Coordinator does on last leave.)
                try:
                    await self._ws.end_coedit(room.path)
                except Exception:
                    # An older extension without `end_coedit`, or a transient
                    # metadata error: a marker left behind is the *safe* failure
                    # direction (a reader is told the bytes may lag when they
                    # don't), so it must not fail the disconnect path.
                    pass
                del self._rooms[key]

    async def checkpoint_tree(
        self, key: tuple, ctx: Any, body: bytes, spans: list
    ) -> None:
        """Land a tree room's bytes: the host's serialized `body` plus the span map
        saying which byte ranges came from which co-edit node (#92).

        Runs against the **live** room when one exists, so the node ids the host
        cites resolve against the same stamps its socket is seeing; falls back to
        the document on disk when the host checkpoints with no socket attached.
        """
        room = self._rooms.get(key)
        path, xml_root = key[2], key[1]
        if room is None:
            # `load_`, not `open_`: a socket-less checkpoint never reaches `leave()`,
            # which is what clears the live marker, so opening here marked the path
            # live for good -- every "Save" with no editor attached leaked one. Same
            # write check, no claim.
            doc = await self._ws.load_coedit_tree_as(ctx, path, xml_root)
            await self._ws.checkpoint_coedit_tree(ctx, path, doc, body, spans)
            return
        await self._ws.checkpoint_coedit_tree(ctx, path, room.doc, body, spans)
        # The host has crystallized these bytes, so the room is no longer behind.
        room.dirty = False
        room.last_checkpoint = time.monotonic()


# --- router factory ---------------------------------------------------------


# Request bodies up to this size are handled in memory; larger ones spill to a
# temp file and take the streaming write path. Chosen well under the Rust API's
# 64 MiB default body cap so an ordinary document write never touches disk, while
# a genuinely large upload never has to be resident.
SPOOL_MAX = 8 * 1024 * 1024


def _content_type(path: str) -> str:
    """Guess a media type from the path, defaulting to ``application/octet-stream``.

    Both range-aware responses below used to hardcode ``application/octet-stream``,
    so a browser downloaded a video instead of playing it however well the ``Range``
    handling worked. Python has ``mimetypes`` in the standard library, so unlike the
    Rust surface (which keeps a deliberate small table) there is nothing to
    hand-maintain here.
    """
    guessed, _ = mimetypes.guess_type(path)
    return guessed or "application/octet-stream"



def build_router(
    ws: Any,
    *,
    authn: AuthnDep,
    reader: Optional[AuthnDep] = None,
    root: Optional[Union[str, AuthnDep]] = None,
    checkpoint: Optional["CheckpointPolicy"] = None,
    include: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
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
        Optional dependency gating read-only routes; raise to reject. Omit to
        leave reads open.

        If it returns a :class:`origofs.WriteCtx`, reads run **as** that actor:
        the router calls ``read_as``/``ls_as``/``stat_as``/… so a workspace with
        ``acl_enforce_reads`` on actually applies ``Perms.READ`` to the answer,
        and a listing hides what a stat would refuse. Returning anything else
        (``None``, a bool) keeps the older gate-only behaviour, where the value
        is ignored and reads are anonymous.

        Anonymous reads stop being served the moment the workspace turns read
        enforcement on: a read that cannot be checked is a 401, not an open
        door. Passing ``reader=authn`` is usually what you want — the same
        dependency that identifies a writer identifies a reader.
    root:
        Scope every route to one subtree, for a host that puts many tenants in
        one workspace (issue #93). Either a fixed path — mount one router per
        tenant — or a dependency resolving it from the request, for a single
        router that scopes itself::

            app.include_router(build_router(ws, authn=authn, root="/tenants/acme"),
                               prefix="/acme")

            async def tenant_root(request: Request) -> str:
                return f"/tenants/{await my_auth.tenant_of(request)}"
            app.include_router(build_router(ws, authn=authn, root=tenant_root))

        Every caller-supplied path is then resolved *under* the root, so
        `/notes.md` means `/tenants/acme/notes.md` and there is no representable
        request for another tenant's file. Listing routes (`/status`, `/diff`,
        `/events`, `/presence`, `/suggestions`) are filtered to the root, and the
        id-addressed suggestion routes answer `404` for a suggestion outside it —
        `404` rather than `403` so a caller cannot probe which ids exist.

        Operations that act on the **whole** working tree are refused with `403`
        rather than filtered, because no filter makes them tenant-scoped: commit,
        branches, checkout, and the commit log (a shared history whose messages
        and authors belong to everybody). Mount an unscoped router for the
        operator surface that needs them.

        Actors and sessions stay available and workspace-wide by design —
        identity is store-wide in origofs (see `docs/MULTI_TENANCY.md`), not per
        workspace, so scoping them here would be a fiction.
    checkpoint:
        When live co-editing rooms are written back to durable storage. Defaults
        to checkpointing 5 seconds after a room goes quiet and at least every 60
        seconds while it stays busy — see :class:`CheckpointPolicy`, which also
        explains what the durability window is without it.
    include:
        Mount only these route groups (issue #153). Names come from
        :data:`ROUTE_GROUPS`: ``files``, ``blame``, ``history``, ``suggestions``,
        ``revert``, ``actors``, ``presence``, ``coedit``, ``trash``, ``health``.
        Omit for the whole surface, which is what every existing caller gets.

        This is for the host that stores bodies in origofs but owns the access
        model: it wants the co-editing socket and routes every mutation through
        its own authorized endpoints. Without it, a caller who satisfies ``authn``
        can reach *every* mutating route, which is an auth bypass in an app whose
        own endpoints enforce more than ``authn`` does — a viewer ``PUT``ting over
        a body their role-and-lock checks would have refused::

            app.include_router(build_router(ws, authn=authn, include=["coedit"]))

        Prefer ``include`` to ``exclude``. An allowlist keeps meaning what it said
        when origofs adds a route group; a denylist quietly starts mounting it.
    exclude:
        The complement of ``include`` — mount everything except these groups.
        Mutually exclusive with it.

        Read the note above before reaching for this: a group added in a later
        origofs release is not in your ``exclude`` list, so it appears on your
        app the moment you upgrade. That is the right default for "I want the
        surface, minus commits", and the wrong one for "I want a small surface".

        An unknown group name in either list raises, rather than being ignored: a
        typo in ``include`` mounts nothing and a typo in ``exclude`` mounts
        everything, and both look like a working call.
    **router_kwargs:
        Forwarded to :class:`~fastapi.APIRouter` (``prefix``, ``tags``,
        router-wide ``dependencies=[...]``, …).

    Notes
    -----
    Read routes and mutating routes already carry **different** gates: ``authn``
    is applied only to routes that change something, and ``reader`` only to the
    ones that do not. So "let reads and writes be gated differently" needs no new
    parameter — pass a stricter ``authn`` than ``reader``. What ``include`` adds
    is the orthogonal thing: which routes exist at all.
    """
    # Resolved before anything is built, so a bad group name fails on the call
    # rather than after a router has been assembled.
    keep = _resolve_groups(include, exclude)

    router = APIRouter(**router_kwargs)

    # Shared, long-lived co-editing rooms — created once here, not per request.
    rooms = _Rooms(ws, checkpoint)

    # Read-route gate: when no `reader` is given, a no-op keeps the signature
    # uniform. Its *value* now matters as well as its success — see `_read_ctx`.
    if reader is None:
        async def _read_gate() -> None:
            return None
    else:
        _read_gate = reader  # type: ignore[assignment]

    async def _read_ctx(who: Any = Depends(_read_gate)) -> Any:
        """The context a read runs under, or ``None`` for an anonymous read.

        Reads are open on this router by default, so a read route cannot demand
        a credential unconditionally — that would turn every anonymous ``GET``
        into a 401 for every existing deployment. It cannot ignore identity
        either: the workspace's ``_as`` reads consult ``Perms.READ`` where
        ``acl_enforce_reads`` is on, and a route calling the unattributed twin
        makes that switch decoration on the one surface facing a network.

        So: use whatever ``reader`` returned when it looks like a ``WriteCtx``,
        and otherwise decide by the workspace's own switch — anonymous while
        enforcement is off, 401 once it is on. A ``reader`` that returns
        ``None`` keeps working exactly as before, which is what the existing
        gate-only readers do.
        """
        if who is not None and hasattr(who, "actor_id"):
            return who
        if await _reads_enforced():
            raise HTTPException(
                status_code=401,
                detail=(
                    "this workspace enforces read permissions, so a read needs a "
                    "credential to check them against"
                ),
            )
        return None

    async def _reads_enforced() -> bool:
        # `getattr` rather than a bare call: `build_router` accepts "any object
        # with the same async methods", and a test double predating read
        # enforcement has no such method. Absent means off, which is the default
        # a real workspace reports anyway.
        probe = getattr(ws, "acl_enforce_reads", None)
        return bool(await _run(probe())) if probe is not None else False

    # The scope every path resolves under. Normalized once for a fixed root;
    # resolved per request when it's a dependency. `""` means unscoped, which is
    # what every helper treats as "the whole workspace" — so the unscoped router
    # runs the identical code path rather than a second one nobody exercises.
    scoped = root is not None
    if root is None:
        async def _root() -> str:
            return ""
    elif isinstance(root, str):
        _fixed = _norm_root(root)

        async def _root() -> str:
            return _fixed
    else:
        _resolver = root

        async def _root(resolved: Any = Depends(root)) -> str:  # type: ignore[misc]
            if not isinstance(resolved, str) or not resolved.strip():
                raise HTTPException(
                    status_code=500,
                    detail="the `root` dependency must return a non-empty path",
                )
            return _norm_root(resolved)

        del _resolver

    # --- files --------------------------------------------------------------

    @router.get("/files/{path:path}", dependencies=[Depends(_read_gate)])
    async def read_file(
        path: str,
        range: Optional[str] = Header(default=None),
        root: str = Depends(_root),
        who: Any = Depends(_read_ctx),
    ):
        p = _scoped(root, path)
        # Before the stat below, not after: the check has to precede the lookup
        # or a refusal and a miss become distinguishable, which is the existence
        # answer the check exists to withhold. The streaming bodies below take a
        # manifest rather than a path, so this is where the gate belongs.
        if who is not None:
            await _run(ws.ensure_may_read_at(who, "read", p))
        # stat() first so a missing file or a directory is a clean error BEFORE
        # any bytes are sent -- once a StreamingResponse has started, the status
        # code can't change (this is the same guarantee the Rust HTTP API's
        # read_stream gets from resolving before it starts streaming).
        st = await _run(ws.stat(p))
        if st["kind"] != "file":
            raise HTTPException(status_code=409, detail=f"not a file: {p}")
        size = st["size"]

        # Both branches stream in bounded pieces. The ranged one used to answer
        # with a single `read_range(start, end - start + 1)` into a `Response`
        # body, which reads as "a range is small" and isn't: the open-ended
        # `bytes=0-` a <video> element sends first parses to the whole file, so
        # the one request `docs/LIMITS.md` uses as its example materialized every
        # byte -- roughly 2N, since pyo3 copies the buffer into Python.
        def chunks(start: int, end: int):
            async def gen():
                offset = start
                while offset <= end:
                    want = min(_STREAM_CHUNK, end - offset + 1)
                    try:
                        chunk = await ws.read_range(p, offset, want)
                    except Exception:
                        # The file changed or vanished between stat() and this
                        # read (a concurrent writer -- origofs is multi-writer by
                        # design). The response is already committed to its status
                        # and Content-Length, so a clean status change isn't
                        # possible here; end the stream rather than let the error
                        # propagate uncaught into the ASGI layer.
                        return
                    if not chunk:
                        return
                    yield bytes(chunk)
                    offset += len(chunk)

            return gen()

        parsed = _parse_range(range, size) if range else None
        if parsed is not None:
            start, end = parsed
            return StreamingResponse(
                chunks(start, end),
                status_code=206,
                media_type=_content_type(p),
                headers={
                    "Content-Range": f"bytes {start}-{end}/{size}",
                    "Content-Length": str(end - start + 1),
                    "Accept-Ranges": "bytes",
                },
            )

        return StreamingResponse(
            chunks(0, size - 1),
            media_type=_content_type(p),
            headers={"Content-Length": str(size), "Accept-Ranges": "bytes"},
        )

    @router.put("/files/{path:path}")
    async def write_file(
        request: Request,
        path: str,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ):
        """Write a file, streaming the request body.

        This used to take ``body: bytes``, so the whole upload sat in memory before
        a single byte was chunked — asymmetric with the ``GET`` beside it, which has
        always streamed and honoured ``Range``.

        Bodies up to ``SPOOL_MAX`` stay in memory, so the common small write pays
        nothing. Past that the body spills to a named temp file which is handed to
        ``write_path_as``, streaming it into the workspace without the bytes ever
        crossing back into Python. (A named file rather than
        ``SpooledTemporaryFile``: on Unix that rolls over to an *unlinked* file, so
        there is no path to hand across.)
        """
        p = _scoped(root, path)

        # A propose-only actor's edit is queued for review, and a suggestion holds
        # the proposed bytes — so that path buffers whatever its size. Fine by
        # construction: nobody reviews a multi-gigabyte diff. Deciding up front
        # keeps the write policy behaving identically either side of SPOOL_MAX.
        # `getattr` because this router is deliberately duck-typed against a
        # workspace-like object (that is how it is tested without the compiled
        # extension), the same way the co-editing rooms probe `is_postgres`.
        #
        # **`ensure_may_write_at`, not `ensure_may_write`.** This reconstructs the
        # fork `write_or_propose` makes internally, so it has to ask the question
        # the engine will actually answer: the *path-scoped* one, where the grant
        # covering `p` decides and the actor's write policy is only the fallback.
        # The path-less check consults the policy alone, so for an actor whose
        # policy is Direct but whose grant here allows PROPOSE only, it answered
        # "may write directly" — and the request then took the direct path and was
        # refused by the engine's real check, turning what the engine would have
        # queued as a suggestion into a 403. The `mkdir_as` below is where it
        # actually bit, so even a small body into an existing directory 403'd.
        #
        # Defaulting to True when absent is safe: the buffered branch below goes
        # through `write_or_propose`, which enforces the policy itself. Only the
        # streaming branch relies on this check, and it is reached solely on a real
        # workspace — which has the method.
        # Awaited directly, *not* through `_run`: `_run` maps `PermissionError` to
        # a 403 for the caller, which is right for a real request but would swallow
        # the answer this probe exists to get.
        may_write_directly = True
        probe = getattr(ws, "ensure_may_write_at", None)
        if probe is not None:
            try:
                await probe(ctx, "write a file", p)
            except PermissionError:
                may_write_directly = False

        buf = bytearray()
        spill = None
        spill_path = None
        size = 0
        try:
            async for chunk in request.stream():
                size += len(chunk)
                if spill is not None:
                    spill.write(chunk)
                    continue
                buf.extend(chunk)
                # Only spill once we know we can use the streaming path.
                if len(buf) > SPOOL_MAX and may_write_directly:
                    spill = tempfile.NamedTemporaryFile(delete=False)
                    spill_path = spill.name
                    spill.write(buf)
                    buf = bytearray()
            if spill is not None:
                spill.close()
                # Only the streaming path needs the parent to exist:
                # `write_path_as` resolves an existing parent rather than creating
                # one, while the buffered branch's `write_or_propose` creates
                # parents itself. Creating it *here* rather than up front is what
                # makes "a queued suggestion leaves the working tree untouched"
                # hold structurally instead of resting on the probe above — the
                # buffered branch never reaches this line, so it cannot mkdir for
                # a write it is only proposing. (Parity with
                # `tests/mcp.rs::a_queued_write_creates_no_directories`.)
                # Attributed, so the directory carries an actor like any other
                # namespace mutation.
                parent, _, _ = p.rpartition("/")
                if parent:
                    await _run(ws.mkdir_as(ctx, parent))
                await _run(ws.write_path_as(ctx, p, spill_path))
                return {"path": p, "written": size}

            body = bytes(buf)
            outcome = await _run(ws.write_or_propose(ctx, p, body, None))
            if outcome.wrote:
                return {"path": p, "written": size}
            return {"path": p, "proposed": outcome.suggestion_id}
        finally:
            if spill is not None and not spill.closed:
                spill.close()
            if spill_path is not None:
                with contextlib.suppress(OSError):
                    os.unlink(spill_path)

    @router.delete("/files/{path:path}")
    async def remove_file(
        path: str, ctx: Any = Depends(authn), root: str = Depends(_root)
    ):
        """Delete a file, governed by the caller's write policy.

        A propose-only actor's delete is **queued for review**, not applied —
        otherwise it could destroy a file it was refused permission to overwrite,
        which is the exact hop the ``PUT`` gate would only have made one step
        longer (issue #78). Mirrors ``DELETE /v1/files`` on the Rust HTTP API,
        response shape included.
        """
        p = _scoped(root, path)
        outcome = await _run(ws.remove_or_propose(ctx, p, f"delete {p}"))
        if outcome.wrote:
            return {"removed": p}
        return {"path": p, "proposed": outcome.suggestion_id}

    # --- directories --------------------------------------------------------

    @router.get("/dirs/{path:path}", dependencies=[Depends(_read_gate)])
    async def list_dir(
        path: str, root: str = Depends(_root), who: Any = Depends(_read_ctx)
    ):
        p = _scoped(root, path)
        if who is not None:
            return await _run(ws.ls_as(who, p))
        return await _run(ws.ls(p))

    @router.post("/dirs/{path:path}")
    async def make_dir(
        path: str, ctx: Any = Depends(authn), root: str = Depends(_root)
    ):
        await _run(ws.mkdir_as(ctx, _scoped(root, path)))
        return {"created": _abs(path)}

    @router.get("/stat/{path:path}", dependencies=[Depends(_read_gate)])
    async def stat(
        path: str, root: str = Depends(_root), who: Any = Depends(_read_ctx)
    ):
        p = _scoped(root, path)
        if who is not None:
            return await _run(ws.stat_as(who, p))
        return await _run(ws.stat(p))

    @router.post("/rename")
    async def rename(
        req: _Rename, ctx: Any = Depends(authn), root: str = Depends(_root)
    ):
        # Both ends resolve under the root, so a rename can never move a file
        # across tenants in either direction.
        await _run(ws.rename_as(ctx, _scoped(root, req.from_), _scoped(root, req.to)))
        return {"from": _abs(req.from_), "to": _abs(req.to)}

    # --- attribution --------------------------------------------------------

    @router.get("/blame/{path:path}", dependencies=[Depends(_read_gate)])
    async def blame(
        path: str, root: str = Depends(_root), who: Any = Depends(_read_ctx)
    ):
        p = _scoped(root, path)
        if who is not None:
            return await _run(ws.blame_as(who, p))
        return await _run(ws.blame(p))

    # --- versioning ---------------------------------------------------------

    @router.post("/commit")
    async def commit(
        req: _Commit, ctx: Any = Depends(authn), root: str = Depends(_root)
    ):
        """Commit the working tree, attributed to the authenticated caller.

        ``req.author`` is the *git-level* author string that lands on the commit
        object; the authenticated ctx is what the policy gate and the audit log
        see. Binding both means a propose-only actor can no longer commit — and a
        client-named ``author`` can no longer be the only identity on a mutating
        route.
        """
        if root:
            raise _unscopable("commit")
        return {"hash": await _run(ws.commit_as(ctx, req.author, req.message))}

    @router.get("/log", dependencies=[Depends(_read_gate)])
    async def log(root: str = Depends(_root)):
        # A shared history: every tenant's commit messages and authors are in it,
        # and there is no per-path view of a commit list to filter down to.
        if root:
            raise _unscopable("the commit log")
        return await _run(ws.log())

    @router.get("/status", dependencies=[Depends(_read_gate)])
    async def status(root: str = Depends(_root)):
        entries = await _run(ws.status())
        return [e for e in entries if _under(root, e.get("path"))]

    @router.get("/diff", dependencies=[Depends(_read_gate)])
    async def diff(
        from_: str = Query(..., alias="from"),
        to: str = Query(...),
        root: str = Depends(_root),
        who: Any = Depends(_read_ctx),
    ):
        entries = await _run(
            ws.diff_as(who, from_, to) if who is not None else ws.diff(from_, to)
        )
        return [e for e in entries if _under(root, e.get("path"))]

    @router.get("/diff/file", response_class=PlainTextResponse, dependencies=[Depends(_read_gate)])
    async def diff_file(
        path: str = Query(...),
        from_: str = Query(..., alias="from"),
        to: str = Query(...),
        root: str = Depends(_root),
        who: Any = Depends(_read_ctx),
    ):
        p = _scoped(root, path)
        if who is not None:
            return await _run(ws.diff_file_as(who, from_, to, p))
        return await _run(ws.diff_file(from_, to, p))

    @router.get("/branches", dependencies=[Depends(_read_gate)])
    async def branches(root: str = Depends(_root)):
        if root:
            raise _unscopable("branches")
        return await _run(ws.branches())

    @router.post("/branches")
    async def create_branch(
        req: _Name, ctx: Any = Depends(authn), root: str = Depends(_root)
    ):
        if root:
            raise _unscopable("creating a branch")
        await _run(ws.create_branch_as(ctx, req.name))
        return {"branch": req.name}

    @router.post("/checkout")
    async def checkout(
        req: _Name, ctx: Any = Depends(authn), root: str = Depends(_root)
    ):
        """Switch branches, rematerializing the whole working tree.

        Attributed and policy-gated for the reason the Rust API gives: a checkout
        discards every uncommitted edit, so an unattributed one let a propose-only
        token — held by an actor deliberately barred from overwriting a single
        file — destroy the workspace.
        """
        # Rematerializing "the whole working tree" means every tenant's files,
        # so this is the sharpest example of an operation a scope cannot narrow.
        if root:
            raise _unscopable("checkout")
        await _run(ws.checkout_as(ctx, req.name))
        return {"branch": req.name}

    async def _in_scope_suggestion(sid: int, root: str) -> None:
        """Refuse an id-addressed suggestion route outside the scope.

        Suggestion ids are workspace-global, so on an unscoped router knowing an
        id is enough to read, accept or reject somebody else's proposal. Answers
        `404` — the same as an id that never existed — so a caller cannot walk the
        id space to learn which suggestions other tenants have open.
        """
        if not root:
            return
        row = await _run(ws.get_suggestion(sid))
        _require_in_scope(root, row.get("path") if row else None)

    # --- agent-suggestion review queue --------------------------------------

    @router.post("/suggestions")
    async def suggest(
        path: str = Query(...),
        body: bytes = Body(default=b""),
        summary: Optional[str] = Query(default=None),
        delete: bool = Query(default=False),
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ):
        p = _scoped(root, path)
        if delete:
            return {"id": await _run(ws.suggest_delete(ctx, p, summary))}
        return {"id": await _run(ws.suggest(ctx, p, body, summary))}

    @router.get("/suggestions", dependencies=[Depends(_read_gate)])
    async def list_suggestions(
        status: Optional[str] = Query(default=None),
        path: Optional[str] = Query(default=None),
        root: str = Depends(_root),
        who: Any = Depends(_read_ctx),
    ):
        # Filtering after the fact rather than trusting the `path` filter: a
        # caller can simply omit it, and omitting it used to return every
        # tenant's queue.
        scoped_filter = _scoped(root, path) if path else None
        rows = await _run(
            ws.list_suggestions_as(who, status, scoped_filter)
            if who is not None
            else ws.list_suggestions(status, scoped_filter)
        )
        return [r for r in rows if _under(root, r.get("path"))]

    @router.get("/suggestions/{sid}", dependencies=[Depends(_read_gate)])
    async def get_suggestion(
        sid: int, root: str = Depends(_root), who: Any = Depends(_read_ctx)
    ):
        await _in_scope_suggestion(sid, root)
        # `get_suggestion_as` answers None for a proposal at a path the actor may
        # not read, so the 404 below covers a denial as well as an absence — the
        # same answer `_in_scope_suggestion` gives, for the same reason.
        row = await _run(
            ws.get_suggestion_as(who, sid) if who is not None else ws.get_suggestion(sid)
        )
        if row is None:
            raise HTTPException(status_code=404, detail=f"no suggestion {sid}")
        return row

    @router.get("/suggestions/{sid}/diff", response_class=PlainTextResponse,
                dependencies=[Depends(_read_gate)])
    async def suggestion_diff(
        sid: int, root: str = Depends(_root), who: Any = Depends(_read_ctx)
    ):
        await _in_scope_suggestion(sid, root)
        if who is not None:
            return await _run(ws.suggestion_diff_as(who, sid))
        return await _run(ws.suggestion_diff(sid))

    @router.post("/suggestions/{sid}/accept")
    async def accept(sid: int, ctx: Any = Depends(authn), root: str = Depends(_root)):
        await _in_scope_suggestion(sid, root)
        await _run(ws.accept_suggestion(sid, ctx))
        return {"accepted": sid}

    @router.post("/suggestions/{sid}/reject")
    async def reject(sid: int, ctx: Any = Depends(authn), root: str = Depends(_root)):
        await _in_scope_suggestion(sid, root)
        await _run(ws.reject_suggestion(sid, ctx))
        return {"rejected": sid}

    @router.post("/revert-session")
    async def revert_session(
        req: _RevertSession,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ):
        """Undo exactly what one actor wrote in one session, leaving others' edits
        in place — the headline "undo just the agent's work" operation.

        ``path_prefix`` bounds it to a subtree, matched on directory boundaries.
        On a **scoped** router omitting it does not mean "everywhere": it resolves
        to the scope's own root, because an unbounded revert walks every file the
        session touched across every tenant.

        Authorized *after* the prefix is resolved and *against* it — a revert
        writes to every file under that prefix, so the grant covering that subtree
        is the one that decides, and an unbounded one is checked at ``/``.
        """
        prefix = req.path_prefix
        if root:
            if prefix is None:
                prefix = root
            else:
                # Absoluteness is checked before scoping rather than left to
                # `_scoped`, which would normalize it, so a scoped caller gets the
                # same error an unscoped one does.
                if not prefix.startswith("/"):
                    raise HTTPException(
                        status_code=400,
                        detail=f"path prefix must be absolute, got {prefix!r}",
                    )
                prefix = _scoped(root, prefix)
        elif prefix is not None and not prefix.startswith("/"):
            raise HTTPException(
                status_code=400, detail=f"path prefix must be absolute, got {prefix!r}"
            )
        paths = await _run(
            ws.revert_session_as(ctx, req.actor, req.session, prefix)
        )
        return {"reverted": paths}

    # --- live collaboration -------------------------------------------------

    @router.get("/events", dependencies=[Depends(_read_gate)])
    async def events(since: int = Query(default=0), root: str = Depends(_root)):
        evs = await _run(ws.watch(since))
        return [e for e in evs if _under(root, e.get("path"))]

    @router.get("/presence", dependencies=[Depends(_read_gate)])
    async def presence(
        window: int = Query(default=60),
        root: str = Depends(_root),
        who: Any = Depends(_read_ctx),
    ):
        rows = await _run(
            ws.presence_as(who, window) if who is not None else ws.presence(window)
        )
        # A row whose `path` is None names no file, so it says "somebody is
        # active" without saying where — which is exactly the cross-tenant leak
        # this scope exists to close. `_under` treats None as out of scope.
        return [r for r in rows if _under(root, r.get("path"))]

    async def _heartbeat(req: Optional[_Touch], ctx: Any, root: str) -> dict:
        """Heartbeat the authenticated caller's presence. Mirrors the Rust HTTP
        API's ``POST /v1/presence``: the body is optional and only ever carries a
        `path`; the actor and session come from the credential.

        Presence is keyed by **session**, so the credential must be bound to one —
        a bare actor context gets a ``400`` telling it to create a session first.
        Minting one here instead would let a heartbeat loop create unbounded
        session rows and make the presence list a directory of connections rather
        than of working sessions.
        """
        if ctx.session_id is None:
            raise HTTPException(
                status_code=400,
                detail="this credential is not bound to a session; create one (POST /sessions) "
                       "and present a session-bound credential to heartbeat presence",
            )
        raw = (req.path or "").strip() if req is not None else ""
        # Scoped like every other path a caller supplies, so a heartbeat cannot
        # advertise this session as working inside another tenant.
        path = _scoped(root, raw) if raw else None
        await _run(ws.touch(ctx.actor_id, ctx.session_id, path))
        return {"session_id": ctx.session_id, "actor_id": ctx.actor_id, "path": path}

    @router.post("/presence")
    async def heartbeat_presence(
        req: Optional[_Touch] = None,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ):
        return await _heartbeat(req, ctx, root)

    # The original spelling of the same heartbeat, kept so existing clients keep
    # working; `POST /presence` is the one that matches the Rust HTTP API.
    @router.post("/presence/touch")
    async def touch(
        req: _Touch, ctx: Any = Depends(authn), root: str = Depends(_root)
    ):
        await _heartbeat(req, ctx, root)
        return {"ok": True}

    # --- actors + sessions ---------------------------------------------------
    # Gated by `authn` like every other mutating route: an already-authenticated
    # caller mints new actor/session identities (e.g. a trusted backend
    # provisioning an actor for a newly-signed-up user) -- not public
    # self-registration.
    #
    # Deliberately NOT scoped by `root`. Identity is store-wide in origofs, not
    # per workspace (`docs/MULTI_TENANCY.md`: "actor/session/tool_calls stay
    # store-wide"), so a tenant-scoped actor would be a fiction the engine does
    # not implement. These routes stay available on a scoped router and mint
    # workspace-wide identities; a host that wants per-tenant identity owns that
    # mapping itself, which is the same place it resolves `root` from.

    @router.post("/actors")
    async def create_actor(req: _CreateActor, ctx: Any = Depends(authn)):
        # Registering actors is an administrative mutation, so it answers to the
        # caller's write policy too -- a propose-only actor may not mint identities
        # (matching `create_actor` on the Rust HTTP API). `getattr` because this
        # router is deliberately duck-typed against a workspace-like object.
        probe = getattr(ws, "ensure_may_write", None)
        if probe is not None:
            await _run(probe(ctx, "register actors"))
        if req.agent:
            # `is not None`, not `or`: an explicit empty-string model should be
            # preserved (matches the Rust API's `req.model.as_deref().unwrap_or(…)`,
            # which only substitutes on None) -- `or` would also replace "".
            model = req.model if req.model is not None else "unknown"
            actor_id = await _run(ws.create_agent(req.name, model, req.controller))
        else:
            actor_id = await _run(ws.create_human(req.name, None))
        return {"id": actor_id}

    @router.post("/sessions")
    async def create_session(
        req: Optional[_CreateSession] = None, ctx: Any = Depends(authn)
    ):
        """Open a session **for the authenticated caller**.

        The body carries no actor, exactly like ``POST /v1/sessions`` on the Rust
        HTTP API: a session belongs to whoever the credential resolves to. It used
        to take ``actor`` from the request, which let an authenticated caller mint
        a session belonging to somebody else — and a host whose ``authn`` trusts a
        client-presented session id would then attribute that actor's edits to it.
        Identity is resolved server-side or not at all.
        """
        client = req.client if req is not None else None
        sid = await _run(ws.create_session(ctx.actor_id, client))
        return {"id": sid, "actor": ctx.actor_id}

    # --- live co-editing (Yjs y-sync) ---------------------------------------

    @router.websocket("/coedit/{path:path}")
    async def coedit(
        websocket: WebSocket,
        path: str,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ) -> None:
        """Live co-editing over the Yjs y-sync binary protocol.

        Authentication reuses ``authn`` — the same dependency as every mutating
        route — so it resolves the socket to the actor its edits are attributed to.
        Content is attributed server-side regardless of what the client's bytes
        claim.

        **Where a browser puts the credential.** It can't set headers on an
        upgrade, so there are two options and they are not equal:

        * ``Sec-WebSocket-Protocol`` — the one header a browser *can* set, via
          ``new WebSocket(url, ["origofs", token])``. Read it in ``authn`` with
          ``sec_websocket_protocol: str = Header(None)`` and take the second
          entry. This router echoes ``origofs`` back as the selected subprotocol
          automatically (below), which the handshake requires.
        * ``?token=`` — works, and stays supported, but a URL is the worst place
          for a credential: it lands in access logs, proxy logs, and
          ``Referer``-adjacent tooling by default.

        Same-origin hosts can also just use cookies, which *are* sent on upgrades.

        **Sessions.** Live edits are only revertible as a unit if the connection
        has a session, so if ``authn`` returns a bare ``WriteCtx.actor(...)`` this
        opens one for the connection. One session per connection is the natural
        unit — it is exactly "what this person typed in this sitting", which is
        what ``revert_session`` undoes.
        """
        await _serve_coedit(ws, rooms, websocket, _scoped(root, path), ctx, None)

    @router.websocket("/coedit-tree/{path:path}")
    async def coedit_tree(
        websocket: WebSocket,
        path: str,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
        xml_root: str = Query(_DEFAULT_TREE_ROOT, alias="root"),
    ) -> None:
        """Live co-editing over a ``Y.XmlFragment`` — the **tree** shape (#92), so
        ``@platejs/yjs``/``@slate-yjs/core``, ``y-prosemirror`` or TipTap bind natively
        instead of mirroring a flat ``Y.Text``.

        Identical to ``/coedit/{path}`` in authentication, credential transport and
        per-connection sessions. It differs only in what reaches durable storage:
        origofs does not own your document schema, so it cannot serialize a tree.
        The server persists the CRDT on the policy's tick (a crash then costs no
        editing history) and **you** land the file by POSTing to
        ``/coedit-tree-checkpoint/{path}`` with your serialized body and a span map.

        ``?root=`` names the ``XmlFragment`` your editor binds to; it must match
        the one you pass at checkpoint time.
        """
        await _serve_coedit(ws, rooms, websocket, _scoped(root, path), ctx, xml_root)

    @router.post("/coedit-tree-checkpoint/{path:path}")
    async def coedit_tree_checkpoint(
        path: str,
        payload: dict,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ) -> dict:
        """Land a tree document's bytes (#92).

        The flat shape needs no equivalent route: the server can materialize a
        ``Y.Text``, so it checkpoints itself. A tree's bytes exist only once your
        serializer has run, so you are the only party that can supply them — along
        with the span map saying which bytes came from which co-edit node.

        Body: ``{"body": "...", "spans": [[start, end, node], ...], "root": "..."}``.
        Authorship is still resolved server-side from origofs's own stamps — the
        request names byte ranges and node ids, never an actor. Bytes no span covers
        (your serializer's own punctuation) are attributed to the caller.
        """
        p = _scoped(root, path)
        body = payload.get("body", "")
        body = body.encode() if isinstance(body, str) else bytes(body)
        spans = [(int(a), int(b), str(node)) for a, b, node in payload.get("spans", [])]
        xml_root = payload.get("root") or _DEFAULT_TREE_ROOT
        ctx = await _session_bound(ws, ctx)
        # Through the shared mapper, so a malformed span map is a 400 and a write
        # that landed outside the session is a 409 -- the same translation every
        # other route gets, rather than a second one that could drift from it.
        await _run(rooms.checkpoint_tree(_tree_key(p, xml_root), ctx, body, spans))
        return {"path": p, "bytes": len(body), "spans": len(spans)}

    # --- trash (issue #115) -------------------------------------------------

    @router.get("/trash", dependencies=[Depends(_read_gate)])
    async def list_trash(root: str = Depends(_root), who: Any = Depends(_read_ctx)):
        """What is still recoverable, newest deletion first.

        A committed file can be read back out of history; an **uncommitted** one
        could not be recovered at all, which matters more here than it would for
        an ordinary filesystem because the users are agents. Off unless the
        workspace enabled retention.
        """
        rows = await _run(ws.list_trash())
        rows = [r for r in rows if _under(root, r.get("path"))]
        if who is None:
            return rows
        # Per entry, like `ls`: a trash entry names a path and when it was
        # deleted, which is what a `stat` on the restored path would say.
        keep = []
        for r in rows:
            try:
                await ws.ensure_may_read_at(who, "read", r["path"])
            except PermissionError:
                continue
            keep.append(r)
        return keep

    async def _trash_entry_in_scope(sid: int, root: str) -> dict:
        """Resolve a trash id inside `root`, or 404.

        A trash id is a workspace-global integer, so without this a scoped caller
        could materialize a neighbour's deleted file by guessing one. 404 rather
        than 403, for the reason the suggestion routes give: a refusal would
        confirm the id exists.
        """
        for r in await _run(ws.list_trash()):
            if r["id"] == sid and _under(root, r.get("path")):
                return r
        raise HTTPException(status_code=404, detail=f"no trash entry {sid}")

    @router.post("/trash/{sid}/restore")
    async def restore_trash(
        sid: int, ctx: Any = Depends(authn), root: str = Depends(_root)
    ) -> dict:
        """Put a deleted file back where it was, attributed to the caller."""
        await _trash_entry_in_scope(sid, root)
        return {"restored": await _run(ws.restore_trash(sid, ctx))}

    @router.delete("/trash/{sid}")
    async def purge_trash(
        sid: int, ctx: Any = Depends(authn), root: str = Depends(_root)
    ) -> dict:
        """Drop one entry permanently.

        Takes the same right as writing where the file used to live, not merely a
        valid credential: this destroys the only remaining copy of an uncommitted
        file.
        """
        entry = await _trash_entry_in_scope(sid, root)
        await _run(ws.ensure_may_write_at(ctx, "purge the trash for", entry["path"]))
        return {"purged": await _run(ws.purge_trash(sid))}

    @router.post("/coedit-tree-suggest/{path:path}")
    async def coedit_tree_suggest(
        path: str,
        payload: dict,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ) -> dict:
        """Propose a change to a tree document as a CRDT merge (#92, #75 §3.2).

        The `XmlFragment` counterpart of the flat shape's CRDT suggestion, and the
        route a **propose-only** agent needs: without it, the only ways to suggest
        a change to a rich-text document were a whole-file byte suggestion — whose
        base goes stale on every keystroke elsewhere in the file, and whose
        acceptance discards concurrent work — or nothing.

        Takes the *propose* right, not write. That is the whole point: gating it
        on write would refuse exactly the callers it exists for.

        Body: ``{"base_sv": <bytes|list[int]>, "update": <bytes|list[int]>,
        "summary": "..."}`` — a browser sends ``encodeStateVector(doc)`` and
        ``encodeStateAsUpdate(doc)``. Returns the suggestion id.
        """
        p = _scoped(root, path)
        base_sv = _as_bytes(payload.get("base_sv", b""))
        update = _as_bytes(payload.get("update", b""))
        sid = await _run(
            ws.suggest_coedit_tree_update(
                ctx, p, base_sv, update, payload.get("summary")
            )
        )
        return {"id": sid, "path": p}

    @router.get("/coedit-tree-suggestions/{sid}/update")
    async def coedit_tree_suggestion_update(
        sid: int, root: str = Depends(_root), who: Any = Depends(_read_ctx)
    ) -> Response:
        """The proposed Yjs update behind a tree suggestion, as raw bytes.

        For a host that wants to merge it into a document it already holds — the
        live room, rather than a replica origofs resumes for it.
        """
        await _in_scope_suggestion(sid, root)
        if who is not None and await _run(ws.get_suggestion_as(who, sid)) is None:
            # Not found rather than denied, for the same reason the engine says
            # so: a suggestion id is a guessable global handle.
            raise HTTPException(status_code=404, detail=f"no suggestion {sid}")
        return Response(
            content=bytes(await _run(ws.coedit_tree_suggestion_update(sid))),
            media_type="application/octet-stream",
        )

    @router.post("/coedit-tree-suggestions/{sid}/accept")
    async def coedit_tree_accept(
        sid: int,
        payload: dict,
        ctx: Any = Depends(authn),
        root: str = Depends(_root),
    ) -> dict:
        """Accept a tree suggestion: land your serialized body, resolve the row.

        origofs cannot turn an `XmlFragment` back into a file — it does not own
        your schema — so the ordinary ``/suggestions/{id}/accept`` route refuses a
        tree proposal and this one takes the bytes, exactly as
        ``/coedit-tree-checkpoint`` does. The bytes land attributed to the
        proposal's **author**; you are recorded as the approver.

        Body: ``{"body": "...", "spans": [[start, end, node], ...],
        "root": "..."}``.
        """
        await _in_scope_suggestion(sid, root)
        body = payload.get("body", "")
        body = body.encode() if isinstance(body, str) else bytes(body)
        spans = [(int(a), int(b), str(node)) for a, b, node in payload.get("spans", [])]
        xml_root = payload.get("root") or _DEFAULT_TREE_ROOT
        ctx = await _session_bound(ws, ctx)
        merged = await _run(ws.merge_coedit_tree_suggestion(sid, xml_root))
        await _run(ws.accept_coedit_tree_suggestion(ctx, sid, merged, body, spans))
        return {"accepted": sid, "bytes": len(body)}

    # --- liveness and readiness ---------------------------------------------    # --- liveness and readiness ---------------------------------------------
    #
    # Deliberately **ungated**, matching the Rust API where these sit outside
    # `/v1`: a load balancer and a Kubernetes probe have no bearer token, and a
    # health endpoint that answers 401 reads as an unhealthy backend. Neither
    # reveals anything about the workspace's contents -- `/readyz` reports which
    # *store* is unreachable, not what is in it.
    #
    # They carry no scope either. A scoped router is one tenant's view of a
    # workspace, and whether the process can reach its stores is a property of the
    # process.

    @router.get("/health")
    async def health():
        """Liveness: the process is up and serving. Always 200.

        Distinct from `/readyz` on purpose — a liveness probe that fails when a
        *dependency* is down gets the container restarted, which fixes nothing and
        drops the connections it was still serving.
        """
        return {"status": "ok"}

    @router.get("/readyz")
    async def readyz(response: Response):
        """Readiness: both stores answered a ping. `503` when either did not.

        The status code is the answer — a probe reads that, not the body — and the
        body names which store failed and why, so the page that follows starts with
        the cause rather than with a bisect.
        """
        report = await _run(ws.ready())
        if not report.get("ready", False):
            response.status_code = 503
        return report

    if keep is not None:
        _filter_routes(router, keep)
    return router


def build_coedit_router(
    ws: Any,
    *,
    authn: AuthnDep,
    root: Optional[Union[str, AuthnDep]] = None,
    checkpoint: Optional["CheckpointPolicy"] = None,
    **router_kwargs: Any,
) -> "APIRouter":
    """A router carrying **only** live co-editing (issue #153).

    The two y-sync WebSockets plus the tree checkpoint route they cannot work
    without — and nothing else: no ``PUT``, no ``DELETE``, no commit, no
    ``revert-session``. For an app that keeps its bodies in origofs but owns its
    own access model, this is the whole integration::

        app.include_router(
            build_coedit_router(ws, authn=my_authn, root=project_root),
            prefix="/fs",
        )

    Exactly ``build_router(..., include=["coedit"])``, named because that is the
    shape people actually asked for and a named function is a stabler thing to
    depend on than a magic string. There is no ``reader``: nothing here is a read
    route.
    """
    return build_router(
        ws,
        authn=authn,
        root=root,
        checkpoint=checkpoint,
        include=["coedit"],
        **router_kwargs,
    )


async def _serve_coedit(
    ws: Any,
    rooms: "_Rooms",
    websocket: WebSocket,
    p: str,
    ctx: Any,
    xml_root: Optional[str],
) -> None:
    """Drive one co-editing socket, flat (``xml_root is None``) or tree.

    Shared by both routes so neither can drift from the other on identity,
    handshake, session binding, or teardown -- the parts that are the same for both
    shapes, and the parts where a divergence would be a security bug rather than a
    behaviour difference.
    """
    key = _flat_key(p) if xml_root is None else _tree_key(p, xml_root)
    # A browser that proposes subprotocols fails the handshake unless the
    # server selects one of them, so echo the marker back when it was offered.
    # Only ever the marker -- never the token beside it.
    offered = websocket.scope.get("subprotocols") or []
    chosen = _COEDIT_SUBPROTOCOL if _COEDIT_SUBPROTOCOL in offered else None
    await websocket.accept(subprotocol=chosen)

    # Bind a session to the connection if the credential didn't carry one,
    # so `revert_session` can undo this sitting's edits.
    ctx = await _session_bound(ws, ctx)
    rooms.ensure_relay()  # idempotent; starts the cross-worker drain on first use
    rooms.ensure_sweeper()  # idempotent; starts periodic checkpointing (#97)
    conn = _Conn(websocket)
    room = await rooms.join(key, ctx, conn)

    async def writer() -> None:
        while True:
            await websocket.send_bytes(await conn.out.get())

    # Greet the client with SyncStep1, then let the writer own all sends.
    conn.out.put_nowait(await room.doc.sync_start())
    writer_task = asyncio.create_task(writer())
    try:
        while True:
            data = await websocket.receive_bytes()
            try:
                reply = await room.doc.handle_sync(ctx, data)
            except ValueError as e:
                # A malformed/corrupt y-sync frame -- the binary protocol has
                # no way to resync mid-stream, so close cleanly (1002:
                # protocol error) instead of leaving this client with a hard
                # reset and crashing the ASGI app with an uncaught exception.
                await _safe_close(websocket, 1002, str(e))
                return
            except Exception as e:
                # Anything else handle_sync raises (an origofs-side failure
                # applying an otherwise-valid frame, or a type this router
                # doesn't specifically anticipate) -- not the client's
                # protocol fault, so 1011: internal error. Broad on purpose:
                # the whole point is that nothing from handle_sync should be
                # able to crash the connection uncleanly, not just the two
                # exception shapes observed so far.
                await _safe_close(websocket, 1011, str(e))
                return
            if reply.reply:
                conn.out.put_nowait(reply.reply)
            if reply.broadcast:
                # Only a *content* delta makes the room due for a checkpoint.
                # Awareness (cursor presence) is broadcast too — and every real
                # Yjs client emits it on each selection change plus a periodic
                # heartbeat, with no typing involved — so gating on `broadcast`
                # alone had an open-but-idle tab writing an op-log entry and a
                # blame rewrite on every sweeper tick, forever.
                if reply.content_changed:
                    room.touch_edit()
                room.fanout(conn, reply.broadcast)  # local sockets
                await rooms.publish(key, reply.broadcast)  # peer workers
    except WebSocketDisconnect:
        pass
    finally:
        # Shielded so the last-leave write-back completes even though closing
        # the socket cancels this endpoint task.
        try:
            await asyncio.shield(_finalize(writer_task, rooms, key, ctx, conn))
        except asyncio.CancelledError:
            pass
