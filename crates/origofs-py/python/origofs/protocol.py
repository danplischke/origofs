"""A structural type for the part of :class:`origofs.Workspace` a host actually uses.

``Workspace`` is a concrete class implemented in Rust, so an integrator who wanted
to name the subset they depend on — for a test double, for a service layer that
takes "something workspace-shaped", for a narrowing wrapper — had to hand-maintain
their own ``Protocol`` and keep it in step by hand (issue #163). This is that
protocol, maintained here instead.

    from origofs.protocol import WorkspaceProtocol

    class DocumentService:
        def __init__(self, ws: WorkspaceProtocol) -> None:
            self._ws = ws

It is deliberately a **subset**: the read, write, review-queue, identity and
co-editing calls a host builds on, not the operator surface (gc, fsck, migrate,
mounts, quotas, object-store configuration). Those exist on ``Workspace`` and are
reachable there; naming them here would turn every test double into a
reimplementation of the whole engine, which is the opposite of what a structural
type is for.

``runtime_checkable``, so ``isinstance(ws, WorkspaceProtocol)`` answers "does this
object have these methods". As with every runtime-checkable protocol that only
checks *presence*, not signatures — a static checker is what verifies those.

The corresponding subset of :class:`origofs.WriteCtx` is nothing: a ``WriteCtx`` is
an opaque token a host obtains and passes back, so it appears here by its real
type. Return shapes are the ``TypedDict``s from ``origofs/__init__.pyi``, quoted so
this module imports without resolving them at runtime.
"""

from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

if TYPE_CHECKING:  # pragma: no cover - types only
    from . import (
        ActorRecord,
        BlameSpan,
        CoeditDoc,
        CoeditTreeDoc,
        DirEntry,
        EventRecord,
        LiveMarker,
        PresenceRecord,
        StatResult,
        SuggestionContent,
        SuggestionRecord,
        WriteCtx,
    )

__all__ = ["WorkspaceProtocol"]


@runtime_checkable
class WorkspaceProtocol(Protocol):
    """The read / write / review / co-edit core of :class:`origofs.Workspace`."""

    # --- reading ---------------------------------------------------------
    async def read(self, path: str) -> bytes: ...
    async def read_range(self, path: str, offset: int, length: int) -> bytes: ...
    async def stat(self, path: str) -> "StatResult": ...
    async def ls(self, path: str) -> "List[DirEntry]": ...
    async def blame(self, path: str) -> "List[BlameSpan]": ...

    # The ACL-checked twins. A multi-tenant host should prefer these: they run
    # `READ` where the workspace enables `acl_enforce_reads`, and filter a listing
    # per entry so it cannot name what a `stat` would refuse.
    async def read_as(self, ctx: "WriteCtx", path: str) -> bytes: ...
    async def stat_as(self, ctx: "WriteCtx", path: str) -> "StatResult": ...
    async def ls_as(self, ctx: "WriteCtx", path: str) -> "List[DirEntry]": ...
    async def blame_as(self, ctx: "WriteCtx", path: str) -> "List[BlameSpan]": ...

    # --- writing ---------------------------------------------------------
    # The attributed forms only. The unattributed `write`/`remove`/`mkdir_p`
    # exist for internal machinery (checkout, merge, applying an accepted
    # suggestion) and are exempt from the write policy by construction, so a host
    # surface must never reach for them -- leaving them out of this type is the
    # same discipline the MCP and CLI classification tests enforce.
    async def write_as(self, ctx: "WriteCtx", path: str, data: bytes) -> None: ...
    async def write_or_propose(
        self, ctx: "WriteCtx", path: str, data: bytes, note: Optional[str] = None
    ) -> Any: ...
    async def remove_or_propose(
        self, ctx: "WriteCtx", path: str, note: Optional[str] = None
    ) -> Any: ...
    async def rename_as(self, ctx: "WriteCtx", from_: str, to: str) -> None: ...
    async def mkdir_as(self, ctx: "WriteCtx", path: str) -> None: ...

    # --- the review queue ------------------------------------------------
    async def suggest(
        self, ctx: "WriteCtx", path: str, data: bytes, note: Optional[str] = None
    ) -> int: ...
    async def list_suggestions(
        self, status: Optional[str] = None, path: Optional[str] = None
    ) -> "List[SuggestionRecord]": ...
    async def get_suggestion(self, id: int) -> "Optional[SuggestionRecord]": ...
    async def suggestion_diff(self, id: int) -> str: ...
    async def suggestion_content(self, id: int) -> "SuggestionContent": ...
    async def accept_suggestion(
        self, id: int, approver: "WriteCtx"
    ) -> Optional[str]: ...
    async def reject_suggestion(self, id: int, approver: "WriteCtx") -> None: ...

    # --- identity --------------------------------------------------------
    async def create_human(
        self, name: str, auth_subject: Optional[str] = None
    ) -> int: ...
    async def create_agent(
        self, name: str, model: str, controller: Optional[int] = None
    ) -> int: ...
    async def create_session(
        self, actor_id: int, client: Optional[str] = None
    ) -> int: ...
    async def actor(self, id: int) -> "Optional[ActorRecord]": ...

    # --- live co-editing -------------------------------------------------
    async def open_coedit(self, ctx: "WriteCtx", path: str) -> "CoeditDoc": ...
    async def load_coedit_as(self, ctx: "WriteCtx", path: str) -> "CoeditDoc": ...
    async def checkpoint_coedit(
        self, ctx: "WriteCtx", path: str, doc: "CoeditDoc"
    ) -> None: ...
    async def open_coedit_tree(
        self, ctx: "WriteCtx", path: str, root: str
    ) -> "CoeditTreeDoc": ...
    async def load_coedit_tree_as(
        self, ctx: "WriteCtx", path: str, root: str
    ) -> "CoeditTreeDoc": ...
    async def checkpoint_coedit_tree(
        self,
        ctx: "WriteCtx",
        path: str,
        doc: "CoeditTreeDoc",
        body: bytes,
        spans: "List[Tuple[int, int, str]]",
    ) -> None: ...
    async def persist_coedit_tree(self, path: str, doc: "CoeditTreeDoc") -> None: ...
    async def live_doc(self, path: str) -> "Optional[LiveMarker]": ...
    async def end_coedit(self, path: str) -> None: ...

    # --- the change feed and presence ------------------------------------
    async def watch(self, after_seq: int = 0) -> "List[EventRecord]": ...
    async def presence(self, window_secs: int = 60) -> "List[PresenceRecord]": ...
    async def touch(
        self, actor_id: int, session_id: int, path: Optional[str] = None
    ) -> None: ...
