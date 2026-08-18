"""Type stubs for the origofs package (re-exported from the ``origofs._origofs`` extension).

Every I/O method is async (returns an awaitable) so it composes with FastAPI's
`async def` handlers. Structured results are plain ``dict``/``list`` objects,
directly JSON-serializable — and typed, as the ``TypedDict``\\s below, so a caller
gets completion and a renamed key fails type-checking instead of surfacing as a
``KeyError`` in production.

The records are **total**: every key is always present. A value that can be
absent is typed ``Optional[...]`` and comes back as ``None``, never missing — the
extension's dict builders set every key unconditionally. So
``span["session"] is None`` is the check, not ``"session" in span``.
"""
from __future__ import annotations
import sys
from typing import Any, Awaitable, Dict, List, Literal, NoReturn, Optional, Tuple, TypedDict

# --- record shapes ----------------------------------------------------------
#
# One TypedDict per dict the extension returns, mirroring the builders in
# `crates/origofs-py/src/lib.rs`. Closed string sets are `Literal`, so a typo in a
# comparison is caught rather than silently never matching.

ActorKind = Literal["human", "agent", "system"]
FileKind = Literal["file", "dir", "symlink"]
DiffStatus = Literal["added", "modified", "deleted"]
SuggestionKind = Literal["bytes", "crdt"]
SuggestionStatus = Literal["pending", "accepted", "rejected", "superseded"]


class ActorRecord(TypedDict):
    """An actor, as returned by ``actor``/``list_actors`` and inlined in blame."""

    id: int
    kind: ActorKind
    display_name: str
    auth_subject: Optional[str]
    agent_model: Optional[str]
    agent_vendor: Optional[str]
    controller_actor_id: Optional[int]
    created_at: int


class BlameSpan(TypedDict):
    """One authored run of a file.

    ``byte_start``/``byte_end`` are the ground truth the design blames by;
    ``line_start``/``line_end`` are derived, for line-oriented views. ``actor`` is
    the **full record inlined**, not an id — there is no second lookup to do.
    """

    byte_start: int
    byte_end: int
    line_start: int
    line_end: int
    session: Optional[int]
    actor: ActorRecord


class PassageRecord(TypedDict):
    """A retrieval passage with its provenance (see ``passages``)."""

    path: str
    byte_start: int
    byte_end: int
    hash: str
    # `None` unless the request asked for text; decoded as UTF-8, lossily.
    text: Optional[str]
    blame: list[BlameSpan]


class SuggestionRecord(TypedDict):
    """A row in the review queue (``list_suggestions``/``get_suggestion``).

    ``path`` is always present — a reviewer UI can check the suggestion belongs to
    the document under review without guessing. ``kind`` decides what
    ``base_hash``/``proposed_hash`` address and how ``accept`` applies them.
    A ``proposed_hash`` of ``None`` on a ``bytes`` suggestion is a proposed
    *deletion*.
    """

    id: int
    actor_id: int
    session_id: Optional[int]
    branch: Optional[str]
    path: str
    base_hash: Optional[str]
    proposed_hash: Optional[str]
    summary: Optional[str]
    kind: SuggestionKind
    status: SuggestionStatus
    created_ts: int
    resolved_ts: Optional[int]
    resolved_by: Optional[int]


class SuggestionContent(TypedDict):
    """The bytes behind a suggestion (``suggestion_content``)."""

    base: Optional[bytes]
    proposed: Optional[bytes]


class EditOp(TypedDict):
    """One append-only op-log entry — the ground truth blame is materialized from."""

    id: int
    actor_id: int
    session_id: Optional[int]
    path: str
    op: str
    byte_start: int
    byte_len: int
    pre_hash: Optional[str]
    post_hash: Optional[str]
    ts: int


class StatResult(TypedDict):
    """An inode (``stat``). ``content`` is the manifest hash, ``None`` for a dir.

    ``mode`` carries the file-type bits as well as the permission bits, so compare
    against ``mode & 0o7777`` for permissions alone. ``uid``/``gid`` default to 0
    (root-owned) and are set by :meth:`Workspace.chown`. None of the three is an
    access check — see ``docs/PERMISSIONS.md``.
    """

    ino: int
    kind: FileKind
    mode: int
    uid: int
    gid: int
    nlink: int
    size: int
    content: Optional[str]
    mtime: int
    ctime: int


class GrantRecord(TypedDict):
    """One path-scoped access grant (``grants``).

    ``perms`` is comma-separated (``"read,write"``) or ``"none"``. Longest matching
    ``prefix`` wins; an actor with no covering grant falls back to its write policy.
    Grants do not restrict a FUSE/NFS mount — see ``docs/PERMISSIONS.md`` §5.
    """

    prefix: str
    perms: str


class DirEntry(TypedDict):
    """One entry of a directory listing (``ls``)."""

    name: str
    ino: int
    kind: FileKind


class CommitRecord(TypedDict):
    """A commit in the history (``log``). ``parents`` has two entries for a merge."""

    hash: str
    author: str
    message: str
    timestamp: int
    parents: list[str]


class DiffEntry(TypedDict):
    """A changed path (``diff``, and ``status`` against the working tree)."""

    path: str
    status: DiffStatus


class BranchRecord(TypedDict):
    """A branch and the commit it points at (``branches``)."""

    name: str
    hash: str


class EventRecord(TypedDict):
    """One entry of the change feed (``watch``, ``Subscription.recv``)."""

    seq: int
    actor_id: Optional[int]
    session_id: Optional[int]
    kind: str
    path: str
    detail: Optional[str]
    ts: int
    branch: Optional[str]


class PresenceRecord(TypedDict):
    """Who is working where (``presence``). Keyed by session, not actor."""

    session_id: int
    actor_id: int
    display_name: str
    kind: ActorKind
    path: Optional[str]
    last_seen: int


class LiveMarker(TypedDict):
    """A path with an open CRDT document, so its durable bytes are a checkpoint
    that may lag what people are typing (``live_doc``/``live_paths``).

    ``content_hash`` is the file's address **as of the last checkpoint**, so an
    out-of-band write is exactly "the file's current address differs from this".
    ``since`` is when the document was opened; ``checkpointed_at`` is when the
    durable bytes were last written, which is the one that answers "how stale
    might this be" (``None`` if it has never been checkpointed).
    """

    path: str
    session_id: Optional[int]
    actor_id: int
    content_hash: Optional[str]
    since: int
    checkpointed_at: Optional[int]


class TreeRun(TypedDict):
    """One attributed text run of a tree co-edited document (``CoeditTreeDoc.runs``).

    ``node`` is the id origofs stamped on the run — the token to cite in a span map
    — and is ``None`` for a run origofs never stamped (content a host seeded
    directly), whose ``actor`` is then ``0``.
    """

    text: str
    node: Optional[str]
    actor: int
    session: int


class ConflictRecord(TypedDict):
    """An unresolved merge conflict (``conflicts``)."""

    path: str
    kind: str


class LockRecord(TypedDict):
    """An advisory path lock (``locks``)."""

    path: str
    owner: str
    acquired_at: int


class MergeResult(TypedDict):
    """The outcome of ``merge``. ``commit`` is ``None`` unless a commit was made;
    ``conflicts`` is non-empty only when the merge stopped on them."""

    outcome: Literal["already_up_to_date", "fast_forward", "merged", "conflicts"]
    commit: Optional[str]
    conflicts: list[ConflictRecord]


class GcReport(TypedDict):
    """What a mark-and-sweep collection did (``gc``/``gc_with_grace``)."""

    reachable: int
    deleted: int
    bytes_freed: int
    skipped_young: int
    skipped_undated: int


class RebuildReport(TypedDict):
    """What ``rebuild``/``scan`` found in the content store.

    ``unsupported`` counts objects written by a newer origofs than this build can
    decode; ``scan`` only reports them, ``rebuild`` raises rather than restoring a
    truncated history. ``branches`` is a list of ``(name, commit_hex)`` pairs.
    """

    objects_scanned: int
    corrupt: int
    commits_found: int
    used_mirror: bool
    branches: list[tuple[str, str]]
    checked_out: Optional[str]
    dirs: int
    files: int
    symlinks: int
    unsupported: int
    unsupported_kinds: list[str]


class SchemaVersion(TypedDict):
    """The metadata schema this workspace is on (``schema_version``)."""

    current: int
    latest: int
    up_to_date: bool


# What `migrate` did; `migrated` is False when it was already current. Declared
# functionally rather than as a class because `from` is a reserved word and so
# cannot be written as a class-body annotation.
MigrateReport = TypedDict("MigrateReport", {"from": int, "to": int, "migrated": bool})


class ReadyReport(TypedDict):
    """Backend health (``ready``). Each store is ``None`` when healthy, or a
    message when it is not."""

    ready: bool
    metadata: Optional[str]
    content: Optional[str]

class OrigoFSError(Exception):
    """Base origofs error (raised for errors without a more specific mapping)."""

class ConflictError(OrigoFSError):
    """A suggestion's base changed since it was proposed (stale base)."""

class WriteCtx:
    """The actor context to attribute a write to."""
    @staticmethod
    def actor(actor: int) -> "WriteCtx": ...
    @staticmethod
    def session(actor: int, session: int) -> "WriteCtx": ...
    @property
    def actor_id(self) -> int: ...
    @property
    def session_id(self) -> Optional[int]: ...

class WriteOutcome:
    """The outcome of a policy-governed write (see ``Workspace.write_or_propose``)."""
    @property
    def wrote(self) -> bool:
        """True if the actor writes directly and the edit landed."""
        ...
    @property
    def suggestion_id(self) -> Optional[int]:
        """The suggestion id if the edit was queued for review; ``None`` if written."""
        ...

class S3Config:
    """Connection settings for an S3-compatible object store (S3/R2/MinIO, or GCS
    via its S3-interop XML API). For GCS, set ``endpoint`` to
    ``https://storage.googleapis.com`` and pass GCS HMAC interop keys; for native
    GCS auth use :class:`GcsConfig` instead. Set ``session_token`` alongside the
    key pair for temporary credentials (AWS SSO / SAML federation)."""
    def __init__(
        self,
        bucket: str,
        region: str,
        endpoint: Optional[str] = None,
        allow_http: bool = False,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> None: ...

class GcsConfig:
    """Connection settings for a native Google Cloud Storage object store (GCS JSON
    API + OAuth2). Credentials resolve as: explicit ``service_account_key`` (inline
    JSON) or ``service_account_path`` (file); then Application Default Credentials
    (``application_credentials``, else ``GOOGLE_APPLICATION_CREDENTIALS`` /
    ``gcloud``); then the GCE/GKE metadata server (workload identity)."""
    def __init__(
        self,
        bucket: str,
        service_account_path: Optional[str] = None,
        service_account_key: Optional[str] = None,
        application_credentials: Optional[str] = None,
        prefix: Optional[str] = None,
        allow_http: bool = False,
    ) -> None: ...

if sys.platform == "linux":
    # Only registered on Linux (`#[cfg(target_os = "linux")]` in lib.rs). FUSE has
    # no Windows equivalent, and on macOS it needs the macFUSE kernel extension --
    # a system dependency a wheel can't carry -- so this class doesn't exist on
    # either. macOS mounts over NFSv3 via `serve_nfs` instead.
    class Mount:
        """A live FUSE mount; unmounts on ``unmount()``, ``with``-exit, or drop."""
        def unmount(self) -> None: ...
        def __enter__(self) -> "Mount": ...
        def __exit__(self, *args: Any) -> None: ...

class Subscription:
    """A push subscription to the change feed (Postgres LISTEN/NOTIFY)."""
    async def recv(self) -> list[EventRecord]: ...

class CoeditSyncReply:
    """The routing for one processed y-sync payload (see ``CoeditDoc.handle_sync``)."""
    @property
    def reply(self) -> bytes:
        """Frames to send back to the originating connection (``b""`` if none)."""
        ...
    @property
    def broadcast(self) -> bytes:
        """Frames to fan out to the room's other connections (``b""`` if none)."""
        ...

class CoeditDoc:
    """A live co-edited document (roadmap M8): a Yjs-compatible CRDT whose inserts
    are attributed per byte range. Obtain a server-side room doc from
    ``Workspace.open_coedit`` and drive it with the Yjs y-sync wire protocol via
    ``handle_sync`` so an unmodified editor (PlateJS, ``y-websocket``) collaborates
    directly; land it with ``Workspace.checkpoint_coedit``. Safe to share one
    instance across many concurrent WebSocket handlers."""
    def __init__(self) -> None:
        """A fresh, empty document (for a Python-side agent or a test client)."""
        ...
    async def insert(self, ctx: WriteCtx, index: int, chunk: str) -> None:
        """Insert ``chunk`` at character ``index`` (UTF-16 offset), attributed to ``ctx``."""
        ...
    async def remove(self, index: int, length: int) -> None:
        """Remove ``length`` characters starting at ``index`` (UTF-16 offsets)."""
        ...
    async def sync_start(self) -> bytes:
        """The y-sync ``SyncStep1`` frame to greet a new client with."""
        ...
    async def state_vector(self) -> bytes:
        """This document's Yjs state vector (``encodeStateVector``) — the base half
        of a CRDT suggestion."""
        ...
    async def state_update(self) -> bytes:
        """This document's full state as a Yjs update (``encodeStateAsUpdate``) —
        the opaque, always-mergeable blob a CRDT suggestion proposes."""
        ...
    async def handle_sync(self, ctx: WriteCtx, data: bytes) -> CoeditSyncReply:
        """Handle one inbound y-sync payload from a connection authenticated as
        ``ctx``; its content is attributed to ``ctx`` server-side."""
        ...
    async def apply_relayed(self, frame: bytes) -> None:
        """Merge a y-sync frame relayed from another worker (already attributed by
        its origin) without re-attribution — the cross-worker relay's apply path.
        Idempotent."""
        ...
    async def text(self) -> str:
        """The full current text."""
        ...

class CoeditTreeDoc:
    """A live **tree-shaped** co-edited document (issue #92): a ``Y.XmlFragment`` a
    rich-text editor (``@platejs/yjs``, ``y-prosemirror``, ``y-slate``, TipTap) binds
    to natively, instead of ``CoeditDoc``'s flat ``Y.Text``.

    Obtain a server-side room doc from ``Workspace.open_coedit_tree``, drive it with
    the same y-sync protocol, and land it with ``Workspace.checkpoint_coedit_tree`` —
    which takes *your* serialized bytes plus a span map, because origofs does not own
    the document schema. Safe to share one instance across many WebSocket handlers."""
    def __init__(self, root: Optional[str] = None) -> None:
        """A fresh, empty document rooted at the ``XmlFragment`` named ``root``
        (default ``"content"``)."""
        ...
    async def append_text(self, ctx: WriteCtx, tag: str, text: str) -> str:
        """Append ``<tag>text</tag>`` to the root attributed to ``ctx``, returning
        the node id stamped on the run — ready to cite in a span map. The tree
        analogue of ``CoeditDoc.insert``, and just as narrow: for an in-process
        agent or a test client, not for a real editor (which drives arbitrary tree
        edits over y-sync)."""
        ...
    async def sync_start(self) -> bytes:
        """The y-sync ``SyncStep1`` frame to greet a new client with."""
        ...
    async def state_vector(self) -> bytes:
        """This document's Yjs state vector (``encodeStateVector``)."""
        ...
    async def state_update(self) -> bytes:
        """This document's full state as a Yjs update (``encodeStateAsUpdate``)."""
        ...
    async def handle_sync(self, ctx: WriteCtx, data: bytes) -> CoeditSyncReply:
        """Handle one inbound y-sync payload from a connection authenticated as
        ``ctx``; its content is attributed to ``ctx`` server-side."""
        ...
    async def apply_relayed(self, frame: bytes) -> None:
        """Merge a y-sync frame relayed from another worker without re-attribution."""
        ...
    async def runs(self) -> List[TreeRun]:
        """Every text run in document order, with the node id and author stamped on
        it — the server-side reading of ``ytext.toDelta()``. Walk it to build a span
        map when *you* serialize the document (rather than a browser editor)."""
        ...
    async def authors(self) -> Dict[str, Tuple[int, int]]:
        """Every node id origofs has stamped, as ``{node: (actor_id, session_id)}`` —
        what a span map resolves against. An id absent here has no author."""
        ...
    async def resumed(self) -> bool:
        """Whether this document came from a coherent sidecar rather than opening
        empty. **Check before binding an editor**: origofs cannot rebuild a tree from
        a flat file (that needs your schema), so seed from ``read(path)`` when this
        is ``False`` or a checkpoint writes an empty body over real content."""
        ...
    async def is_empty(self) -> bool:
        """Whether the tree has no nodes at all."""
        ...
    async def plain_text(self) -> str:
        """The whole tree's text in document order, with no structure — for
        inspection and tests. Not the durable body: that is your serialization."""
        ...

class CoeditRelayNote:
    """One relayed co-editing update from another worker (see
    ``Workspace.coedit_subscribe`` / ``coedit_replay``)."""
    @property
    def seq(self) -> int: ...
    @property
    def origin(self) -> str:
        """The publishing worker's id (skip your own)."""
        ...
    @property
    def path(self) -> str: ...
    @property
    def delta(self) -> bytes:
        """The update payload (a y-sync frame) to feed ``CoeditDoc.apply_relayed``."""
        ...

class CoeditRelaySub:
    """A live subscription to the cross-worker co-editing relay (Postgres
    LISTEN/NOTIFY). Returned by ``Workspace.coedit_subscribe``."""
    async def recv(self) -> list[CoeditRelayNote]:
        """Block until peers publish, then return their updates in ``seq`` order
        (``[]`` once the connection closes)."""
        ...

class Workspace:
    # --- constructors (async) ---
    @staticmethod
    async def open_local(db_path: str, cas_dir: str) -> "Workspace": ...
    # Encrypted at rest: XChaCha20-Poly1305 with an Argon2id-derived key.
    # Key derivation is deliberately slow and runs on the calling thread — open at
    # startup, not per request. Addresses stay the *plaintext* hash (convergent
    # encryption) so dedup still works, which makes a shared encrypted store an
    # existence oracle; use per-tenant keys if that matters. The same passphrase
    # must be given on every open.
    @staticmethod
    async def open_local_encrypted(
        db_path: str, cas_dir: str, passphrase: str
    ) -> "Workspace": ...
    @staticmethod
    async def open_s3_encrypted(
        db_path: str, cfg: S3Config, passphrase: str
    ) -> "Workspace": ...
    @staticmethod
    async def open_pg_s3_encrypted(
        dsn: str, cfg: S3Config, passphrase: str
    ) -> "Workspace": ...
    @staticmethod
    async def open_gcs_encrypted(
        db_path: str, cfg: GcsConfig, passphrase: str
    ) -> "Workspace": ...
    @staticmethod
    async def open_pg_gcs_encrypted(
        dsn: str, cfg: GcsConfig, passphrase: str
    ) -> "Workspace": ...
    @staticmethod
    async def open_local_packed(db_path: str, data_dir: str, index_dir: str) -> "Workspace": ...
    @staticmethod
    async def open_pg(dsn: str, cas_dir: str) -> "Workspace": ...
    @staticmethod
    async def open_s3(db_path: str, cfg: S3Config) -> "Workspace": ...
    @staticmethod
    async def open_s3_packed(db_path: str, cfg: S3Config, index_dir: str) -> "Workspace": ...
    @staticmethod
    async def open_pg_s3(dsn: str, cfg: S3Config) -> "Workspace": ...
    @staticmethod
    async def open_pg_s3_packed(dsn: str, cfg: S3Config, index_dir: str) -> "Workspace": ...
    @staticmethod
    async def open_gcs(db_path: str, cfg: GcsConfig) -> "Workspace": ...
    @staticmethod
    async def open_gcs_packed(db_path: str, cfg: GcsConfig, index_dir: str) -> "Workspace": ...
    @staticmethod
    async def open_pg_gcs(dsn: str, cfg: GcsConfig) -> "Workspace": ...
    @staticmethod
    async def open_pg_gcs_packed(dsn: str, cfg: GcsConfig, index_dir: str) -> "Workspace": ...
    @staticmethod
    async def open_object_memory(db_path: str) -> "Workspace": ...

    # --- files ---
    async def read(self, path: str) -> bytes: ...
    # Read `[off, off+len)` of a file, clamped at EOF (fetches only the covering
    # chunks). The primitive fsspec ranged reads go through.
    async def read_range(self, path: str, off: int, len: int) -> bytes: ...
    async def write(self, path: str, data: bytes) -> None: ...
    async def write_as(self, ctx: WriteCtx, path: str, data: bytes) -> None: ...

    # --- streaming: the way to write a file larger than memory ---------------
    # `write`/`write_as` take a bytes object and copy it into Rust, so an N-byte
    # write holds ~3N transiently. These open the file in Rust — no bytes cross
    # into Python — so resident memory is bounded regardless of file size.
    #
    # `write_path_as` is subject to the write policy (PermissionError for a
    # propose-only actor) and attributes the WHOLE file to the actor rather than
    # diffing against the previous body: a streamed write is a wholesale replace,
    # and not holding the previous body is the point. Use `write_as` when the file
    # fits in memory and its line-level provenance matters.
    async def write_path_as(self, ctx: WriteCtx, path: str, src_path: str) -> None: ...
    async def write_path(self, path: str, src_path: str) -> None: ...
    # Returns the number of bytes written. `read` materializes the whole body;
    # this streams, and `read_range` already fetches only the covering chunks.
    async def read_to_path(self, path: str, dest_path: str) -> int: ...
    # Governed by the actor's write policy: a direct actor writes; a propose-only
    # actor's edit is queued as a suggestion (`WriteOutcome.suggestion_id`).
    async def write_or_propose(
        self, ctx: WriteCtx, path: str, data: bytes, summary: Optional[str] = None
    ) -> WriteOutcome: ...
    async def set_write_policy(self, actor_id: int, policy: str) -> None: ...
    async def grant(self, actor: int, prefix: str, perms: str) -> None: ...
    async def revoke(self, actor: int, prefix: str) -> bool: ...
    async def grants(self, actor: int) -> list[GrantRecord]: ...
    async def effective_perms(self, actor: int, path: str) -> str: ...
    async def mkdir_p(self, path: str) -> None: ...
    async def ls(self, path: str) -> list[DirEntry]: ...
    async def stat(self, path: str) -> StatResult: ...
    async def chmod(self, path: str, mode: int) -> StatResult: ...
    async def chown(
        self, path: str, uid: Optional[int] = None, gid: Optional[int] = None
    ) -> StatResult: ...
    async def chmod_as(self, ctx: WriteCtx, path: str, mode: int) -> StatResult: ...
    async def chown_as(
        self,
        ctx: WriteCtx,
        path: str,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
    ) -> StatResult: ...
    async def remove(self, path: str) -> None: ...
    async def rename(self, from_: str, to: str) -> None: ...

    # --- versioning ---
    async def commit(self, author: str, message: str) -> str: ...
    async def log(self) -> list[CommitRecord]: ...
    async def status(self) -> list[DiffEntry]: ...
    async def diff(self, from_: str, to: str) -> list[DiffEntry]: ...
    async def diff_file(self, from_: str, to: str, path: str) -> str: ...
    async def create_branch(self, name: str) -> None: ...
    async def checkout(self, name: str) -> None: ...
    async def branches(self) -> list[BranchRecord]: ...
    async def current_branch(self) -> Optional[str]: ...

    # --- disaster recovery (rebuild metadata from the content store) ---
    async def rebuild(self) -> RebuildReport: ...
    async def scan(self) -> RebuildReport: ...

    # --- schema / migrations ---
    # origofs migrates its own metadata schema forward automatically on open; these
    # expose that for introspection/operator control. Forward-only, idempotent.
    async def schema_version(self) -> SchemaVersion: ...
    async def migrate(self) -> MigrateReport: ...

    # --- attribution ---
    async def create_human(self, name: str, auth_subject: Optional[str] = None) -> int: ...
    async def create_agent(self, name: str, model: str, controller: Optional[int] = None) -> int: ...
    async def actor_by_subject(self, subject: str) -> Optional[ActorRecord]: ...
    async def actor(self, id: int) -> Optional[ActorRecord]: ...  # resolve an actor_id
    async def list_actors(self) -> list[ActorRecord]: ...  # every actor, oldest first
    async def find_or_create_human(self, auth_subject: str, display_name: str) -> int: ...
    async def find_or_create_agent(self, auth_subject: str, display_name: str, model: str, controller: Optional[int] = None) -> int: ...
    async def create_session(self, actor_id: int, client: Optional[str] = None) -> int: ...
    # Each blame span is a dict with `byte_start`/`byte_end` (the ground-truth byte
    # range), the derived `line_start`/`line_end`, `session`, and `actor`.
    async def blame(self, path: str) -> list[BlameSpan]: ...
    # Extract retrieval passages from the working tree (technology-agnostic RAG).
    # Each dict carries `path`, `byte_start`/`byte_end`, a content-address `hash`
    # (dedup / incremental key), `text`, and per-passage `blame`. `segmentation` is
    # one of `content_defined` (default) | `fixed` | `lines` | `whole_file`.
    async def passages(
        self,
        root: Optional[str] = None,
        exts: Optional[list[str]] = None,
        segmentation: Optional[str] = None,
        size: int = 1024,
        overlap: int = 0,
        with_text: bool = True,
        with_blame: bool = True,
        max_file_bytes: int = 0,
    ) -> list[PassageRecord]: ...

    # --- live co-editing (M8) ---
    # `open_coedit` also marks the path *live* (see `live_doc`); `end_coedit`
    # clears that marker once the session's final checkpoint has landed.
    async def open_coedit(self, ctx: WriteCtx, path: str) -> CoeditDoc: ...
    async def checkpoint_coedit(self, ctx: WriteCtx, path: str, doc: CoeditDoc) -> None: ...
    # The tree shape (#92): a Y.XmlFragment a rich-text editor binds to natively.
    # origofs does not own the document schema, so the *host* serializes and says
    # which byte ranges came from which co-edit node; origofs resolves each node to
    # the author it stamped itself. Bytes no span covers go to `ctx`.
    async def open_coedit_tree(
        self, ctx: WriteCtx, path: str, root: Optional[str] = None
    ) -> CoeditTreeDoc: ...
    async def checkpoint_coedit_tree(
        self,
        ctx: WriteCtx,
        path: str,
        doc: CoeditTreeDoc,
        body: bytes,
        spans: List[Tuple[int, int, str]],
    ) -> None: ...
    # Persist the CRDT alone, with no body -- durability for a shape only the host
    # can serialize. Deliberately does not stamp "last saved": the file has not moved.
    async def persist_coedit_tree(self, path: str, doc: CoeditTreeDoc) -> None: ...
    async def end_coedit(self, path: str) -> None: ...
    # Propose against a co-edited path as a CRDT merge rather than a whole file
    # body: base = the document's Yjs state vector, proposal = an
    # `encodeStateAsUpdate` blob, so `accept_suggestion` merges instead of
    # overwriting. The resulting suggestion's `kind` is `"crdt"`.
    async def suggest_coedit(
        self, ctx: WriteCtx, path: str, doc: CoeditDoc, summary: Optional[str] = None
    ) -> int: ...
    async def suggest_coedit_update(
        self,
        ctx: WriteCtx,
        path: str,
        base_sv: bytes,
        update: bytes,
        summary: Optional[str] = None,
    ) -> int: ...
    # --- live/dirty markers ---
    # A live path's durable bytes are a *checkpoint* that may lag the open Y.Doc.
    # These surface that; they never block, fail, or force a checkpoint. Each
    # marker is a dict with `path`, `session_id`, `actor_id`, `content_hash`
    # (the file's address as of the last checkpoint) and `since`.
    async def live_doc(self, path: str) -> Optional[LiveMarker]: ...
    async def live_paths(self) -> list[LiveMarker]: ...
    # `read` plus that marker: (bytes, live | None).
    async def read_live(self, path: str) -> tuple[bytes, Optional[LiveMarker]]: ...
    # Cross-worker relay (Postgres-backed workspaces). `is_postgres` gates it.
    def is_postgres(self) -> bool: ...
    async def coedit_relay_init(self) -> None: ...
    async def coedit_publish(self, path: str, origin: str, delta: bytes) -> None: ...
    async def coedit_replay(self, path: str) -> list[CoeditRelayNote]: ...
    async def coedit_subscribe(self) -> CoeditRelaySub: ...

    # --- live collaboration ---
    async def watch(self, after_seq: int = 0) -> list[EventRecord]: ...
    async def subscribe(self, after_seq: int = 0, branch: Optional[str] = None) -> Subscription: ...
    async def presence(self, window_secs: int = 60) -> list[PresenceRecord]: ...
    async def touch(self, actor_id: int, session_id: int, path: Optional[str] = None) -> None: ...

    # --- agent-suggestion review queue ---
    # Each suggestion dict carries a `kind`: `"bytes"` (a whole file body, whose
    # `base_hash` gates the accept) or `"crdt"` (a Yjs update, which merges). A
    # stale byte proposal is retired as `"superseded"` rather than left pending.
    async def suggest(self, ctx: WriteCtx, path: str, data: bytes, summary: Optional[str] = None) -> int: ...
    async def suggest_delete(self, ctx: WriteCtx, path: str, summary: Optional[str] = None) -> int: ...
    async def list_suggestions(self, status: Optional[str] = None, path: Optional[str] = None) -> list[SuggestionRecord]: ...
    async def get_suggestion(self, id: int) -> Optional[SuggestionRecord]: ...
    async def suggestion_diff(self, id: int) -> str: ...
    async def suggestion_content(self, id: int) -> SuggestionContent: ...
    async def accept_suggestion(self, id: int, approver: WriteCtx) -> None: ...
    async def reject_suggestion(self, id: int, approver: WriteCtx) -> None: ...

    # --- mounting / serving (Unix only) ---
    # FUSE/NFS are Unix-only (`#[cfg(unix)]` in lib.rs); off Unix both always
    # raise OSError synchronously when called -- note `serve_nfs` is a plain
    # sync method there too (not `async def`), since there's no awaitable to
    # produce. Use the HTTP API (origofs.fastapi) or embed the SDK there instead.
    # ── multi-workspace (docs/MULTI_TENANCY.md) ──────────────────────────────
    # Workspaces share the store's content and identity (actors, blame, audit) and
    # are separated by a `workspace_id`; each has its own root, refs, working tree,
    # suggestion queue, change feed, and presence. There is no actor->workspace
    # mapping in origofs — enforce that in whatever resolves identity.
    async def workspace(self, name: str) -> "Workspace": ...
    async def workspaces(self) -> list[str]: ...

    # ── attributed mutations (the §6 write policy) ───────────────────────────
    # The `*_as` / `*_or_propose` forms are the ones subject to an actor's write
    # policy. The bare `remove`/`rename`/`mkdir_p`/`commit`/`checkout`/
    # `create_branch` above are exempt by construction and record no blame — use
    # them only where there is genuinely no actor.
    async def remove_or_propose(
        self, ctx: WriteCtx, path: str, summary: Optional[str] = None
    ) -> WriteOutcome: ...
    async def rename_as(self, ctx: WriteCtx, from_: str, to: str) -> None: ...
    async def mkdir_as(self, ctx: WriteCtx, path: str) -> None: ...
    async def symlink_as(self, ctx: WriteCtx, target: str, linkpath: str) -> None: ...
    async def commit_as(self, ctx: WriteCtx, author: str, message: str) -> str: ...
    async def create_branch_as(self, ctx: WriteCtx, name: str) -> None: ...
    # Destructive: rematerializes the whole working tree, discarding uncommitted
    # edits. Raises PermissionError for a propose-only actor.
    async def checkout_as(self, ctx: WriteCtx, branch: str) -> None: ...
    # Raises PermissionError if the actor is propose-only. For administrative
    # operations that have no attributed variant of their own.
    async def ensure_may_write(self, ctx: WriteCtx, op: str) -> None: ...

    # ── symlinks ─────────────────────────────────────────────────────────────
    async def symlink(self, target: str, linkpath: str) -> None: ...
    async def readlink(self, path: str) -> str: ...

    # ── maintenance ──────────────────────────────────────────────────────────
    # `gc` returns {reachable, deleted, bytes_freed, skipped_young,
    # skipped_undated}. Safe alongside writers; a packed store needs `repack`
    # afterwards for the space to actually come back.
    async def gc(self) -> GcReport: ...
    async def gc_with_grace(self, grace_secs: int) -> GcReport: ...
    async def flush(self) -> None: ...
    async def repack(self) -> int: ...
    # The metadata DB is the half nothing can reconstruct: blame, the audit log,
    # actors, and uncommitted edits live only there. This is the thing to back up.
    async def backup_metadata(self, dest: str) -> str: ...
    async def reap_presence(self, grace_secs: int) -> int: ...
    async def supersede_stale_suggestions(self, path: str) -> int: ...
    # {ready, metadata, content} — each store None when healthy. Mirrors /readyz.
    async def ready(self) -> ReadyReport: ...

    # ── versioning: merge and mode ───────────────────────────────────────────
    # merge_branch returns {outcome, commit, conflicts}; `outcome` is one of
    # "already_up_to_date" | "fast_forward" | "merged" | "conflicts".
    async def merge_branch(
        self, branch: str, author: str, message: Optional[str] = None
    ) -> MergeResult: ...
    async def conflicts(self) -> list[ConflictRecord]: ...
    async def versioning_mode(self) -> str: ...
    async def set_versioning_mode(self, mode: str) -> None: ...

    # ── locks ────────────────────────────────────────────────────────────────
    async def lock(self, path: str, owner: str) -> bool: ...
    async def unlock(self, path: str, owner: str) -> bool: ...
    async def locks(self) -> list[LockRecord]: ...

    # ── attribution: the op-log and session revert ───────────────────────────
    # Remove exactly the lines an actor authored in one session, across every file
    # that session touched, leaving other actors' edits intact. Returns the number
    # of files changed.
    async def revert_session(
        self, actor_id: int, session_id: int, path_prefix: Optional[str] = None
    ) -> list[str]:
        """Remove exactly the lines ``actor_id`` authored in ``session_id``,
        across every file that session touched, and return the paths changed.

        ``path_prefix`` bounds the revert to one subtree, matched on directory
        boundaries — ``/tenant-a`` covers ``/tenant-a/notes.txt`` and never
        ``/tenant-abc/notes.txt``. Omit it to revert everywhere the session
        wrote."""
        ...
    async def edit_ops(
        self, actor_id: int, session_id: Optional[int] = None
    ) -> list[EditOp]: ...

    if sys.platform == "linux":
        def mount(self, mountpoint: str) -> Mount: ...
    else:
        def mount(self, mountpoint: str) -> NoReturn: ...

    if sys.platform != "win32":
        async def serve_nfs(
            self, addr: str, shutdown: Optional[Awaitable[Any]] = None
        ) -> None:
            """Serve NFSv3 at ``addr`` until cancelled, until ``shutdown`` (any
            awaitable, e.g. ``asyncio.Event().wait()``) resolves, or until the
            server fails. Either way the server is fully torn down before the
            call ends: the listener's fd/port is released and every
            per-connection task and socket goes with it."""
            ...
    else:
        def serve_nfs(
            self, addr: str, shutdown: Optional[Awaitable[Any]] = None
        ) -> NoReturn: ...

def content_hash(data: bytes) -> str:
    """The origofs content address (BLAKE3, hex) of ``data`` — the same hash a
    passage carries, so a Python pipeline can key derived/converted content by the
    same scheme."""
    ...

def fuse_mountable() -> bool:
    """Whether a FUSE mount is possible here (``/dev/fuse`` present). Always
    ``False`` off Linux: Windows has no FUSE, and the macOS wheel ships without it
    (macFUSE is a kernel extension a wheel can't carry) — use ``serve_nfs`` there."""
    ...
