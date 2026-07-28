"""origofs — an agent-and-human filesystem, driven from Python.

The async workspace API (``Workspace``, ``WriteCtx``, ``Mount`` …) is implemented
in Rust and imported from the compiled ``origofs._origofs`` extension; everything it
exports is re-exported here, so ``import origofs`` is unchanged::

    import origofs
    ws = await origofs.Workspace.open_local("meta.db", "cas")
    ctx = origofs.WriteCtx.session(actor_id, session_id)
    await ws.write_as(ctx, "/notes.txt", b"hello")

Optional integrations live in submodules you import explicitly (each pulls in
its own extra dependencies only when used):

    from origofs.fastapi import build_router   # needs `pip install "origofs[fastapi]"`
"""
from ._origofs import (
    OrigoFSError,
    ConflictError,
    CoeditDoc,
    CoeditRelayNote,
    CoeditRelaySub,
    CoeditSyncReply,
    GcsConfig,
    Mount,
    S3Config,
    Subscription,
    Workspace,
    WriteCtx,
    fuse_mountable,
)

__all__ = [
    "OrigoFSError",
    "ConflictError",
    "CoeditDoc",
    "CoeditRelayNote",
    "CoeditRelaySub",
    "CoeditSyncReply",
    "GcsConfig",
    "Mount",
    "S3Config",
    "Subscription",
    "Workspace",
    "WriteCtx",
    "fuse_mountable",
]
