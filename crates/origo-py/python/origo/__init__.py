"""origo — an agent-and-human filesystem, driven from Python.

The async workspace API (``Workspace``, ``WriteCtx``, ``Mount`` …) is implemented
in Rust and imported from the compiled ``origo._origo`` extension; everything it
exports is re-exported here, so ``import origo`` is unchanged::

    import origo
    ws = await origo.Workspace.open_local("meta.db", "cas")
    ctx = origo.WriteCtx.session(actor_id, session_id)
    await ws.write_as(ctx, "/notes.txt", b"hello")

The distribution on PyPI is ``origofs`` (the name ``origo`` was already taken by
an unrelated project); the import name is this one either way::

    pip install origofs

Optional integrations live in submodules you import explicitly (each pulls in
its own extra dependencies only when used):

    from origo.fastapi import build_router   # needs `pip install "origofs[fastapi]"`
"""
from importlib.metadata import PackageNotFoundError, version as _dist_version

from ._origo import (
    OrigoError,
    ConflictError,
    GcsConfig,
    Mount,
    S3Config,
    Subscription,
    Workspace,
    WriteCtx,
    fuse_mountable,
)

try:
    __version__ = _dist_version("origofs")
except PackageNotFoundError:  # running from a source tree, not an installed dist
    __version__ = "0.0.0+unknown"

__all__ = [
    "__version__",
    "OrigoError",
    "ConflictError",
    "GcsConfig",
    "Mount",
    "S3Config",
    "Subscription",
    "Workspace",
    "WriteCtx",
    "fuse_mountable",
]
