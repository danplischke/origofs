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
import sys

from ._origofs import (
    OrigoFSError,
    ConflictError,
    StaleBaseError,
    ForeignWriteError,
    AlreadyResolvedError,
    CoeditDoc,
    CoeditRelayNote,
    CoeditRelaySub,
    CoeditSyncReply,
    CoeditTreeDoc,
    CacheConfig,
    GcsConfig,
    S3Config,
    Scope,
    Subscription,
    Workspace,
    WriteCtx,
    WriteOutcome,
    content_hash,
    fuse_mountable,
)

# Set by the extension from `CARGO_PKG_VERSION` (see the `m.add` in src/lib.rs),
# so it is the crate version the wheel was actually built from rather than a
# second copy to keep in step. The redundant-looking `as __version__` is the
# explicit re-export form type checkers require.
#
# Deliberately *not* in `__all__`: a caller's `from origofs import *` would then
# overwrite their own module's `__version__`.
from ._origofs import __version__ as __version__

# A structural type for the subset of `Workspace` a host builds on (#163). Pure
# Python and import-cheap (its record types are `TYPE_CHECKING`-only), so it can
# live here rather than behind an extra import.
from .protocol import WorkspaceProtocol as WorkspaceProtocol

__all__ = [
    "OrigoFSError",
    "ConflictError",
    "StaleBaseError",
    "ForeignWriteError",
    "AlreadyResolvedError",
    "CoeditDoc",
    "CoeditRelayNote",
    "CoeditRelaySub",
    "CoeditSyncReply",
    "CoeditTreeDoc",
    "CacheConfig",
    "GcsConfig",
    "S3Config",
    "Scope",
    "Subscription",
    "Workspace",
    "WriteCtx",
    "WriteOutcome",
    "content_hash",
    "fuse_mountable",
    "WorkspaceProtocol",
]

# `Mount` is registered by the extension only on Linux (`#[cfg(target_os =
# "linux")]` in src/lib.rs): FUSE has no Windows equivalent, and on macOS it needs
# the macFUSE kernel extension, which a `pip install` cannot provide.
#
# Importing it unconditionally therefore made `import origofs` raise
#     ImportError: cannot import name 'Mount' from 'origofs._origofs'
# on every non-Linux wheel — so the published macOS and Windows wheels installed
# cleanly and then could not be imported at all. Nothing caught it because no
# wheel had ever been built or run outside Linux (#107).
#
# `__init__.pyi` already gates the class behind this same `sys.platform` check,
# so this is the runtime catching up with the typed contract rather than a change
# to it. `Workspace.mount()` still exists on every platform and raises a clear
# OSError off Linux, which is the surface a caller actually reaches for.
if sys.platform == "linux":
    from ._origofs import Mount

    __all__.append("Mount")
