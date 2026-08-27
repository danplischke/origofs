"""Run an agent in a live native overlay over an origofs workspace, from Python.

The overlay is *host orchestration* — an unprivileged kernel overlayfs plus a
live upper->origofs sync loop, supervising an external agent process — provided by
the ``origofs`` CLI, not by this embedded library. Reimplementing that privileged
machinery inside the extension would buy nothing, so this module is a thin async
wrapper that shells out to the CLI: the agent works in a fast native mount while
its edits stream into origofs, attributed to ``actor``, as it goes.

The overlay operates on a CLI-managed workspace **directory** (holding
``meta.db`` and ``cas/``) — the same one the API opens as
``Workspace.open_local(f"{ws}/meta.db", f"{ws}/cas")``. So a FastAPI server can
serve the workspace over HTTP *and* launch agents into an overlay over the same
store:

    import origofs
    from origofs.overlay import run

    ws_dir = "./ws"
    api = await origofs.Workspace.open_local(f"{ws_dir}/meta.db", f"{ws_dir}/cas")
    actor = await api.find_or_create_agent("agent-token", "claude", "opus")
    code = await run(ws_dir, actor, ["claude", "-p", "refactor the parser"])

Requires the ``origofs`` binary on PATH (``cargo install --path crates/origofs-cli``) and
a Linux host with unprivileged user-namespace overlay support
(``origofs.overlay`` does not run on macOS/Windows).
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Sequence

__all__ = ["overlay_command", "run"]


def overlay_command(
    workspace_dir: "os.PathLike[str] | str",
    actor: int,
    cmd: Sequence[str],
    *,
    sync_ms: int = 500,
    origofs_bin: str = "origofs",
) -> list[str]:
    """The argv that runs ``cmd`` in a live overlay over ``workspace_dir``,
    attributing the agent's edits to ``actor`` and syncing every ``sync_ms`` ms.
    """
    cmd = list(cmd)
    if not cmd:
        raise ValueError("cmd must be a non-empty command, e.g. ['claude', '-p', '...']")
    return [
        origofs_bin,
        "--workspace",
        os.fspath(workspace_dir),
        "overlay",
        "--actor",
        str(actor),
        "--sync-ms",
        str(sync_ms),
        "--",
        *cmd,
    ]


async def run(
    workspace_dir: "os.PathLike[str] | str",
    actor: int,
    cmd: Sequence[str],
    *,
    sync_ms: int = 500,
    origofs_bin: str = "origofs",
    **subprocess_kwargs: Any,
) -> int:
    """Run ``cmd`` in a live overlay over ``workspace_dir`` and wait for it to
    exit, returning the agent's exit code. The agent's file changes stream into
    origofs attributed to ``actor``. Extra keyword args are forwarded to
    :func:`asyncio.create_subprocess_exec` (e.g. ``cwd``, ``env``, ``stdout``).
    """
    argv = overlay_command(workspace_dir, actor, cmd, sync_ms=sync_ms, origofs_bin=origofs_bin)
    proc = await asyncio.create_subprocess_exec(*argv, **subprocess_kwargs)
    return await proc.wait()
