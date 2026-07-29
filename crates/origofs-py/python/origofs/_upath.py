"""universal-pathlib (`UPath`) support for the ``origofs://`` protocol.

`universal-pathlib <https://github.com/fsspec/universal_pathlib>`_ layers a
:class:`pathlib.Path`-style API over any fsspec filesystem. Because
:class:`origofs.fsspec.OrigoFileSystem` is a well-behaved, POSIX-shaped filesystem
(absolute ``/`` paths, a real directory tree, standard ``info``/``ls``), UPath's
generic implementation already drives it correctly. This module exists only to
register an *explicit* ``UPath`` subclass for the protocol — wired up through the
``universal_pathlib.implementations`` entry point — so ``UPath("origofs://…")``
resolves to a named class instead of the fallback, and UPath doesn't warn that the
filesystem is "not explicitly implemented".

No behavior is customized: the default flavour is correct for origofs. Use it like
any UPath::

    from upath import UPath
    root = UPath("origofs:///", db_path="meta.db", cas_dir="cas")   # storage_options
    (root / "notes.txt").write_text("hello")
    for child in root.iterdir():
        ...

Attribution and every other origofs-specific capability live on the underlying
:class:`~origofs.fsspec.OrigoFileSystem` (``UPath(...).fs``) and the
:class:`origofs.Workspace` behind it.

Requires universal-pathlib: ``pip install "origofs[upath]"``.
"""
from __future__ import annotations

try:
    from upath import UPath
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        'origofs UPath support requires universal-pathlib. Install it with: '
        'pip install "origofs[upath]"'
    ) from exc

# Importing this registers OrigoFileSystem in fsspec's live class registry. upath
# imports this entry-point module before it resolves the protocol's path "flavour"
# from that registry, so the registration must have happened by now — otherwise
# upath can't find the class and falls back to a synthesized default (with a
# warning). This import guarantees it's there.
from . import fsspec as _fsspec_module  # noqa: F401

__all__ = ["OrigofsPath"]


class OrigofsPath(UPath):
    """A :class:`~upath.UPath` over an origofs workspace (the ``origofs://`` protocol).

    Behaves as UPath's default filesystem-shaped path; see the module docstring.
    """

    __slots__ = ()
