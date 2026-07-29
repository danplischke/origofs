"""An ``fsspec`` filesystem over an origofs workspace — async-native.

`fsspec <https://filesystem-spec.readthedocs.io/>`_ is the interface pandas,
Dask, PyArrow, Polars, Zarr, and much of the PyData stack use to read and write
"a filesystem" without caring which one. This module makes an origofs workspace one
of those filesystems, so those tools — and anything else that speaks fsspec — can
read and write origofs paths directly::

    import origofs.fsspec  # registers the "origofs" protocol
    import pandas as pd

    df = pd.read_parquet("origofs:///data/events.parquet",
                         storage_options={"db_path": "meta.db", "cas_dir": "cas"})

Because every origofs I/O method is already a coroutine (the extension is built on
`pyo3-async-runtimes`), this is a genuine :class:`fsspec.asyn.AsyncFileSystem`:
the ``_``-prefixed methods are real ``async def`` you can ``await`` on your own
event loop, and fsspec derives the blocking versions (``fs.ls``, ``fs.cat`` …)
from them for synchronous callers. Both work::

    from origofs.fsspec import OrigoFileSystem

    # sync — runs on fsspec's background loop
    fs = OrigoFileSystem(db_path="meta.db", cas_dir="cas")
    fs.pipe_file("/notes.txt", b"hello")
    assert fs.cat_file("/notes.txt") == b"hello"

    # async — on your loop
    fs = OrigoFileSystem(db_path="meta.db", cas_dir="cas", asynchronous=True)
    await fs._pipe_file("/notes.txt", b"hello")
    data = await fs._cat_file("/notes.txt", start=0, end=5)

Two ways to point it at a workspace:

* **Connection kwargs** — ``backend=`` plus its parameters (``"local"`` is the
  default when ``db_path``/``cas_dir`` are given). This is what
  ``fsspec.filesystem("origofs", ...)`` and ``storage_options=`` pass, and it lets
  fsspec open (and cache) the workspace for you, lazily, on first use::

      fsspec.filesystem("origofs", backend="pg_s3", dsn=dsn, s3={"bucket": "b", "region": "r"})

* **A live workspace** — ``ws=`` an already-open :class:`origofs.Workspace`. The
  escape hatch that works for *every* backend and shares one workspace with the
  rest of your app (a FastAPI server, an agent runner)::

      ws = await origofs.Workspace.open_pg_s3(dsn, cfg)
      fs = OrigoFileSystem(ws=ws)

**Attribution.** origofs records *who* wrote each byte. Pass ``actor=`` (and
optionally ``session=``), or a ready ``ctx=origofs.WriteCtx``, and every write this
filesystem makes is attributed to that principal (``write_as``) and shows up in
``blame``. Without one, writes are unattributed (plain ``write``). You can also
override per call — ``fs.pipe_file(path, data, actor=42)`` or
``fs.open(path, "wb", actor=42)``. As everywhere in origofs, the *caller* owns
identity; nothing here trusts a client to name someone else.

**Freshness.** An origofs working tree is live — humans and agents write to it
concurrently — so this filesystem disables fsspec's directory-listing cache by
default (``use_listings_cache=False``); every ``ls``/``info`` reflects current
state. Pass ``use_listings_cache=True`` if you want the caching behavior.

Requires fsspec: ``pip install "origofs[fsspec]"``.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Optional

try:
    from fsspec.asyn import AsyncFileSystem, sync
    from fsspec.spec import AbstractBufferedFile
    from fsspec.utils import stringify_path
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        'origofs.fsspec requires fsspec. Install it with: pip install "origofs[fsspec]"'
    ) from exc

__all__ = ["OrigoFileSystem", "OrigoBufferedFile"]

# A length that ``Workspace.read_range`` clamps to EOF — "read to the end".
_READ_TO_EOF = (1 << 63) - 1

# backend -> the positional parameter names its Workspace.open_* constructor needs
# (S3/GCS backends additionally take an `s3=`/`gcs=` config, handled separately).
_BACKENDS: dict[str, tuple[str, ...]] = {
    "local": ("db_path", "cas_dir"),
    "local_packed": ("db_path", "data_dir", "index_dir"),
    "memory": ("db_path",),
    "pg": ("dsn", "cas_dir"),
    "s3": ("db_path",),
    "s3_packed": ("db_path", "index_dir"),
    "pg_s3": ("dsn",),
    "pg_s3_packed": ("dsn", "index_dir"),
    "gcs": ("db_path",),
    "gcs_packed": ("db_path", "index_dir"),
    "pg_gcs": ("dsn",),
    "pg_gcs_packed": ("dsn", "index_dir"),
}


class OrigoFileSystem(AsyncFileSystem):
    """An :mod:`fsspec` filesystem backed by an origofs workspace.

    See the module docstring for the full story. In short: construct it with
    ``ws=<open Workspace>`` or with ``backend=`` + connection kwargs, optionally
    attribute writes with ``actor=``/``session=``/``ctx=``, then use it like any
    fsspec filesystem — synchronously (``fs.ls`` …) or, with
    ``asynchronous=True``, by awaiting the ``_``-prefixed coroutines.
    """

    protocol = "origofs"
    root_marker = "/"

    def __init__(
        self,
        ws: Any = None,
        *,
        backend: Optional[str] = None,
        db_path: Optional[str] = None,
        cas_dir: Optional[str] = None,
        data_dir: Optional[str] = None,
        index_dir: Optional[str] = None,
        dsn: Optional[str] = None,
        s3: Any = None,
        gcs: Any = None,
        ctx: Any = None,
        actor: Optional[int] = None,
        session: Optional[int] = None,
        asynchronous: bool = False,
        loop: Any = None,
        **kwargs: Any,
    ) -> None:
        # An origofs working tree is mutable and multi-writer; stale listings would
        # be a footgun. Default the fsspec listing cache off (caller can override).
        kwargs.setdefault("use_listings_cache", False)
        super().__init__(asynchronous=asynchronous, loop=loop, **kwargs)

        # A pre-opened workspace wins over every connection parameter.
        self._ws = ws
        self._open_lock: Optional[asyncio.Lock] = None

        # Resolve the backend: explicit, else inferred from the params present.
        if ws is None:
            if backend is None:
                if db_path is not None and cas_dir is not None:
                    backend = "local"
                else:
                    raise ValueError(
                        "OrigoFileSystem needs either ws=<open Workspace> or a "
                        "backend=... with its parameters (e.g. backend='local', "
                        "db_path=..., cas_dir=...)."
                    )
            if backend not in _BACKENDS:
                raise ValueError(
                    f"unknown origofs backend {backend!r}; expected one of "
                    f"{', '.join(sorted(_BACKENDS))}"
                )
        self._backend = backend
        self._params = {
            "db_path": db_path,
            "cas_dir": cas_dir,
            "data_dir": data_dir,
            "index_dir": index_dir,
            "dsn": dsn,
        }
        self._s3 = s3
        self._gcs = gcs

        # Attribution: a ready WriteCtx, or (actor, session) to build one lazily.
        self._ctx_obj = ctx
        self._actor = actor
        self._session = session

    # --- workspace lifecycle ------------------------------------------------

    async def _get_ws(self) -> Any:
        """The open workspace, opening (and caching) it on first use."""
        if self._ws is not None:
            return self._ws
        if self._open_lock is None:
            self._open_lock = asyncio.Lock()
        async with self._open_lock:
            if self._ws is None:
                self._ws = await self._open_workspace()
        return self._ws

    def _req(self, name: str) -> str:
        val = self._params.get(name)
        if val is None:
            raise ValueError(f"origofs backend {self._backend!r} requires {name!r}")
        return val

    def _s3_config(self, origofs: Any) -> Any:
        if self._s3 is None:
            raise ValueError(
                f"origofs backend {self._backend!r} requires an s3=... config "
                "(a dict of S3Config kwargs, or an origofs.S3Config)"
            )
        return self._s3 if isinstance(self._s3, origofs.S3Config) else origofs.S3Config(**self._s3)

    def _gcs_config(self, origofs: Any) -> Any:
        if self._gcs is None:
            raise ValueError(
                f"origofs backend {self._backend!r} requires a gcs=... config "
                "(a dict of GcsConfig kwargs, or an origofs.GcsConfig)"
            )
        return self._gcs if isinstance(self._gcs, origofs.GcsConfig) else origofs.GcsConfig(**self._gcs)

    async def _open_workspace(self) -> Any:
        import origofs

        W = origofs.Workspace
        b = self._backend
        if b == "local":
            return await W.open_local(self._req("db_path"), self._req("cas_dir"))
        if b == "local_packed":
            return await W.open_local_packed(
                self._req("db_path"), self._req("data_dir"), self._req("index_dir")
            )
        if b == "memory":
            return await W.open_object_memory(self._req("db_path"))
        if b == "pg":
            return await W.open_pg(self._req("dsn"), self._req("cas_dir"))
        if b == "s3":
            return await W.open_s3(self._req("db_path"), self._s3_config(origofs))
        if b == "s3_packed":
            return await W.open_s3_packed(
                self._req("db_path"), self._s3_config(origofs), self._req("index_dir")
            )
        if b == "pg_s3":
            return await W.open_pg_s3(self._req("dsn"), self._s3_config(origofs))
        if b == "pg_s3_packed":
            return await W.open_pg_s3_packed(
                self._req("dsn"), self._s3_config(origofs), self._req("index_dir")
            )
        if b == "gcs":
            return await W.open_gcs(self._req("db_path"), self._gcs_config(origofs))
        if b == "gcs_packed":
            return await W.open_gcs_packed(
                self._req("db_path"), self._gcs_config(origofs), self._req("index_dir")
            )
        if b == "pg_gcs":
            return await W.open_pg_gcs(self._req("dsn"), self._gcs_config(origofs))
        if b == "pg_gcs_packed":
            return await W.open_pg_gcs_packed(
                self._req("dsn"), self._gcs_config(origofs), self._req("index_dir")
            )
        raise ValueError(f"unknown origofs backend {b!r}")

    async def get_workspace(self) -> Any:
        """The underlying open :class:`origofs.Workspace` (await it in async mode).

        Everything origofs can do that fsspec has no vocabulary for — suggestions,
        commits and branches, the change feed, presence, disaster recovery — lives
        on this object. The sync twin is the :attr:`workspace` property.
        """
        return await self._get_ws()

    @property
    def workspace(self) -> Any:
        """The underlying open :class:`origofs.Workspace` (blocking; sync mode).

        In ``asynchronous=True`` mode use :meth:`get_workspace` instead.
        """
        if self.asynchronous:
            raise RuntimeError(
                "use `await fs.get_workspace()` on an asynchronous OrigoFileSystem"
            )
        return sync(self.loop, self._get_ws)

    # --- attribution --------------------------------------------------------

    def _default_ctx(self) -> Any:
        """The instance-wide WriteCtx (built once from actor/session), or None."""
        if self._ctx_obj is None and self._actor is not None:
            import origofs

            self._ctx_obj = (
                origofs.WriteCtx.session(self._actor, self._session)
                if self._session is not None
                else origofs.WriteCtx.actor(self._actor)
            )
        return self._ctx_obj

    def _resolve_ctx(self, kwargs: dict) -> Any:
        """Pop a per-call attribution override from ``kwargs``, else the default.

        Accepts ``ctx=<WriteCtx>`` or ``actor=``/``session=``; consumes them so
        they don't leak into the workspace call.
        """
        ctx = kwargs.pop("ctx", None)
        actor = kwargs.pop("actor", None)
        session = kwargs.pop("session", None)
        if ctx is not None:
            return ctx
        if actor is not None:
            import origofs

            return (
                origofs.WriteCtx.session(actor, session)
                if session is not None
                else origofs.WriteCtx.actor(actor)
            )
        return self._default_ctx()

    # --- path helpers -------------------------------------------------------

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        """Normalize any accepted spelling to an absolute origofs path (``/a/b``)."""
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        for proto in ("origofs://", "origofs:"):
            if path.startswith(proto):
                path = path[len(proto):]
                break
        path = path.strip()
        # collapse to a single leading slash, drop a trailing one (except root)
        return "/" + path.strip("/") if path.strip("/") else "/"

    @staticmethod
    def _child(parent: str, name: str) -> str:
        return f"/{name}" if parent == "/" else f"{parent}/{name}"

    def _inode_info(self, path: str, st: dict) -> dict:
        """An fsspec info dict from an origofs inode ``stat`` dict."""
        kind = st["kind"]
        info = {
            "name": path,
            "size": st["size"],
            "type": "directory" if kind == "dir" else "file",
            "kind": kind,
            "islink": kind == "symlink",
            "ino": st["ino"],
            "mode": st["mode"],
            "mtime": st["mtime"],
            "ctime": st["ctime"],
        }
        if st.get("content") is not None:
            info["content"] = st["content"]
        return info

    # --- reads --------------------------------------------------------------

    async def _info(self, path: str, **kwargs: Any) -> dict:
        p = self._strip_protocol(path)
        if p == "/":  # the root is always a directory (and needn't be stat-able)
            return {"name": "/", "size": 0, "type": "directory", "kind": "dir", "islink": False}
        ws = await self._get_ws()
        st = await ws.stat(p)  # raises FileNotFoundError for a missing path
        return self._inode_info(p, st)

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> list:
        ws = await self._get_ws()
        p = self._strip_protocol(path)
        info = await self._info(p)
        if info["type"] != "directory":
            return [info] if detail else [info["name"]]
        entries = await ws.ls(p)
        names = [self._child(p, e["name"]) for e in entries]
        if not detail:
            return names
        return list(await asyncio.gather(*(self._info(n) for n in names)))

    async def _cat_file(
        self, path: str, start: Optional[int] = None, end: Optional[int] = None, **kwargs: Any
    ) -> bytes:
        ws = await self._get_ws()
        p = self._strip_protocol(path)
        # Fall back to a whole-file read + slice if the extension predates read_range.
        if not hasattr(ws, "read_range"):
            data = bytes(await ws.read(p))
            return data[start:end]
        # Fast path: non-negative offsets map straight to a clamped range read, so
        # only the chunks covering [start, end) are fetched from the content store.
        if (start is None or start >= 0) and (end is None or end >= 0):
            off = start or 0
            if end is None:
                length = _READ_TO_EOF
            else:
                length = end - off
                if length <= 0:
                    return b""
            return bytes(await ws.read_range(p, off, length))
        # Negative offsets (suffix reads): normalize against the file size.
        size = (await self._info(p))["size"]
        s = 0 if start is None else (start if start >= 0 else max(0, size + start))
        e = size if end is None else (end if end >= 0 else max(0, size + end))
        if e <= s:
            return b""
        return bytes(await ws.read_range(p, s, e - s))

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        try:
            await self._info(path)
            return True
        except FileNotFoundError:
            return False

    # --- writes -------------------------------------------------------------

    async def _pipe_file(self, path: str, value: Any, mode: str = "overwrite", **kwargs: Any) -> None:
        ws = await self._get_ws()
        p = self._strip_protocol(path)
        if mode == "create" and await self._exists(p):
            raise FileExistsError(p)
        ctx = self._resolve_ctx(kwargs)
        parent = self._parent(p)
        if parent not in ("", "/"):
            await ws.mkdir_p(parent)
        data = bytes(value)
        if ctx is not None:
            await ws.write_as(ctx, p, data)
        else:
            await ws.write(p, data)
        self.invalidate_cache(parent)
        self.invalidate_cache(p)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        ws = await self._get_ws()
        p = self._strip_protocol(path)
        await ws.remove(p)
        self.invalidate_cache(self._parent(p))

    async def _rm(
        self, path: Any, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs: Any
    ) -> None:
        # origofs removes only files and *empty* directories, so we can't hand the
        # default reverse-order rm a tree. Expand, delete files, then delete the
        # now-empty directories deepest-first. A missing target surfaces as
        # FileNotFoundError (from _expand_path or _info), matching fsspec/POSIX rm.
        paths = await self._expand_path(path, recursive=recursive, maxdepth=maxdepth)
        ws = await self._get_ws()
        infos = await asyncio.gather(*(self._info(p) for p in paths))
        files = [i["name"] for i in infos if i["type"] != "directory"]
        dirs = [i["name"] for i in infos if i["type"] == "directory"]
        await asyncio.gather(*(self._rm_file(f) for f in files))
        for d in sorted(set(dirs), key=lambda x: x.count("/"), reverse=True):
            if d == "/":
                continue
            await ws.remove(d)
            self.invalidate_cache(self._parent(d))

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        # A recursive copy expands to files *and* directories and calls _cp_file on
        # each; a directory source is reproduced as a directory (so empty dirs
        # survive the copy), not read as bytes.
        if await self._isdir(path1):
            await self._makedirs(path2, exist_ok=True)
            return
        data = await self._cat_file(path1)
        await self._pipe_file(path2, data, **kwargs)

    async def _mv(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Native, atomic rename/move (origofs moves the inode, subtree and all)."""
        ws = await self._get_ws()
        a, b = self._strip_protocol(path1), self._strip_protocol(path2)
        parent = self._parent(b)
        if parent not in ("", "/"):
            await ws.mkdir_p(parent)
        await ws.rename(a, b)
        for pth in {self._parent(a), self._parent(b), a, b}:
            self.invalidate_cache(pth)

    def mv(self, path1: str, path2: str, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs: Any) -> None:
        """Move/rename a path (native origofs rename; blocking)."""
        return sync(self.loop, self._mv, path1, path2, **kwargs)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        ws = await self._get_ws()
        p = self._strip_protocol(path)
        if await self._exists(p):
            raise FileExistsError(p)
        parent = self._parent(p)
        if not create_parents and parent not in ("", "/") and not await self._exists(parent):
            raise FileNotFoundError(parent)
        await ws.mkdir_p(p)
        self.invalidate_cache(parent)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        ws = await self._get_ws()
        p = self._strip_protocol(path)
        if not exist_ok and await self._exists(p):
            raise FileExistsError(p)
        await ws.mkdir_p(p)
        self.invalidate_cache(self._parent(p))

    async def _rmdir(self, path: str) -> None:
        ws = await self._get_ws()
        p = self._strip_protocol(path)
        await ws.remove(p)  # errors if not empty (mapped from DirectoryNotEmpty)
        self.invalidate_cache(self._parent(p))

    def rmdir(self, path: str) -> None:
        """Remove an empty directory (blocking)."""
        return sync(self.loop, self._rmdir, path)

    # --- local <-> workspace transfer (single files; _get/_put orchestrate) --

    async def _get_file(self, rpath: str, lpath: Any, **kwargs: Any) -> None:
        if await self._isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
            return
        data = await self._cat_file(rpath)
        parent = os.path.dirname(lpath)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(lpath, "wb") as f:
            f.write(data)

    async def _put_file(self, lpath: Any, rpath: str, mode: str = "overwrite", **kwargs: Any) -> None:
        if os.path.isdir(lpath):
            await self._makedirs(rpath, exist_ok=True)
            return
        with open(lpath, "rb") as f:
            data = f.read()
        await self._pipe_file(rpath, data, mode=mode, **kwargs)

    # --- file objects -------------------------------------------------------

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[dict] = None,
        **kwargs: Any,
    ) -> "OrigoBufferedFile":
        p = self._strip_protocol(path)
        if "x" in mode and self.exists(p):
            raise FileExistsError(p)
        return OrigoBufferedFile(
            self,
            p,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    # --- origofs-native conveniences (attribution / versioning) -----------------
    # fsspec has no vocabulary for these; they're the reason to reach for origofs.
    # The async `_x` forms are for asynchronous=True; the blocking twins for sync.

    async def _blame(self, path: str) -> list:
        ws = await self._get_ws()
        return await ws.blame(self._strip_protocol(path))

    def blame(self, path: str) -> list:
        """Per-byte-range authorship for a path (blocking). See ``Workspace.blame``."""
        return sync(self.loop, self._blame, path)

    async def _commit(self, message: str, author: str = "origofs") -> str:
        ws = await self._get_ws()
        return await ws.commit(author, message)

    def commit(self, message: str, author: str = "origofs") -> str:
        """Snapshot the working tree into a commit; returns its hash (blocking)."""
        return sync(self.loop, self._commit, message, author)

    async def _log(self) -> list:
        ws = await self._get_ws()
        return await ws.log()

    def log(self) -> list:
        """Commit history, newest first (blocking). See ``Workspace.log``."""
        return sync(self.loop, self._log)


class OrigoBufferedFile(AbstractBufferedFile):
    """A buffered file over an origofs path.

    Reads pull byte ranges on demand through ``read_range`` (so a large file isn't
    slurped whole). Writes buffer in memory and land as a single attributed write
    when the file closes — origofs content is immutable and addressed whole-file, so
    an open writable file is staged and committed atomically on ``close()`` rather
    than streamed. Any ``ctx=``/``actor=``/``session=`` passed to ``open`` rides
    along, attributing that final write.
    """

    def __init__(self, fs: OrigoFileSystem, path: str, mode: str = "rb", **kwargs: Any) -> None:
        # Capture attribution to replay on the final write (on close).
        self._ctx_kwargs = {k: kwargs.pop(k) for k in ("ctx", "actor", "session") if k in kwargs}
        self._commit = bytearray()
        super().__init__(fs, path, mode=mode, **kwargs)

    def _fetch_range(self, start: int, end: int) -> bytes:
        return self.fs.cat_file(self.path, start=start, end=end)

    def _initiate_upload(self) -> None:
        self._commit = bytearray()
        if "a" in self.mode:  # append: seed with the current contents
            try:
                self._commit += self.fs.cat_file(self.path)
            except FileNotFoundError:
                pass

    def _upload_chunk(self, final: bool = False) -> bool:
        # Accumulate every flushed block; only commit the whole file at the end
        # (origofs replaces file content wholesale — there is no partial write).
        self._commit += self.buffer.getvalue()
        if final:
            self.fs.pipe_file(self.path, bytes(self._commit), **self._ctx_kwargs)
        return True


# Register the "origofs://" protocol on import, so `import origofs.fsspec` is enough
# for `fsspec.filesystem("origofs")` even where entry-point discovery isn't wired up.
try:  # pragma: no cover - trivial, and fsspec is a hard dependency of this module
    import fsspec as _fsspec

    _fsspec.register_implementation("origofs", OrigoFileSystem, clobber=True)
except Exception:  # pragma: no cover
    pass
