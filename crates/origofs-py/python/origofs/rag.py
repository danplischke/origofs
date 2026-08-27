"""Technology-agnostic retrieval passages over an origofs workspace.

This is the framework-neutral core of origofs's RAG support. It turns a workspace
into provenance-carrying :class:`Passage` records and stops there — **no
embeddings, no vector store, no framework types**. Point any stack at the records:
LlamaIndex (via :mod:`origofs.llamaindex`), LangChain, Haystack, or your own loop.

Two document kinds, two provenance grades:

* **Native text** (``.md``, ``.txt``, code, …) is split by the Rust core
  (``Workspace.passages``), so each passage carries *precise* per-byte
  :class:`~origofs.Author` blame — a retrieved passage knows exactly who wrote it.
* **Non-text documents** (PDF, DOCX, PPTX, images, HTML, …) are projected to
  Markdown by a pluggable :class:`Converter` (see
  :class:`origofs.converters.MarkItDownConverter`). Their provenance is
  *document-level* — the source path, who added the document, and which converter
  produced the text — because the extracted Markdown no longer maps to the
  original bytes, so byte-accurate blame would be a lie.

Everything is customizable: swap the :class:`Converter` (or pass a plain
``callable(path, data, mime) -> str | None``), override which extensions count as
native text (``text_exts=``), or bring your own splitter for converted text
(``segment=``).
"""
from __future__ import annotations

import mimetypes
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional, Protocol, Union, runtime_checkable

from . import content_hash

__all__ = ["Author", "Passage", "Converter", "read_passages", "DEFAULT_TEXT_EXTS"]


# --- provenance records -----------------------------------------------------


@dataclass(frozen=True)
class Author:
    """One actor credited with (part of) a passage."""

    id: int
    name: str
    kind: str  # "human" | "agent" | "system"


@dataclass
class Passage:
    """A provenance-carrying passage, ready to embed and index."""

    path: str
    text: str
    byte_start: int
    byte_end: int
    #: Content address of the passage bytes — dedup / incremental-embedding key.
    hash: str
    #: Authors of this passage (precise for native text; the document's author(s)
    #: for converted docs).
    authors: list[Author] = field(default_factory=list)
    #: For a converted document, the original source path (else ``None``).
    source: Optional[str] = None
    #: For a converted document, ``{"id", "version"}`` of the converter (else ``None``).
    converter: Optional[dict] = None

    @property
    def metadata(self) -> dict[str, Any]:
        """A flat provenance dict to hang on a framework ``Document``/``Node``."""
        md: dict[str, Any] = {
            "path": self.path,
            "byte_start": self.byte_start,
            "byte_end": self.byte_end,
            "passage_hash": self.hash,
            "authors": [a.name for a in self.authors],
            "author_ids": [a.id for a in self.authors],
        }
        if self.source is not None:
            md["source"] = self.source
        if self.converter is not None:
            md["converter"] = f"{self.converter.get('id')}@{self.converter.get('version')}"
        return md


# --- the converter seam -----------------------------------------------------


@runtime_checkable
class Converter(Protocol):
    """Projects a non-text document to Markdown/plain text.

    Return ``None`` to decline a document (e.g. it's already text you'd rather
    keep native, or the format isn't supported) — the reader then treats it as
    native text or skips it. ``id``/``version`` identify the converter for
    provenance and cache-keying.
    """

    id: str
    version: str

    def convert(self, path: str, data: bytes, mime: Optional[str]) -> Optional[str]: ...


#: A :class:`Converter`, or a plain ``callable(path, data, mime) -> str | None``.
ConverterLike = Union[Converter, Callable[[str, bytes, Optional[str]], Optional[str]]]


class _CallableConverter:
    """Adapt a bare callable to the :class:`Converter` protocol."""

    def __init__(self, fn: Callable[..., Optional[str]], id: str = "callable", version: str = "") -> None:
        self._fn = fn
        self.id = id
        self.version = version

    def convert(self, path: str, data: bytes, mime: Optional[str]) -> Optional[str]:
        return self._fn(path, data, mime)


def _as_converter(convert: Optional[ConverterLike]) -> Optional[Converter]:
    if convert is None or isinstance(convert, Converter):
        return convert  # type: ignore[return-value]
    if callable(convert):
        return _CallableConverter(convert)
    raise TypeError("convert must be a Converter, a callable, or None")


# --- extensions treated as native text (never converted) --------------------

DEFAULT_TEXT_EXTS = frozenset(
    """
    md markdown mdx txt text rst adoc log csv tsv json jsonl ndjson yaml yml toml
    ini cfg conf env py pyi rs go js jsx ts tsx c h cc cpp hpp cs java kt kts rb
    php swift scala sh bash zsh sql graphql proto tex bib
    """.split()
)


# --- extraction -------------------------------------------------------------


async def read_passages(
    ws: Any,
    *,
    root: str = "/",
    exts: Optional[Iterable[str]] = None,
    segmentation: str = "content_defined",
    size: int = 1024,
    overlap: int = 0,
    with_blame: bool = True,
    convert: Optional[ConverterLike] = None,
    text_exts: Iterable[str] = DEFAULT_TEXT_EXTS,
    segment: Optional[Callable[[str], list[str]]] = None,
) -> list[Passage]:
    """Extract :class:`Passage` records from ``ws`` under ``root``.

    Native-text files are split by the core (precise blame); everything else is
    offered to ``convert`` and, if it returns Markdown, split here with
    document-level provenance. ``exts`` restricts which files are considered at
    all; ``text_exts`` decides which are treated as native text; ``segment`` (a
    ``callable(text) -> list[str]``) overrides splitting for *converted* text.
    """
    convert = _as_converter(convert)
    text_exts = {e.lower().lstrip(".") for e in text_exts}
    keep_exts = {e.lower().lstrip(".") for e in exts} if exts is not None else None

    out: list[Passage] = []
    for path in await _list_files(ws, root, keep_exts):
        ext = _ext(path)
        if ext in text_exts:
            # Native text: precise, per-byte blame from the Rust core.
            dicts = await ws.passages(
                root=path,
                segmentation=segmentation,
                size=size,
                overlap=overlap,
                with_blame=with_blame,
                with_text=True,
            )
            out.extend(_passage_from_dict(d) for d in dicts)
        elif convert is not None:
            data = bytes(await ws.read(path))
            md = convert.convert(path, data, mimetypes.guess_type(path)[0])
            if md is None:
                continue
            authors = _doc_authors(await ws.blame(path)) if with_blame else []
            conv = {"id": getattr(convert, "id", "converter"), "version": str(getattr(convert, "version", ""))}
            out.extend(
                _converted_passages(path, md, segmentation, size, overlap, authors, conv, segment)
            )
        # else: non-text with no converter -> skipped
    return out


def _converted_passages(
    path: str,
    md: str,
    segmentation: str,
    size: int,
    overlap: int,
    authors: list[Author],
    conv: dict,
    segment: Optional[Callable[[str], list[str]]],
) -> list[Passage]:
    data = md.encode("utf-8")
    out: list[Passage] = []
    if segment is not None:
        # Custom splitter: chunk strings, offsets tracked by scan (best-effort on
        # derived text, which has no byte correspondence to the original anyway).
        pos = 0
        for chunk in segment(md):
            cb = chunk.encode("utf-8")
            i = data.find(cb, pos)
            s = i if i >= 0 else pos
            e = s + len(cb)
            pos = e
            out.append(_mk_converted(path, cb, s, e, authors, conv))
    else:
        for s, e in _segment_spans(data, segmentation, size, overlap):
            out.append(_mk_converted(path, data[s:e], s, e, authors, conv))
    return out


def _mk_converted(path, seg: bytes, s: int, e: int, authors, conv) -> Passage:
    return Passage(
        path=path,
        text=seg.decode("utf-8", "replace"),
        byte_start=s,
        byte_end=e,
        hash=content_hash(seg),
        authors=list(authors),
        source=path,
        converter=conv,
    )


# --- helpers ----------------------------------------------------------------


async def _list_files(ws, root: str, keep_exts: Optional[set]) -> list[str]:
    """Sorted regular-file paths under ``root`` (symlinks skipped)."""
    root = _norm(root)
    st = await ws.stat(root)
    if st["kind"] == "file":
        return [root]
    files: list[str] = []
    stack = [root]
    while stack:
        d = stack.pop()
        for e in await ws.ls(d):
            child = f"/{e['name']}" if d == "/" else f"{d}/{e['name']}"
            if e["kind"] == "dir":
                stack.append(child)
            elif e["kind"] == "file" and (keep_exts is None or _ext(child) in keep_exts):
                files.append(child)
    files.sort()
    return files


def _passage_from_dict(d: dict) -> Passage:
    return Passage(
        path=d["path"],
        text=d.get("text") or "",
        byte_start=d["byte_start"],
        byte_end=d["byte_end"],
        hash=d["hash"],
        authors=_doc_authors(d.get("blame") or []),
    )


def _doc_authors(blame: list) -> list[Author]:
    """Distinct actors from a blame list, first-seen order."""
    seen: dict[int, Author] = {}
    for b in blame:
        a = b["actor"]
        seen.setdefault(a["id"], Author(a["id"], a["display_name"], a["kind"]))
    return list(seen.values())


def _ext(path: str) -> str:
    base = path.rsplit("/", 1)[-1]
    return base.rsplit(".", 1)[-1].lower() if "." in base else ""


def _norm(path: str) -> str:
    t = (path or "/").strip().rstrip("/")
    if not t:
        return "/"
    return t if t.startswith("/") else "/" + t


def _segment_spans(data: bytes, strategy: str, size: int, overlap: int) -> list[tuple[int, int]]:
    """Byte spans for *converted* text (Python-side; the Rust core handles native
    text). ``content_defined`` falls back to fixed windows here — converted docs
    are re-derived on change, so the edit-stability win doesn't apply."""
    n = len(data)
    if n == 0:
        return []
    if strategy in ("whole_file", "whole"):
        return [(0, n)]
    if strategy == "lines":
        return _line_windows(data, max_lines=max(size, 1), overlap=overlap)
    # fixed, content_defined, cdc, and anything else -> fixed windows
    size = max(size, 1)
    step = size - min(overlap, size - 1)
    out, start = [], 0
    while start < n:
        end = min(start + size, n)
        out.append((start, end))
        if end == n:
            break
        start += step
    return out


def _line_windows(data: bytes, max_lines: int, overlap: int) -> list[tuple[int, int]]:
    lines, start = [], 0
    for i, b in enumerate(data):
        if b == 0x0A:
            lines.append((start, i + 1))
            start = i + 1
    if start < len(data):
        lines.append((start, len(data)))
    if not lines:
        return []
    step = max_lines - min(overlap, max_lines - 1)
    out, i = [], 0
    while i < len(lines):
        end_line = min(i + max_lines, len(lines))
        out.append((lines[i][0], lines[end_line - 1][1]))
        if end_line == len(lines):
            break
        i += step
    return out
