"""A LlamaIndex reader over an origofs workspace — RAG with provenance.

:class:`SimpleWorkspaceReader` is the origofs counterpart to LlamaIndex's
``SimpleDirectoryReader``: point it at a workspace (or a subtree) and it returns
LlamaIndex ``Document``\\s already split into passages, each carrying **who wrote
it** (blame) and **where it came from** in its ``metadata``. Non-text documents are
projected to Markdown by a :class:`~origofs.rag.Converter` first (pass
``convert="markitdown"`` or your own).

    from origofs.llamaindex import SimpleWorkspaceReader
    from llama_index.core import VectorStoreIndex

    reader = SimpleWorkspaceReader(ws, root="/docs", convert="markitdown")
    docs = reader.load_data()                     # or: await reader.aload_data()
    index = VectorStoreIndex.from_documents(docs)
    # every retrieved node's metadata carries {path, authors, passage_hash, …}

Because each passage is keyed by a content ``passage_hash``, re-indexing a changed
workspace only produces new hashes for genuinely-changed passages — the basis for
cheap incremental embedding.

The heavy lifting lives in :mod:`origofs.rag` (framework-neutral); this module only
maps :class:`~origofs.rag.Passage` records onto LlamaIndex objects. Requires
LlamaIndex: ``pip install "origofs[llamaindex]"`` (and ``origofs[markitdown]`` for
the MarkItDown converter).
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Iterable, Optional

from .rag import DEFAULT_TEXT_EXTS, ConverterLike, Passage, read_passages

__all__ = ["SimpleWorkspaceReader"]

# Provenance metadata that's useful to keep but noisy to feed into embeddings/LLM.
_EXCLUDED_META = ["byte_start", "byte_end", "passage_hash", "author_ids"]


class SimpleWorkspaceReader:
    """Load provenance-carrying LlamaIndex documents from an origofs workspace.

    Parameters mirror :func:`origofs.rag.read_passages`. Provide either an open
    ``ws`` (any backend) or ``db_path``/``cas_dir`` to open a local workspace
    lazily. ``convert`` accepts a :class:`~origofs.rag.Converter`, a plain
    ``callable(path, data, mime) -> str | None``, the string ``"markitdown"``, or
    ``None`` (native text only).
    """

    def __init__(
        self,
        ws: Any = None,
        *,
        db_path: Optional[str] = None,
        cas_dir: Optional[str] = None,
        root: str = "/",
        exts: Optional[Iterable[str]] = None,
        segmentation: str = "content_defined",
        size: int = 1024,
        overlap: int = 0,
        with_blame: bool = True,
        convert: Optional[ConverterLike | str] = None,
        text_exts: Iterable[str] = DEFAULT_TEXT_EXTS,
        segment: Optional[Callable[[str], list[str]]] = None,
    ) -> None:
        if ws is None and not (db_path and cas_dir):
            raise ValueError(
                "SimpleWorkspaceReader needs either ws=<open Workspace> or "
                "db_path=... and cas_dir=... to open a local workspace"
            )
        self._ws = ws
        self._db_path = db_path
        self._cas_dir = cas_dir
        self._convert = _resolve_convert(convert)
        self._kw = dict(
            root=root,
            exts=exts,
            segmentation=segmentation,
            size=size,
            overlap=overlap,
            with_blame=with_blame,
            text_exts=text_exts,
            segment=segment,
        )

    async def _get_ws(self) -> Any:
        if self._ws is None:
            import origofs

            self._ws = await origofs.Workspace.open_local(self._db_path, self._cas_dir)
        return self._ws

    # --- passages (framework-neutral) --------------------------------------

    async def aload_passages(self) -> list[Passage]:
        """The raw :class:`~origofs.rag.Passage` records (no LlamaIndex types)."""
        ws = await self._get_ws()
        return await read_passages(ws, convert=self._convert, **self._kw)

    # --- LlamaIndex documents / nodes --------------------------------------

    async def aload_data(self) -> list:
        """Async: passages as LlamaIndex ``Document``\\s (one per passage)."""
        Document = _document_cls()
        return [self._to_document(Document, p) for p in await self.aload_passages()]

    def load_data(self) -> list:
        """Blocking: passages as LlamaIndex ``Document``\\s. Call
        :meth:`aload_data` instead from inside a running event loop."""
        return _run_sync(self.aload_data())

    async def aload_nodes(self) -> list:
        """Async: passages as LlamaIndex ``TextNode``\\s (already the retrieval
        unit, so no further node-parsing is needed)."""
        TextNode = _textnode_cls()
        return [self._to_node(TextNode, p) for p in await self.aload_passages()]

    def load_nodes(self) -> list:
        """Blocking form of :meth:`aload_nodes`."""
        return _run_sync(self.aload_nodes())

    # --- mapping ------------------------------------------------------------

    @staticmethod
    def _to_document(Document, p: Passage):
        return Document(
            text=p.text,
            id_=_passage_id(p),
            metadata=p.metadata,
            excluded_embed_metadata_keys=list(_EXCLUDED_META),
            excluded_llm_metadata_keys=list(_EXCLUDED_META),
        )

    @staticmethod
    def _to_node(TextNode, p: Passage):
        return TextNode(
            text=p.text,
            id_=_passage_id(p),
            metadata=p.metadata,
            excluded_embed_metadata_keys=list(_EXCLUDED_META),
            excluded_llm_metadata_keys=list(_EXCLUDED_META),
        )


# --- helpers ----------------------------------------------------------------


def _passage_id(p: Passage) -> str:
    return f"{p.path}#{p.byte_start}-{p.byte_end}"


def _resolve_convert(convert):
    if isinstance(convert, str):
        from .converters import get_converter

        return get_converter(convert)
    return convert


def _run_sync(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    coro.close()
    raise RuntimeError(
        "load_data()/load_nodes() are blocking; you're inside a running event "
        "loop — use `await reader.aload_data()` (or aload_nodes) instead."
    )


def _document_cls():
    return _import_llama("Document")


def _textnode_cls():
    return _import_llama("TextNode")


def _import_llama(name: str):
    for mod in ("llama_index.core.schema", "llama_index.core", "llama_index"):
        try:
            module = __import__(mod, fromlist=[name])
            return getattr(module, name)
        except (ImportError, AttributeError):
            continue
    raise ImportError(
        'SimpleWorkspaceReader requires LlamaIndex. Install it with: '
        'pip install "origofs[llamaindex]"'
    )
