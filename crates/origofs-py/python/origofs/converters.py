"""Document → Markdown converters for the RAG passage pipeline.

Non-text documents (PDF, DOCX, PPTX, images, HTML, …) must be projected to text
before they can be split into passages. That projection is *userland* and
format-specific, so origofs keeps it behind the :class:`origofs.rag.Converter`
seam and ships one batteries-included implementation here — a thin wrapper over
`Microsoft MarkItDown <https://github.com/microsoft/markitdown>`_. Swap it for
``unstructured``, ``pandoc``, ``docling``, an LLM, or a plain
``callable(path, data, mime) -> str | None``; the pipeline doesn't care.

    from origofs.converters import MarkItDownConverter
    from origofs.llamaindex import SimpleWorkspaceReader

    reader = SimpleWorkspaceReader(ws, root="/docs", convert=MarkItDownConverter())

Requires MarkItDown: ``pip install "origofs[markitdown]"``.
"""
from __future__ import annotations

import io
from typing import Optional

__all__ = ["MarkItDownConverter", "get_converter", "DEFAULT_DOC_EXTS"]

#: Document formats MarkItDown handles well by default. ``None`` (the converter's
#: ``handle_exts``) means "try any document offered".
DEFAULT_DOC_EXTS = frozenset(
    """
    pdf docx doc pptx ppt xlsx xls html htm epub odt rtf
    png jpg jpeg gif bmp tiff webp
    wav mp3 m4a flac ogg
    zip
    """.split()
)


class MarkItDownConverter:
    """Project documents to Markdown with `MarkItDown`.

    ``handle_exts`` limits which extensions this converter claims (others return
    ``None`` so the reader skips them or treats them as native text); pass ``None``
    to attempt every document the reader offers. Extra keyword args are forwarded
    to ``markitdown.MarkItDown(...)`` (e.g. ``llm_client=``/``llm_model=`` for image
    descriptions) — note those paths can be nondeterministic, which is why
    ``version`` is part of a passage's converter provenance.
    """

    id = "markitdown"

    def __init__(self, *, handle_exts: Optional[frozenset] = DEFAULT_DOC_EXTS, md=None, **markitdown_kwargs) -> None:
        if md is None:
            try:
                from markitdown import MarkItDown
            except ImportError as exc:  # pragma: no cover - only without the extra
                raise ImportError(
                    'MarkItDownConverter requires MarkItDown. Install it with: '
                    'pip install "origofs[markitdown]"'
                ) from exc
            md = MarkItDown(**markitdown_kwargs)
        self._md = md
        self.handle_exts = handle_exts
        try:
            from importlib.metadata import version as _v

            self.version = _v("markitdown")
        except Exception:  # pragma: no cover - metadata may be absent
            self.version = ""

    def convert(self, path: str, data: bytes, mime: Optional[str]) -> Optional[str]:
        ext = path.rsplit("/", 1)[-1].rsplit(".", 1)[-1].lower() if "." in path else ""
        if self.handle_exts is not None and ext not in self.handle_exts:
            return None
        stream = io.BytesIO(data)
        hint = f".{ext}" if ext else None
        try:
            result = self._convert_stream(stream, hint)
        except Exception:
            # MarkItDown couldn't parse it — skip rather than fail the whole run.
            return None
        text = (
            getattr(result, "text_content", None)
            or getattr(result, "markdown", None)
            or (result if isinstance(result, str) else None)
        )
        text = text.strip() if isinstance(text, str) else None
        return text or None

    def _convert_stream(self, stream: io.BytesIO, hint: Optional[str]):
        """Call MarkItDown across a couple of API shapes (the signature has moved
        between releases)."""
        md = self._md
        errors = []
        for kwargs in ([{"file_extension": hint}] if hint else []) + [{}]:
            stream.seek(0)
            try:
                return md.convert_stream(stream, **kwargs)
            except TypeError as e:  # unexpected kwarg on this version
                errors.append(e)
        # Last resort: some versions only expose convert() over a path-like/stream.
        stream.seek(0)
        return md.convert(stream)


def get_converter(name: str, **kwargs):
    """Resolve a converter by name. Currently ``"markitdown"``."""
    key = name.lower().replace("-", "").replace("_", "")
    if key in ("markitdown", "md"):
        return MarkItDownConverter(**kwargs)
    raise ValueError(f"unknown converter {name!r} (known: 'markitdown')")
