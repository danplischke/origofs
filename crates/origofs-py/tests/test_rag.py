"""RAG passage extraction: provenance grades, the converter seam, and the
LlamaIndex reader.

The engine tests (``origofs.rag.read_passages``) need only the compiled extension.
The reader tests gate on LlamaIndex; a converter test gates on MarkItDown.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install llama-index-core
    pytest tests/test_rag.py
"""
import asyncio
import os
import tempfile

import pytest

import origofs
from origofs.rag import Author, Passage, read_passages


async def _ws():
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(os.path.join(d, "meta.db"), os.path.join(d, "cas"))
    return ws


def _varied(n):
    return "".join(f"line {i:03}: the quick brown fox jumps over the lazy dog\n" for i in range(n)).encode()


def test_native_passages_have_precise_blame():
    async def scenario():
        ws = await _ws()
        alice = await ws.create_human("alice", "alice@x")
        claude = await ws.create_agent("claude", "opus", alice)
        await ws.mkdir_p("/docs")
        await ws.write_as(origofs.WriteCtx.actor(alice), "/docs/a.md", _varied(60))
        await ws.write_as(origofs.WriteCtx.actor(claude), "/docs/b.md", b"claude wrote this\n")
        await ws.write("/docs/c.bin", b"\x00\x01\x02binary")  # no converter -> skipped

        ps = await read_passages(ws, root="/docs", segmentation="content_defined", size=256)
        assert {p.path for p in ps} == {"/docs/a.md", "/docs/b.md"}, "binary skipped without a converter"
        a = [p for p in ps if p.path == "/docs/a.md"]
        assert len(a) >= 1
        # precise blame: every a.md passage is authored by alice, and nothing else
        assert all([au.name for au in p.authors] == ["alice"] for p in a)
        assert a[0].source is None and a[0].converter is None
        # content address matches the origofs scheme (dedup / incremental key)
        assert a[0].hash == origofs.content_hash(a[0].text.encode())
        b = next(p for p in ps if p.path == "/docs/b.md")
        assert b.authors == [Author(claude, "claude", "agent")]

    asyncio.run(scenario())


def test_content_addressed_dedup():
    async def scenario():
        ws = await _ws()
        await ws.write("/x.md", b"identical body text\n")
        await ws.write("/y.md", b"identical body text\n")
        ps = await read_passages(ws, root="/", segmentation="whole_file", with_blame=False)
        by = {p.path: p.hash for p in ps}
        assert by["/x.md"] == by["/y.md"], "identical passages share a hash"

    asyncio.run(scenario())


def test_converter_gives_document_level_provenance():
    async def scenario():
        ws = await _ws()
        alice = await ws.create_human("alice", "alice@x")
        await ws.mkdir_p("/docs")
        # a "binary" doc only a converter can read, added by alice
        await ws.write_as(origofs.WriteCtx.actor(alice), "/docs/report.pdf", b"%PDF-1.4 \x00\x01 bytes")

        def fake(path, data, mime):
            return "# Report\n\nParagraph one.\n\nParagraph two.\n" if path.endswith(".pdf") else None

        ps = await read_passages(ws, root="/docs", convert=fake, segmentation="fixed", size=16)
        conv = [p for p in ps if p.path == "/docs/report.pdf"]
        assert conv, "the pdf was converted and split"
        c = conv[0]
        # document-level provenance: source path, the converter, and *who added the doc*
        assert c.source == "/docs/report.pdf"
        assert c.converter and c.converter["id"] == "callable"
        assert [au.name for au in c.authors] == ["alice"]
        assert c.hash == origofs.content_hash(c.text.encode())
        assert c.metadata["source"] == "/docs/report.pdf"
        assert c.metadata["converter"].startswith("callable@")

    asyncio.run(scenario())


def test_custom_segmenter_for_converted_text():
    async def scenario():
        ws = await _ws()
        await ws.write("/d.pdf", b"%PDF fake")
        fake = lambda p, d, m: "AAA\n\nBBB\n\nCCC\n"  # noqa: E731
        ps = await read_passages(
            ws, root="/", convert=fake,
            segment=lambda md: [blk for blk in md.split("\n\n") if blk.strip()],
        )
        texts = [p.text.strip() for p in ps if p.path == "/d.pdf"]
        assert texts == ["AAA", "BBB", "CCC"]

    asyncio.run(scenario())


def test_ext_filter_and_subtree_root():
    async def scenario():
        ws = await _ws()
        await ws.mkdir_p("/a/b")
        await ws.write("/a/keep.md", b"keep me\n")
        await ws.write("/a/skip.txt", b"skip me\n")
        await ws.write("/a/b/deep.md", b"deep\n")
        ps = await read_passages(ws, root="/a", exts=["md"], segmentation="whole_file")
        assert {p.path for p in ps} == {"/a/keep.md", "/a/b/deep.md"}

    asyncio.run(scenario())


# --- LlamaIndex reader (gated) ----------------------------------------------


def test_reader_documents_and_nodes():
    pytest.importorskip("llama_index.core")
    from origofs.llamaindex import SimpleWorkspaceReader

    async def scenario():
        ws = await _ws()
        alice = await ws.create_human("alice", "alice@x")
        await ws.mkdir_p("/docs")
        await ws.write_as(origofs.WriteCtx.actor(alice), "/docs/a.md", _varied(40))

        reader = SimpleWorkspaceReader(ws, root="/docs", segmentation="content_defined", size=256)
        docs = await reader.aload_data()
        assert docs and type(docs[0]).__name__ == "Document"
        d0 = docs[0]
        assert d0.metadata["authors"] == ["alice"]
        assert d0.metadata["path"] == "/docs/a.md"
        assert d0.id_.startswith("/docs/a.md#")
        # provenance kept in metadata but excluded from what gets embedded
        assert "passage_hash" in d0.metadata
        assert "passage_hash" in d0.excluded_embed_metadata_keys

        nodes = await reader.aload_nodes()
        assert nodes and type(nodes[0]).__name__ == "TextNode"
        # the framework-neutral records line up 1:1 with the documents
        assert len(await reader.aload_passages()) == len(docs)

    asyncio.run(scenario())


def test_reader_sync_load_via_local_open():
    pytest.importorskip("llama_index.core")
    from origofs.llamaindex import SimpleWorkspaceReader

    d = tempfile.mkdtemp()

    async def seed():
        ws = await origofs.Workspace.open_local(os.path.join(d, "meta.db"), os.path.join(d, "cas"))
        await ws.write("/x.md", b"# Title\nhello sync\n")

    asyncio.run(seed())

    reader = SimpleWorkspaceReader(
        db_path=os.path.join(d, "meta.db"), cas_dir=os.path.join(d, "cas"), root="/"
    )
    docs = reader.load_data()  # blocking; opens the workspace on the fsspec-style loop
    assert docs and docs[0].metadata["path"] == "/x.md"


def test_reader_requires_ws_or_local():
    pytest.importorskip("llama_index.core")
    from origofs.llamaindex import SimpleWorkspaceReader

    with pytest.raises(ValueError):
        SimpleWorkspaceReader()


# --- MarkItDown converter (gated) -------------------------------------------


def test_markitdown_converter_declines_unhandled_ext():
    pytest.importorskip("markitdown")
    from origofs.converters import MarkItDownConverter

    conv = MarkItDownConverter()
    # a plain-text extension isn't in the handled-doc set -> declines (returns None)
    assert conv.convert("/notes.md", b"# hi", "text/markdown") is None
    assert conv.id == "markitdown"
