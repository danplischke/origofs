"""RAG with provenance — the passage knows who wrote it.

Runs the whole story end to end with no API keys and no vector database:

    python examples/rag_provenance.py

A human and an agent co-author a small doc set (plus a "PDF" that a converter
projects to Markdown). We extract passages with `origofs.rag.read_passages`, do a
trivial keyword retrieval, and print — for the winning passage — *who wrote it* and
*where it came from*. That provenance is the thing a plain "S3 + embeddings" RAG
pipeline cannot produce: every answer traces back to an author and a source.

The retrieval here is deliberately dumb (keyword overlap) so the example needs
nothing but origofs. To make it real, hand the same records to LlamaIndex:

    from origofs.llamaindex import SimpleWorkspaceReader
    from llama_index.core import VectorStoreIndex
    docs = SimpleWorkspaceReader(ws, root="/docs", convert="markitdown").load_data()
    index = VectorStoreIndex.from_documents(docs)   # each node.metadata carries the provenance
"""
import asyncio
import os
import tempfile

import origofs
from origofs.rag import read_passages


# A stand-in for MarkItDown so the example runs with no extra deps. In real use:
#   from origofs.converters import MarkItDownConverter; convert = MarkItDownConverter()
def fake_pdf_converter(path, data, mime):
    if path.endswith(".pdf"):
        return (
            "# Origo Spec\n\n"
            "Attribution is recorded per byte range against the actor that wrote it.\n\n"
            "Blame survives checkout because it travels with the content hash.\n"
        )
    return None


def retrieve(passages, query, k=1):
    """Dumb keyword-overlap retrieval — stands in for an embedding index."""
    terms = set(query.lower().split())
    scored = sorted(
        passages,
        key=lambda p: len(terms & set(p.text.lower().split())),
        reverse=True,
    )
    return scored[:k]


def show(passage):
    who = ", ".join(f"{a.name} ({a.kind})" for a in passage.authors) or "unattributed"
    print(f'    "{passage.text.strip().splitlines()[0][:70]}…"')
    print(f"    ✍  written by: {who}")
    if passage.source:
        conv = passage.converter or {}
        print(f"    📄 extracted from: {passage.source}  (via {conv.get('id')})")
    else:
        print(f"    📄 from: {passage.path}  [bytes {passage.byte_start}:{passage.byte_end}]")
    print(f"    #  passage hash: {passage.hash[:16]}…")


async def main():
    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(f"{d}/meta.db", f"{d}/cas")

    # Two actors sharing the workspace: a person and an agent.
    dan = await ws.create_human("dan", "dan@example.com")
    claude = await ws.create_agent("claude", "claude-opus", dan)
    await ws.mkdir_p("/docs")

    # Dan writes the intro; Claude drafts the attribution section; Dan adds a PDF.
    await ws.write_as(
        origofs.WriteCtx.actor(dan),
        "/docs/intro.md",
        b"# origofs\n\norigofs is a filesystem humans and agents share.\n"
        b"Content is addressed by its BLAKE3 hash and deduplicated.\n",
    )
    await ws.write_as(
        origofs.WriteCtx.actor(claude),
        "/docs/attribution.md",
        b"# Attribution\n\nEvery write carries a WriteCtx (actor + session).\n"
        b"blame reports, per byte range, whether a human or an agent wrote it.\n",
    )
    await ws.write_as(origofs.WriteCtx.actor(dan), "/docs/spec.pdf", b"%PDF-1.4 (binary) \x00\x01")

    # Extract passages: native text keeps precise blame; the PDF is converted and
    # carries document-level provenance (source + who added it + the converter).
    passages = await read_passages(
        ws, root="/docs", convert=fake_pdf_converter, segmentation="content_defined", size=256
    )
    print(f"indexed {len(passages)} passages from {len({p.path for p in passages})} documents\n")

    for query in [
        "how does attribution and blame work",
        "what is content addressed and deduplicated",
        "does blame survive checkout",
    ]:
        print(f"?  {query}")
        for hit in retrieve(passages, query, k=1):
            show(hit)
        print()

    # Incremental indexing: edit one doc, re-extract, and see that only the changed
    # passages get new hashes — the rest of the corpus needn't be re-embedded.
    before = {p.hash for p in passages}
    await ws.write_as(
        origofs.WriteCtx.actor(claude),
        "/docs/attribution.md",
        b"# Attribution\n\nEvery write carries a WriteCtx (actor + session).\n"
        b"blame reports, per byte range, whether a human or an agent wrote it.\n"
        b"Suggestions let an agent propose edits for review before they land.\n",  # appended
    )
    after = await read_passages(
        ws, root="/docs", convert=fake_pdf_converter, segmentation="content_defined", size=256
    )
    fresh = [p for p in after if p.hash not in before]
    print(
        f"after editing one file: {len(fresh)} new passage(s) to re-embed, "
        f"{len(after) - len(fresh)} unchanged (reused)."
    )


if __name__ == "__main__":
    asyncio.run(main())
