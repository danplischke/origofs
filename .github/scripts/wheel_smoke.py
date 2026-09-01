"""Smoke-test an *installed* origofs wheel.

A wheel that imports is the minimum bar for publishing one, but importing alone
would pass on a wheel whose compiled core is broken in every way that matters —
so this exercises the actual engine through the bindings: open a workspace,
register an actor, write attributed bytes, read them back, and pull the blame
trail out. Attribution is the whole point of origofs, and it is the part that
crosses the pyo3 boundary into `origofs-core`, so it is what makes the wheel
"works" rather than "loads".

Run against the wheel installed into the current interpreter (`pip install
--no-index --find-links dist origofs`), NOT the source tree — the point is to
test the artifact that ships.

This lives in `.github/scripts/` and is shared by two workflows rather than
inlined in either:

  * `release.yml`  — every wheel it builds, before it is attached to a release;
  * `ci.yml`       — the Windows leg, which builds the same wheel on every PR.

Inlined twice, the two copies would drift the first time a binding was renamed,
and the release-time copy is precisely the one nobody runs until a tag. Deliberately
dependency-free (no pytest): it has to run against a bare `pip install` of the
wheel alone, on a runner where nothing else is installed.
"""

import asyncio
import importlib.metadata
import os
import sys
import tempfile

import origofs


def check_exports() -> None:
    """Every name the package promises actually resolves.

    `origofs/__init__.py` re-exports the compiled extension's symbols by hand, so
    a class registered under a `#[cfg]` — as the Linux-only `Mount` is — can sit
    in that import list and simply not exist in the wheel for another platform.
    The result is an `ImportError` on `import origofs` itself, on that platform
    only, which no amount of Linux testing will show. That is exactly how the
    macOS and Windows wheels came to be un-importable (#107).

    Reaching this function at all means the import already succeeded; this then
    catches the subtler direction, a name in `__all__` that was never bound.
    """
    missing = [n for n in origofs.__all__ if not hasattr(origofs, n)]
    assert not missing, f"names in __all__ missing from the module: {missing}"


async def main() -> None:
    check_exports()

    d = tempfile.mkdtemp()
    ws = await origofs.Workspace.open_local(
        os.path.join(d, "meta.db"), os.path.join(d, "cas")
    )

    actor = await ws.create_human("ci", None)
    sess = await ws.create_session(actor, "smoke")
    ctx = origofs.WriteCtx.session(actor, sess)

    await ws.write_as(ctx, "/hello.txt", b"hi\n")
    got = bytes(await ws.read("/hello.txt"))
    assert got == b"hi\n", f"read back {got!r}"

    spans = await ws.blame("/hello.txt")
    assert spans, "blame came back empty"
    # The write was attributed, so the span must name the actor that made it. A
    # non-empty blame list alone would pass the check above while attributing the
    # bytes to nobody in particular. Spans are TypedDicts -- plain dicts at
    # runtime -- with the actor record inlined, so there is no second lookup.
    got_actor = spans[0]["actor"]["id"]
    assert got_actor == actor, f"blame names actor {got_actor}, expected {actor}"

    # Both versions trace back to `[workspace.package].version` — the wheel's
    # through maturin, the extension's through `CARGO_PKG_VERSION` — so they can
    # only disagree if the artifact was assembled from mismatched parts. That is
    # precisely what a smoke test of a *publishable* wheel should refuse to pass,
    # and `getattr(..., "")` used to hide it: `__version__` did not exist at all,
    # so this line printed a blank version on every wheel it ever cleared.
    version = origofs.__version__
    declared = importlib.metadata.version("origofs")
    assert version == declared, (
        f"extension was built as {version}, but the wheel declares {declared}"
    )

    print(f"origofs {version} ok on {sys.platform} (python {sys.version.split()[0]})")


asyncio.run(main())
