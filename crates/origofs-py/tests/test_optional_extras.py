"""Every declared extra is actually *exercised*, not merely declared (#104).

`origofs` ships six optional integrations as pyproject extras. Their tests guard
themselves with ``pytest.importorskip``, which is right for a contributor who has
not installed the extras — but CI installed only some of them, so
``origofs/llamaindex.py`` (201 lines) and ``converters.MarkItDownConverter`` had
**never been imported by a test that ran**. Four tests in ``test_rag.py`` skipped
on every single run, and coverage looked fine the whole time: the tests existed.

This is the same silent-skip failure mode the ``fuse`` CI job was created to end,
so it gets the same treatment — assert the preconditions instead of skipping past
them:

* :func:`test_every_declared_extra_is_mapped_to_an_import` keeps this file honest
  as extras are added. Declare a new extra in ``pyproject.toml`` and this fails
  until it is mapped here *and* installed in CI — so a new integration cannot
  arrive already unexercised.
* :func:`test_extra_is_importable` runs only where the extras are supposed to be
  installed (``ORIGOFS_REQUIRE_EXTRAS=1``, set by the ``python`` CI job) and fails
  if one isn't. Since ``importorskip`` skips exactly when the module is missing,
  a green run of this test is what guarantees the guarded tests really ran.
"""

from __future__ import annotations

import importlib
import os
import sys
import tomllib
from pathlib import Path

import pytest

# Distribution names are not import names ("llama-index-core" imports as
# `llama_index.core`, "universal_pathlib" as `upath`), so the mapping is written
# out rather than derived. The module named is the one the integration actually
# imports — the thing that has to work, not merely be present on disk.
EXTRA_IMPORTS: dict[str, tuple[str, ...]] = {
    "fastapi": ("fastapi",),
    "fsspec": ("fsspec",),
    "upath": ("upath",),
    "llamaindex": ("llama_index.core",),
    "markitdown": ("markitdown",),
    "db": ("sqlalchemy", "alembic"),
}

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"

REQUIRE_EXTRAS = os.environ.get("ORIGOFS_REQUIRE_EXTRAS") == "1"


def declared_extras() -> set[str]:
    """The extras `pyproject.toml` advertises to `pip install origofs[...]`."""
    with PYPROJECT.open("rb") as fh:
        return set(tomllib.load(fh)["project"]["optional-dependencies"])


def test_every_declared_extra_is_mapped_to_an_import() -> None:
    declared = declared_extras()
    mapped = set(EXTRA_IMPORTS)
    assert mapped == declared, (
        "extras in pyproject.toml and the map in this file have diverged.\n"
        f"  declared but unmapped: {sorted(declared - mapped)}\n"
        f"  mapped but undeclared: {sorted(mapped - declared)}\n"
        "An unmapped extra is one nothing checks is installed, which is how an "
        "integration ends up shipped but never imported by a test that ran. Add "
        "it here AND to the `python` job's pip install line in "
        ".github/workflows/ci.yml."
    )


@pytest.mark.skipif(
    not REQUIRE_EXTRAS,
    reason="ORIGOFS_REQUIRE_EXTRAS unset: extras are optional for a local run",
)
@pytest.mark.parametrize(
    "module", sorted({m for mods in EXTRA_IMPORTS.values() for m in mods})
)
def test_extra_is_importable(module: str) -> None:
    """Where the extras are meant to be installed, a missing one is a failure.

    Without this, the only symptom of an extra dropping out of the CI install
    line is that some tests quietly stop running.
    """
    try:
        importlib.import_module(module)
    except ImportError as exc:  # pragma: no cover - the failure path is the point
        extras = sorted(e for e, mods in EXTRA_IMPORTS.items() if module in mods)
        pytest.fail(
            f"{module!r} is not importable, so every test guarded on it silently "
            f"skipped. It backs the {extras} extra(s); add the distribution to the "
            f"`python` job's pip install line in .github/workflows/ci.yml.\n"
            f"  ({type(exc).__name__}: {exc})"
        )


@pytest.mark.skipif(
    not REQUIRE_EXTRAS,
    reason="ORIGOFS_REQUIRE_EXTRAS unset: extras are optional for a local run",
)
def test_the_integrations_those_extras_exist_for_import() -> None:
    """The origofs-side modules, not just their third-party dependencies.

    An extra being installed does not prove our own integration module imports
    against it — a renamed upstream symbol breaks `origofs.llamaindex` while
    `llama_index.core` keeps importing perfectly well.
    """
    for module in ("origofs.fastapi", "origofs.fsspec", "origofs.llamaindex",
                   "origofs.converters", "origofs.rag", "origofs.db"):
        importlib.import_module(module)
        assert module in sys.modules
