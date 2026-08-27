"""fsspec's own compliance suite, run against OrigoFileSystem.

fsspec ships a reusable battery of filesystem-conformance tests
(``fsspec.tests.abstract``) that pins down the fiddly, easy-to-get-wrong corners
of the interface — recursive copy/get/put, trailing-slash semantics, glob edge
cases, copying into an existing vs. a new directory, list sources, directories
and files sharing a name prefix. Wiring it up here is how "is every fsspec edge
case handled?" gets a real, maintained answer instead of a hand-picked sample.

The suite drives copy, get (remote→local), put (local→remote), pipe, and open.
Each concrete ``Test*`` class below composes one abstract battery with the origofs
fixtures.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install fsspec
    pytest tests/test_fsspec_compliance.py
"""
import os
import posixpath
import tempfile
import uuid

import pytest

pytest.importorskip("fsspec")  # skip the module without the fsspec extra

import origofs.fsspec  # noqa: F401 - registers the "origofs" protocol

from fsspec.tests.abstract import AbstractFixtures
from fsspec.tests.abstract.copy import AbstractCopyTests
from fsspec.tests.abstract.get import AbstractGetTests
from fsspec.tests.abstract.open import AbstractOpenTests
from fsspec.tests.abstract.pipe import AbstractPipeTests
from fsspec.tests.abstract.put import AbstractPutTests

from origofs.fsspec import OrigoFileSystem


class OrigoFixtures(AbstractFixtures):
    """Bind the abstract batteries to a fresh origofs workspace."""

    # One workspace per test class; each test gets its own unique root dir
    # (`fs_path`) within it, so scenarios never collide. Class-scoped fixtures
    # must be classmethods (matching fsspec's own ``local_fs`` fixture).
    @pytest.fixture(scope="class")
    @classmethod
    def fs(cls):
        d = tempfile.mkdtemp()
        return OrigoFileSystem(
            db_path=os.path.join(d, "meta.db"),
            cas_dir=os.path.join(d, "cas"),
            skip_instance_cache=True,
        )

    @pytest.fixture
    def fs_join(self):
        # origofs paths are POSIX and absolute; join with forward slashes.
        return posixpath.join

    @pytest.fixture
    def fs_path(self):
        return "/" + uuid.uuid4().hex


class TestOrigoCopy(AbstractCopyTests, OrigoFixtures):
    pass


class TestOrigoGet(AbstractGetTests, OrigoFixtures):
    pass


class TestOrigoPut(AbstractPutTests, OrigoFixtures):
    pass


class TestOrigoPipe(AbstractPipeTests, OrigoFixtures):
    pass


class TestOrigoOpen(AbstractOpenTests, OrigoFixtures):
    pass
