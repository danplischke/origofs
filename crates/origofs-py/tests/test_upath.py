"""universal-pathlib (`UPath`) compatibility for the ``origofs://`` protocol.

Proves that a pathlib-style ``UPath("origofs://…")`` drives the origofs filesystem
end to end, resolves to the explicit :class:`origofs._upath.OrigofsPath` (via the
``universal_pathlib.implementations`` entry point), and does so without upath's
"not explicitly implemented" / "could not find default flavour" warnings.

Build + run (from crates/origofs-py, in a venv):
    maturin develop && pip install universal_pathlib
    pytest tests/                      # or: python tests/test_upath.py
"""
import os
import tempfile
import warnings

import pytest

pytest.importorskip("upath")  # skip the module without the universal-pathlib extra

from upath import UPath

import origofs.fsspec  # noqa: F401 - register OrigoFileSystem before UPath resolves it


def _root():
    d = tempfile.mkdtemp()
    return UPath(
        "origofs:///",
        db_path=os.path.join(d, "meta.db"),
        cas_dir=os.path.join(d, "cas"),
    )


def test_upath_resolves_to_explicit_impl_without_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any protocol/flavour warning becomes a failure
        root = _root()
        assert type(root).__name__ == "OrigofsPath"
        assert type(root.fs).__name__ == "OrigoFileSystem"
        assert root.protocol == "origofs"


def test_upath_pathlib_surface():
    root = _root()

    # joining + name/suffix/parent/parts
    p = root / "notes.txt"
    assert str(p) == "origofs:///notes.txt"
    assert p.name == "notes.txt" and p.suffix == ".txt"
    assert str(p.parent) == "origofs:///"
    assert p.parts == ("/", "notes.txt")

    # read / write, text and bytes
    assert p.write_text("line one\nline two\n") == 18
    assert p.read_text() == "line one\nline two\n"
    assert p.read_bytes() == b"line one\nline two\n"

    # predicates
    assert p.exists() and p.is_file() and root.is_dir()
    assert not (root / "missing").exists()

    # mkdir(parents=True) + nested write, then iterdir / glob / stat
    nested = root / "sub" / "dir"
    nested.mkdir(parents=True, exist_ok=True)
    (nested / "a.txt").write_text("aaa")
    assert sorted(str(x) for x in root.iterdir()) == ["origofs:///notes.txt", "origofs:///sub"]
    assert sorted(str(x) for x in root.glob("**/*.txt")) == [
        "origofs:///notes.txt",
        "origofs:///sub/dir/a.txt",
    ]
    assert p.stat().st_size == 18

    # open() through UPath: buffered write then seekable ranged read
    with (root / "big.bin").open("wb") as f:
        f.write(b"0123456789" * 100)
    with (root / "big.bin").open("rb") as f:
        f.seek(20)
        assert f.read(5) == b"01234"

    # rename (native) + unlink
    moved = p.rename(root / "renamed.txt")
    assert moved.read_text() == "line one\nline two\n" and not p.exists()
    moved.unlink()
    assert not moved.exists()


if __name__ == "__main__":
    test_upath_resolves_to_explicit_impl_without_warnings()
    test_upath_pathlib_surface()
    print("OK  origofs UPath")
