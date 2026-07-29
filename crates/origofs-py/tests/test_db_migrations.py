"""origofs.db: SQLAlchemy models + Alembic migrations for the metadata schema.

Needs the `db` extra (`pip install "origofs[db]"`). Build + run (from
crates/origofs-py, in a venv):
    maturin develop
    pip install "origofs[db]"
    python tests/test_db_migrations.py       # or: pytest tests/
"""
import os
import tempfile

import pytest

origofs_db = pytest.importorskip("origofs.db")

from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine


def _sqlite_url(tmpdir: str) -> str:
    return f"sqlite:///{os.path.join(tmpdir, 'meta.db')}"


def test_upgrade_creates_every_table_and_is_idempotent():
    d = tempfile.mkdtemp()
    url = _sqlite_url(d)

    origofs_db.upgrade(url)
    origofs_db.upgrade(url)  # re-running head must not error or duplicate seed rows

    engine = create_engine(url)
    with engine.connect() as conn:
        tables = {
            row[0]
            for row in conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    expected = set(origofs_db.Base.metadata.tables) | {
        "alembic_version",
        "sqlite_sequence",
    }
    assert expected <= tables


def test_upgrade_seeds_engine_compatible_bootstrap_rows():
    """The engine's own MetadataStore::init seeds the `default` workspace, its
    root inode (ino=1, mode=0o040755), and schema_meta stamped through the
    latest version on a fresh store. An Alembic-provisioned store must match,
    or the engine treats migrations as pending and re-applies (for V11/V13,
    rebuilds) tables Alembic already created rows in."""
    d = tempfile.mkdtemp()
    url = _sqlite_url(d)
    origofs_db.upgrade(url)

    engine = create_engine(url)
    with engine.connect() as conn:
        versions = [
            v
            for (v,) in conn.exec_driver_sql(
                "SELECT version FROM schema_meta ORDER BY version"
            )
        ]
        assert versions == list(range(1, origofs_db.SCHEMA_VERSION + 1))

        ws = list(conn.exec_driver_sql("SELECT id, name, root_ino FROM workspace"))
        assert ws == [(1, "default", 1)]

        inode = list(
            conn.exec_driver_sql(
                "SELECT ino, workspace_id, kind, mode FROM inode WHERE ino = 1"
            )
        )
        assert inode == [(1, 1, "dir", 0o040755)]


def test_models_match_the_migration_exactly():
    """No drift between origofs.db.models (Alembic's autogenerate target and
    the schema the engine must interoperate with) and what the initial
    revision actually creates."""
    d = tempfile.mkdtemp()
    url = _sqlite_url(d)
    origofs_db.upgrade(url)

    engine = create_engine(url)
    with engine.connect() as conn:
        ctx = MigrationContext.configure(conn)
        diff = compare_metadata(ctx, origofs_db.Base.metadata)
    assert diff == []


def test_downgrade_drops_everything():
    d = tempfile.mkdtemp()
    url = _sqlite_url(d)
    origofs_db.upgrade(url)
    origofs_db.downgrade(url, "base")

    engine = create_engine(url)
    with engine.connect() as conn:
        tables = {
            row[0]
            for row in conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' AND name != 'alembic_version'"
            )
        }
    assert tables == set()


def test_get_alembic_config_falls_back_to_env_var(monkeypatch):
    d = tempfile.mkdtemp()
    url = _sqlite_url(d)
    monkeypatch.setenv("ORIGOFS_DATABASE_URL", url)

    cfg = origofs_db.get_alembic_config()
    assert cfg.get_main_option("sqlalchemy.url") == url


def _run_all():
    import inspect

    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and inspect.isfunction(fn):
            sig = inspect.signature(fn)
            if "monkeypatch" in sig.parameters:
                continue  # pytest-only fixture; skipped under plain `python …`
            fn()
            print("ok  ", name)
    print("ALL OK")


if __name__ == "__main__":
    _run_all()
