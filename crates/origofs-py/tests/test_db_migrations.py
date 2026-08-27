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
from sqlalchemy import create_engine, text


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


# --- Postgres (self-skips unless ORIGOFS_PG_TEST_URL is set, same convention
# as test_subscribe.py / test_coedit_cluster.py) -----------------------------
#
# Its own dedicated database, not the shared `dbname=origofs` those tests use:
# this suite does full-schema create_all()/drop_all(), which would be
# destructive against a database other tests keep workspace-scoped state in.

def _pg_admin_and_test_urls():
    """Parse the libpq keyword/value DSN in ORIGOFS_PG_TEST_URL (host=... "
    port=... user=... password=... dbname=...) into two SQLAlchemy URLs on the
    same server: one to the admin-supplied dbname (for CREATE/DROP DATABASE),
    one to a dedicated `origofs_py_db_test` database for the actual test."""
    dsn = os.environ.get("ORIGOFS_PG_TEST_URL")
    if not dsn:
        return None, None
    parts = dict(p.split("=", 1) for p in dsn.split())
    host = parts.get("host", "localhost")
    port = parts.get("port", "5432")
    user = parts.get("user", "postgres")
    password = parts.get("password")
    auth = f"{user}:{password}" if password else user
    admin_url = f"postgresql+psycopg://{auth}@{host}:{port}/{parts.get('dbname', 'postgres')}"
    test_url = f"postgresql+psycopg://{auth}@{host}:{port}/origofs_py_db_test"
    return admin_url, test_url


def _fresh_pg_test_db():
    """(test_url, or None if ORIGOFS_PG_TEST_URL is unset) -- (re)creates the
    dedicated test database empty each call."""
    admin_url, test_url = _pg_admin_and_test_urls()
    if admin_url is None:
        return None
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        conn.exec_driver_sql("DROP DATABASE IF EXISTS origofs_py_db_test")
        conn.exec_driver_sql("CREATE DATABASE origofs_py_db_test")
    admin_engine.dispose()
    return test_url


def test_pg_upgrade_seeds_rows_and_advances_sequences_without_collision():
    """Regression test for the Postgres setval guard (mirrors the Rust fix
    'don't reset the inode identity sequence on every open'): after the
    initial revision seeds workspace id=1 / inode ino=1, a real
    nextval-driven insert (the shape the engine's own writes take) must get
    the NEXT id, not collide with the seeded row."""
    test_url = _fresh_pg_test_db()
    if test_url is None:
        pytest.skip("ORIGOFS_PG_TEST_URL unset")

    origofs_db.upgrade(test_url)
    origofs_db.upgrade(test_url)  # idempotency, same as the SQLite test above

    engine = create_engine(test_url)
    with engine.connect() as conn:
        versions = [v for (v,) in conn.execute(text("SELECT version FROM schema_meta ORDER BY version"))]
        assert versions == list(range(1, origofs_db.SCHEMA_VERSION + 1))
        assert list(conn.execute(text("SELECT id, name, root_ino FROM workspace"))) == [(1, "default", 1)]
        assert list(conn.execute(text(
            "SELECT ino, workspace_id, kind, mode FROM inode WHERE ino = 1"
        ))) == [(1, 1, "dir", 0o040755)]

        # The actual bug: a real nextval-driven insert must not collide with
        # the explicitly-seeded id=1 rows.
        conn.execute(text("INSERT INTO workspace(name, root_ino, created_at) VALUES ('second', 1, 0)"))
        conn.execute(text(
            "INSERT INTO inode(workspace_id, kind, mode, mtime, ctime) VALUES (1, 'dir', 16877, 0, 0)"
        ))
        conn.commit()
        assert list(conn.execute(text("SELECT id, name FROM workspace ORDER BY id"))) == [
            (1, "default"), (2, "second"),
        ]
        assert list(conn.execute(text("SELECT ino, kind FROM inode ORDER BY ino"))) == [
            (1, "dir"), (2, "dir"),
        ]

        ctx = MigrationContext.configure(conn)
        assert compare_metadata(ctx, origofs_db.Base.metadata) == []

    origofs_db.downgrade(test_url, "base")
    with engine.connect() as conn:
        tables = [r[0] for r in conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename != 'alembic_version'"
        ))]
        assert tables == []


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
