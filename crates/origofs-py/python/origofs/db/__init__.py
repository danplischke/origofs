"""SQLAlchemy models + Alembic migrations for the origofs metadata store.

``origofs.db.models`` declares the schema (see that module's docstring for the
metadata/content split and dual-dialect rationale); this module packages Alembic
migrations for it under ``origofs.db.migrations`` and gives you a programmatic
way to run them without hand-writing an ``alembic.ini``::

    import origofs.db

    origofs.db.upgrade("sqlite:///meta.db")                 # or a postgresql+psycopg:// URL
    origofs.db.upgrade("postgresql+psycopg://user@host/db")

This targets the same schema the Rust engine's own migrator builds
(``crates/origofs-core/src/migrations.rs``), so a database Alembic creates is
readable by the engine and vice versa — see ``origofs.db.models.SCHEMA_VERSION``
and the initial revision for how the two migration ledgers (Alembic's
``alembic_version`` table and the engine's ``schema_meta`` table) coexist.

Developing origofs itself — editing ``models.py`` and authoring a new revision
with ``alembic revision --autogenerate`` — uses the ``alembic.ini`` at the
`origofs-py` crate root instead of this module (see that file's header).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from .models import (
    Actor,
    Base,
    BlobBlame,
    Config,
    Conflict,
    Dentry,
    EditOp,
    FileLock,
    FsEvent,
    Inode,
    LineBlame,
    Presence,
    Ref,
    SCHEMA_VERSION,
    SchemaMeta,
    Session,
    Suggestion,
    Symlink,
    ToolCall,
    Workspace,
)

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def get_alembic_config(url: Optional[str] = None):
    """Build an Alembic ``Config`` pointing at origofs's packaged migrations.

    ``url`` is a SQLAlchemy database URL (e.g. ``"sqlite:///meta.db"`` or
    ``"postgresql+psycopg://user@host/db"``); if omitted, the
    ``ORIGOFS_DATABASE_URL`` environment variable is used. Use this directly if
    you want the ``Config`` object for something other than a plain
    upgrade/downgrade (e.g. ``alembic.command.current``).
    """
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))
    resolved = url or os.environ.get("ORIGOFS_DATABASE_URL")
    if resolved:
        cfg.set_main_option("sqlalchemy.url", resolved)
    return cfg


def upgrade(url: Optional[str] = None, revision: str = "head") -> None:
    """Run origofs's Alembic migrations up to ``revision`` (default: latest)."""
    from alembic import command

    command.upgrade(get_alembic_config(url), revision)


def downgrade(url: Optional[str] = None, revision: str = "-1") -> None:
    """Run origofs's Alembic migrations down to ``revision`` (default: one step back)."""
    from alembic import command

    command.downgrade(get_alembic_config(url), revision)


__all__ = [
    "Base",
    "SCHEMA_VERSION",
    "get_alembic_config",
    "upgrade",
    "downgrade",
    "Actor",
    "BlobBlame",
    "Config",
    "Conflict",
    "Dentry",
    "EditOp",
    "FileLock",
    "FsEvent",
    "Inode",
    "LineBlame",
    "Presence",
    "Ref",
    "SchemaMeta",
    "Session",
    "Suggestion",
    "Symlink",
    "ToolCall",
    "Workspace",
]
