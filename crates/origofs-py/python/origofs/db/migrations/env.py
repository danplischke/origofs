"""Alembic environment for the origofs metadata schema.

Resolves the database URL from, in order: the ``-x db_url=...`` command-line
argument, the ``ORIGOFS_DATABASE_URL`` environment variable, or the
``sqlalchemy.url`` key in whatever ``alembic.ini`` invoked this (blank by
default — ``origofs.db.get_alembic_config`` sets it from its ``url`` argument
instead of relying on a config file). Autogenerate diffs against
``origofs.db.models.Base.metadata``, the single source of truth for this schema
shared with the Rust engine (``crates/origofs-core/src/migrations.rs``).
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from origofs.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_url() -> str:
    x_args = context.get_x_argument(as_dictionary=True)
    url = (
        x_args.get("db_url")
        or os.environ.get("ORIGOFS_DATABASE_URL")
        or config.get_main_option("sqlalchemy.url")
    )
    if not url:
        raise RuntimeError(
            "No database URL: pass one to origofs.db.upgrade()/get_alembic_config(), "
            "set ORIGOFS_DATABASE_URL, or run alembic with -x db_url=<url>."
        )
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = _resolve_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
