"""Alembic environment script.

Runs every time you execute `alembic upgrade`, `alembic revision`, etc.
Bridges Alembic with the ARGOS config and SQLAlchemy models.
"""

import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# Import the ARGOS settings and models
from argos.config import settings
from argos.storage.base import Base
from argos.storage.models import Listing, Property, Source  # noqa: F401

# ============================================================
# Config
# ============================================================

# The Alembic Config object — gives access to the values in alembic.ini
config = context.config

# Override the sqlalchemy.url from our settings
# This way the password is not needed in alembic.ini
config.set_main_option("sqlalchemy.url", settings.database_url)

# Setup logging from alembic.ini if it exists
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# The metadata that Alembic will compare with the DB
# This is what allows --autogenerate
target_metadata = Base.metadata


# ============================================================
# Offline mode — generates SQL without connecting to the DB
# ============================================================


def run_migrations_offline() -> None:
    """Run migrations in offline mode.

    No active connection to the DB is needed — just generates the SQL.
    Useful to see what will run without actually running it.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# ============================================================
# Online mode — connects to the DB and applies migrations
# ============================================================


def do_run_migrations(connection: Connection) -> None:
    """Run migrations on a given connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,  # detect column type changes
        compare_server_default=True,  # detect default value changes
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Async flavor of the online mode."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Entry point for online mode."""
    asyncio.run(run_async_migrations())


# ============================================================
# Dispatch
# ============================================================

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
