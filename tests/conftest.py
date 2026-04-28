"""Shared pytest fixtures for the ARGOS test suite.

This module is auto-discovered by pytest and its fixtures are available
in any test under tests/ without explicit import.

Fixture conventions:
- function-scoped: fresh instance per test (the default; used for DB sessions)
- session-scoped: created once for the entire test run (used for engines)
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from argos.config import settings


@pytest.fixture(scope="session")
def db_engine() -> Iterator[Engine]:
    """Create a SQLAlchemy engine for the entire test session.

    Reuses the same Postgres database that local development uses, but each
    test gets its own transaction that is rolled back at the end (see the
    'db_session' fixture below). That makes the tests fast and isolated
    while still exercising the real schema.
    """
    engine = create_engine(settings.database_url_sync, pool_pre_ping=True)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine: Engine) -> Iterator[Session]:
    """Yield a database session wrapped in a transaction that is rolled back.

    Pattern: every test starts a transaction, executes whatever it needs
    and the fixture rolls back on teardown. The database ends up in the
    same state as it started - no test pollution.

    This works even if the code under test calls 'session.commit()',
    because we use a SAVEPOINT trick: the outer transaction is never
    committed.
    """
    connection = db_engine.connect()
    transaction = connection.begin()

    session_factory = sessionmaker(bind=connection, expire_on_commit=False)
    session = session_factory()

    # Begin a SAVEPOINT so that user-level commits become nested commits
    # that are still rolled back when the outer transaction is rolled back.
    nested = connection.begin_nested()

    # When the SAVEPOINT ends (commit or release), automatically start a
    # new one. This keeps every user-level commit() inside the test
    # isolated within the outer transaction.
    @event.listens_for(session, "after_transaction_end")
    def restart_savepoint(sess: Session, trans: object) -> None:
        nonlocal nested
        if not nested.is_active:
            nested = connection.begin_nested()

    try:
        yield session
    finally:
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()
