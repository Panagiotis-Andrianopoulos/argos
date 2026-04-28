"""Database engine and session management.

Exposes:
- `engine` — the SQLAlchemy engine (connection pool)
- `SessionLocal` — factory for sessions
- `get_session()` — context manager for use in scripts
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from argos.config import settings


def _create_engine() -> Engine:
    """Creates the SQLAlchemy engine from the settings."""
    return create_engine(
        settings.database_url_sync,
        echo=False,  # True to see every SQL statement in stdout
        pool_pre_ping=True,  # checks connection health
        pool_size=5,
        max_overflow=10,
    )


engine: Engine = _create_engine()

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager for a database session.

    Usage:
        with get_session() as session:
            source = Source(name="spitogatos", base_url="https://...")
            session.add(source)
            session.commit()
    """
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
