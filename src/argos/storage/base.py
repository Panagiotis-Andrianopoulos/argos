"""Declarative base for all SQLAlchemy models.

Each model in `models/` inherits from the `Base` class defined
here. SQLAlchemy uses it to know which tables exist and to build the schema.
"""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Root declarative base for all ARGOS models."""


class UUIDMixin:
    """Provides `id` primary key UUID to each model."""

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)


class TimestampMixin:
    """Provides `created_at` and `updated_at` to each model."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
