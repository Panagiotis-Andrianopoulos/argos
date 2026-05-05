"""SQLAlchemy model for economic time series metadata, multi-source."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from argos.storage.base import Base

if TYPE_CHECKING:
    from argos.storage.models.economic.observation import EconomicObservation


class DataSource(str, enum.Enum):
    """Origin of an economic data series.

    str-mixed enum so SQLAlchemy stores the value as a varchar in Postgres
    while Python code can use DataSource.FRED directly. Adding a new source
    means adding one enum member and running a single Alembic migration
    to extend the Postgres enum type.
    """

    FRED = "fred"
    ECB = "ecb"
    EUROSTAT = "eurostat"


class EconomicSeries(Base):
    """Metadata for a single time series, regardless of upstream provider.

    Composite primary key (source, series_id) accomodates the fact that
    different providers can in principle issue identical series identifiers.
    Source-specific fields that we do not query directly (e.g. ECB's
    PRE_BREAK_VALUE, FRED's seasonal_adjustment_short) live in extra_metadata.
    """

    __tablename__ = "economic_series"

    source: Mapped[DataSource] = mapped_column(
        Enum(
            DataSource,
            name="data_source",
            values_callable=lambda x: [e.value for e in x],
        ),
        primary_key=True,
    )
    series_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    title: Mapped[str] = mapped_column(String(512))
    frequency: Mapped[str] = mapped_column(String(10))
    units: Mapped[str | None] = mapped_column(String(255), nullable=True)
    seasonal_adjustment: Mapped[str | None] = mapped_column(String(50), nullable=True)
    last_updated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    extra_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    observations: Mapped[list[EconomicObservation]] = relationship(
        back_populates="series",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return f"EconomicSeries(source={self.source.value!r}, series_id={self.series_id!r})"
