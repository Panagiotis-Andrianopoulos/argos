"""SQLAlchemy model for individual economic observations, multi-source."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import Date, DateTime, Enum, ForeignKeyConstraint, Numeric, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from argos.storage.base import Base
from argos.storage.models.economic.series import DataSource

if TYPE_CHECKING:
    from argos.storage.models.economic.series import EconomicSeries


class EconomicObservation(Base):
    """A single (period, value) observation for a series.

    Composite PK (source, series_id, observation_date) ensures idempotency:
    re-ingesting the same period_overwrites in place via ON CONFLICT.
    Composite FK to economic_series enforces referential integrity.
    """

    __tablename__ = "economic_observations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source", "series_id"],
            ["economic_series.source", "economic_series.series_id"],
            ondelete="CASCADE",
        ),
    )

    source: Mapped[DataSource] = mapped_column(
        Enum(
            DataSource,
            name="data_source",
            values_callable=lambda x: [e.value for e in x],
            create_type=False,
        ),
        primary_key=True,
    )
    series_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    observation_date: Mapped[date] = mapped_column(Date, primary_key=True)
    value: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    extra_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    series: Mapped[EconomicSeries] = relationship(back_populates="observations")

    def __repr__(self) -> str:
        return (
            f"EconomicObservation("
            f"source={self.source.value!r}, "
            f"series_id={self.series_id!r}, "
            f"observation_date={self.observation_date!r}, "
            f"value={self.value!r})"
        )
