"""SQLAlchemy model for FRED observations (data points)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from argos.storage.base import Base

if TYPE_CHECKING:
    from argos.storage.models.fred.series import FredSeries


class FredObservation(Base):
    """A single data point for a FRED series.

    Composite primary key (series_id, observation_date) because there is
    exactly one observation per series per date. No surrogate UUID needed.

    Note: does NOT use TimestampMixin because the "created_at / updated_at"
    concept is replaced by a single 'ingested_at', tracking the last time
    we wrote or updated this row from FRED.
    """

    __tablename__ = "fred_observations"

    series_id: Mapped[str] = mapped_column(
        String(50),
        ForeignKey("fred_series.series_id", ondelete="CASCADE"),
        primary_key=True,
    )
    observation_date: Mapped[date] = mapped_column(primary_key=True)

    value: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))

    realtime_start: Mapped[date] = mapped_column(nullable=False)
    realtime_end: Mapped[date] = mapped_column(nullable=False)

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    series: Mapped[FredSeries] = relationship(back_populates="observations")

    def __repr__(self) -> str:
        return (
            f"FredObservation(series_id={self.series_id!r}, "
            f"date={self.observation_date}, value={self.value})"
        )
