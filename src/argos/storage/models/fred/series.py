"""SQLAlchemy model for FRED series metadata.

One row per FRED time series (e.g. QGRN628BIS for Greek residential
property prices). Updated whenever we re-fetch metadata from FRED.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from argos.storage.base import Base, TimestampMixin

if TYPE_CHECKING:
    from argos.storage.models.fred.observation import FredObservation


class FredSeries(TimestampMixin, Base):
    """FRED time series metadata.

    Natural primary key is the FRED series_id - no surrogate UUID needed
    because FRED guarantees uniqueness and stability of these IDs.
    """

    __tablename__ = "fred_series"

    series_id: Mapped[str] = mapped_column(String(50), primary_key=True)

    title: Mapped[str] = mapped_column(String(500), nullable=False)
    frequency: Mapped[str] = mapped_column(String(50), nullable=False)
    frequency_short: Mapped[str] = mapped_column(String(5), nullable=False)
    units: Mapped[str] = mapped_column(String(200), nullable=False)
    units_short: Mapped[str] = mapped_column(String(50), nullable=False)
    seasonal_adjustment: Mapped[str] = mapped_column(String(100), nullable=False)
    seasonal_adjustment_short: Mapped[str] = mapped_column(String(10), nullable=False)
    notes: Mapped[str] = mapped_column(String)

    observation_start: Mapped[date] = mapped_column(nullable=False)
    observation_end: Mapped[date] = mapped_column(nullable=False)

    last_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    last_ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    observations: Mapped[list[FredObservation]] = relationship(
        back_populates="series",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"FredSeries(series_id={self.series_id!r}, title={self.title!r}...)"
