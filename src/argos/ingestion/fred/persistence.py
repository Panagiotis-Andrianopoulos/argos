"""Persistence layer for FRED data.

Takes validated Pydantic models from the FRED client and writes them
to Postgres using upsert semantics (INSERT ... ON CONFLICT UPDATE).

This is where the `ingestion` package and the `storage` package meet:
the former knows how to talk to FRED, the latter knows how to talk
to Postgres, and this module bridges the two.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import insert as pg_insert

from argos.storage.models.fred import FredObservation, FredSeries

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from argos.ingestion.fred.models import Observation, Series

logger = logging.getLogger(__name__)


def upsert_series(session: Session, series: Series) -> None:
    """Insert or update a FRED series in the database.

    Uses Postgres INSERT ... ON CONFLICT DO UPDATE so that re-running
    ingestion on an already-persisted series refreshes the metadata and
    bumps 'last_ingested_at' without raising duplicate-key errors.

    Args:
        session: Active SQLAlchemy session. The caller is responsible
            for commit/rollback.
        series: Validated Pydantic Series from the FRED API.
    """

    values = {
        "series_id": series.id,
        "title": series.title,
        "frequency": series.frequency,
        "frequency_short": series.frequency_short,
        "units": series.units,
        "units_short": series.units_short,
        "seasonal_adjustment": series.seasonal_adjustment,
        "seasonal_adjustment_short": series.seasonal_adjustment_short,
        "notes": series.notes,
        "observation_start": series.observation_start,
        "observation_end": series.observation_end,
        "last_updated": series.last_updated,
    }

    stmt = pg_insert(FredSeries).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["series_id"],
        set_={k: v for k, v in values.items() if k != "series_id"},
    )
    session.execute(stmt)
    logger.debug("Upserted FRED series %s", series.id)


def upsert_observations(
    session: Session,
    series_id: str,
    observations: list[Observation],
) -> int:
    """Bulk insert or update FRED observations for a series.

    Args:
        session: Active SQLAlchemy session. The caller is responsible
            from commit/rollback.
        series_id: The FRED series these observations belong to.
        observations: List of validated Pydantic Observation models.

    Returns:
        The number of rows processed (same as len(observations)).
    """
    if not observations:
        return 0

    rows = [
        {
            "series_id": series_id,
            "observation_date": obs.date,
            "value": obs.value,
            "realtime_start": obs.realtime_start,
            "realtime_end": obs.realtime_end,
        }
        for obs in observations
    ]

    stmt = pg_insert(FredObservation).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["series_id", "observation_date"],
        set_={
            "value": stmt.excluded.value,
            "realtime_start": stmt.excluded.realtime_start,
            "realtime_end": stmt.excluded.realtime_end,
        },
    )
    session.execute(stmt)
    logger.debug("Upserted %d FRED observations for %s", len(rows), series_id)
    return len(rows)
