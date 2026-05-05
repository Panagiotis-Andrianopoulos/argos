"""Persistence layer for FRED data.

Takes validated Pydantic models from the FRED client and writes them
to the unified economic_* tables in Postgres using upsert semantics
(INSERT ... ON CONFLICT UPDATE).

This module bridges the ingestion package (which knows how to talk to
FRED) and the storage package (which knows how to talk to Postgres).
The public API takes FRED-specific Pydantic models but writes to the
provider-agnostic economic_series / economic_observations tables,
mapping FRED-specific fields into the JSONB extra_metadata columns.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import insert as pg_insert

from argos.storage.models.economic import (
    DataSource,
    EconomicObservation,
    EconomicSeries,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from argos.ingestion.fred.models import Observation, Series

logger = logging.getLogger(__name__)

_SOURCE = DataSource.FRED


def upsert_series(session: Session, series: Series) -> None:
    """Insert or update a FRED series in the unified economic_series table.

    Uses Postgres INSERT ... ON CONFLICT DO UPDATE so that re-running
    ingestion on an already-persisted series refreshes the metadata
    without raising duplicate-key errors. FRED-specific fields that
    don't have a first-class home in the unified schema are folded
    into the JSONB extra_metadata columns.

    Args:
        session: Active SQLAlchemy session. The caller is responsible
            for commit/rollback.
        series: Validated Pydantic Series from the FRED API.
    """
    extra_metadata = {
        "notes": series.notes,
        "frequency_short": series.frequency_short,
        "units_short": series.units_short,
        "seasonal_adjustment_short": series.seasonal_adjustment_short,
        "observation_start": series.observation_start.isoformat(),
        "observation_end": series.observation_end.isoformat(),
    }

    values = {
        "source": _SOURCE,
        "series_id": series.id,
        "title": series.title,
        "frequency": series.frequency,
        "units": series.units,
        "seasonal_adjustment": series.seasonal_adjustment,
        "last_updated": series.last_updated,
        "extra_metadata": extra_metadata,
    }

    stmt = pg_insert(EconomicSeries).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "series_id"],
        set_={k: v for k, v in values.items() if k not in ("source", "series_id")},
    )
    session.execute(stmt)
    logger.debug("Upserted FRED series %s", series.id)


def upsert_observations(
    session: Session,
    series_id: str,
    observations: list[Observation],
) -> int:
    """Bulk insert or update FRED observations for a series.

    FRED revision metadata (realtime_start and realtime_end) is preserved
    in the JSONB extra_metadata column for audit/reconstruction. The
    'status' column is left NULL because FRED does not expose a status
    field (it's an ECB/SDMX concept).

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
            "source": _SOURCE,
            "series_id": series_id,
            "observation_date": obs.date,
            "value": obs.value,
            "status": None,
            "extra_metadata": {
                "realtime_start": obs.realtime_start.isoformat(),
                "realtime_end": obs.realtime_end.isoformat(),
            },
        }
        for obs in observations
    ]

    stmt = pg_insert(EconomicObservation).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "series_id", "observation_date"],
        set_={
            "value": stmt.excluded.value,
            "status": stmt.excluded.status,
            "extra_metadata": stmt.excluded.extra_metadata,
        },
    )
    session.execute(stmt)
    logger.debug("Upserted %d FRED observations for %s", len(rows), series_id)
    return len(rows)
