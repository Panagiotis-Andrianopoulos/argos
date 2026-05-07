"""Persistence layer for ECB Data Portal data.

Takes validated Pydantic models from the ECB client and writes them
to the unified economic_* tables in Postgres using upsert sematnics
(INSERT ... IN CONFLICT UPDATE).

Mirrors the structure of argos.ingestion.fred.persistence so the two
sources behave identically from the storages layer's perspective.
The mapping from ECB-specific shapes to the unified schema:

    EcbSeries.key               -> economic_series.series_id
    EcbSeries.title             -> economic_series.title
    EcbSeries.freq              -> economic_series.frequency
    EcbSeries.unit_index_base   -> economic_series.units
    EcbSeries.adjustment        -> economic_series.seasonal_adjustment
    SDMX dimensions + extras    -> economic_series.extra_metadata (JSONB)

    EcbObservation.time_period  -> observation_date (via period_to_date)
    EcbObservation.obs_value    -> value
    EcbObservation.obs_status   -> status
    conf_status, pre_break, ... -> extra_metadata (JSONB)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import insert as pg_insert

from argos.ingestion.ecb.period import period_to_date
from argos.storage.models.economic import (
    DataSource,
    EconomicObservation,
    EconomicSeries,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from argos.ingestion.ecb.models import EcbObservation, EcbSeries

logger = logging.getLogger(__name__)

_SOURCE = DataSource.ECB


def upsert_series(session: Session, series: EcbSeries) -> None:
    """Insert or update an ECB series in the unified economic_series table.

    Uses Postgres INSERT ... IN CONFLICT DO UPDATE so that re-running
    ingestion on an already-persisted series refreshes the metadata
    without raising duplicate-key errors. ECB-specific fields (the 10
    SDMX dimensions, extra CSV columns) are folded into the JSONB
    extra_metadata column.

    Args:
        session: Active SQLAlchemy session. The caller is responsible
            for commit/rollback.
        series: Validated Pydantic EcbSeries from the ECB client.
    """
    extra_metadata = {
        # The 10 SDMX dimensions, preserved for analytical drill-down.
        "ref_area": series.ref_area,
        "region": series.region,
        "adjustment": series.adjustment,
        "property_type": series.property_type,
        "indicator": series.indicator,
        "data_provider": series.data_provider,
        "price_type": series.price_type,
        "transformation": series.transformation,
        "unit_measure": series.unit_measure,
        # Optional ECB metadata fields.
        "title_compl": series.title_compl,
        "time_format": series.time_format,
        "decimals": series.decimals,
        # Anything else the CSV had that we don't model first-class.
        **series.extra_fields,
    }

    values = {
        "source": _SOURCE,
        "series_id": series.key,
        "title": series.title,
        "frequency": series.freq,
        "units": series.unit_index_base,
        "seasonal_adjustment": series.adjustment,
        "last_updated": None,
        "extra_metadata": extra_metadata,
    }

    stmt = pg_insert(EconomicSeries).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "series_id"],
        set_={k: v for k, v in values.items() if k not in ("source", "series_id")},
    )
    session.execute(stmt)
    logger.debug("Upserted ECB series %s", series.key)


def upsert_observations(
    session: Session,
    series_key: str,
    observations: list[EcbObservation],
    *,
    frequency: str,
) -> int:
    """Bulk insert ot update ECB observations for a series.

    The frequency is required because ECB time periods (e.g. '2025-Q1')
    can only be mapped to a Postgres DATE if we know whether they are
    quarterly, monthly or annual. The caller (typically the DAG) reads
    it from the corresponding EcbSeries.freq.

    SDMC-specific observation fields (conf_status, pre_break_value,
    comment_obs) are preserved in the JSONB extra_metadata column.

    Args:
        session: Active SQLAlchemy session. The caller is responsible
            from commit/rollback.
        series_key: The full ECB series key (e.g. RESR.Q.GR..."), used
            as the series_id in the unified schema.
        observations: List of validated Pydantic EcbObservation models.
        frequency: Series frequency code ('Q', 'M', 'A') driving the
            period-to-date mapping.

    Returns:
        The number of rows processed (same as len(observations)).

    Raises:
        UnsupportedFrequencyError: If 'frequency' has no period parser.
        ValueError: If a time_period string doesn't match its frequency
            format.
    """
    if not observations:
        return 0

    rows = [
        {
            "source": _SOURCE,
            "series_id": series_key,
            "observation_date": period_to_date(obs.time_period, frequency),
            "value": obs.obs_value,
            "status": obs.obs_status,
            "extra_metadata": {
                "time_period": obs.time_period,
                "conf_status": obs.conf_status,
                "pre_break_value": (
                    str(obs.pre_break_value) if obs.pre_break_value is not None else None
                ),
                "comment_obs": obs.comment_obs,
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
    logger.debug("Upserted %d ECB observations for %s", len(rows), series_key)
    return len(rows)
