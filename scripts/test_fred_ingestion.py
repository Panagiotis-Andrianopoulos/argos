"""Smoke test for the full FRED ingestion pipeline.

Downloads Greek residential property prices from FRED, stores them in MinIO,
reads them back, persists them to Postgres and verifies round-trip integrity
plus upsert idempotency.

Usage:
    python scripts/test_fred_ingestion.py
"""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

from sqlalchemy import func, select

from argos.ingestion.fred import (
    RESIDENTIAL_PROPERTY_PRICE_NOMINAL,
    FredClient,
)
from argos.ingestion.fred.persistence import upsert_observations, upsert_series
from argos.storage.database import get_session
from argos.storage.models.fred import FredObservation, FredSeries
from argos.storage.raw_data import RawDataReader, RawDataWriter


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    log = logging.getLogger("argos.smoke_test")

    series_id = RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id
    snapshot_date = datetime.now(UTC).date()

    log.info("Fetching FRED data for %s", series_id)
    with FredClient.from_settings() as fred:
        series = fred.get_series(series_id)
        observations = fred.get_observations(series_id)

    log.info("Got %d observations, most recent: %s", len(observations), observations[-1].date)

    log.info("Writing to object store")
    with RawDataWriter.from_settings() as writer:
        series_key = writer.save_fred_series(series, snapshot_date=snapshot_date)
        obs_key = writer.save_fred_observations(
            series_id, observations, snapshot_date=snapshot_date
        )

    log.info("Reading back from object store")
    with RawDataReader.from_settings() as reader:
        loaded_series = reader.load_fred_series(series_id, snapshot_date=snapshot_date)
        loaded_observations = reader.load_fred_observations(series_id, snapshot_date=snapshot_date)

    log.info("Verifying data integrity")
    assert loaded_series.id == series.id, "Series ID mismatch after round-trip"
    assert loaded_series.title == series.title, "Series title mismatch"
    assert loaded_series.frequency == series.frequency, "Series frequency mismatch"
    log.info("Series metadata round-trip: OK")

    assert len(loaded_observations) == len(
        observations
    ), f"Observation count mismatch: {len(loaded_observations)} vs {len(observations)}"
    assert loaded_observations[0].date == observations[0].date, "First date mismatch"
    assert loaded_observations[-1].date == observations[-1].date, "Last date mismatch"
    assert loaded_observations[0].value == observations[0].value, "First value mismatch"
    log.info("Observations round-trip: OK (%d data points verified)", len(observations))

    log.info("Persisting to Postgres")
    with get_session() as session:
        upsert_series(session, series)
        rows_affected = upsert_observations(session, series_id, observations)
        session.commit()
    log.info("Upserted series + %d observations", rows_affected)

    log.info("Verifying Postgres state")
    with get_session() as session:
        db_series = session.get(FredSeries, series_id)
        assert db_series is not None, f"Series {series_id} not found in DB after upsert"
        assert db_series.title == series.title, "DB series title mismatch"
        assert db_series.frequency == series.frequency, "DB series frequency mismatch"
        log.info("FredSeries row: OK")

        obs_count = session.scalar(
            select(func.count())
            .select_from(FredObservation)
            .where(FredObservation.series_id == series_id)
        )
        assert obs_count == len(
            observations
        ), f"DB observation count mismatch: {obs_count} vs {len(observations)}"
        log.info("FredObservation count: OK (%d rows)", obs_count)

    log.info("Re-running upsert to verify idempotency")
    with get_session() as session:
        upsert_series(session, series)
        upsert_observations(session, series_id, observations)
        session.commit()

    with get_session() as session:
        obs_count_after = session.scalar(
            select(func.count())
            .select_from(FredObservation)
            .where(FredObservation.series_id == series_id)
        )
        assert obs_count_after == len(
            observations
        ), f"Idempotency violated: count went from {len(observations)} to {obs_count_after}"
        log.info("Idempotency: OK (count stable at %d)", obs_count_after)

    log.info("")
    log.info("Summary")
    log.info("  Series key:         %s", series_key)
    log.info("  Observations key:   %s", obs_key)
    log.info("  Data points:        %d", len(observations))
    log.info("  Date range:         %s to %s", observations[0].date, observations[-1].date)
    log.info("  DB rows persisted:  1 series + %d observations", obs_count_after)
    log.info("")
    log.info("Smoke test passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
