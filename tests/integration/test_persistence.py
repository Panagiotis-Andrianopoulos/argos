"""Integration tests for the FRED persistence layer.

These tests run against a real Postgres database (the same one Docker
Compose spins up). Each test runs in its own transaction that is rolled
back at the end, so they leave no trace.

Although the persistence layer takes FRED-specific Pydantic models as
input, it now writes to the unified economic_* tables. The tests assert
on those tables directly to verify the mapping behaviour, including
that FRED-specific fields land in the JSONB extra_metadata columns.

Mark all tests in this module with `integration` so we can selectively
skip them in fast feedback loops:

    pytest -m "not integration"
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from argos.ingestion.fred.models import Observation, Series
from argos.ingestion.fred.persistence import upsert_observations, upsert_series
from argos.storage.models.economic import (
    DataSource,
    EconomicObservation,
    EconomicSeries,
)

pytestmark = pytest.mark.integration

# ============================================================
# Fixtures: factory functions for test data
# ============================================================


def _make_series(series_id: str = "TEST_SERIES") -> Series:
    """Build a valid Series pydantic model for tests."""
    return Series(
        id=series_id,
        title="Test Series",
        frequency="Quarterly",
        frequency_short="Q",
        units="Index",
        units_short="Index",
        seasonal_adjustment="Not Seasonally Adjusted",
        seasonal_adjustment_short="NSA",
        observation_start=date(2020, 1, 1),
        observation_end=date(2025, 1, 1),
        last_updated=datetime(2025, 12, 30, 11, 3, 27, tzinfo=UTC),
        notes=None,
    )


def _make_observation(
    obs_date: date,
    value: Decimal | None = Decimal("100.0"),
) -> Observation:
    """Build a valid Observation Pydantic model for tests."""
    return Observation(
        date=obs_date,
        value=value,
        realtime_start=date(2025, 1, 1),
        realtime_end=date(9999, 12, 31),
    )


def _select_series(session: Session, series_id: str) -> EconomicSeries:
    """Helper: fetch a FRED-sourced series row by ID."""
    return session.execute(
        select(EconomicSeries).where(
            EconomicSeries.source == DataSource.FRED,
            EconomicSeries.series_id == series_id,
        )
    ).scalar_one()


def _select_observations(session: Session, series_id: str) -> list[EconomicObservation]:
    """Helper: fetch all FRED-sourced observations for a series, ordered by date."""
    return list(
        session.execute(
            select(EconomicObservation)
            .where(
                EconomicObservation.source == DataSource.FRED,
                EconomicObservation.series_id == series_id,
            )
            .order_by(EconomicObservation.observation_date)
        )
        .scalars()
        .all()
    )


# ============================================================
# Series upsert tests
# ============================================================


class TestUpsertSeries:
    """Verify EconomicSeries upsert behaviour for FRED-sourced data."""

    def test_inserts_new_series(self, db_session: Session) -> None:
        """A series that does not yet exist is created with source='fred'."""
        series = _make_series("NEW_SERIES")

        upsert_series(db_session, series)
        db_session.flush()

        result = _select_series(db_session, "NEW_SERIES")
        assert result.source == DataSource.FRED
        assert result.title == "Test Series"
        assert result.frequency == "Quarterly"

    def test_fred_specific_fields_land_in_metadata(self, db_session: Session) -> None:
        """Fields without a first-class column are preserved in extra_metadata."""
        series = _make_series("METADATA_TEST")

        upsert_series(db_session, series)
        db_session.flush()

        result = _select_series(db_session, "METADATA_TEST")
        meta = result.extra_metadata
        assert meta["frequency_short"] == "Q"
        assert meta["units_short"] == "Index"
        assert meta["seasonal_adjustment_short"] == "NSA"
        assert meta["observation_start"] == "2020-01-01"
        assert meta["observation_end"] == "2025-01-01"
        assert meta["notes"] is None

    def test_updates_existing_series(self, db_session: Session) -> None:
        """Re-running upsert with a changed title updates the row."""
        series_v1 = _make_series("CHANGING_SERIES")
        upsert_series(db_session, series_v1)
        db_session.flush()

        series_v2 = _make_series("CHANGING_SERIES")
        series_v2 = series_v2.model_copy(update={"title": "New Title"})
        upsert_series(db_session, series_v2)
        db_session.flush()

        result = _select_series(db_session, "CHANGING_SERIES")
        assert result.title == "New Title"

    def test_idempotent_repeated_upsert(self, db_session: Session) -> None:
        """Running the same upsert N times produces exactly one row."""
        series = _make_series("IDEMPOTENT")

        for _ in range(3):
            upsert_series(db_session, series)
        db_session.flush()

        rows = db_session.execute(
            select(EconomicSeries).where(
                EconomicSeries.source == DataSource.FRED,
                EconomicSeries.series_id == "IDEMPOTENT",
            )
        ).all()
        assert len(rows) == 1


# ============================================================
# Observations upsert tests
# ============================================================


class TestUpsertObservations:
    """Verify EconomicObservation upsert behaviour for FRED-sourced data."""

    def test_inserts_observations(self, db_session: Session) -> None:
        """A batch of new Observations gets persisted."""
        upsert_series(db_session, _make_series("OBS_TEST"))

        observations = [
            _make_observation(date(2024, 1, 1), Decimal("100.0")),
            _make_observation(date(2024, 4, 1), Decimal("101.5")),
            _make_observation(date(2024, 7, 1), Decimal("103.0")),
        ]
        n = upsert_observations(db_session, "OBS_TEST", observations)
        db_session.flush()

        assert n == 3

        rows = _select_observations(db_session, "OBS_TEST")

        assert len(rows) == 3
        assert rows[0].value == Decimal("100.0")
        assert rows[2].value == Decimal("103.0")
        assert all(row.source == DataSource.FRED for row in rows)

    def test_realtime_fields_land_in_metadata(self, db_session: Session) -> None:
        """FRED revision vintage fields are preserved in extra_metadata."""
        upsert_series(db_session, _make_series("VINTAGE_TEST"))
        upsert_observations(
            db_session,
            "VINTAGE_TEST",
            [_make_observation(date(2024, 1, 1), Decimal("100.0"))],
        )
        db_session.flush()

        rows = _select_observations(db_session, "VINTAGE_TEST")
        assert len(rows) == 1
        meta = rows[0].extra_metadata
        assert meta["realtime_start"] == "2025-01-01"
        assert meta["realtime_end"] == "9999-12-31"

    def test_revision_updates_existing_values(self, db_session: Session) -> None:
        """When FRED revises a data point, our upsert reflects the new value.

        This is the *real* reason the persistnece layer uses upsert: FRED
        publishes prelimanary numbers and revises them later. Our pipeline
        must track the latest known truth without manual intervention.
        """
        upsert_series(db_session, _make_series("REVISION_TEST"))

        upsert_observations(
            db_session,
            "REVISION_TEST",
            [_make_observation(date(2024, 1, 1), Decimal("100.0"))],
        )
        db_session.flush()

        upsert_observations(
            db_session,
            "REVISION_TEST",
            [_make_observation(date(2024, 1, 1), Decimal("105.7"))],
        )
        db_session.flush()

        rows = _select_observations(db_session, "REVISION_TEST")
        assert rows[0].value == Decimal("105.7")

    def test_empty_batch_returns_zero(self, db_session: Session) -> None:
        """Empty observation list is a valid no-op."""
        upsert_series(db_session, _make_series("EMPTY_TEST"))

        n = upsert_observations(db_session, "EMPTY_TEST", [])
        db_session.flush()

        assert n == 0

    def test_handles_missing_value(self, db_session: Session) -> None:
        """Observations with None values are persisted as NULL."""
        upsert_series(db_session, _make_series("NULL_TEST"))

        upsert_observations(
            db_session,
            "NULL_TEST",
            [_make_observation(date(2024, 1, 1), value=None)],
        )
        db_session.flush()

        rows = _select_observations(db_session, "NULL_TEST")
        assert rows[0].value is None


# ============================================================
# Cascade behavior
# ============================================================


class TestCascadeBehaviour:
    """Verify ON DELETE CASCADE actually works at the DB level."""

    def test_deleting_series_removes_observations(self, db_session: Session) -> None:
        """Foreign key constraint cascades the delete."""
        upsert_series(db_session, _make_series("CASCADE_TEST"))
        upsert_observations(
            db_session,
            "CASCADE_TEST",
            [_make_observation(date(2024, 1, 1))],
        )
        db_session.flush()

        before = _select_observations(db_session, "CASCADE_TEST")
        assert len(before) == 1

        series_obj = _select_series(db_session, "CASCADE_TEST")
        db_session.delete(series_obj)
        db_session.flush()

        after = _select_observations(db_session, "CASCADE_TEST")
        assert len(after) == 0
