"""Integration tests for the FRED persistence layer.

These tests run against a real Postgres database (the same one Docker
Compose spins up). Each test runs in its own transaction that is rolled
back at the end, so they leave no trace.

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
from argos.storage.models.fred import FredObservation, FredSeries

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


# ============================================================
# Series upsert tests
# ============================================================


class TestUpsertSeries:
    """Verify FredSeries upsert behaviour."""

    def test_inserts_new_series(self, db_session: Session) -> None:
        """A series that does not yet exist is created."""
        series = _make_series("NEW_SERIES")

        upsert_series(db_session, series)
        db_session.flush()

        result = db_session.execute(
            select(FredSeries).where(FredSeries.series_id == "NEW_SERIES")
        ).scalar_one()
        assert result.title == "Test Series"
        assert result.frequency == "Quarterly"

    def test_updates_existing_series(self, db_session: Session) -> None:
        """Re-running upsert with a changed title updates the row."""
        series_v1 = _make_series("CHANGING_SERIES")
        upsert_series(db_session, series_v1)
        db_session.flush()

        series_v2 = _make_series("CHANGING_SERIES")
        series_v2 = series_v2.model_copy(update={"title": "New Title"})
        upsert_series(db_session, series_v2)
        db_session.flush()

        result = db_session.execute(
            select(FredSeries).where(FredSeries.series_id == "CHANGING_SERIES")
        ).scalar_one()
        assert result.title == "New Title"

    def test_idempotent_repeated_upsert(self, db_session: Session) -> None:
        """Running the same upsert N times produces exactly one row."""
        series = _make_series("IDEMPOTENT")

        for _ in range(3):
            upsert_series(db_session, series)
        db_session.flush()

        count = db_session.execute(
            select(FredSeries).where(FredSeries.series_id == "IDEMPOTENT")
        ).all()
        assert len(count) == 1


# ============================================================
# Observations upsert tests
# ============================================================


class TestUpsertObservations:
    """Verify FredObservation upsert behaviour."""

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

        rows = (
            db_session.execute(
                select(FredObservation)
                .where(FredObservation.series_id == "OBS_TEST")
                .order_by(FredObservation.observation_date)
            )
            .scalars()
            .all()
        )

        assert len(rows) == 3
        assert rows[0].value == Decimal("100.0")
        assert rows[2].value == Decimal("103.0")

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

        result = db_session.execute(
            select(FredObservation).where(
                FredObservation.series_id == "REVISION_TEST",
                FredObservation.observation_date == date(2024, 1, 1),
            )
        ).scalar_one()
        assert result.value == Decimal("105.7")

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

        result = db_session.execute(
            select(FredObservation).where(FredObservation.series_id == "NULL_TEST")
        ).scalar_one()
        assert result.value is None


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

        before = (
            db_session.execute(
                select(FredObservation).where(FredObservation.series_id == "CASCADE_TEST")
            )
            .scalars()
            .all()
        )
        assert len(before) == 1

        series_obj = db_session.execute(
            select(FredSeries).where(FredSeries.series_id == "CASCADE_TEST")
        ).scalar_one()
        db_session.delete(series_obj)
        db_session.flush()

        after = (
            db_session.execute(
                select(FredObservation).where(FredObservation.series_id == "CASCADE_TEST")
            )
            .scalars()
            .all()
        )
        assert len(after) == 0
