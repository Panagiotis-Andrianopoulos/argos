"""Integration tests for the ECB persistence layer.

These tests run against a real Postgres database (the same one Docker
Compose spins up). Each test runs in its own transaction that is rolled
back at the end, so they leave no trace.

The persistence layer takes ECB-specific Pydantic models as input but
writes to the unified economic_* tables. The tests assert on those
tables directly to verify the mapping behaviour, including:
- Source is stamped as DataSource.ECB
- ECB-specific fields (SDMX dimensions, OBS_STATUS) are persisted correctly
- Period-to-date conversion happens at write time
- ON CONFLICT DO UPDATE semantics work for revisions

Mark all tests in this module with `integration` so we can selectively
skip them in fast feedback loops:

    pytest -m "not integration"
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from argos.ingestion.ecb.models import EcbObservation, EcbSeries
from argos.ingestion.ecb.persistence import (
    upsert_observations,
    upsert_series,
)
from argos.storage.models.economic import (
    DataSource,
    EconomicObservation,
    EconomicSeries,
)

pytestmark = pytest.mark.integration

# ============================================================
# Fixtures: factory functions for test data
# ============================================================


def _make_series(key: str = "TEST.Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX") -> EcbSeries:
    """Build a valid EcbSeries Pydantic model for tests."""
    return EcbSeries(
        key=key,
        freq="Q",
        ref_area="GR",
        region="_T",
        adjustment="N",
        property_type="RTF",
        indicator="TVAL",
        data_provider="GR2",
        price_type="TB",
        transformation="N",
        unit_measure="IX",
        title="Test ECB Series",
        title_compl="Test ECB Series, full description",
        unit_index_base="2015=100",
        time_format="P3M",
        decimals=2,
        extra_fields={"UNIT_MULT": "0", "TIME_PER_COLLECT": "A"},
    )


def _make_observation(
    period: str = "2024-Q1",
    value: Decimal | None = Decimal("100.0"),
    obs_status: str | None = "A",
) -> EcbObservation:
    """Build a valid EcbObservation Pydantic model for tests."""
    return EcbObservation(
        time_period=period,
        obs_value=value,
        obs_status=obs_status,
        conf_status="F",
        pre_break_value=None,
        comment_obs=None,
    )


def _select_series(session: Session, series_key: str) -> EconomicSeries:
    """Helper: fetch an ECB-sourced series row by key."""
    return session.execute(
        select(EconomicSeries).where(
            EconomicSeries.source == DataSource.ECB,
            EconomicSeries.series_id == series_key,
        )
    ).scalar_one()


def _select_observations(session: Session, series_key: str) -> list[EconomicObservation]:
    """Helper: fetch all ECB-sourced observations for a series, ordered by date."""
    return list(
        session.execute(
            select(EconomicObservation)
            .where(
                EconomicObservation.source == DataSource.ECB,
                EconomicObservation.series_id == series_key,
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
    """Verify EconomicSeries upsert behaviour for ECB-sourced data."""

    def test_inserts_new_series(self, db_session: Session) -> None:
        """A series that does not yet exist is created with source='ecb'."""
        series = _make_series("NEW.SERIES")

        upsert_series(db_session, series)
        db_session.flush()

        result = _select_series(db_session, "NEW.SERIES")
        assert result.source == DataSource.ECB
        assert result.title == "Test ECB Series"
        assert result.frequency == "Q"
        assert result.units == "2015=100"
        assert result.seasonal_adjustment == "N"

    def test_sdmx_dimensions_land_in_metadata(self, db_session: Session) -> None:
        """The 10 SDMX dimensions are preserved in extra_metadata."""
        series = _make_series("DIMS.TEST")

        upsert_series(db_session, series)
        db_session.flush()

        result = _select_series(db_session, "DIMS.TEST")
        meta = result.extra_metadata
        assert meta["ref_area"] == "GR"
        assert meta["region"] == "_T"
        assert meta["adjustment"] == "N"
        assert meta["property_type"] == "RTF"
        assert meta["indicator"] == "TVAL"
        assert meta["data_provider"] == "GR2"
        assert meta["price_type"] == "TB"
        assert meta["transformation"] == "N"
        assert meta["unit_measure"] == "IX"

    def test_extra_fields_land_in_metadata(self, db_session: Session) -> None:
        """Unmodelled CSV columns are merged into extra_metadata."""
        series = _make_series("EXTRA.TEST")

        upsert_series(db_session, series)
        db_session.flush()

        result = _select_series(db_session, "EXTRA.TEST")
        meta = result.extra_metadata
        assert meta["UNIT_MULT"] == "0"
        assert meta["TIME_PER_COLLECT"] == "A"

    def test_updates_existing_series(self, db_session: Session) -> None:
        """Re-running upsert with a changed title updates the row."""
        v1 = _make_series("CHANGING.SERIES")
        upsert_series(db_session, v1)
        db_session.flush()

        v2 = _make_series("CHANGING.SERIES").model_copy(update={"title": "Updated Title"})
        upsert_series(db_session, v2)
        db_session.flush()

        result = _select_series(db_session, "CHANGING.SERIES")
        assert result.title == "Updated Title"

    def test_idempotent_repeated_upsert(self, db_session: Session) -> None:
        """Running the same upsert N times produces exactly one row."""
        series = _make_series("IDEMPOTENT.SERIES")

        for _ in range(3):
            upsert_series(db_session, series)
        db_session.flush()

        rows = db_session.execute(
            select(EconomicSeries).where(
                EconomicSeries.source == DataSource.ECB,
                EconomicSeries.series_id == "IDEMPOTENT.SERIES",
            )
        ).all()
        assert len(rows) == 1


# ============================================================
# Observations upsert tests
# ============================================================


class TestUpsertObservations:
    """Verify EconomicObservation upsert behaviour for ECB-sourced data."""

    def test_inserts_observations(self, db_session: Session) -> None:
        """A batch of new Observations gets persisted."""
        upsert_series(db_session, _make_series("OBS.TEST"))

        observations = [
            _make_observation("2024-Q1", Decimal("100.0")),
            _make_observation("2024-Q2", Decimal("101.5")),
            _make_observation("2024-Q3", Decimal("103.0")),
        ]
        n = upsert_observations(db_session, "OBS.TEST", observations, frequency="Q")
        db_session.flush()

        assert n == 3

        rows = _select_observations(db_session, "OBS.TEST")
        assert len(rows) == 3
        assert rows[0].value == Decimal("100.0")
        assert rows[2].value == Decimal("103.0")
        assert all(row.source == DataSource.ECB for row in rows)

    def test_period_to_date_conversion_quarterly(self, db_session: Session) -> None:
        """Q1 maps to Jan 1, Q2 to Apr 1, Q3 to Jul 1, Q4 to Oct 1."""
        from datetime import date

        upsert_series(db_session, _make_series("PERIOD.TEST"))
        upsert_observations(
            db_session,
            "PERIOD.TEST",
            [
                _make_observation("2024-Q1"),
                _make_observation("2024-Q2"),
                _make_observation("2024-Q3"),
                _make_observation("2024-Q4"),
            ],
            frequency="Q",
        )
        db_session.flush()

        rows = _select_observations(db_session, "PERIOD.TEST")
        assert [row.observation_date for row in rows] == [
            date(2024, 1, 1),
            date(2024, 4, 1),
            date(2024, 7, 1),
            date(2024, 10, 1),
        ]

    def test_obs_status_persisted_to_status_column(self, db_session: Session) -> None:
        """ECB OBS_STATUS values map directly to the status column."""
        upsert_series(db_session, _make_series("STATUS.TEST"))
        upsert_observations(
            db_session,
            "STATUS.TEST",
            [
                _make_observation("2024-Q1", obs_status="A"),
                _make_observation("2024-Q2", obs_status="P"),
            ],
            frequency="Q",
        )
        db_session.flush()

        rows = _select_observations(db_session, "STATUS.TEST")
        assert rows[0].status == "A"
        assert rows[1].status == "P"

    def test_time_period_preserved_in_metadata(self, db_session: Session) -> None:
        """Raw period string is kept in extra_metadata for losslessness."""
        upsert_series(db_session, _make_series("RAW.TEST"))
        upsert_observations(
            db_session,
            "RAW.TEST",
            [_make_observation("2024-Q3")],
            frequency="Q",
        )
        db_session.flush()

        rows = _select_observations(db_session, "RAW.TEST")
        assert rows[0].extra_metadata["time_period"] == "2024-Q3"

    def test_revision_updates_existing_value(self, db_session: Session) -> None:
        """When ECB revises a value, our upsert reflects the new one."""
        upsert_series(db_session, _make_series("REVISION.TEST"))

        upsert_observations(
            db_session,
            "REVISION.TEST",
            [_make_observation("2024-Q1", Decimal("100.0"), obs_status="P")],
            frequency="Q",
        )
        db_session.flush()

        upsert_observations(
            db_session,
            "REVISION.TEST",
            [_make_observation("2024-Q1", Decimal("105.7"), obs_status="A")],
            frequency="Q",
        )
        db_session.flush()

        rows = _select_observations(db_session, "REVISION.TEST")
        assert len(rows) == 1
        assert rows[0].value == Decimal("105.7")
        assert rows[0].status == "A"

    def test_empty_batch_returns_zero(self, db_session: Session) -> None:
        """Empty observation list is a valid no-op."""
        upsert_series(db_session, _make_series("EMPTY.TEST"))

        n = upsert_observations(db_session, "EMPTY.TEST", [], frequency="Q")
        db_session.flush()

        assert n == 0

    def test_handles_missing_value(self, db_session: Session) -> None:
        """Observations with None values are persisted as NULL."""
        upsert_series(db_session, _make_series("NULL.TEST"))

        upsert_observations(
            db_session,
            "NULL.TEST",
            [_make_observation("2024-Q1", value=None)],
            frequency="Q",
        )
        db_session.flush()

        rows = _select_observations(db_session, "NULL.TEST")
        assert len(rows) == 1
        assert rows[0].value is None


# ============================================================
# Cross-source isolation
# ============================================================


class TestCrossSourceIsolation:
    """ECB writes must not interfere with FRED data and vice versa."""

    def test_same_series_id_different_sources_coexist(self, db_session: Session) -> None:
        """Composite (source, series_id) PK lets identical IDs coexist."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        # Insert a FRED row directly to bypass the FRED persistence layer.
        db_session.execute(
            pg_insert(EconomicSeries).values(
                source=DataSource.FRED,
                series_id="OVERLAP",
                title="FRED-sourced",
                frequency="Q",
                units="Pct",
                seasonal_adjustment="NSA",
                last_updated=None,
                extra_metadata={},
            )
        )

        # Now insert an ECB row with the SAME series_id.
        upsert_series(db_session, _make_series("OVERLAP"))
        db_session.flush()

        rows = list(
            db_session.execute(select(EconomicSeries).where(EconomicSeries.series_id == "OVERLAP"))
            .scalars()
            .all()
        )
        assert len(rows) == 2
        sources = {row.source for row in rows}
        assert sources == {DataSource.FRED, DataSource.ECB}
