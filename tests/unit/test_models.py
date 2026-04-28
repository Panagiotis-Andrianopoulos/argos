"""Unit tests for the FRED Pydantic models.

We focus on the custom validators we wrote — that is where bugs are
likely. We don't bother testing trivial Pydantic field assignment.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from argos.ingestion.fred.models import (
    Observation,
    ObservationsResponse,
    Series,
    SeriesResponse,
)

# ============================================================
# Fixtures: minimal valid payloads
# ============================================================


@pytest.fixture
def valid_series_payload() -> dict[str, object]:
    """A minimal payload that should parse cleanly into a Series."""
    return {
        "id": "QGRN628BIS",
        "title": "Residential Property Prices for Greece",
        "frequency": "Quarterly",
        "frequency_short": "Q",
        "units": "Index",
        "units_short": "Index",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "seasonal_adjustment_short": "NSA",
        "observation_start": "1997-01-01",
        "observation_end": "2025-07-01",
        "last_updated": "2025-12-30 11:03:27-06",
        "notes": None,
    }


@pytest.fixture
def valid_observation_payload() -> dict[str, object]:
    return {
        "date": "2025-07-01",
        "value": "123.45",
        "realtime_start": "2025-12-30",
        "realtime_end": "2025-12-30",
    }


# ============================================================
# Observation: missing value parsing
# ============================================================


class TestObservationValueParsing:
    """The custom validator on 'value' handles FRED's quirks."""

    def test_dot_string_becomes_none(self, valid_observation_payload: dict[str, object]) -> None:
        """FRED uses '.' to represent missing values."""
        valid_observation_payload["value"] = "."
        obs = Observation.model_validate(valid_observation_payload)
        assert obs.value is None

    def test_empty_string_becomes_none(self, valid_observation_payload: dict[str, object]) -> None:
        valid_observation_payload["value"] = ""
        obs = Observation.model_validate(valid_observation_payload)
        assert obs.value is None

    def test_explicit_none_stays_none(self, valid_observation_payload: dict[str, object]) -> None:
        valid_observation_payload["value"] = None
        obs = Observation.model_validate(valid_observation_payload)
        assert obs.value is None

    def test_numeric_string_becomes_decimal(
        self, valid_observation_payload: dict[str, object]
    ) -> None:
        """Numeric strings are coerced to Decimal for precision."""
        valid_observation_payload["value"] = "123.45"
        obs = Observation.model_validate(valid_observation_payload)
        assert obs.value == Decimal("123.45")
        assert isinstance(obs.value, Decimal)

    @pytest.mark.parametrize(
        ("raw_value", "expected"),
        [
            ("0", Decimal("0")),
            ("0.000001", Decimal("0.000001")),
            ("-15.5", Decimal("-15.5")),
            ("1000000", Decimal("1000000")),
        ],
    )
    def test_various_numeric_inputs(
        self,
        valid_observation_payload: dict[str, object],
        raw_value: str,
        expected: Decimal,
    ) -> None:
        valid_observation_payload["value"] = raw_value
        obs = Observation.model_validate(valid_observation_payload)
        assert obs.value == expected


# ============================================================
# Series: datetime parsing
# ============================================================


class TestSeriesDateParsing:
    """The validator on 'last_updated' handles FRED format and ISO 8601."""

    def test_fred_native_format(self, valid_series_payload: dict[str, object]) -> None:
        valid_series_payload["last_updated"] = "2025-12-30 11:03:27-06"
        series = Series.model_validate(valid_series_payload)
        assert series.last_updated.year == 2025
        assert series.last_updated.month == 12
        assert series.last_updated.day == 30
        assert series.last_updated.tzinfo is not None

    def test_iso_format_after_round_trip(self, valid_series_payload: dict[str, object]) -> None:
        """JSON round-trip produces ISO 8601 with full timezone offset."""
        valid_series_payload["last_updated"] = "2025-12-30T11:03:27-06:00"
        series = Series.model_validate(valid_series_payload)
        assert series.last_updated.year == 2025

    def test_already_a_datetime_passes_through(
        self, valid_series_payload: dict[str, object]
    ) -> None:
        """If a Python datetime is passed, no parsing needed."""
        from datetime import timedelta, timezone

        dt = datetime(2025, 12, 30, 11, 3, 27, tzinfo=timezone(timedelta(hours=-6)))
        valid_series_payload["last_updated"] = dt
        series = Series.model_validate(valid_series_payload)
        assert series.last_updated == dt


# ============================================================
# extra="ignore" behavior
# ============================================================


class TestForwardCompatibility:
    """Unknown fields should be silently dropped, never crash the parser."""

    def test_unknown_fields_are_ignored_in_observation(
        self, valid_observation_payload: dict[str, object]
    ) -> None:
        """If FRED adds a new field tomorrow, our parser still works."""
        valid_observation_payload["new_future_field"] = "something"
        obs = Observation.model_validate(valid_observation_payload)
        assert not hasattr(obs, "new_future_field")

    def test_unknown_fields_are_ignored_in_series(
        self, valid_series_payload: dict[str, object]
    ) -> None:
        valid_series_payload["popularity"] = 100
        series = Series.model_validate(valid_series_payload)
        assert not hasattr(series, "popularity")


# ============================================================
# Validation errors for malformed input
# ============================================================


class TestValidationErrors:
    """We do still want errors when input is genuinely malformed."""

    def test_missing_required_field_raises(self) -> None:
        """Removing a required field should produce a clear error."""
        payload = {"date": "2025-07-01"}
        with pytest.raises(ValidationError):
            Observation.model_validate(payload)

    def test_invalid_date_format_raises(self, valid_observation_payload: dict[str, object]) -> None:
        valid_observation_payload["date"] = "not-a-date"
        with pytest.raises(ValidationError):
            Observation.model_validate(valid_observation_payload)


# ============================================================
# Validation errors for malformed input
# ============================================================


class TestResponseWrappers:
    """The list-based response wrappers parse arrays of items."""

    def test_series_response_with_one_item(self, valid_series_payload: dict[str, object]) -> None:
        response = SeriesResponse.model_validate({"seriess": [valid_series_payload]})
        assert len(response.seriess) == 1
        assert response.seriess[0].id == "QGRN628BIS"

    def test_observations_response_count_must_be_present(
        self, valid_observation_payload: dict[str, object]
    ) -> None:
        response = ObservationsResponse.model_validate(
            {"observations": [valid_observation_payload], "count": 1}
        )
        assert response.count == 1
        assert len(response.observations) == 1

    def test_empty_observations_response(self) -> None:
        response = ObservationsResponse.model_validate({"observations": [], "count": 0})
        assert response.count == 0
        assert response.observations == []
