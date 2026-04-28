"""Unit tests for the storage paths module.

These tests verify that the key-building functions produce consistent,
predictable strings. No I/O — just pure function behavior.
"""

from __future__ import annotations

from datetime import date

import pytest

from argos.storage import paths

# ============================================================
# Series metadata key
# ============================================================


class TestSeriesMetadataKey:
    """Tests for fred_series_metadata_key()."""

    def test_basic_key(self) -> None:
        """A typical call produces the expected hierchical key."""
        result = paths.fred_series_metadata_key("QGRN628BIS", snapshot_date=date(2026, 4, 22))
        assert result == "raw/fred/series/QGRN628BIS/2026-04-22/metadata.json"

    def test_key_starts_with_raw_prefix(self) -> None:
        """Every series key lives under the raw/ prefix."""
        result = paths.fred_series_metadata_key("ANY_SERIES", snapshot_date=date(2025, 1, 1))
        assert result.startswith("raw/")

    def test_key_includes_iso_formatted_date(self) -> None:
        """The date component is always YYYY-MM-DD (ISO 8601)."""
        result = paths.fred_series_metadata_key("TEST", snapshot_date=date(2025, 12, 31))
        assert "2025-12-31" in result

    @pytest.mark.parametrize(
        "series_id",
        ["QGRN628BIS", "IRLTLT01GRM156N", "A_B_C", "test-series"],
    )
    def test_arbitrary_series_ids_are_embedded_verbatim(self, series_id: str) -> None:
        """The series_id appears unchanged inside the key."""
        result = paths.fred_series_metadata_key(series_id, snapshot_date=date(2026, 1, 1))
        assert f"/{series_id}/" in result


# ============================================================
# Observations key
# ============================================================


class TestObservationsKey:
    """Tests for fred_observations_key()."""

    def test_basic_key(self) -> None:
        result = paths.fred_observations_key("QGRN628BIS", snapshot_date=date(2026, 4, 22))
        assert result == ("raw/fred/observations/QGRN628BIS/2026-04-22/observations.json")

    def test_observations_path_differs_from_series_path(self) -> None:
        """Series and observations live under sibling subtrees."""
        snapshot = date(2026, 4, 22)
        series_key = paths.fred_series_metadata_key("X", snapshot_date=snapshot)
        obs_key = paths.fred_observations_key("X", snapshot_date=snapshot)
        assert series_key != obs_key
        assert "/series/" in series_key
        assert "/observations/" in obs_key


# ============================================================
# Prefixes
# ============================================================


class TestPrefixes:
    """Tests for the prefix builders used in list_keys()."""

    def test_series_prefix_without_series_id(self) -> None:
        """Without series_id, returns the base for all series."""
        result = paths.fred_series_prefix()
        assert result == "raw/fred/series/"

    def test_series_prefix_with_series_id(self) -> None:
        """With a series_id, narrows the prefix to that series."""
        result = paths.fred_series_prefix("QGRN628BIS")
        assert result == "raw/fred/series/QGRN628BIS/"

    def test_observations_prefix_with_series_id(self) -> None:
        result = paths.fred_observations_prefix("QGRN628BIS")
        assert result == "raw/fred/observations/QGRN628BIS/"

    def test_full_key_starts_with_its_prefix(self) -> None:
        """Sanity check: a full key is always a continuation of its prefix.

        This is the propery that makes list_keys(prefix=...) actually find
        the corresponding objects.
        """
        snapshot = date(2026, 4, 22)
        full_key = paths.fred_series_metadata_key("QGRN628BIS", snapshot_date=snapshot)
        prefix = paths.fred_series_prefix("QGRN628BIS")
        assert full_key.startswith(prefix)
