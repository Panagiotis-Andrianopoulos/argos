"""Unit tests for the ECB period-string parser.

The parser is small but every line matters: it sits on the boundary
between raw API strings and the Postgres DATE column, and bugs here
silently corrupt data downstream.
"""

from __future__ import annotations

from datetime import date

import pytest

from argos.ingestion.ecb.period import (
    UnsupportedFrequencyError,
    period_to_date,
)

# ============================================================
# Quarterly parsing
# ============================================================


class TestQuarterlyParsing:
    """The 'Q' frequency maps periods to the first day of the quarter."""

    @pytest.mark.parametrize(
        ("period", "expected"),
        [
            ("2025-Q1", date(2025, 1, 1)),
            ("2025-Q2", date(2025, 4, 1)),
            ("2025-Q3", date(2025, 7, 1)),
            ("2025-Q4", date(2025, 10, 1)),
        ],
    )
    def test_each_quarter_maps_to_first_day(self, period: str, expected: date) -> None:
        assert period_to_date(period, "Q") == expected

    def test_handles_old_years(self) -> None:
        """The series we ingest go back to the 1990s."""
        assert period_to_date("1997-Q1", "Q") == date(1997, 1, 1)

    @pytest.mark.parametrize(
        "bad_period",
        [
            "2025-Q0",  # quarters are 1-4
            "2025-Q5",
            "2025-q1",  # case-sensitive
            "25-Q1",  # year must be 4 digits
            "2025Q1",  # missing dash
            "2025-Q",  # missing quarter number
            "2025",  # annual format, not quarterly
            "2025-04",  # monthly format
            "",
        ],
    )
    def test_malformed_quarterly_raises(self, bad_period: str) -> None:
        with pytest.raises(ValueError, match="Invalid quarterly period"):
            period_to_date(bad_period, "Q")


# ============================================================
# Monthly parsing
# ============================================================


class TestMonthlyParsing:
    """The 'M' frequency maps periods to the first day of the month."""

    @pytest.mark.parametrize(
        ("period", "expected"),
        [
            ("2025-01", date(2025, 1, 1)),
            ("2025-04", date(2025, 4, 1)),
            ("2025-12", date(2025, 12, 1)),
        ],
    )
    def test_each_month_maps_to_first_day(self, period: str, expected: date) -> None:
        assert period_to_date(period, "M") == expected

    @pytest.mark.parametrize(
        "bad_period",
        [
            "2025-00",  # month 00 invalid
            "2025-13",  # month 13 invalid
            "2025-1",  # month must be zero-padded
            "2025-04-15",  # daily format, not monthly
            "2025",  # annual format
            "2025-Q1",  # quarterly format
            "",
        ],
    )
    def test_malformed_monthly_raises(self, bad_period: str) -> None:
        with pytest.raises(ValueError, match="Invalid monthly period"):
            period_to_date(bad_period, "M")


# ============================================================
# Annual parsing
# ============================================================


class TestAnnualParsing:
    """The 'A' frequency maps periods to January 1 of the year."""

    @pytest.mark.parametrize(
        ("period", "expected"),
        [
            ("2025", date(2025, 1, 1)),
            ("2000", date(2000, 1, 1)),
            ("1997", date(1997, 1, 1)),
        ],
    )
    def test_year_maps_to_january_first(self, period: str, expected: date) -> None:
        assert period_to_date(period, "A") == expected

    @pytest.mark.parametrize(
        "bad_period",
        [
            "25",  # must be 4 digits
            "2025-Q1",  # quarterly format
            "2025-04",  # monthly format
            "year2025",  # not all digits
            "",
        ],
    )
    def test_malformed_annual_raises(self, bad_period: str) -> None:
        with pytest.raises(ValueError, match="Invalid annual period"):
            period_to_date(bad_period, "A")


# ============================================================
# Unsupported frequency dispatch
# ============================================================


class TestUnsupportedFrequency:
    """Frequencies without a registered parser raise a typed error."""

    @pytest.mark.parametrize("freq", ["D", "W", "H", "S", "X", ""])
    def test_unsupported_codes_raise(self, freq: str) -> None:
        with pytest.raises(UnsupportedFrequencyError):
            period_to_date("2025-Q1", freq)

    def test_error_message_lists_supported_frequencies(self) -> None:
        """The error tells the developer which codes are supported."""
        with pytest.raises(UnsupportedFrequencyError) as exc_info:
            period_to_date("2025-Q1", "D")
        message = str(exc_info.value)
        assert "'A'" in message
        assert "'M'" in message
        assert "'Q'" in message

    def test_lowercase_frequency_is_unsupported(self) -> None:
        """ECB frequency codes are uppercase. Lowercase is a bug, not a feature."""
        with pytest.raises(UnsupportedFrequencyError):
            period_to_date("2025-Q1", "q")
