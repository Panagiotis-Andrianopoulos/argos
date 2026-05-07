"""Convert ECB SDMX time period strings to Postgres-compatible dates.

ECB returns time periods as strings whose format depends on the series
frequency: '2025-Q1' for quarterly, '2025-04' for monthly, '2025' for
annual, '2025-04-15' for daily.

Our unified storage schema models observation_date as DATE. We adopt
the convention that a period is mapped to its FIRST DAY: Q1 -> Jan 1,
M04 -> Apr 1, year 2025 -> Jan 1, etc. This matches the convention
used by the FRED ingestion layer and is the standard for time-series
analytics.

Only the frequencies actually used by series ARGOS ingests are
supported (Q, M, A). Adding a new frequency means adding a parser here
and updating the FREQUENCY_TO_PARSER dispatch dict.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date


class UnsupportedFrequencyError(ValueError):
    """Raised when an ECB frequency code has no parser registered."""


_QUARTERLY_RE = re.compile(r"^(\d{4})-Q([1-4])$")
_MONTHLY_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
_ANNUAL_RE = re.compile(r"^(\d{4})$")

# First month of each calendar quarter.
_QUARTER_TO_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}


def _parse_quarterly(period: str) -> date:
    """Parse 'YYYY-Q[1-4]' into the first day of that quarter."""
    match = _QUARTERLY_RE.match(period)
    if match is None:
        msg = f"Invalid quarterly period: {period!r} (expected 'YYYY-Q[1-4]')"
        raise ValueError(msg)
    year = int(match.group(1))
    quarter = int(match.group(2))
    return date(year, _QUARTER_TO_MONTH[quarter], 1)


def _parse_monthly(period: str) -> date:
    """Parse 'YYYY-MM' into the first day of that month."""
    match = _MONTHLY_RE.match(period)
    if match is None:
        msg = f"Invalid monthly period: {period!r} (expected 'YYYY-MM')"
        raise ValueError(msg)
    year = int(match.group(1))
    month = int(match.group(2))
    return date(year, month, 1)


def _parse_annual(period: str) -> date:
    """Parse 'YYYY' into January 1 of that year."""
    match = _ANNUAL_RE.match(period)
    if match is None:
        msg = f"Invalid annual period: {period!r} (expected 'YYYY')"
        raise ValueError(msg)
    year = int(match.group(1))
    return date(year, 1, 1)


# Frequency code -> parser dispatch. Add new entries as new frequencies
# are encountered in upstream data, not preemptively.
_FREQUENCY_TO_PARSER: dict[str, Callable[[str], date]] = {
    "Q": _parse_quarterly,
    "M": _parse_monthly,
    "A": _parse_annual,
}


def period_to_date(period: str, frequency: str) -> date:
    """Convert an ECB time period string to its first-day Postgres DATE.

    Args:
        period: The raw period string from the ECB CSV (e.g. '2025-Q1').
        frequency: The series frequency code from the FREQ column
            (e.g. 'Q' for quarterly, 'M' for monthly, 'A' for annual).

    Returns:
        A date object representing the first day of the period.

    Raises:
        UnsupportedFrequencyError: If the frequency code has no parser
            registered. To add support, write a parser and register it
            in _FREQUENCY_TO_PARSER.
        ValueError: If the period string doesn't match the expected
            format for the given frequency.
    """
    parser = _FREQUENCY_TO_PARSER.get(frequency)
    if parser is None:
        msg = (
            f"Unsupported ECB frequency code: {frequency!r}. "
            f"Supported: {sorted(_FREQUENCY_TO_PARSER.keys())!r}. "
            f"Add a parser in argos.ingestion.ecb.period if needed."
        )
        raise UnsupportedFrequencyError(msg)
    return parser(period)
