"""FRED series IDs used in ARGOS.

Every constant represents a specific time series from FRED.
Documentation for each series is available at:
    https://fred.stlouisfed.org/series/<SERIES_ID>

Organized in categories for better readability.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Category(StrEnum):
    """Categorize series for better organization."""

    REAL_ESTATE = "real_estate"
    INTEREST_RATES = "interest_rates"
    LABOR = "labor"
    INFLATION = "inflation"


@dataclass(frozen=True)
class FredSeries:
    """Metadata for a FRED series that interests ARGOS.

    Immutable (frozen=True) because these values shouldn't change
    at runtime.
    """

    id: str
    category: Category
    description: str


# ============================================================
# Real estate prices (main target variables)
# ============================================================

RESIDENTIAL_PROPERTY_PRICE_NOMINAL = FredSeries(
    id="QGRN628BIS",
    category=Category.REAL_ESTATE,
    description=(
        "Residential Property Prices for Greece, nominal. "
        "Quarterly index covering all new and existing flats."
    ),
)


RESIDENTIAL_PROPERTY_PRICE_REAL = FredSeries(
    id="QGRR628BIS",
    category=Category.REAL_ESTATE,
    description=(
        "Residential Property Prices for Greece (inflation-adjusted). "
        "Quarterly index covering all new and existing flats."
    ),
)

# ============================================================
# Macroeconomic indicators (features for the ML model)
# ============================================================

GREECE_10Y_BOND_YIELD = FredSeries(
    id="IRLTLT01GRM156N",
    category=Category.INTEREST_RATES,
    description=(
        "Greece 10-Year Long-Term Government Bond Yield, monthly. "
        "Proxy for the cost of borrowing - effects mortage rates and real estate demand."
    ),
)

GREECE_UNEMPLOYMENT_RATE = FredSeries(
    id="LRHUTTTTGRQ156S",
    category=Category.LABOR,
    description=(
        "Greece unemployment rate, quarterly. "
        "Macroeconomic indicator that can influence housing demand."
    ),
)

GREECE_CPI_INFLATION = FredSeries(
    id="CPALTT01GRM659N",
    category=Category.INFLATION,
    description=(
        "Greece Consumer Price Index, monthly. " "Inflation tracker for context in nominal prices."
    ),
)

# ============================================================
# Convenience collections
# ============================================================

ALL_SERIES: tuple[FredSeries, ...] = (
    RESIDENTIAL_PROPERTY_PRICE_NOMINAL,
    RESIDENTIAL_PROPERTY_PRICE_REAL,
    GREECE_10Y_BOND_YIELD,
    GREECE_UNEMPLOYMENT_RATE,
    GREECE_CPI_INFLATION,
)

REAL_ESTATE_SERIES: tuple[FredSeries, ...] = tuple(
    s for s in ALL_SERIES if s.category == Category.REAL_ESTATE
)

MACRO_SERIES: tuple[FredSeries, ...] = tuple(
    s for s in ALL_SERIES if s.category != Category.REAL_ESTATE
)
