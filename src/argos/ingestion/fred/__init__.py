from argos.ingestion.fred.client import FredClient
from argos.ingestion.fred.models import (
    Observation,
    ObservationsResponse,
    Series,
    SeriesResponse,
)
from argos.ingestion.fred.series import (
    ALL_SERIES,
    GREECE_10Y_BOND_YIELD,
    GREECE_CPI_INFLATION,
    GREECE_UNEMPLOYMENT_RATE,
    MACRO_SERIES,
    REAL_ESTATE_SERIES,
    RESIDENTIAL_PROPERTY_PRICE_NOMINAL,
    RESIDENTIAL_PROPERTY_PRICE_REAL,
    Category,
    FredSeries,
)

__all__ = [
    "ALL_SERIES",
    "GREECE_10Y_BOND_YIELD",
    "GREECE_CPI_INFLATION",
    "GREECE_UNEMPLOYMENT_RATE",
    "MACRO_SERIES",
    "REAL_ESTATE_SERIES",
    "RESIDENTIAL_PROPERTY_PRICE_NOMINAL",
    "RESIDENTIAL_PROPERTY_PRICE_REAL",
    "Category",
    "FredClient",
    "FredSeries",
    "Observation",
    "ObservationsResponse",
    "Series",
    "SeriesResponse",
]
