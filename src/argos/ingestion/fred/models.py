"""Pydantic models for the responses of the FRED API.

Every model represents a logical entity as returned by FRED.
The models perform validation at the boundary — if FRED sends something
unexpected, we get a ValidationError at the moment we receive it, not
hours later in another part of the code.

Reference: https://fred.stlouisfed.org/docs/api/fred/
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Series(BaseModel):
    """Metadata a FRED series.

    Returned from the /series?series_id=XXX endpoint.
    """

    model_config = ConfigDict(
        # Allow extra fields that we haven't modeled,
        # so if FRED adds a new field, the code won't break
        extra="ignore",
    )

    id: str = Field(description="Series identifier (e.g. QGRN628BIS).")
    title: str = Field(description="Human-readable title.")
    frequency: str = Field(description="Frequency (e.g. 'Quarterly').")
    frequency_short: str = Field(description="Short frequency code (Q, M, A).")
    units: str = Field(description="Units of measurement.")
    units_short: str = Field(description="Short units code.")
    seasonal_adjustment: str = Field(
        description="Seasonal adjustment description.",
    )
    seasonal_adjustment_short: str = Field(
        description="Short seasonal adjustment code (SA, NSA).",
    )
    observation_start: date_type = Field(description="First available date.")
    observation_end: date_type = Field(description="Last available date.")
    last_updated: datetime = Field(description="Last updated on FRED.")
    notes: str | None = Field(default=None, description="Optional description.")

    @field_validator("last_updated", mode="before")
    @classmethod
    def _parse_fred_datetime(cls, v: str | datetime) -> datetime:
        """Parse a datetime string from FRED or from a round-tripped JSON.

        FRED sends values like "2025-12-30 11:03:27-06" (space separator,
        short timezone). When we dump to JSON and read back, Pydantic uses
        ISO 8601 format like "2025-12-30T11:03:27-06:00" (T separator,
        full timezone).

        This validator handles both.
        """
        if isinstance(v, datetime):
            return v

        try:
            return datetime.fromisoformat(v)
        except ValueError:
            pass

        if len(v) > 19 and v[-3] in ("-", "+"):
            v = v + "00"  # "-06" → "-0600"
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S%z")


class Observation(BaseModel):
    """An observation (i.e., a data point) for a series.

    The FRED returns '.' for missing values — we convert it to None.
    """

    model_config = ConfigDict(extra="ignore")

    date: date_type = Field(description="Date of the observation.")
    value: Decimal | None = Field(
        default=None,
        description="Numeric value. None if missing.",
    )
    realtime_start: date_type = Field(
        description="From when this value is valid (for revisions).",
    )
    realtime_end: date_type = Field(
        description="Until when this value is valid.",
    )

    @field_validator("value", mode="before")
    @classmethod
    def _parse_missing_value(cls, v: str | Decimal | None) -> Decimal | None:
        """The FRED returns '.' for missing values — convert to None."""
        if v == "." or v == "" or v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))


class SeriesResponse(BaseModel):
    """Wrapper for the /series endpoint response."""

    model_config = ConfigDict(extra="ignore")

    seriess: list[Series] = Field(
        description="List of series metadata (the typo 'seriess' is from FRED).",
    )


class ObservationsResponse(BaseModel):
    """Wrapper for the /series/observations endpoint response."""

    model_config = ConfigDict(extra="ignore")

    observations: list[Observation]
    count: int = Field(description="Total number of observations.")
