"""Pydantic models for the ECB Data Portal API repsonses.

The ECB Data Portal exposes time series via SDMX. We request CSV format
(format=csvdata) which gives one per (series, time_period) pair,
with all series metadata denormalized onto every row.

The client splits this denormalized stream into:
- EcbSeries: one instance per unique series KEY, holding metadata
- EcbObservation: one instance per row, holding time-keyed value

Reference: https://data.ecb.europa.eu/help/api/data
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EcbSeries(BaseModel):
    """Metadata for a single ECB time series.

    The 10-dimensional series key (e.g. RESR.Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX)
    uniquely identifies a series within a dataflow. We model the dimensions
    as discrete fields rather than parsing the dot-separated key in code,
    because the dimensions have semantic meaning (frequency, country,
    indicator type, etc) that we may want to filter on later.
    """

    model_config = ConfigDict(extra="ignore")

    key: str = Field(
        description=(
            "Full dot-separated series indetifier " "(e.g. RESR.Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX)."
        ),
    )

    # 10 SDMX dimensions. Names match the ECB CSV column names verbatim.
    freq: str = Field(description="Frequency code (Q, M, A, etc).")
    ref_area: str = Field(description="Reference area / country (ISO code).")
    region: str = Field(description="Sub-national region code (_T = total).")
    adjustment: str = Field(description="Seasonal adjustment code (N, S, W, etc).")
    property_type: str = Field(description="Property type indicator.")
    indicator: str = Field(description="Indicator type (TVAL = transaction value, etc).")
    data_provider: str = Field(description="Data source provider code.")
    price_type: str = Field(description="Price type code (TB = transaction based).")
    transformation: str = Field(description="Transformation code (N = non-transformed).")
    unit_measure: str = Field(description="Unit of measure (IX = index).")

    # Series-level metadata (fields that repeat across every observation row
    # of the same series in the CSV; we deduplicate at the client layer).
    title: str = Field(description="Short human-readable title.")
    title_compl: str | None = Field(
        default=None,
        description="Complete title with full dimension descriptions.",
    )
    unit_index_base: str | None = Field(
        default=None,
        description="Index base year, e.g. '2015=100'.",
    )
    time_format: str | None = Field(
        default=None,
        description="ISO 8601 duration code for the period (P3M = quarterly, etc).",
    )
    decimals: int | None = Field(
        default=None,
        description="Number of decimal places used in OBS_VALUE.",
    )

    extra_fields: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Additional CSV columns we don't model first-class "
            "(EMBARGO_TIME, BREAKS, COMPILING_ORG, COVERAGE, etc). "
            "Persisted to extra_metadata JSONB."
        ),
    )

    @field_validator("decimals", mode="before")
    @classmethod
    def _parse_decimals(cls, v: str | int | None) -> int | None:
        """ECB CSV stores decimals as a string ('2'), we want int.

        Empty string means 'not specified', mapped to None.
        """
        if v == "" or v is None:
            return None
        if isinstance(v, int):
            return v
        return int(v)


class EcbObservation(BaseModel):
    """A single observation (data point) for an ECB series.

    Time is stored as the raw period string (e.g. '2025-Q1', '2025-04')
    rather than parsed to a date. Conversion to a Postgres DATE happens
    at persistence time, where the series frequency is known and can
    drive the period-to-date mapping convention (Q1 -> Jan 1, etc).
    """

    model_config = ConfigDict(extra="ignore")

    time_period: str = Field(description="Raw period string from the API, e.g. '2025-Q1'.")
    obs_value: Decimal | None = Field(
        default=None,
        description="The observation value. None if missing.",
    )
    obs_status: str | None = Field(
        default=None,
        description=(
            "SDMX observation status code: A=normal, P=provisional, "
            "F=forecast, M=missing, etc. Persisted as 'status' column."
        ),
    )
    conf_status: str | None = Field(
        default=None,
        description="Confidentiality status code (F=free, etc).",
    )
    pre_break_value: Decimal | None = Field(
        default=None,
        description="Value before a break in the series, when applicable.",
    )
    comment_obs: str | None = Field(
        default=None,
        description="Free-text comment on this specific observation.",
    )

    @field_validator("obs_value", "pre_break_value", mode="before")
    @classmethod
    def _parse_decimal(cls, v: str | Decimal | None) -> Decimal | None:
        """Empty CSV cells become None; otherwise parse as Decimal."""
        if v == "" or v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))

    @field_validator(
        "obs_status",
        "conf_status",
        "comment_obs",
        mode="before",
    )
    @classmethod
    def _parse_optional_str(cls, v: str | None) -> str | None:
        """Empty CSV cells become None for optional string fields."""
        if v == "" or v is None:
            return None
        return v
