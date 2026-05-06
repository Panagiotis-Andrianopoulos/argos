"""ECB Data Portal API client for ARGOS.

Wraps the ECB Data Portal SDMX REST API in a Pydantic interface that
returns validated Pydantic models. The ECB API is fully public (no
authentication) and serves CSV responses (format=csvdata) where each
row contains both observation values and denormalized series metadata.

Typical usage:
    from argos.ingestion.ecb import EcbClient

    with EcbClient.from_settings() as client:
        series, observations = client.get_series_with_observations(
            dataflow="RESR",
            series_key="Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX",
        )

Reference: https://data.ecb.europa.eu/help/api/data
"""

from __future__ import annotations

import csv
import io
import logging
from types import TracebackType
from typing import TYPE_CHECKING, Self

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from argos.config import settings
from argos.ingestion.ecb.models import EcbObservation, EcbSeries
from argos.ingestion.http import make_http_client

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Series-level CSV columns that we promote to first-class EcbSeries fields.
# Anything not in this set (and not in _OBSERVATION_FIELDS) lands in
# EcbSeries.extra_fields for JSONB persistence.
_SERIES_CORE_FIELDS = {
    "KEY",
    "FREQ",
    "REF_AREA",
    "REGION",
    "ADJUSTMENT",
    "PROPERTY_TYPE",
    "INDICATOR",
    "DATA_PROVIDER",
    "PRICE_TYPE",
    "TRANSFORMATION",
    "UNIT_MEASURE",
    "TITLE",
    "TITLE_COMPL",
    "UNIT_INDEX_BASE",
    "TIME_FORMAT",
    "DECIMALS",
}

# CSV columns that belong to a single observation rather than to the
# series as a whole. These are extracted per-row into EcbObservation.
_OBSERVATION_FIELDS = {
    "TIME_PERIOD",
    "OBS_VALUE",
    "OBS_STATUS",
    "CONF_STATUS",
    "PRE_BREAK_VALUE",
    "COMMENT_OBS",
}


def _is_retryable(exc: Exception) -> bool:
    """Decide if an exception should trigger a retry.

    Retry: 5xx server errors, 408 Request Timeout, 429 Too Many Requests,
    network/timeout errors.

    Don't retry: 4xx client errors (bad URL, malformed series key).
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status >= 500 or status in {408, 429}
    return isinstance(exc, httpx.TransportError)


class EcbClient:
    """Client for the ECB Data Portal SDMX REST API."""

    def __init__(self, base_url: str) -> None:
        """
        Args:
            base_url: Base URL for the API (no trailing slash needed).
        """
        self._http = make_http_client(
            user_agent=settings.scraper.user_agent,
            base_url=base_url,
            accept="text/csv",
        )

    @classmethod
    def from_settings(cls) -> Self:
        """Factory that creates a client from global settings."""
        return cls(base_url=settings.ecb_base_url)

    # ============================================================
    # Context manager support
    # ============================================================

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()

    # ============================================================
    # Retry-wrapped HTTP call
    # ============================================================

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.0, max=30.0),
        retry=retry_if_exception(_is_retryable),  # type: ignore[arg-type]
        before_sleep=before_sleep_log(logger, logging.WARNING),  # type: ignore[arg-type]
        reraise=True,
    )
    def _get_csv(self, endpoint: str, params: dict[str, str]) -> str:
        """Make a GET request with retries. Returns the raw response body
        as text.

        Internal - callers use the typed methods below.
        """
        full_params = {**params, "format": "csvdata"}
        response = self._http.get(endpoint, params=full_params)
        response.raise_for_status()
        return response.text

    # ============================================================
    # Public typed methods
    # ============================================================

    def get_series_with_observations(
        self,
        dataflow: str,
        series_key: str,
        *,
        start_period: str | None = None,
        end_period: str | None = None,
        last_n_observations: int | None = None,
    ) -> tuple[EcbSeries, list[EcbObservation]]:
        """Fetch a single series and all its observations in one round trip.

        The ECB CSV format embeds series metadata in every observation row,
        so a single API call yields both the metadata and the data points.
        We deduplicate the metadata into one EcbSeries and produce one
        EcbObservation per row.

        Args:
            dataflow: SDMX dataflow code (e.g. "RESR" for residential
                property prices).
            series_key: Dot-separated series key
                (e.g. "Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX").
            start_period: Optional, ISO 8601 period
                (e.g. "2020-Q1", "2020-01").
            end_period: Optional, ISO 8601 period.
            last_n_observations: Optional, fetch only the most recent N.

        Returns:
            Tuple of (series_metadata, observations_sorted_by_period).

        Raises:
            httpx.HTTPStatusError: 4xx error (invalid dataflow or key).
            ValueError: If the response is empty or contains rows for
                multiple series keys.
            ValidationError: if a row doesn't match the expected schema.
        """
        endpoint = f"/data/{dataflow}/{series_key}"

        params: dict[str, str] = {}
        if start_period is not None:
            params["startPeriod"] = start_period
        if end_period is not None:
            params["endPeriod"] = end_period
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)

        body = self._get_csv(endpoint, params)
        return self._parse_csv(body, expected_key=f"{dataflow}.{series_key}")

    # ============================================================
    # CSV parsing
    # ============================================================

    @staticmethod
    def _parse_csv(
        body: str,
        *,
        expected_key: str,
    ) -> tuple[EcbSeries, list[EcbObservation]]:
        """Parse the raw ECB CSV response into typed models.

        Args:
            body: Raw CSV text from the API.
            expected_key: The full series key we asked for, used as a
                consistency check against the KEY columns of every row.

        Returns:
            Tuple of (series, observations).

        Raises:
            ValueError: If the CSV is empty, malformed, or contains rows
                for an unexpected series key.
        """
        reader = csv.DictReader(io.StringIO(body))
        rows = list(reader)

        if not rows:
            msg = f"ECB returned an empty CSV response for series {expected_key!r}"
            raise ValueError(msg)

        # Sanity check: every row must reference the same series.
        unexpected_keys = {row["KEY"] for row in rows} - {expected_key}
        if unexpected_keys:
            msg = (
                f"ECB returned rows for unexpected series keys "
                f"{sorted(unexpected_keys)!r} when fetching {expected_key!r}"
            )
            raise ValueError(msg)

        # Series metadata is denormalized across rows; the first row is
        # representative. We split each row's columns into series-level,
        # observation-level, and 'extra' (preserved as JSONB downstream).
        first_row = rows[0]
        series = EcbClient._build_series(first_row)
        observations = [EcbClient._build_observation(row) for row in rows]

        # ECB usually returns sorted; sort defensively to guarantee
        # downstream invariant.
        observations.sort(key=lambda obs: obs.time_period)

        return series, observations

    @staticmethod
    def _build_series(row: dict[str, str]) -> EcbSeries:
        """Construct an EcbSeries from a single CSV row.

        Series-level fields go to first-class attributes, everything else
        (excluding observation-only fields) lands in extra_fields.
        """
        extra_fields = {
            col: value
            for col, value in row.items()
            if col not in _SERIES_CORE_FIELDS and col not in _OBSERVATION_FIELDS and value != ""
        }
        return EcbSeries(
            key=row["KEY"],
            freq=row["FREQ"],
            ref_area=row["REF_AREA"],
            region=row["REGION"],
            adjustment=row["ADJUSTMENT"],
            property_type=row["PROPERTY_TYPE"],
            indicator=row["INDICATOR"],
            data_provider=row["DATA_PROVIDER"],
            price_type=row["PRICE_TYPE"],
            transformation=row["TRANSFORMATION"],
            unit_measure=row["UNIT_MEASURE"],
            title=row["TITLE"],
            title_compl=row.get("TITLE_COMPL") or None,
            unit_index_base=row.get("UNIT_INDEX_BASE") or None,
            time_format=row.get("TIME_FORMAT") or None,
            decimals=row.get("DECIMALS") or None,  # type: ignore[arg-type]
            extra_fields=extra_fields,
        )

    @staticmethod
    def _build_observation(row: dict[str, str]) -> EcbObservation:
        """Construct an EcbObservation from a single CSV row."""
        return EcbObservation(
            time_period=row["TIME_PERIOD"],
            obs_value=row.get("OBS_VALUE") or None,  # type: ignore[arg-type]
            obs_status=row.get("OBS_STATUS") or None,
            conf_status=row.get("CONF_STATUS") or None,
            pre_break_value=row.get("PRE_BREAK_VALUE") or None,  # type: ignore[arg-type]
            comment_obs=row.get("COMMENT_OBS") or None,
        )
