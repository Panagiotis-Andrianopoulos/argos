"""FRED API client for the ARGOS.

Wraps the FRED REST API in Pythonic interface that returns
validated Pydantic models.

Typical usage:
    from argos.ingestion.fred import FredClient
    from argos.ingestion.fred.series import RESIDENTIAL_PROPERTY_PRICE_NOMINAL

    with FredClient.from_settings() as client:
        series = client.get_series(RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id)
        observations = client.get_observations(RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id)

Reference: https://fred.stlouisfed.org/docs/api/fred/
"""

from __future__ import annotations

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
from argos.ingestion.fred.models import ObservationsResponse, SeriesResponse
from argos.ingestion.http import make_http_client

if TYPE_CHECKING:
    from datetime import date

    from argos.ingestion.fred.models import Observation, Series

logger = logging.getLogger(__name__)


def _is_retryable(exc: Exception) -> bool:
    """Decides if an exception should trigger a retry.

    Retry: 5xx server errors, 408 Request Timeout, 429 Too Many Requests,
    network/timeout errors.

    Don't retry: 4xx client errors (bug in code or auth issue).
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status >= 500 or status in {408, 429}
    return isinstance(exc, httpx.TransportError)


class FredClient:
    """Client for the FRED (Federal Reserve Economic Data) API.

    Use:
        # With context manager (recommended)
        with FredClient.from_settings() as client:
            series = client.get_series("QGRN628BIS")

        # Without context manager (make sure to call .close())
        client = FredClient.from_settings()
        try:
            ...
        finally:
            client.close()
    """

    def __init__(self, api_key: str, base_url: str) -> None:
        """
        Args:
            api_key: FRED API key (32 chars).
            base_url: Base URL for the API.
        """
        self._api_key = api_key
        self._http = make_http_client(user_agent=settings.scraper.user_agent, base_url=base_url)

    @classmethod
    def from_settings(cls) -> Self:
        """Factory that creates client from global settings."""
        return cls(
            api_key=settings.fred_api_key.get_secret_value(),
            base_url=settings.fred_base_url,
        )

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
        """Closes the underlying HTTP client."""
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
    def _get(self, endpoint: str, params: dict[str, str]) -> dict[str, object]:
        """Makes GET request with retries. Returns parse JSON.

        Internal - the callers uses the typed methods below.
        """
        full_params = {
            **params,
            "api_key": self._api_key,
            "file_type": "json",
        }
        response = self._http.get(endpoint, params=full_params)
        response.raise_for_status()
        data: dict[str, object] = response.json()
        return data

    # ============================================================
    # Public typed methods
    # ============================================================

    def get_series(self, series_id: str) -> Series:
        """Fetch metadata about a series.

        Args:
            series_id: FRED series ID (e.g. "QGRN628BIS").

        Returns:
            Validated Series model.

        Raises:
            httpx.HttpstatusError: 4xx error (invalid ID, bad auth).
            ValidationError: If the response doesn't match the expected schema.
        """
        data = self._get("/series", {"series_id": series_id})
        response = SeriesResponse.model_validate(data)
        if not response.seriess:
            msg = f"No series found for ID: {series_id}"
            raise ValueError(msg)
        return response.seriess[0]

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: date | None = None,
        observation_end: date | None = None,
    ) -> list[Observation]:
        """Fetch observations (data points) for a series.

        Args:
            series_id: FRED series ID.
            observation_start: Optional, filter from this date.
            observation_end: Optional, filter up to this date.

            Returns:
                List of validated Observation models, sorted by date ascending.
        """
        params: dict[str, str] = {"series_id": series_id}
        if observation_start:
            params["observation_start"] = observation_start.isoformat()
        if observation_end:
            params["observation_end"] = observation_end.isoformat()

        data = self._get("/series/observations", params)
        response = ObservationsResponse.model_validate(data)
        return response.observations
