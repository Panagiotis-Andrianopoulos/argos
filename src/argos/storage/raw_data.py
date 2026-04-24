"""High-level API for persisting raw data to the object store.

This module sits on top of `object_store.py` (the S3 client) and
`paths.py` (the key builders). Callers in the ingestion layer should
import from here rather than reaching into those modules directly.

Typical usage:

    from argos.storage.raw_data import RawDataWriter
    from argos.ingestion.fred import FredClient, RESIDENTIAL_PROPERTY_PRICE_NOMINAL

    with FredClient.from_settings() as fred, RawDataWriter.from_settings() as writer:
        series = fred.get_series(RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id)
        observations = fred.get_observations(RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id)

        writer.save_fred_series(series)
        writer.save_fred_observations(
            RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id, observations
        )
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Self

from argos.ingestion.fred.models import Observation, Series
from argos.storage import paths
from argos.storage.object_store import ObjectStore

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(__name__)

JSON_CONTENT_TYPE = "application/json"


class RawDataWriter:
    """Persists raw API responses to the object store.

    Usage:
        with RawDataWriter.from_settings() as writer:
            writer.save_fred_series(series)
    """

    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    @classmethod
    def from_settings(cls) -> Self:
        """Build a writer using the global settings."""
        return cls(store=ObjectStore.from_settings())

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
        """Close the underlying object store."""
        self._store.close()

    # ============================================================
    # FRED persistence
    # ============================================================

    def save_fred_series(
        self,
        series: Series,
        *,
        snapshot_date: date | None = None,
    ) -> str:
        """Save a FRED series metadata snapshot.

        Args:
            series: The validated FRED Series model.
            snapshot_date: The date to partition by. Defaults to today UTC.

        Returns:
            The S3 key under which the snapshot was stored.
        """
        snapshot_date = snapshot_date or datetime.now(UTC).date()
        key = paths.fred_series_metadata_key(series.id, snapshot_date=snapshot_date)

        payload = series.model_dump(mode="json")
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        self._store.put(body, key=key, content_type=JSON_CONTENT_TYPE)
        logger.info("Saved FRED series %s to %s", series.id, key)
        return key

    def save_fred_observations(
        self,
        series_id: str,
        observations: list[Observation],
        *,
        snapshot_date: date | None = None,
    ) -> str:
        """Save a FRED observations snapshot.

        Args:
            series_id: The FRED series this batch belongs to.
            observations: The list of validated observations.
            snapshot_date: The date to partition by. Defaults to today UTC.

        Returns:
            The S3 key under which the snapshot was stored.
        """
        snapshot_date = snapshot_date or datetime.now(UTC).date()
        key = paths.fred_observations_key(series_id, snapshot_date=snapshot_date)

        payload = {
            "series_id": series_id,
            "count": len(observations),
            "observations": [obs.model_dump(mode="json") for obs in observations],
        }
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        self._store.put(body, key=key, content_type=JSON_CONTENT_TYPE)
        logger.info(
            "Saved %d FRED observations for %s to %s",
            len(observations),
            series_id,
            key,
        )
        return key


class RawDataReader:
    """Reads raw API snapshots back from the object store.

    Useful for reprocessing historical data or auditing what we fetched
    on a specific date.
    """

    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    @classmethod
    def from_settings(cls) -> Self:
        return cls(store=ObjectStore.from_settings())

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
        self._store.close()

    # ============================================================
    # FRED retrieval
    # ============================================================

    def load_fred_series(self, series_id: str, *, snapshot_date: date) -> Series:
        """Load a FRED series metadata snapshot from storage.

        Args:
            series_id: The FRED series identifier.
            snapshot_date: The snapshot date to load.

        Returns:
            The validated Series model.
        """
        key = paths.fred_series_metadata_key(series_id, snapshot_date=snapshot_date)
        body = self._store.get(key)
        data = json.loads(body.decode("utf-8"))
        return Series.model_validate(data)

    def load_fred_observations(self, series_id: str, *, snapshot_date: date) -> list[Observation]:
        """Load a FRED observation snapshot from storage.

        Args:
            series_id: The FRED series identifier.
            snapshot_date: The snapshot date to load.

        Returns:
            A list of validated Observation models.
        """
        key = paths.fred_observations_key(series_id, snapshot_date=snapshot_date)
        body = self._store.get(key)
        payload = json.loads(body.decode("utf-8"))
        return [Observation.model_validate(obs) for obs in payload["observations"]]
