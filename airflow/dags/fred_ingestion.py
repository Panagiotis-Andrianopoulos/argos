"""Daily FRED ingestion DAG.

Fetches one or more FRED series, archives raw data to object storage,
and persists to Postgres for analytical queries.

Pipeline (per series):
1. fetch_and_archive — FRED API → MinIO snapshot → return storage key
2. persist_to_postgres — read MinIO snapshot → upsert to Postgres

Architecture decisions:
- Two tasks (not one) for granular retries: if Postgres is briefly down,
  we retry only the persist step without re-hitting the FRED API.
- XCom carries only the MinIO key (a few bytes), not the data itself.
  Data lives in MinIO between tasks. This scales regardless of dataset size.
- Idempotent by construction: MinIO writes are date-partitioned (same key
  on same day), and Postgres upserts use ON CONFLICT DO UPDATE.
"""

# pyright: reportMissingImports=false

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from airflow.sdk import dag, task

if TYPE_CHECKING:
    pass


@dag(
    dag_id="fred_ingestion",
    description="Daily ingestion of FRED economic indicator for the Greek market",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "fred"],
    default_args={
        "owner": "argos",
        "retries": 0,
    },
)
def fred_ingestion() -> None:
    """Ingest Greek residential property price index from FRED."""

    @task
    def fetch_and_archive() -> dict[str, str]:
        """Hit FRED API and archive the response to MinIO.

        Returns the MinIO key so downstream tasks can read the snapshot
        without re-hitting the FRED API.
        """
        from datetime import UTC
        from datetime import datetime as dt

        from argos.ingestion.fred import RESIDENTIAL_PROPERTY_PRICE_NOMINAL, FredClient
        from argos.storage.raw_data import RawDataWriter

        series_id = RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id
        snapshot_date = dt.now(UTC).date()

        with FredClient.from_settings() as fred:
            series = fred.get_series(series_id)
            observations = fred.get_observations(series_id)

        with RawDataWriter.from_settings() as writer:
            series_key = writer.save_fred_series(series, snapshot_date=snapshot_date)
            obs_key = writer.save_fred_observations(
                series_id, observations, snapshot_date=snapshot_date
            )

        return {
            "series_id": series_id,
            "snapshot_date": snapshot_date.isoformat(),
            "series_key": series_key,
            "obs_key": obs_key,
            "obs_count": str(len(observations)),
        }

    @task
    def persist_to_postgres(archive_info: dict[str, str]) -> dict[str, int]:
        """Read the archived snapshot from MinIO and upsert into Postgres."""
        from datetime import date as date_cls

        from argos.ingestion.fred.persistence import upsert_observations, upsert_series
        from argos.storage.database import get_session
        from argos.storage.raw_data import RawDataReader

        series_id = archive_info["series_id"]
        snapshot_date = date_cls.fromisoformat(archive_info["snapshot_date"])

        with RawDataReader.from_settings() as reader:
            series = reader.load_fred_series(series_id, snapshot_date=snapshot_date)
            observations = reader.load_fred_observations(series_id, snapshot_date=snapshot_date)

        with get_session() as session:
            upsert_series(session, series)
            rows_affected = upsert_observations(session, series_id, observations)
            session.commit()

        return {
            "series_persisted": 1,
            "observations_persisted": rows_affected,
        }

    persist_to_postgres(fetch_and_archive())


fred_ingestion()
