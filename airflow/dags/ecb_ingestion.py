"""Daily ECB Data Portal ingestion DAG.

Fetches the Greek residential property price series from the ECB Data
Portal, archives the raw response to object storage, and persists to
Postgres for analytical queries.

Pipeline (per series):
1. fetch_and_archive — ECB API → MinIO snapshot → return storage key
2. persist_to_postgres — read MinIO snapshot → upsert to Postgres

Architecture decisions:
- Mirrors the structure of fred_ingestion.py for consistency. The two
  DAGs share the same shape: two tasks, XCom carries only MinIO keys
  plus the metadata needed for persistence (series key, frequency).
- The series frequency is passed through XCom because the persistence
  layer needs it to map period strings ('2025-Q1') to DATE values.
- Two tasks (not one) for granular retries: if Postgres is briefly
  down, we retry only the persist step without re-hitting the ECB API.
- Idempotent by construction: MinIO writes are date-partitioned and
  Postgres upserts use ON CONFLICT DO UPDATE.

Schedule rationale:
- ECB updates the RESR Greek HPI series quarterly, with revisions to
  recent quarters appearing month-to-month (the OBS_STATUS=P flag we
  see on Q4 2024 onward indicates provisional values).
- Monthly schedule (1st of month) catches both new quarters and
  revisions to previous ones with comfortable margin.
- 06:30 UTC = 30 min after the FRED DAG, to avoid contention on the
  shared Postgres connection pool.
"""

# pyright: reportMissingImports=false

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from airflow.sdk import dag, task

if TYPE_CHECKING:
    pass

# Series we ingest from this DAG. Keep as module-level constants so they
# are visible without unpacking the task functions.
_DATAFLOW = "RESR"
_SERIES_KEY = "Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX"


@dag(
    dag_id="ecb_ingestion",
    description="Daily ingestion of ECB Data Portal indicators for the Greek market",
    start_date=datetime(2026, 5, 1),
    schedule="30 6 1 * *",  # 1st of each month, 06:30 UTC
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "ecb"],
    default_args={
        "owner": "argos",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
    },
    dagrun_timeout=timedelta(minutes=15),
)
def ecb_ingestion() -> None:
    """Ingest Greek residential property price index from the ECB Data Portal."""

    @task
    def fetch_and_archive() -> dict[str, str]:
        """Hit the ECB Data Portal and archive the response to MinIO."""
        from datetime import UTC
        from datetime import datetime as dt

        from argos.ingestion.ecb import EcbClient
        from argos.storage.raw_data import RawDataWriter

        snapshot_date = dt.now(UTC).date()

        with EcbClient.from_settings() as ecb:
            series, observations = ecb.get_series_with_observations(
                dataflow=_DATAFLOW,
                series_key=_SERIES_KEY,
            )

        with RawDataWriter.from_settings() as writer:
            series_storage_key = writer.save_ecb_series(series, snapshot_date=snapshot_date)
            obs_storage_key = writer.save_ecb_observations(
                series.key, observations, snapshot_date=snapshot_date
            )

        return {
            "series_key": series.key,
            "frequency": series.freq,
            "snapshot_date": snapshot_date.isoformat(),
            "series_storage_key": series_storage_key,
            "obs_storage_key": obs_storage_key,
            "obs_count": str(len(observations)),
        }

    @task
    def persist_to_postgres(archive_info: dict[str, str]) -> dict[str, int]:
        """Read the archived snapshot from MinIO and upsert into Postgres."""
        from datetime import date as date_cls

        from argos.ingestion.ecb.persistence import (
            upsert_observations,
            upsert_series,
        )
        from argos.storage.database import get_session
        from argos.storage.raw_data import RawDataReader

        series_key = archive_info["series_key"]
        frequency = archive_info["frequency"]
        snapshot_date = date_cls.fromisoformat(archive_info["snapshot_date"])

        with RawDataReader.from_settings() as reader:
            series = reader.load_ecb_series(series_key, snapshot_date=snapshot_date)
            observations = reader.load_ecb_observations(series_key, snapshot_date=snapshot_date)

        with get_session() as session:
            upsert_series(session, series)
            rows_affected = upsert_observations(
                session, series_key, observations, frequency=frequency
            )
            session.commit()

        return {
            "series_persisted": 1,
            "observations_persisted": rows_affected,
        }

    persist_to_postgres(fetch_and_archive())


ecb_ingestion()
