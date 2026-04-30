# pyright: reportMissingImports=false
"""Diagnostic DAG: tries to import the argos package from inside Airflow.

Expected to fail initially. Each failure mode tells us which layer needs fixing:
- ModuleNotFoundError: 'argos' → Layer 1 (sys.path)
- ModuleNotFoundError: 'pydantic_settings' or other dep → Layer 2 (deps)
- ValidationError or AttributeError on settings → Layer 3 (env vars)
"""

from __future__ import annotations

import sys
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="test_argos_import",
    description="Diagnostic: verify argos package is importable inside Airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke-test", "diagnostic"],
)
def test_argos_import() -> None:
    """Attempts to import argos and report what works."""

    @task
    def diagnose() -> dict[str, str]:
        report: dict[str, str] = {}

        report["sys_path"] = "\n".join(sys.path)

        try:
            import argos

            report["argos_import"] = f"OK - loaded from {argos.__file__}"
        except ModuleNotFoundError as e:
            report["argos_import"] = f"FAIL - {e}"
            return report

        try:
            import argos.config

            report["argos_config"] = "OK - Settings imported"
        except ModuleNotFoundError as e:
            report["argos_config"] = f"FAIL (missing dep) - {e}"
            return report
        except Exception as e:
            report["argos_config"] = f"FAIL (other) - {e}"
            return report

        try:
            from argos.config import settings

            report["settings_db_host"] = settings.db_host
            report["settings_series_id"] = "checking FRED constants..."

            from argos.ingestion.fred import RESIDENTIAL_PROPERTY_PRICE_NOMINAL

            report["fred_constant"] = RESIDENTIAL_PROPERTY_PRICE_NOMINAL.id
        except Exception as e:
            report["functional_check"] = f"FAIL - {type(e).__name__}: {e}"
            return report

        report["status"] = "ALL LAYERS OK"
        return report

    @task
    def print_report(report: dict[str, str]) -> None:
        print("=" * 60)
        print("ARGOS IMPORT DIAGNOSTIC REPORT")
        print("=" * 60)
        for key, value in report.items():
            print(f"\n[{key}]")
            print(value)
        print("=" * 60)

    print_report(diagnose())


test_argos_import()
