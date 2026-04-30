# pyright: reportMissingImports=false
"""Hello world DAG — verifies that Airflow infrastructure is wired correctly.

This DAG is intentionally trivial. Its purpose is to confirm:
- the scheduler discovers DAG files in /opt/airflow/dags
- task parsing succeeds
- LocalExecutor can launch and complete a task
- logs are captured

If this DAG fails, the issue is infrastructure, not application code.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_argos",
    description="Smoke test DAG to verify Airflow setup",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke-test"],
)
def hello_argos() -> None:
    """Single-task DAG that prints a greeting."""

    @task
    def greet() -> str:
        message = "Hello from ARGOS - Airflow is working!"
        print(message)
        return message

    greet()


hello_argos()
