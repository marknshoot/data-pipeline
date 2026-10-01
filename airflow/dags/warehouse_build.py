"""Load the lake into ClickHouse, then rebuild the warehouse with dbt.

Scheduled on the ``RAW_OLTP`` asset rather than on a clock: whenever
``oltp_extract`` writes new Parquet, this DAG fires. Two tasks:

1. ``load_raw`` — idempotent lake -> ClickHouse load (ReplacingMergeTree dedups).
2. ``dbt_build`` — dbt snapshots + models + tests in one pass.

Because the load appends and dbt facts are incremental with a lookback, re-running
this DAG is safe and does not duplicate rows.
"""

from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from shopstream_assets import RAW_OLTP

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="warehouse_build",
    description="Load raw Parquet into ClickHouse and dbt build (data-triggered)",
    schedule=[RAW_OLTP],
    start_date=datetime(2026, 9, 27, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["phase3", "warehouse", "dbt"],
)
def warehouse_build():
    @task
    def load_raw() -> dict:
        # Imported lazily so DAG parsing does not need clickhouse-connect/pyarrow.
        from warehouse.load_raw import load_all

        results = load_all()
        rows = {result.table: result.rows for result in results}
        print(f"loaded {sum(rows.values()):,} rows into ClickHouse raw tables")
        return rows

    @task
    def dbt_build(rows: dict) -> None:
        print(f"raw tables loaded: {rows}")
        # dbt artefacts go to DBT_TARGET_PATH / DBT_LOG_PATH (tmp) because the repo
        # is mounted read-only in the container.
        env = os.environ.copy()
        process = subprocess.run(
            ["dbt", "build"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        tail = (process.stdout or "")[-8000:]
        print(tail)
        if process.returncode != 0:
            print((process.stderr or "")[-8000:])
            raise RuntimeError(f"dbt build failed with exit code {process.returncode}")

    dbt_build(load_raw())


warehouse_build()
