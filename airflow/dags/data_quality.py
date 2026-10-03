"""Data-quality DAG: is the pipeline telling the truth, and is it keeping up?

Two independent checks, deliberately *not* wired into the build:

1. ``source_freshness`` — `dbt source freshness`. Each source has thresholds in
   `_sources.yml`; a source past its error threshold fails the task.
2. ``reconcile`` — row counts in Postgres and the lake against the warehouse, per day
   for the fact tables and whole-table for the dimensions, plus a health snapshot
   written to `analytics.dq_health` for the "Pipeline Health" dashboard.

Why a separate DAG rather than a gate on `warehouse_build`: a stale source does not
mean the last build was wrong. Blocking the build on freshness would stop the
warehouse refreshing precisely when it is behind, which is the opposite of what you
want. Failing *here* raises the alarm without withholding data.

The tasks run in parallel because they share nothing but the warehouse they read.

Both checks will genuinely fail if the producers are stopped: `raw.events` has a 12h
error threshold, and the stream and clean run ages are measured against hourly
expectations. That is the intended behaviour -- a stale pipeline should be visible,
not quietly green.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from alerts import alert_on_failure

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": alert_on_failure,
}


def _run(command: list[str], label: str) -> None:
    """Run a subprocess and surface its output; raise with the tail on failure."""
    print(f"running: {' '.join(command)}", flush=True)
    process = subprocess.run(
        command, capture_output=True, text=True, env=os.environ.copy(), check=False
    )
    print(process.stdout[-8000:], flush=True)
    if process.returncode != 0:
        print(process.stderr[-8000:], flush=True)
        raise RuntimeError(f"{label} failed with exit code {process.returncode}")


@dag(
    dag_id="data_quality",
    description="Source freshness and source-vs-warehouse row-count reconciliation",
    schedule="0 * * * *",
    start_date=datetime(2026, 9, 27, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["phase7", "quality", "monitoring"],
)
def data_quality():
    @task
    def source_freshness() -> None:
        # dbt exits non-zero when a source is past its *error* threshold; warnings do
        # not fail the task.
        _run(["dbt", "source", "freshness"], "dbt source freshness")

    @task
    def reconcile() -> dict:
        # --fail-on-mismatch makes a row-count disagreement fail the task, which is
        # what turns a metric into an alert.
        process = subprocess.run(
            [sys.executable, "-m", "monitoring.quality", "all", "--fail-on-mismatch"],
            capture_output=True,
            text=True,
            env=os.environ.copy(),
            check=False,
        )
        print(process.stdout[-8000:], flush=True)
        if process.returncode != 0:
            print(process.stderr[-8000:], flush=True)
            raise RuntimeError("row-count reconciliation found a mismatch")

        mismatches = [
            line.strip()
            for line in process.stdout.splitlines()
            if "MISMATCH" in line or "checks_failed" in line
        ]
        return {"summary": mismatches}

    # Independent, so they run in parallel.
    source_freshness()
    reconcile()


data_quality()
