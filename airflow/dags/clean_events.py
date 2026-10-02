"""Spark: clean the lake's raw clickstream into its clean layer.

Hourly, and deliberately not backfilled: the job reprocesses a rolling lookback window
and *replaces* those partitions, so a late event is absorbed by the next run rather
than needing a historical correction. Running often is what keeps that window small.

The task shells out to `python -m spark.clean_events` instead of importing Spark into
the scheduler process. The JVM is then isolated to the task, and the job can be run
and tested identically by hand (`make spark-clean`, `make spark-test`).

Publishing the ``CLEAN_EVENTS`` asset is what makes the warehouse follow along:
``warehouse_build`` is scheduled on it, so a new clean partition triggers the raw load
and the dbt build without anyone wiring the two DAGs together.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from shopstream_assets import CLEAN_EVENTS

# Must exceed the producer's maximum lateness (2h), or a late event could arrive for a
# partition that has already been published. The job enforces this too.
LOOKBACK_HOURS = 3

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # Spark start-up dominates a run of this size; the timeout only needs to catch a
    # genuinely stuck job.
    "execution_timeout": timedelta(minutes=20),
}


@dag(
    dag_id="clean_events",
    description="Spark: dedup, merge schema v1/v2 and partition the clickstream by event time",
    schedule="0 * * * *",
    start_date=datetime(2026, 9, 27, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["phase6", "spark", "lake"],
)
def clean_events():
    @task(outlets=[CLEAN_EVENTS])
    def clean() -> dict:
        # The job sizes the driver itself; the container's limit is the ceiling.
        command = [
            sys.executable,
            "-m",
            "spark.clean_events",
            "--lookback-hours",
            str(LOOKBACK_HOURS),
        ]
        print(f"running: {' '.join(command)}", flush=True)
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
            check=False,
        )
        # Spark logs a lot of noise to stderr; the job's own summary is on stdout.
        print(process.stdout[-8000:], flush=True)
        if process.returncode != 0:
            print(process.stderr[-8000:], flush=True)
            raise RuntimeError(f"spark clean_events failed with exit code {process.returncode}")

        summary = next(
            (line for line in reversed(process.stdout.splitlines()) if line.startswith("clean: ")),
            "",
        )
        return {"lookback_hours": LOOKBACK_HOURS, "summary": summary}

    clean()


clean_events()
