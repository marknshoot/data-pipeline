"""Hourly incremental extract from the shop OLTP database into the data lake.

One task extracts every source table for the DAG's *data interval*
``(data_interval_start, data_interval_end]``. The lake object key is derived from
the interval, not from Airflow's ``run_id``, so clearing or retrying a run
overwrites the same object instead of duplicating it — which is what makes
backfills safe. ``catchup=True`` means missed hours are filled in automatically.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from alerts import alert_on_failure
from shopstream_assets import RAW_OLTP

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="oltp_extract",
    description="Extract Postgres increments to the S3 lake (hourly, backfill-safe)",
    schedule="@hourly",
    start_date=datetime(2026, 9, 26, tzinfo=UTC),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["phase2", "ingestion", "batch"],
)
def oltp_extract():
    @task(outlets=[RAW_OLTP])
    def extract(
        data_interval_start: datetime | None = None,
        data_interval_end: datetime | None = None,
    ) -> dict:
        # Scheduled runs get a real interval from the timetable. A manual trigger
        # (`airflow dags trigger`) has no interval, so fall back to the last hour.
        from datetime import UTC, timedelta

        end = data_interval_end or datetime.now(UTC)
        start = data_interval_start or (end - timedelta(hours=1))

        # Imported lazily so DAG parsing does not need boto3/pyarrow.
        from generators import db
        from ingestion.oltp_extract import default_run_id, run

        run_id = default_run_id(start, end)
        conn = db.connect()
        try:
            results = run(
                start=start,
                end=end,
                run_id=run_id,
                conn=conn,
            )
        finally:
            conn.close()

        summary = {
            "run_id": run_id,
            "rows": {result.table: result.row_count for result in results},
            "written": {
                result.table: result.object_uri
                for result in results
                if result.written and result.row_count > 0
            },
        }
        print(f"extracted {sum(summary['rows'].values()):,} rows for {run_id}")
        return summary

    @task
    def verify(summary: dict) -> int:
        """Fail loudly if an object we claim to have written is missing."""
        from pyarrow.fs import FileType

        from ingestion.lake import build_filesystem

        filesystem = build_filesystem()
        missing = []
        for table, uri in summary["written"].items():
            key = uri.removeprefix("s3://")
            if filesystem.get_file_info(key).type != FileType.File:
                missing.append(table)
        if missing:
            raise FileNotFoundError(f"extract objects missing: {', '.join(missing)}")
        total = sum(summary["rows"].values())
        print(f"verified {len(summary['written'])} objects, {total:,} rows")
        return total

    verify(extract())


oltp_extract()
