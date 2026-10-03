"""Data-quality checks: source-vs-warehouse reconciliation and a health snapshot.

Two jobs in one module, because they answer the same question at different
resolutions:

1. **Reconciliation** (`reconcile`) counts rows in the *source* and in the
   *warehouse* and reports the difference, per day for the fact tables and
   whole-table for the dimensions. This is the check that catches a silently dropped
   extract, a dedup that ate real rows, or a join that fanned out.
2. **Health snapshot** (`snapshot`) records how fresh and how healthy each component
   is — last run ages, dead-letter counts, duplicate deliveries removed, how many
   checks failed — into one narrow table a dashboard can read.

**Why the source query is bounded by the extract watermark.** The warehouse only
holds what has been extracted, so comparing it against *all* of Postgres would report
a difference every time someone placed an order since the last extract. Each check
therefore filters the source to rows created before that table's last extract window
end. Without that bound the check is noise; with it, any non-zero difference is a real
defect.

Usage:
    uv run python -m monitoring.quality all
    uv run python -m monitoring.quality reconcile --fail-on-mismatch
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg
from clickhouse_connect.driver.client import Client

from generators import db
from generators.config import Settings, get_settings
from ingestion import lake
from warehouse.clickhouse import get_client

DDL_FILE = Path(__file__).resolve().parents[1] / "clickhouse" / "init" / "03_quality.sql"

# Freshness thresholds, in hours. They encode how often each component is *expected*
# to run: the extract and the Spark job are hourly, and the consumer is a continuous
# stream. A component that has not run inside its window is genuinely behind.
EXTRACT_WARN_HOURS, EXTRACT_ERROR_HOURS = 3.0, 6.0
STREAM_WARN_HOURS, STREAM_ERROR_HOURS = 1.0, 3.0
CLEAN_WARN_HOURS, CLEAN_ERROR_HOURS = 2.0, 6.0
# Dead-lettered records are expected (~0.5% of the generator's output), so this is
# judged as a *rate*: a rate that jumps means the producer changed, not that the
# pipeline is broken.
MALFORMED_WARN_RATE, MALFORMED_ERROR_RATE = 0.02, 0.10


@dataclass(frozen=True)
class SeriesCheck:
    """A row-count comparison between the source and the warehouse."""

    entity: str
    # "day" compares per calendar day; "total" compares whole-table counts, which is
    # the only sensible grain for a dimension whose rows have no single date.
    grain: str
    source_sql: str
    warehouse_sql: str
    # The table whose extract watermark bounds the source query.
    watermark_table: str


@dataclass(frozen=True)
class RowCountResult:
    entity: str
    grain: str
    check_date: date | None
    source_count: int
    warehouse_count: int

    @property
    def difference(self) -> int:
        return self.source_count - self.warehouse_count

    @property
    def status(self) -> str:
        return "ok" if self.difference == 0 else "error"


@dataclass(frozen=True)
class HealthMetric:
    component: str
    entity: str
    metric: str
    value: float
    status: str


# The fact tables are compared per day: a mismatch confined to one day is a much
# better clue than a total that is off by 4. The dimensions are compared whole-table,
# on their business key, because `dim_users` and `dim_products` are SCD2 and hold many
# rows per key on purpose.
CHECKS: tuple[SeriesCheck, ...] = (
    SeriesCheck(
        "orders",
        "day",
        "select created_at::date as d, count(*) as n from public.orders "
        "where created_at < %(watermark)s group by 1",
        "select toDate(created_at) as d, count() as n from marts.fct_orders group by 1",
        "orders",
    ),
    SeriesCheck(
        "order_items",
        "day",
        "select created_at::date as d, count(*) as n from public.order_items "
        "where created_at < %(watermark)s group by 1",
        "select toDate(created_at) as d, count() as n from marts.fct_order_items group by 1",
        "order_items",
    ),
    SeriesCheck(
        "payments",
        "day",
        "select created_at::date as d, count(*) as n from public.payments "
        "where created_at < %(watermark)s group by 1",
        "select toDate(created_at) as d, count() as n from marts.fct_payments group by 1",
        "payments",
    ),
    SeriesCheck(
        "users",
        "total",
        "select count(*) as n from public.users where created_at < %(watermark)s",
        "select uniqExact(user_id) as n from marts.dim_users",
        "users",
    ),
    SeriesCheck(
        "products",
        "total",
        "select count(*) as n from public.products where created_at < %(watermark)s",
        "select uniqExact(product_id) as n from marts.dim_products",
        "products",
    ),
    SeriesCheck(
        "sellers",
        "total",
        "select count(*) as n from public.sellers where created_at < %(watermark)s",
        "select uniqExact(seller_id) as n from marts.dim_sellers",
        "sellers",
    ),
    SeriesCheck(
        "categories",
        "total",
        "select count(*) as n from public.categories where created_at < %(watermark)s",
        "select uniqExact(category_id) as n from marts.dim_categories",
        "categories",
    ),
)


# ------------------------------------------------------------------ pure logic


def status_for(
    value: float,
    *,
    warn_at: float,
    error_at: float,
    higher_is_worse: bool = True,
) -> str:
    """Classify a metric against two thresholds."""
    if higher_is_worse:
        if value >= error_at:
            return "error"
        return "warn" if value >= warn_at else "ok"
    if value <= error_at:
        return "error"
    return "warn" if value <= warn_at else "ok"


def compare_series(
    entity: str,
    grain: str,
    source: dict[date | None, int],
    warehouse: dict[date | None, int],
) -> list[RowCountResult]:
    """Compare two count series, including keys present on only one side.

    A day that exists in the source but not the warehouse (or the reverse) is the
    interesting case -- it is exactly the shape of a dropped or duplicated extract --
    so the union of keys is compared rather than the intersection.
    """
    return [
        RowCountResult(
            entity=entity,
            grain=grain,
            check_date=key,
            source_count=source.get(key, 0),
            warehouse_count=warehouse.get(key, 0),
        )
        for key in sorted(source.keys() | warehouse.keys(), key=lambda d: (d is None, d))
    ]


# ------------------------------------------------------------------ warehouse


def ensure_tables(client: Client) -> None:
    """Apply the canonical DDL, so the job works on a fresh warehouse."""
    for statement in DDL_FILE.read_text().split(";"):
        if statement.strip():
            client.command(statement)


def write_row_counts(client: Client, results: list[RowCountResult], checked_at: datetime) -> None:
    if not results:
        return
    client.insert(
        "analytics.dq_row_counts",
        [
            [
                checked_at,
                result.entity,
                result.grain,
                result.check_date,
                result.source_count,
                result.warehouse_count,
                result.difference,
                result.status,
            ]
            for result in results
        ],
        column_names=[
            "checked_at",
            "entity",
            "grain",
            "check_date",
            "source_count",
            "warehouse_count",
            "difference",
            "status",
        ],
    )


def write_health(client: Client, metrics: list[HealthMetric], checked_at: datetime) -> None:
    if not metrics:
        return
    client.insert(
        "analytics.dq_health",
        [[checked_at, m.component, m.entity, m.metric, m.value, m.status] for m in metrics],
        column_names=["checked_at", "component", "entity", "metric", "metric_value", "status"],
    )


# --------------------------------------------------------------- reconciliation


def extract_watermarks(conn: psycopg.Connection) -> dict[str, datetime]:
    """The end of the last extract window per source table."""
    with conn.cursor() as cur:
        cur.execute("select table_name, max(window_end) from pipeline_meta.extract_run group by 1")
        return {name: window_end for name, window_end in cur.fetchall() if window_end}


def _counts(rows) -> dict[date | None, int]:
    """Normalise a query result into ``{date|None: count}``."""
    series: dict[date | None, int] = {}
    for row in rows:
        if len(row) == 1:
            series[None] = int(row[0])
        else:
            key, count = row
            series[key] = int(count)
    return series


def run_series_check(
    conn: psycopg.Connection,
    client: Client,
    check: SeriesCheck,
    watermarks: dict[str, datetime],
) -> list[RowCountResult]:
    watermark = watermarks.get(check.watermark_table)
    if watermark is None:
        # No extract has run for this table, so there is nothing to compare against.
        return []
    with conn.cursor() as cur:
        cur.execute(check.source_sql, {"watermark": watermark})
        source = _counts(cur.fetchall())
    warehouse = _counts(client.query(check.warehouse_sql).result_rows)
    return compare_series(check.entity, check.grain, source, warehouse)


def check_events(settings: Settings, client: Client) -> list[RowCountResult]:
    """Events have no Postgres counterpart, so the lake's clean layer is the source.

    This is the check that proves the load is faithful: whatever Spark published
    should be exactly what the warehouse holds.
    """
    _objects, lake_rows = lake.lake_stats("clean/events", bucket=settings.s3_bucket)
    warehouse_rows = int(client.query("select count() from raw.events final").first_row[0])
    return compare_series("events", "total", {None: lake_rows}, {None: warehouse_rows})


def reconcile(
    *,
    settings: Settings | None = None,
    client: Client | None = None,
    conn: psycopg.Connection | None = None,
) -> list[RowCountResult]:
    settings = settings or get_settings()
    client = client or get_client(settings)
    owns_conn = conn is None
    conn = conn or db.connect()
    try:
        ensure_tables(client)
        watermarks = extract_watermarks(conn)
        results: list[RowCountResult] = []
        for check in CHECKS:
            results.extend(run_series_check(conn, client, check, watermarks))
        results.extend(check_events(settings, client))
        return results
    finally:
        if owns_conn:
            conn.close()


# ------------------------------------------------------------------- snapshot


def _age_hours(now: datetime, then: datetime | None) -> float:
    if then is None:
        return float("inf")
    return round((now - then).total_seconds() / 3600, 2)


def collect_health(
    conn: psycopg.Connection,
    results: list[RowCountResult],
    *,
    now: datetime | None = None,
) -> list[HealthMetric]:
    """Build the health snapshot from the pipeline's own run records."""
    now = now or datetime.now(UTC)
    since = now - timedelta(hours=24)
    metrics: list[HealthMetric] = []

    with conn.cursor() as cur:
        cur.execute("select table_name, max(recorded_at) from pipeline_meta.extract_run group by 1")
        for table, last_run in cur.fetchall():
            age = _age_hours(now, last_run)
            metrics.append(
                HealthMetric(
                    "extract",
                    table,
                    "last_run_age_hours",
                    age,
                    status_for(age, warn_at=EXTRACT_WARN_HOURS, error_at=EXTRACT_ERROR_HOURS),
                )
            )

        cur.execute(
            """
            select coalesce(sum(records), 0), coalesce(sum(malformed), 0), max(started_at)
            from pipeline_meta.stream_run where started_at >= %s
            """,
            (since,),
        )
        records, malformed, last_stream = cur.fetchone()
        stream_age = _age_hours(now, last_stream)
        rate = (malformed / records) if records else 0.0
        metrics.extend(
            [
                HealthMetric("stream", "", "records_last_24h", float(records), "ok"),
                HealthMetric(
                    "stream",
                    "",
                    "malformed_last_24h",
                    float(malformed),
                    status_for(rate, warn_at=MALFORMED_WARN_RATE, error_at=MALFORMED_ERROR_RATE),
                ),
                HealthMetric(
                    "stream",
                    "",
                    "last_run_age_hours",
                    stream_age,
                    status_for(stream_age, warn_at=STREAM_WARN_HOURS, error_at=STREAM_ERROR_HOURS),
                ),
            ]
        )

        cur.execute(
            """
            select coalesce(sum(input_rows), 0), coalesce(sum(duplicates_removed), 0),
                   max(recorded_at)
            from pipeline_meta.clean_run where recorded_at >= %s
            """,
            (since,),
        )
        cleaned, duplicates, last_clean = cur.fetchone()
        clean_age = _age_hours(now, last_clean)
        metrics.extend(
            [
                HealthMetric("clean", "", "rows_cleaned_last_24h", float(cleaned), "ok"),
                HealthMetric("clean", "", "duplicates_removed_last_24h", float(duplicates), "ok"),
                HealthMetric(
                    "clean",
                    "",
                    "last_run_age_hours",
                    clean_age,
                    status_for(clean_age, warn_at=CLEAN_WARN_HOURS, error_at=CLEAN_ERROR_HOURS),
                ),
            ]
        )

    failed = [result for result in results if result.status != "ok"]
    metrics.extend(
        [
            HealthMetric("reconcile", "", "checks_run", float(len(results)), "ok"),
            HealthMetric(
                "reconcile",
                "",
                "checks_failed",
                float(len(failed)),
                "error" if failed else "ok",
            ),
        ]
    )
    return metrics


def snapshot(
    results: list[RowCountResult] | None = None,
    *,
    settings: Settings | None = None,
    client: Client | None = None,
    conn: psycopg.Connection | None = None,
) -> list[HealthMetric]:
    settings = settings or get_settings()
    client = client or get_client(settings)
    owns_conn = conn is None
    conn = conn or db.connect()
    try:
        ensure_tables(client)
        if results is None:
            results = reconcile(settings=settings, client=client, conn=conn)
        metrics = collect_health(conn, results)
        return metrics
    finally:
        if owns_conn:
            conn.close()


def report(results: list[RowCountResult], metrics: list[HealthMetric], log=print) -> None:
    mismatches = [result for result in results if result.status != "ok"]
    log(f"row-count reconciliation: {len(results)} check(s), {len(mismatches)} mismatch(es)")
    for result in mismatches:
        when = result.check_date.isoformat() if result.check_date else "total"
        log(
            f"  MISMATCH {result.entity:<12} {when:<12} "
            f"source={result.source_count} warehouse={result.warehouse_count} "
            f"diff={result.difference:+d}"
        )
    for metric in metrics:
        where = f"{metric.component}/{metric.entity}" if metric.entity else metric.component
        log(f"  {metric.status:<5} {where:<20} {metric.metric}={metric.value}")


# ----------------------------------------------------------------------- cli


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Data-quality checks and health snapshot")
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["reconcile", "snapshot", "all"],
        help="which job to run (default: all)",
    )
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="exit non-zero when any row count disagrees (for the Airflow task)",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    checked_at = datetime.now(UTC)
    client = get_client(settings)
    try:
        results: list[RowCountResult] = []
        metrics: list[HealthMetric] = []
        if args.command in {"reconcile", "all"}:
            results = reconcile(settings=settings, client=client)
        if args.command in {"snapshot", "all"}:
            metrics = snapshot(results or None, settings=settings, client=client)
        if args.command == "all":
            write_row_counts(client, results, checked_at)
            write_health(client, metrics, checked_at)
        report(results, metrics)
    except Exception as exc:  # noqa: BLE001 - surface any client/HTTP error to the CLI
        print(f"quality check failed: {exc}", file=sys.stderr)
        return 1

    mismatches = [result for result in results if result.status != "ok"]
    if args.fail_on_mismatch and mismatches:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
