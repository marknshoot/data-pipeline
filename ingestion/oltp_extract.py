"""Incremental OLTP extract: Postgres -> Parquet in the S3 data lake.

Window semantics
----------------
Every run extracts rows with ``updated_at > window_start AND updated_at <=
window_end`` for each source table, then writes one Parquet object per table:

    raw/oltp/<table>/dt=<window_end date>/<run_id>.parquet

The object key is a pure function of the window (``run_id`` defaults to the
window bounds), so **re-running an interval overwrites the same object and cannot
duplicate data**. That is what makes backfills safe.

Late updates
------------
A row updated after its interval's end is picked up again by a later interval,
so the same business key can appear in several interval files. Deduplication by
newest ``updated_at`` happens downstream (Spark/ClickHouse), which is the standard
trade-off documented in the README.

Usage:
    uv run python -m ingestion.oltp_extract --incremental
    uv run python -m ingestion.oltp_extract --start 2026-09-27T00:00:00Z --end 2026-09-28T00:00:00Z
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
import pyarrow as pa
from psycopg.rows import dict_row

from generators import db
from generators.config import Settings, get_settings

from . import lake

SOURCE_TABLES: tuple[str, ...] = (
    "users",
    "sellers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
)

# Postgres `information_schema.columns.data_type` -> Arrow type.
_SIMPLE_TYPES: dict[str, pa.DataType] = {
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "boolean": pa.bool_(),
    "text": pa.string(),
    "character varying": pa.string(),
    "character": pa.string(),
    "uuid": pa.string(),
    "double precision": pa.float64(),
    "real": pa.float32(),
    "date": pa.date32(),
    "timestamp with time zone": pa.timestamp("us", tz="UTC"),
    "timestamp without time zone": pa.timestamp("us"),
}


@dataclass(frozen=True)
class ExtractResult:
    table: str
    row_count: int
    object_uri: str
    window_start: datetime
    window_end: datetime


def arrow_type(data_type: str, precision: int | None, scale: int | None) -> pa.DataType:
    if data_type == "numeric":
        return pa.decimal128(precision or 38, scale or 9)
    try:
        return _SIMPLE_TYPES[data_type]
    except KeyError as exc:
        raise ValueError(f"no Arrow mapping for Postgres type {data_type!r}") from exc


def columns_to_schema(columns: list[dict]) -> pa.Schema:
    """Build an Arrow schema from ``information_schema.columns`` rows."""
    fields = [
        pa.field(
            column["column_name"],
            arrow_type(column["data_type"], column["numeric_precision"], column["numeric_scale"]),
            nullable=column["is_nullable"] == "YES",
        )
        for column in columns
    ]
    return pa.schema(fields)


def _columns(conn: psycopg.Connection, table: str) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return cur.fetchall()


def _primary_key(conn: psycopg.Connection, table: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            """,
            (table,),
        )
        rows = [row[0] for row in cur.fetchall()]
    return rows[0] if rows else "updated_at"


def default_run_id(start: datetime, end: datetime) -> str:
    return f"{start.astimezone(UTC):%Y%m%dT%H%M%S}-{end.astimezone(UTC):%Y%m%dT%H%M%S}"


def build_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)


def extract_table(
    conn: psycopg.Connection,
    table: str,
    *,
    start: datetime,
    end: datetime,
    run_id: str,
    bucket: str,
    filesystem=None,
    settings: Settings | None = None,
    dry_run: bool = False,
) -> ExtractResult:
    schema = columns_to_schema(_columns(conn, table))
    primary_key = _primary_key(conn, table)
    # Identifiers come from SOURCE_TABLES or Postgres introspection, never user input.
    sql = (
        f"SELECT * FROM {table} "  # noqa: S608
        f"WHERE updated_at > %s AND updated_at <= %s "
        f"ORDER BY {primary_key}, updated_at"
    )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (start, end))
        rows = cur.fetchall()

    arrow_table = build_table(rows, schema)
    key = lake.raw_oltp_key(table, lake.partition_date_for(end), run_id)
    if dry_run:
        return ExtractResult(
            table=table,
            row_count=arrow_table.num_rows,
            object_uri=f"s3://{bucket}/{key} (dry run)",
            window_start=start,
            window_end=end,
        )

    uri = lake.write_parquet(
        arrow_table, key, bucket=bucket, filesystem=filesystem or lake.build_filesystem(settings)
    )
    return ExtractResult(
        table=table,
        row_count=arrow_table.num_rows,
        object_uri=uri,
        window_start=start,
        window_end=end,
    )


def record_run(conn: psycopg.Connection, results: list[ExtractResult], run_id: str) -> None:
    """Upsert one observability row per extracted table (idempotent)."""
    if not results:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO pipeline_meta.extract_run
                (run_id, table_name, window_start, window_end, row_count, object_path, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (run_id, table_name) DO UPDATE SET
                row_count = EXCLUDED.row_count,
                object_path = EXCLUDED.object_path,
                recorded_at = now()
            """,
            [
                (
                    run_id,
                    result.table,
                    result.window_start,
                    result.window_end,
                    result.row_count,
                    result.object_uri,
                )
                for result in results
            ],
        )


def load_watermark(conn: psycopg.Connection, table: str, default: datetime) -> datetime:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT watermark FROM pipeline_meta.extract_state WHERE table_name = %s",
            (table,),
        )
        row = cur.fetchone()
    return row[0] if row else default


def save_watermark(conn: psycopg.Connection, table: str, watermark: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_meta.extract_state (table_name, watermark, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (table_name) DO UPDATE SET
                watermark = EXCLUDED.watermark, updated_at = now()
            """,
            (table, watermark),
        )


def run(
    *,
    tables: tuple[str, ...] = SOURCE_TABLES,
    start: datetime,
    end: datetime,
    run_id: str | None = None,
    bucket: str | None = None,
    lookback: timedelta = timedelta(0),
    incremental: bool = False,
    history_days: int = 30,
    dry_run: bool = False,
    conn: psycopg.Connection | None = None,
    filesystem=None,
) -> list[ExtractResult]:
    """Extract ``[start, end]`` for each table; in incremental mode, per-table watermarks."""
    settings = get_settings()
    bucket = bucket or settings.s3_bucket
    run_id = run_id or default_run_id(start, end)
    owns_conn = conn is None
    conn = conn or db.connect()
    try:
        results: list[ExtractResult] = []
        for table in tables:
            if incremental:
                default_watermark = end - timedelta(days=history_days)
                watermark = load_watermark(conn, table, default_watermark)
                table_start = watermark - lookback
            else:
                table_start = start - lookback
            results.append(
                extract_table(
                    conn,
                    table,
                    start=table_start,
                    end=end,
                    run_id=run_id,
                    bucket=bucket,
                    filesystem=filesystem,
                    settings=settings,
                    dry_run=dry_run,
                )
            )
            if incremental and not dry_run:
                save_watermark(conn, table, end)
        if not dry_run:
            record_run(conn, results, run_id)
        conn.commit()
        return results
    finally:
        if owns_conn:
            conn.close()


def _parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract OLTP increments to the lake")
    window = parser.add_mutually_exclusive_group(required=True)
    window.add_argument("--incremental", action="store_true", help="use stored watermarks")
    window.add_argument("--start", help="window start (ISO-8601)")
    parser.add_argument("--end", help="window end (ISO-8601, default: now)")
    parser.add_argument("--tables", help="comma-separated subset (default: all)")
    parser.add_argument("--run-id", help="object key stem (default: derived from the window)")
    parser.add_argument("--bucket", help="override S3 bucket")
    parser.add_argument("--lookback-minutes", type=float, default=0.0, help="re-read overlap")
    parser.add_argument("--history-days", type=int, default=30, help="first-run backfill depth")
    parser.add_argument("--dry-run", action="store_true", help="count rows, write nothing")
    args = parser.parse_args(argv)

    end = _parse_instant(args.end) if args.end else datetime.now(UTC)
    if args.incremental:
        start = end
    else:
        if not args.start:
            parser.error("--start is required unless --incremental")
        start = _parse_instant(args.start)
        if start >= end:
            parser.error("--start must be before --end")

    tables = tuple(args.tables.split(",")) if args.tables else SOURCE_TABLES
    unknown = set(tables) - set(SOURCE_TABLES)
    if unknown:
        parser.error(f"unknown tables: {', '.join(sorted(unknown))}")

    try:
        results = run(
            tables=tables,
            start=start,
            end=end,
            run_id=args.run_id,
            bucket=args.bucket,
            lookback=timedelta(minutes=args.lookback_minutes),
            incremental=args.incremental,
            history_days=args.history_days,
            dry_run=args.dry_run,
        )
    except (psycopg.Error, ValueError, OSError) as exc:
        print(f"extract failed: {exc}", file=sys.stderr)
        return 1

    mode = "incremental" if args.incremental else f"{start.isoformat()} -> {end.isoformat()}"
    print(f"oltp_extract ({mode}, {'dry run' if args.dry_run else 'written'}):")
    total = 0
    for result in results:
        total += result.row_count
        print(f"  {result.table:<12} {result.row_count:>8,} rows  {result.object_uri}")
    print(f"  {'TOTAL':<12} {total:>8,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
