"""Load the raw warehouse layer: lake Parquet -> ClickHouse `raw.*`.

ClickHouse reads the Parquet directly with the `s3()` table function, so nothing
is copied through Python. Credentials are passed as server-side query parameters
rather than interpolated into the SQL text, which keeps them out of
`system.query_log`.

Usage:
    uv run python -m warehouse.load_raw --truncate
    uv run python -m warehouse.load_raw --tables orders,payments
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from clickhouse_connect.driver.client import Client

from generators.config import Settings, get_settings
from ingestion import lake

from .clickhouse import get_client

# Column order must match the Parquet files written by ingestion/oltp_extract.py.
RAW_COLUMNS: dict[str, tuple[str, ...]] = {
    "sellers": (
        "seller_id",
        "name",
        "email",
        "city",
        "country",
        "rating",
        "created_at",
        "updated_at",
    ),
    "categories": (
        "category_id",
        "name",
        "slug",
        "parent_category_id",
        "created_at",
        "updated_at",
    ),
    "users": (
        "user_id",
        "email",
        "full_name",
        "phone",
        "city",
        "country",
        "created_at",
        "updated_at",
    ),
    "products": (
        "product_id",
        "seller_id",
        "category_id",
        "name",
        "sku",
        "price",
        "cost",
        "stock",
        "is_active",
        "created_at",
        "updated_at",
    ),
    "orders": (
        "order_id",
        "user_id",
        "status",
        "currency",
        "shipping_city",
        "total_amount",
        "created_at",
        "updated_at",
    ),
    "order_items": (
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "created_at",
        "updated_at",
    ),
    "payments": (
        "payment_id",
        "order_id",
        "method",
        "amount",
        "status",
        "created_at",
        "updated_at",
    ),
}


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows: int
    skipped: bool = False
    reason: str = ""


def lake_glob(table: str, settings: Settings) -> str:
    """URL ClickHouse's s3() uses: it must be the in-network endpoint, not localhost."""
    base = settings.s3_endpoint_internal.rstrip("/")
    return f"{base}/{settings.s3_bucket}/raw/oltp/{table}/**/*.parquet"


def row_count(client: Client, table: str) -> int:
    """Row count after ReplacingMergeTree dedup (FINAL is required for exactness)."""
    return int(client.query(f"SELECT count() FROM raw.{table} FINAL").first_row[0])  # noqa: S608


def load_table(
    client: Client,
    table: str,
    settings: Settings,
    *,
    truncate: bool = False,
) -> LoadResult:
    columns = RAW_COLUMNS[table]
    if not lake.list_keys(f"raw/oltp/{table}", bucket=settings.s3_bucket):
        return LoadResult(table, 0, skipped=True, reason="no objects in the lake")

    if truncate:
        client.command(f"TRUNCATE TABLE IF EXISTS raw.{table}")  # noqa: S608

    column_list = ", ".join(columns)
    sql = (
        f"INSERT INTO raw.{table} ({column_list}) "  # noqa: S608
        f"SELECT {column_list} "
        "FROM s3({url:String}, {key:String}, {secret:String}, 'Parquet')"
    )
    client.command(
        sql,
        parameters={
            "url": lake_glob(table, settings),
            "key": settings.s3_access_key,
            "secret": settings.s3_secret_key,
        },
    )
    return LoadResult(table, row_count(client, table))


def load_all(
    *,
    tables: tuple[str, ...] = tuple(RAW_COLUMNS),
    truncate: bool = False,
    settings: Settings | None = None,
    client: Client | None = None,
) -> list[LoadResult]:
    settings = settings or get_settings()
    client = client or get_client(settings)
    return [load_table(client, table, settings, truncate=truncate) for table in tables]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load lake Parquet into ClickHouse raw tables")
    parser.add_argument("--tables", help="comma-separated subset (default: all)")
    parser.add_argument("--truncate", action="store_true", help="empty each table before loading")
    args = parser.parse_args(argv)

    tables = tuple(args.tables.split(",")) if args.tables else tuple(RAW_COLUMNS)
    unknown = set(tables) - set(RAW_COLUMNS)
    if unknown:
        parser.error(f"unknown tables: {', '.join(sorted(unknown))}")

    try:
        results = load_all(tables=tables, truncate=args.truncate)
    except Exception as exc:  # noqa: BLE001 - surface any client/HTTP error to the CLI
        print(f"load failed: {exc}", file=sys.stderr)
        return 1

    print(f"loaded raw layer ({'truncate+load' if args.truncate else 'append'}):")
    total = 0
    for result in results:
        if result.skipped:
            print(f"  {result.table:<12} {'-':>8}  skipped ({result.reason})")
            continue
        total += result.rows
        print(f"  {result.table:<12} {result.rows:>8,}  rows")
    print(f"  {'TOTAL':<12} {total:>8,}  rows (FINAL, i.e. after dedup)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
