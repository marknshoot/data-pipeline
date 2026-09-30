"""Tests for the OLTP extractor: schema mapping, object layout, idempotency."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

from generators import db
from ingestion import lake
from ingestion.oltp_extract import (
    build_table,
    columns_to_schema,
    default_run_id,
    run,
)


def _column(name: str, data_type: str, nullable: str = "NO", precision=None, scale=None) -> dict:
    return {
        "column_name": name,
        "data_type": data_type,
        "is_nullable": nullable,
        "numeric_precision": precision,
        "numeric_scale": scale,
    }


def test_arrow_type_maps_common_postgres_types() -> None:
    from ingestion.oltp_extract import arrow_type

    assert arrow_type("bigint", None, None) == pa.int64()
    assert arrow_type("integer", None, None) == pa.int32()
    assert arrow_type("boolean", None, None) == pa.bool_()
    assert arrow_type("text", None, None) == pa.string()
    assert arrow_type("character", None, None) == pa.string()
    assert arrow_type("timestamp with time zone", None, None) == pa.timestamp("us", tz="UTC")
    assert arrow_type("numeric", 14, 2) == pa.decimal128(14, 2)


def test_arrow_type_rejects_unknown_types() -> None:
    from ingestion.oltp_extract import arrow_type

    with pytest.raises(ValueError, match="no Arrow mapping"):
        arrow_type("tsvector", None, None)


def test_columns_to_schema_preserves_order_nullability_and_scale() -> None:
    schema = columns_to_schema(
        [
            _column("order_id", "bigint"),
            _column("shipping_city", "text", nullable="YES"),
            _column("total_amount", "numeric", precision=14, scale=2),
            _column("created_at", "timestamp with time zone"),
        ]
    )
    assert schema.names == ["order_id", "shipping_city", "total_amount", "created_at"]
    assert schema.field("order_id").nullable is False
    assert schema.field("shipping_city").nullable is True
    assert schema.field("total_amount").type == pa.decimal128(14, 2)


def test_build_table_round_trips_values() -> None:
    schema = columns_to_schema(
        [
            _column("order_id", "bigint"),
            _column("total_amount", "numeric", precision=14, scale=2),
            _column("created_at", "timestamp with time zone"),
        ]
    )
    rows = [
        {
            "order_id": 1,
            "total_amount": Decimal("1234.50"),
            "created_at": datetime(2026, 9, 27, 12, 0, tzinfo=UTC),
        }
    ]
    table = build_table(rows, schema)
    assert table.num_rows == 1
    assert table.column("total_amount")[0].as_py() == Decimal("1234.50")
    assert table.column("created_at")[0].as_py() == datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def test_default_run_id_is_deterministic() -> None:
    start = datetime(2026, 9, 27, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 28, 0, 0, tzinfo=UTC)
    assert default_run_id(start, end) == "20260927T000000-20260928T000000"
    assert default_run_id(start, end) == default_run_id(start, end)


def test_object_layout() -> None:
    key = lake.raw_oltp_key("orders", datetime(2026, 9, 28, tzinfo=UTC).date(), "run123")
    assert key == "raw/oltp/orders/dt=2026-09-28/run123.parquet"
    assert (
        lake.partition_date_for(datetime(2026, 9, 28, 23, 30, tzinfo=UTC))
        == datetime(2026, 9, 28, tzinfo=UTC).date()
    )


def test_parse_endpoint_accepts_urls_and_bare_hosts() -> None:
    assert lake.parse_endpoint("http://localhost:8333") == ("localhost:8333", "http")
    assert lake.parse_endpoint("https://s3.example.com") == ("s3.example.com", "https")
    assert lake.parse_endpoint("localhost:8333") == ("localhost:8333", "http")
    with pytest.raises(ValueError, match="unsupported"):
        lake.parse_endpoint("ftp://host:21")


# ------------------------------------------------------------------ integration


def _connect_or_skip():
    try:
        conn = db.connect()
        conn.execute("SELECT 1 FROM pipeline_meta.extract_state LIMIT 1")
        return conn
    except Exception as exc:  # pragma: no cover - depends on local services
        pytest.skip(f"Postgres/pipeline_meta unavailable: {exc}")


def _parquet_rows(root) -> tuple[int, int]:
    files = sorted(root.rglob("*.parquet"))
    total = sum(pq.read_table(path).num_rows for path in files)
    return len(files), total


def test_backfill_is_idempotent(tmp_path) -> None:
    """Running the same 3-day backfill twice must not change the lake contents."""
    conn = _connect_or_skip()
    try:
        end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(days=3)
        filesystem = LocalFileSystem()
        tables = ("orders", "order_items", "payments", "users", "products")

        def backfill() -> None:
            run(
                tables=tables,
                start=start,
                end=end,
                run_id=default_run_id(start, end),
                bucket=str(tmp_path),
                filesystem=filesystem,
                conn=conn,
            )

        backfill()
        files_first, rows_first = _parquet_rows(tmp_path)
        backfill()
        files_second, rows_second = _parquet_rows(tmp_path)

        assert rows_first > 0
        assert files_first > 0
        assert (files_second, rows_second) == (files_first, rows_first)
    finally:
        conn.close()


def test_empty_window_writes_no_object(tmp_path) -> None:
    """An interval with no changes must not leave an empty Parquet file behind."""
    conn = _connect_or_skip()
    try:
        results = run(
            tables=("orders",),
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 2, tzinfo=UTC),
            run_id="empty-window",
            bucket=str(tmp_path),
            filesystem=LocalFileSystem(),
            conn=conn,
        )

        assert results[0].row_count == 0
        assert results[0].written is False
        assert list(tmp_path.rglob("*.parquet")) == []
    finally:
        conn.close()
