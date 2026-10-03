"""Seed a minimal, internally consistent dataset into the raw warehouse layer.

CI cannot run the real pipeline: it needs Postgres, Kafka, SeaweedFS and a JVM, which
is far too much for a GitHub runner. This loads a small fixture directly into `raw.*`
so `dbt build` exercises every model and every data test on data that actually
satisfies their invariants.

The fixture is small but not arbitrary, because a fixture that cannot fail a test
proves nothing:

* **Two months of orders**, with one user ordering in both, so cohort retention has a
  real `month_number = 1` row rather than only the `1.0` baseline.
* **Order totals are computed from their line items**, so
  `assert_order_total_matches_items` and `assert_fact_revenue_reconciles` have
  something true to check.
* **Non-revenue statuses are present** (pending, cancelled), so the revenue filters in
  the marts are exercised rather than trivially passing.
* **A complete funnel** (5 sessions viewing, 3 carting, 2 checking out, 1 purchasing),
  so the monotonicity test has a shape to validate.
* **Both schema versions** in the events, so the clean layer's merge is represented.

Usage:
    python -m ci.seed_sample_data
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from decimal import Decimal

import clickhouse_connect
from clickhouse_connect.driver.client import Client

TS = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


def _at(month: int, day: int, hour: int = 9) -> datetime:
    return datetime(2026, month, day, hour, tzinfo=UTC)


# (category_id, name, slug, parent_category_id, created_at, updated_at)
CATEGORIES = [
    (1, "Electronics", "electronics", None, TS, TS),
    (2, "Phones", "phones", 1, TS, TS),
    (3, "Books", "books", None, TS, TS),
]

# (seller_id, name, email, city, country, rating, created_at, updated_at)
SELLERS = [
    (1, "Toko Satu", "satu@example.com", "Jakarta", "ID", Decimal("4.80"), TS, TS),
    (2, "Toko Dua", "dua@example.com", "Bandung", "ID", Decimal("4.50"), TS, TS),
]

# (product_id, seller_id, category_id, name, sku, price, cost, stock, is_active, ...)
PRODUCTS = [
    (
        1,
        1,
        1,
        "Laptop 14",
        "SKU-1",
        Decimal("15000000.00"),
        Decimal("12000000.00"),
        10,
        True,
        TS,
        TS,
    ),
    (
        2,
        1,
        2,
        "Phone X",
        "SKU-2",
        Decimal("8000000.00"),
        Decimal("6500000.00"),
        25,
        True,
        TS,
        TS,
    ),
    (3, 2, 3, "Novel", "SKU-3", Decimal("150000.00"), Decimal("90000.00"), 100, True, TS, TS),
    (4, 2, 3, "Textbook", "SKU-4", Decimal("300000.00"), Decimal("200000.00"), 50, True, TS, TS),
]

# (user_id, email, full_name, phone, city, country, created_at, updated_at)
USERS = [
    (1, "andi@example.com", "Andi", "+62811000001", "Jakarta", "ID", _at(9, 1), _at(9, 1)),
    (2, "budi@example.com", "Budi", "+62811000002", "Bandung", "ID", _at(9, 1), _at(9, 1)),
    (3, "citra@example.com", "Citra", "+62811000003", "Surabaya", "ID", _at(10, 1), _at(10, 1)),
    (4, "dewi@example.com", "Dewi", "+62811000004", "Medan", "ID", _at(9, 2), _at(9, 2)),
]

# (order_id, user_id, status, currency, shipping_city, created_at, items)
# items: (product_id, quantity, unit_price). The order's total_amount is derived from
# these, never written by hand -- that is the invariant the tests check.
ORDERS = [
    (1001, 1, "delivered", "IDR", "Jakarta", _at(9, 5), [(1, 1, Decimal("15000000.00"))]),
    (1002, 2, "paid", "IDR", "Bandung", _at(9, 6), [(2, 1, Decimal("8000000.00"))]),
    (1003, 4, "shipped", "IDR", "Medan", _at(9, 7), [(3, 2, Decimal("150000.00"))]),
    # User 1 orders again a month later: the cohort's month_number = 1 row.
    (1004, 1, "delivered", "IDR", "Jakarta", _at(10, 5), [(4, 1, Decimal("300000.00"))]),
    (1005, 3, "paid", "IDR", "Surabaya", _at(10, 6), [(2, 1, Decimal("8000000.00"))]),
    # Non-revenue orders: the marts' revenue filters must actually exclude these.
    (1006, 4, "cancelled", "IDR", "Medan", _at(10, 7), [(1, 1, Decimal("15000000.00"))]),
    (1007, 2, "pending", "IDR", "Bandung", _at(10, 8), [(3, 3, Decimal("150000.00"))]),
]

# (payment_id, order_id, method, status) -- amount comes from the order.
PAYMENTS = [
    (5001, 1001, "card", "captured"),
    (5002, 1002, "va", "captured"),
    (5003, 1003, "ewallet", "captured"),
    (5004, 1004, "bank_transfer", "captured"),
    (5005, 1005, "cod", "pending"),
    (5006, 1006, "card", "failed"),
    (5007, 1007, "va", "authorized"),
]

# (event_id, schema_version, event_type, user_id, session_id, product_id, event_time)
# A funnel that narrows at every step, plus two view-only sessions.
EVENTS = [
    ("evt-1", 1, "page_view", 1, "s1", 1, _at(10, 2, 10)),
    ("evt-2", 1, "add_to_cart", 1, "s1", 1, _at(10, 2, 10)),
    ("evt-3", 1, "checkout", 1, "s1", 1, _at(10, 2, 11)),
    ("evt-4", 2, "purchase", 1, "s1", 1, _at(10, 2, 11)),
    ("evt-5", 2, "page_view", 2, "s2", 2, _at(10, 2, 12)),
    ("evt-6", 2, "add_to_cart", 2, "s2", 2, _at(10, 2, 12)),
    ("evt-7", 2, "checkout", 2, "s2", 2, _at(10, 2, 13)),
    ("evt-8", 1, "page_view", 3, "s3", 3, _at(10, 2, 14)),
    ("evt-9", 1, "add_to_cart", 3, "s3", 3, _at(10, 2, 14)),
    ("evt-10", 1, "page_view", 4, "s4", 4, _at(10, 2, 15)),
    ("evt-11", 2, "page_view", 1, "s5", 1, _at(10, 2, 16)),
]

EVENT_CITIES = {1: "Jakarta", 2: "Bandung", 3: "Surabaya", 4: "Medan"}


def connect() -> Client:
    """Connect using only the ClickHouse variables, so CI needs no other config."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def order_rows() -> tuple[list, list]:
    """Orders with totals derived from their line items, and the line items."""
    orders, items = [], []
    item_id = 9000
    for order_id, user_id, status, currency, city, created_at, lines in ORDERS:
        total = Decimal("0.00")
        for product_id, quantity, unit_price in lines:
            item_id += 1
            line_amount = (unit_price * quantity).quantize(Decimal("0.01"))
            total += line_amount
            items.append(
                (
                    item_id,
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    created_at,
                    created_at,
                )
            )
        orders.append((order_id, user_id, status, currency, city, total, created_at, created_at))
    return orders, items


def payment_rows() -> list:
    amounts = {order_id: total for order_id, _, _, _, _, total, _, _ in order_rows()[0]}
    return [
        (payment_id, order_id, method, amounts[order_id], status, TS, TS)
        for payment_id, order_id, method, status in PAYMENTS
    ]


def event_rows() -> list:
    return [
        (
            event_id,
            schema_version,
            event_type,
            user_id,
            session_id,
            product_id,
            event_time,
            EVENT_CITIES[user_id],
            # v1 sent `device`; the clean layer resolves both into device_type.
            "android" if schema_version == 1 else "ios",
            None if schema_version == 1 else "3.2",
            event_time,
        )
        for (
            event_id,
            schema_version,
            event_type,
            user_id,
            session_id,
            product_id,
            event_time,
        ) in EVENTS
    ]


def seed(client: Client, *, log=print) -> dict[str, int]:
    """Truncate and reload the raw layer with the fixture. Idempotent."""
    orders, items = order_rows()
    payloads: dict[str, tuple[list[list], list[str]]] = {
        "categories": (
            [list(row) for row in CATEGORIES],
            [
                "category_id",
                "name",
                "slug",
                "parent_category_id",
                "created_at",
                "updated_at",
            ],
        ),
        "sellers": (
            [list(row) for row in SELLERS],
            ["seller_id", "name", "email", "city", "country", "rating", "created_at", "updated_at"],
        ),
        "products": (
            [list(row) for row in PRODUCTS],
            [
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
            ],
        ),
        "users": (
            [list(row) for row in USERS],
            [
                "user_id",
                "email",
                "full_name",
                "phone",
                "city",
                "country",
                "created_at",
                "updated_at",
            ],
        ),
        "orders": (
            [list(row) for row in orders],
            [
                "order_id",
                "user_id",
                "status",
                "currency",
                "shipping_city",
                "total_amount",
                "created_at",
                "updated_at",
            ],
        ),
        "order_items": (
            [list(row) for row in items],
            [
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
                "created_at",
                "updated_at",
            ],
        ),
        "payments": (
            [list(row) for row in payment_rows()],
            ["payment_id", "order_id", "method", "amount", "status", "created_at", "updated_at"],
        ),
        "events": (
            [list(row) for row in event_rows()],
            [
                "event_id",
                "schema_version",
                "event_type",
                "user_id",
                "session_id",
                "product_id",
                "event_time",
                "city",
                "device_type",
                "platform_version",
                "consumed_at",
            ],
        ),
    }

    counts: dict[str, int] = {}
    for table, (rows, columns) in payloads.items():
        client.command(f"TRUNCATE TABLE IF EXISTS raw.{table}")
        client.insert(f"raw.{table}", rows, column_names=columns)
        counts[table] = len(rows)
        log(f"  raw.{table:<12} {len(rows):>3} row(s)")
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed the raw warehouse layer for CI")
    args = parser.parse_args(argv)
    del args

    client = connect()
    print(f"seeding raw layer on {client.server_version}")
    counts = seed(client)
    print(f"seeded {sum(counts.values())} rows across {len(counts)} tables")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
