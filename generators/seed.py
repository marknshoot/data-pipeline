"""Seed the ``shop`` OLTP database with realistic Faker data.

Creates the reference tables (categories, sellers, users, products) and, by
default, a few thousand historical orders spread over the last 30 days. Order
timestamps follow the same traffic curve as the live generator, so a backfill
over seeded history looks like a backfill over real traffic.

Usage:
    uv run python -m generators.seed --reset
    uv run python -m generators.seed --users 1000 --sellers 50 --products 500 --orders 500
"""

from __future__ import annotations

import argparse
import random
import re
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

import psycopg
from faker import Faker

from . import db
from .order_factory import OrderFactory, ProductRef, is_terminal, next_status, payment_status_for
from .patterns import is_flash_sale, sample_timestamp
from .reference import ADJECTIVES, CATEGORY_TREE, CITIES, LEAF_CATALOG

CENTS = Decimal("0.01")
TRUNCATE_ORDER = (
    "payments",
    "order_items",
    "orders",
    "products",
    "users",
    "sellers",
    "categories",
)


def _slug_email(name: str, index: int) -> str:
    """Deterministic, guaranteed-unique email (Faker's unique pool can exhaust)."""
    handle = re.sub(r"[^a-z0-9]+", ".", name.lower()).strip(".") or "user"
    return f"{handle}.{index}@example.com"


def _round_idr(value: float) -> Decimal:
    return (Decimal(str(value)) / 1000).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * 1000


def reset(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE " + ", ".join(TRUNCATE_ORDER) + " RESTART IDENTITY CASCADE")
    conn.commit()


def _insert_categories(conn: psycopg.Connection) -> dict[str, int]:
    ids: dict[str, int] = {}
    with conn.cursor() as cur:
        for name, slug, parent_slug in CATEGORY_TREE:
            cur.execute(
                """
                INSERT INTO categories (name, slug, parent_category_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
                RETURNING category_id
                """,
                (name, slug, ids.get(parent_slug)),
            )
            ids[slug] = cur.fetchone()[0]
    conn.commit()
    return ids


def _insert_sellers(conn: psycopg.Connection, faker: Faker, rng: random.Random, count: int) -> None:
    now = datetime.now(UTC)
    rows = []
    for i in range(1, count + 1):
        name = faker.company()
        created = now - timedelta(seconds=rng.uniform(0, 730 * 86400))
        rows.append(
            (
                name,
                _slug_email(name, i),
                rng.choice(CITIES),
                "ID",
                round(rng.uniform(3.0, 5.0), 2),
                created,
                created,
            )
        )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO sellers (name, email, city, country, rating, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()


def _insert_users(conn: psycopg.Connection, faker: Faker, rng: random.Random, count: int) -> None:
    now = datetime.now(UTC)
    rows = []
    for i in range(1, count + 1):
        name = faker.name()
        created = now - timedelta(seconds=rng.uniform(0, 730 * 86400))
        rows.append(
            (
                _slug_email(name, i),
                name,
                faker.phone_number(),
                rng.choice(CITIES),
                "ID",
                created,
                created,
            )
        )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO users (email, full_name, phone, city, country, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()


def _insert_products(
    conn: psycopg.Connection,
    rng: random.Random,
    count: int,
    *,
    seller_ids: list[int],
    category_ids: dict[str, int],
) -> None:
    now = datetime.now(UTC)
    leaves = list(LEAF_CATALOG.items())
    rows = []
    for i in range(1, count + 1):
        slug, spec = rng.choice(leaves)
        low, high = spec["price"]
        price = _round_idr(rng.uniform(low, high))
        cost = (price * Decimal(str(rng.uniform(0.45, 0.75)))).quantize(CENTS)
        created = now - timedelta(seconds=rng.uniform(0, 730 * 86400))
        rows.append(
            (
                rng.choice(seller_ids),
                category_ids[slug],
                f"{rng.choice(ADJECTIVES)} {rng.choice(spec['nouns'])}",
                f"SKU-{i:06d}",
                price,
                cost,
                rng.randint(0, 400),
                rng.random() < 0.95,
                created,
                created,
            )
        )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO products
                (seller_id, category_id, name, sku, price, cost, stock, is_active,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()


def _load_users(conn: psycopg.Connection) -> list[tuple[int, str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, city FROM users ORDER BY user_id")
        return cur.fetchall()


def _load_products(conn: psycopg.Connection) -> list[ProductRef]:
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, price FROM products WHERE is_active ORDER BY product_id")
        return [ProductRef(product_id=pid, price=price) for pid, price in cur.fetchall()]


def _insert_historical_orders(
    conn: psycopg.Connection,
    rng: random.Random,
    *,
    count: int,
    days: int,
    users: list[tuple[int, str]],
    products: list[ProductRef],
) -> None:
    if count == 0 or not products or not users:
        return

    factory = OrderFactory(rng)
    now = datetime.now(UTC)
    start = now - timedelta(days=days)

    with conn.cursor() as cur:
        for _ in range(count):
            user_id, city = rng.choice(users)
            created = sample_timestamp(rng, start, now)
            draft = factory.create_order(
                user_id=user_id,
                shipping_city=city,
                products=products,
                flash_sale=is_flash_sale(created),
            )

            # Walk the lifecycle, leaving some orders deliberately in flight.
            status = draft.status
            updated = created
            while not is_terminal(status):
                candidate = next_status(rng, status)
                if candidate is None:
                    break
                status = candidate
                updated += timedelta(minutes=rng.expovariate(1 / 90.0))
                if rng.random() < 0.20:
                    break

            cur.execute(
                """
                INSERT INTO orders
                    (user_id, status, currency, shipping_city, total_amount,
                     created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING order_id
                """,
                (
                    draft.user_id,
                    status,
                    draft.currency,
                    draft.shipping_city,
                    draft.total_amount,
                    created,
                    max(updated, created),
                ),
            )
            order_id = cur.fetchone()[0]

            cur.executemany(
                """
                INSERT INTO order_items
                    (order_id, product_id, quantity, unit_price, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (order_id, item.product_id, item.quantity, item.unit_price, created, created)
                    for item in draft.items
                ],
            )
            cur.execute(
                """
                INSERT INTO payments
                    (order_id, method, amount, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    draft.payment_method,
                    draft.total_amount,
                    payment_status_for(status, draft.payment_status),
                    created,
                    max(updated, created),
                ),
            )
    conn.commit()


def _is_empty(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM users)")
        return not cur.fetchone()[0]


def seed(
    *,
    users: int = 10_000,
    sellers: int = 500,
    products: int = 5_000,
    orders: int = 5_000,
    days: int = 30,
    reset_first: bool = False,
    seed_value: int = 42,
    conn: psycopg.Connection | None = None,
) -> dict[str, int]:
    owns_conn = conn is None
    conn = conn or db.connect()
    try:
        if reset_first:
            reset(conn)
        elif not _is_empty(conn):
            raise RuntimeError("shop database already has users; re-run with --reset to wipe it")

        rng = random.Random(seed_value)
        faker = Faker("id_ID")
        Faker.seed(seed_value)

        category_ids = _insert_categories(conn)
        _insert_sellers(conn, faker, rng, sellers)
        _insert_users(conn, faker, rng, users)

        with conn.cursor() as cur:
            cur.execute("SELECT seller_id FROM sellers ORDER BY seller_id")
            seller_ids = [row[0] for row in cur.fetchall()]
        _insert_products(conn, rng, products, seller_ids=seller_ids, category_ids=category_ids)

        _insert_historical_orders(
            conn,
            rng,
            count=orders,
            days=days,
            users=_load_users(conn),
            products=_load_products(conn),
        )
        return _counts(conn)
    finally:
        if owns_conn:
            conn.close()


def _counts(conn: psycopg.Connection) -> dict[str, int]:
    tables = ("categories", "sellers", "users", "products", "orders", "order_items", "payments")
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed table names
            counts[table] = cur.fetchone()[0]
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed the shop OLTP database")
    parser.add_argument("--users", type=int, default=10_000)
    parser.add_argument("--sellers", type=int, default=500)
    parser.add_argument("--products", type=int, default=5_000)
    parser.add_argument("--orders", type=int, default=5_000, help="historical orders")
    parser.add_argument("--days", type=int, default=30, help="history window for orders")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed")
    parser.add_argument("--reset", action="store_true", help="truncate existing data first")
    args = parser.parse_args(argv)

    try:
        counts = seed(
            users=args.users,
            sellers=args.sellers,
            products=args.products,
            orders=args.orders,
            days=args.days,
            reset_first=args.reset,
            seed_value=args.seed,
        )
    except (RuntimeError, psycopg.Error) as exc:
        print(f"seed failed: {exc}")
        return 1

    print("seeded shop database:")
    for table, count in counts.items():
        print(f"  {table:<12} {count:>8,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
