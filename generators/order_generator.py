"""Continuously mutate the OLTP database: new orders, status changes, price moves.

The write path is behind ``OrderSink`` so the pacing/lifecycle loop can be tested
with an in-memory sink, while ``PostgresSink`` does the real thing. The sink shares
the loop's ``random.Random`` so a seed makes the whole run reproducible.

Usage:
    uv run python -m generators.order_generator --rate 10 --mutations 3
    uv run python -m generators.order_generator --dry-run --duration 5 --tick 1
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

import psycopg

from . import db
from .order_factory import (
    TERMINAL_STATUSES,
    OrderDraft,
    OrderFactory,
    ProductRef,
    drift_price,
    next_status,
    payment_status_for,
)
from .patterns import is_flash_sale, sample_poisson, traffic_multiplier
from .reference import CITIES

MUTATION_KINDS = ("status", "price", "city")
MUTATION_WEIGHTS = (0.60, 0.25, 0.15)


class OrderSink(Protocol):
    """Where generated orders and mutations go."""

    def insert_order(self, draft: OrderDraft, created_at: datetime) -> int: ...

    def advance_random_order(self, now: datetime) -> bool: ...

    def drift_random_price(self, now: datetime) -> bool: ...

    def move_random_user(self, now: datetime) -> bool: ...

    def commit(self) -> None: ...


@dataclass
class RunStats:
    ticks: int = 0
    orders: int = 0
    status_changes: int = 0
    price_changes: int = 0
    city_changes: int = 0


class NullSink:
    """Dry-run sink: exercises the flow without touching Postgres."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def insert_order(self, draft: OrderDraft, created_at: datetime) -> int:
        return 0

    def advance_random_order(self, now: datetime) -> bool:
        return True

    def drift_random_price(self, now: datetime) -> bool:
        return True

    def move_random_user(self, now: datetime) -> bool:
        return True

    def commit(self) -> None:
        return None


class PostgresSink:
    def __init__(self, conn: psycopg.Connection, rng: random.Random) -> None:
        self.conn = conn
        self.rng = rng

    def insert_order(self, draft: OrderDraft, created_at: datetime) -> int:
        with self.conn.cursor() as cur:
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
                    draft.status,
                    draft.currency,
                    draft.shipping_city,
                    draft.total_amount,
                    created_at,
                    created_at,
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
                    (
                        order_id,
                        item.product_id,
                        item.quantity,
                        item.unit_price,
                        created_at,
                        created_at,
                    )
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
                    draft.payment_status,
                    created_at,
                    created_at,
                ),
            )
            # Selling decrements stock; the updated_at trigger makes this visible to
            # the incremental extractor, which is realistic product churn.
            cur.executemany(
                "UPDATE products SET stock = GREATEST(stock - %s, 0) WHERE product_id = %s",
                [(item.quantity, item.product_id) for item in draft.items],
            )
        return order_id

    def advance_random_order(self, now: datetime) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT order_id, status FROM orders WHERE status <> ALL(%s) "
                "ORDER BY random() LIMIT 1",
                (list(TERMINAL_STATUSES),),
            )
            row = cur.fetchone()
            if row is None:
                return False
            order_id, status = row
            candidate = next_status(self.rng, status)
            if candidate is None:
                return False
            cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (candidate, order_id))
            cur.execute(
                "UPDATE payments SET status = %s WHERE order_id = %s",
                (payment_status_for(candidate, "pending"), order_id),
            )
        return True

    def drift_random_price(self, now: datetime) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if row is None:
                return False
            product_id, price = row
            cur.execute(
                "UPDATE products SET price = %s WHERE product_id = %s",
                (drift_price(self.rng, price), product_id),
            )
        return True

    def move_random_user(self, now: datetime) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT user_id, city FROM users ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if row is None:
                return False
            user_id, city = row
            new_city = self.rng.choice([c for c in CITIES if c != city] or list(CITIES))
            cur.execute("UPDATE users SET city = %s WHERE user_id = %s", (new_city, user_id))
        return True

    def commit(self) -> None:
        self.conn.commit()


def run(
    sink: OrderSink,
    factory: OrderFactory,
    rng: random.Random,
    *,
    users: list[tuple[int, str]],
    products: list[ProductRef],
    orders_per_second: float,
    mutations_per_second: float,
    duration: float | None,
    tick_seconds: float = 1.0,
    now_fn=datetime.now,
    sleep=time.sleep,
) -> RunStats:
    """Pump orders and mutations until ``duration`` elapses (or forever if ``None``)."""
    stats = RunStats()
    start = now_fn(UTC)
    while True:
        now = now_fn(UTC)
        if duration is not None and (now - start).total_seconds() >= duration:
            break

        multiplier = traffic_multiplier(now)
        for _ in range(sample_poisson(rng, orders_per_second * multiplier * tick_seconds)):
            user_id, city = rng.choice(users)
            draft = factory.create_order(
                user_id=user_id,
                shipping_city=city,
                products=products,
                flash_sale=is_flash_sale(now),
            )
            sink.insert_order(draft, now)
            stats.orders += 1

        for _ in range(sample_poisson(rng, mutations_per_second * tick_seconds)):
            kind = rng.choices(MUTATION_KINDS, weights=MUTATION_WEIGHTS, k=1)[0]
            changed = {
                "status": sink.advance_random_order,
                "price": sink.drift_random_price,
                "city": sink.move_random_user,
            }[kind](now)
            if changed:
                if kind == "status":
                    stats.status_changes += 1
                elif kind == "price":
                    stats.price_changes += 1
                else:
                    stats.city_changes += 1

        sink.commit()
        stats.ticks += 1
        sleep(tick_seconds)

    return stats


def _load_pools(conn: psycopg.Connection) -> tuple[list[tuple[int, str]], list[ProductRef]]:
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, city FROM users")
        users = cur.fetchall()
        cur.execute("SELECT product_id, price FROM products WHERE is_active")
        products = [ProductRef(product_id=pid, price=price) for pid, price in cur.fetchall()]
    return users, products


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate orders and OLTP mutations")
    parser.add_argument("--rate", type=float, default=8.0, help="orders/s at average traffic")
    parser.add_argument("--mutations", type=float, default=3.0, help="mutations/s")
    parser.add_argument(
        "--duration", type=float, default=None, help="seconds to run (default: forever)"
    )
    parser.add_argument("--tick", type=float, default=1.0, help="loop interval in seconds")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed (default: random)")
    parser.add_argument("--dry-run", action="store_true", help="do not write to Postgres")
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    factory = OrderFactory(rng)
    conn = None
    try:
        if args.dry_run:
            users = [(i, rng.choice(CITIES)) for i in range(1, 1001)]
            products = [ProductRef(product_id=i, price=Decimal("50000")) for i in range(1, 501)]
            sink: OrderSink = NullSink(rng)
        else:
            conn = db.connect()
            users, products = _load_pools(conn)
            if not users or not products:
                print("shop database is empty - run `make seed` first", file=sys.stderr)
                return 1
            sink = PostgresSink(conn, rng)

        print(
            f"order_generator: rate={args.rate}/s mutations={args.mutations}/s "
            f"users={len(users)} products={len(products)}"
            f"{' (dry run)' if args.dry_run else ''}",
            flush=True,
        )
        stats = run(
            sink,
            factory,
            rng,
            users=users,
            products=products,
            orders_per_second=args.rate,
            mutations_per_second=args.mutations,
            duration=args.duration,
            tick_seconds=args.tick,
        )
        print(
            f"done: ticks={stats.ticks} orders={stats.orders} "
            f"status={stats.status_changes} price={stats.price_changes} city={stats.city_changes}"
        )
        return 0
    except KeyboardInterrupt:
        print("\ninterrupted")
        return 0
    except psycopg.Error as exc:
        print(f"database error: {exc}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
