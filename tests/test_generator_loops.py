"""Tests for the generator loops, using fake clocks and in-memory sinks."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from generators.event_factory import ClickstreamFactory
from generators.event_generator import pump
from generators.order_factory import OrderDraft, OrderFactory, ProductRef
from generators.order_generator import run

START = datetime(2026, 3, 10, 13, 0, tzinfo=UTC)  # 20:00 in Jakarta => evening peak
USER_IDS = list(range(1, 101))
PRODUCT_IDS = list(range(1, 51))
PRODUCTS = [ProductRef(product_id=i, price=Decimal("75000.00")) for i in PRODUCT_IDS]
# A user id -> city pool, as loaded from Postgres.
USERS = [(i, "Jakarta") for i in USER_IDS]


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self, tz=None) -> datetime:  # noqa: ARG002 - mirrors datetime.now(tz)
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class RecordingSink:
    def __init__(self) -> None:
        self.orders: list[OrderDraft] = []
        self.status = self.price = self.city = 0

    def insert_order(self, draft: OrderDraft, created_at: datetime) -> int:
        self.orders.append(draft)
        return len(self.orders)

    def advance_random_order(self, now: datetime) -> bool:
        self.status += 1
        return True

    def drift_random_price(self, now: datetime) -> bool:
        self.price += 1
        return True

    def move_random_user(self, now: datetime) -> bool:
        self.city += 1
        return True

    def commit(self) -> None:
        return None


def test_order_run_pumps_orders_and_mutations() -> None:
    clock = FakeClock(START)
    sink = RecordingSink()
    rng = random.Random(1)
    stats = run(
        sink,
        OrderFactory(rng),
        rng,
        users=USERS,
        products=PRODUCTS,
        orders_per_second=10,
        mutations_per_second=2,
        duration=3,
        tick_seconds=1,
        now_fn=clock,
        sleep=clock.sleep,
    )

    assert stats.ticks == 3
    assert stats.orders == len(sink.orders) > 0
    assert stats.status_changes + stats.price_changes + stats.city_changes > 0
    assert sink.status + sink.price + sink.city == (
        stats.status_changes + stats.price_changes + stats.city_changes
    )


def test_order_run_duration_zero_does_nothing() -> None:
    clock = FakeClock(START)
    sink = RecordingSink()
    rng = random.Random(2)
    stats = run(
        sink,
        OrderFactory(rng),
        rng,
        users=USERS,
        products=PRODUCTS,
        orders_per_second=10,
        mutations_per_second=2,
        duration=0,
        tick_seconds=1,
        now_fn=clock,
        sleep=clock.sleep,
    )
    assert stats.ticks == 0
    assert sink.orders == []


def test_event_pump_emits_messy_records() -> None:
    clock = FakeClock(START)
    emitted = []
    factory = ClickstreamFactory(rng=random.Random(3), schema_v2_start=START - timedelta(days=365))
    stats = pump(
        factory,
        emitted.append,
        user_ids=USER_IDS,
        product_ids=PRODUCT_IDS,
        base_rate=1000,
        duration=2,
        tick_seconds=1,
        now_fn=clock,
        sleep=clock.sleep,
    )

    assert stats.ticks == 2
    assert stats.produced == len(emitted) > 0
    assert stats.malformed > 0
    assert stats.duplicates > 0
