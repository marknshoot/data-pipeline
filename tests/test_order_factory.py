"""Tests for order totals, lifecycle transitions and price drift."""

from __future__ import annotations

import random
from decimal import Decimal

import pytest

from generators.order_factory import (
    OrderFactory,
    ProductRef,
    drift_price,
    is_terminal,
    next_status,
    payment_status_for,
)

BASE_PRICE = Decimal("100000.00")
PRODUCTS = [ProductRef(product_id=i, price=BASE_PRICE) for i in range(1, 21)]
TERMINAL = ("delivered", "cancelled", "refunded")


def test_create_order_total_equals_sum_of_lines() -> None:
    factory = OrderFactory(random.Random(1))
    draft = factory.create_order(user_id=1, shipping_city="Jakarta", products=PRODUCTS)

    assert draft.total_amount == sum((item.line_total for item in draft.items), Decimal("0.00"))
    assert 1 <= len(draft.items) <= 4
    assert len({item.product_id for item in draft.items}) == len(draft.items)
    assert all(1 <= item.quantity <= 3 for item in draft.items)
    assert all(item.unit_price <= BASE_PRICE for item in draft.items)
    assert draft.status in {"paid", "pending"}
    assert draft.currency == "IDR"


def test_flash_sale_always_discounts() -> None:
    factory = OrderFactory(random.Random(2))
    for _ in range(200):
        draft = factory.create_order(
            user_id=1, shipping_city="Jakarta", products=PRODUCTS, flash_sale=True
        )
        assert all(item.unit_price < BASE_PRICE for item in draft.items)


def test_create_order_requires_products() -> None:
    factory = OrderFactory(random.Random(3))
    with pytest.raises(ValueError):
        factory.create_order(user_id=1, shipping_city="Jakarta", products=[])


def test_terminal_statuses_have_no_transition() -> None:
    rng = random.Random(4)
    for status in TERMINAL:
        assert is_terminal(status)
        assert next_status(rng, status) is None


def test_transitions_stay_within_the_lifecycle() -> None:
    rng = random.Random(5)
    expected = {
        "pending": {"paid", "cancelled"},
        "paid": {"shipped", "refunded"},
        "shipped": {"delivered", "refunded"},
    }
    for status, allowed in expected.items():
        assert {next_status(rng, status) for _ in range(200)} == allowed


def test_drift_price_stays_within_eight_percent() -> None:
    rng = random.Random(6)
    for _ in range(1_000):
        new_price = drift_price(rng, BASE_PRICE)
        assert new_price > 0
        assert BASE_PRICE * Decimal("0.919") <= new_price <= BASE_PRICE * Decimal("1.081")


def test_payment_status_follows_order_status() -> None:
    assert payment_status_for("cancelled", "pending") == "failed"
    assert payment_status_for("refunded", "pending") == "refunded"
    assert payment_status_for("delivered", "pending") == "captured"
    assert payment_status_for("pending", "authorized") == "authorized"
