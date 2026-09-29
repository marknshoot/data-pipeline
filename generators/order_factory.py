"""Pure order/OLTP mutation logic.

Mirrors what a real marketplace app would write to Postgres. Kept free of psycopg
imports so the rules (order totals, status transitions, price drift, promo
discounts) are unit-testable in isolation.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from .reference import CITIES, PAYMENT_METHODS

CENTS = Decimal("0.01")

TERMINAL_STATUSES: frozenset[str] = frozenset({"delivered", "cancelled", "refunded"})

# pending -> paid -> shipped -> delivered, with a chance of cancelling/refunding.
TRANSITIONS: dict[str, tuple[tuple[str, float], ...]] = {
    "pending": (("paid", 0.85), ("cancelled", 0.15)),
    "paid": (("shipped", 0.92), ("refunded", 0.08)),
    "shipped": (("delivered", 0.97), ("refunded", 0.03)),
}


@dataclass(frozen=True)
class ProductRef:
    product_id: int
    price: Decimal


@dataclass(frozen=True)
class OrderItemDraft:
    product_id: int
    quantity: int
    unit_price: Decimal

    @property
    def line_total(self) -> Decimal:
        return (self.unit_price * self.quantity).quantize(CENTS, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class OrderDraft:
    user_id: int
    shipping_city: str
    status: str
    currency: str
    items: tuple[OrderItemDraft, ...]
    total_amount: Decimal
    payment_method: str
    payment_status: str


def is_terminal(status: str) -> bool:
    return status in TERMINAL_STATUSES


def payment_status_for(order_status: str, fallback: str = "pending") -> str:
    """Map an order status to the payment status it implies."""
    if order_status == "cancelled":
        return "failed"
    if order_status == "refunded":
        return "refunded"
    if order_status in {"paid", "shipped", "delivered"}:
        return "captured"
    return fallback


def next_status(rng: random.Random, status: str) -> str | None:
    """Return the order's next status, or ``None`` when it is already terminal."""
    options = TRANSITIONS.get(status)
    if not options:
        return None
    names = [name for name, _ in options]
    weights = [weight for _, weight in options]
    return rng.choices(names, weights=weights, k=1)[0]


def drift_price(rng: random.Random, price: Decimal) -> Decimal:
    """Nudge a product price by up to +/-8%, as a seller would."""
    factor = Decimal(str(1 + rng.uniform(-0.08, 0.08)))
    new_price = (price * factor).quantize(CENTS, rounding=ROUND_HALF_UP)
    return max(new_price, CENTS)


def pick_city(rng: random.Random) -> str:
    return rng.choice(CITIES)


class OrderFactory:
    """Generates new orders and the mutations that follow them."""

    def __init__(
        self,
        rng: random.Random,
        *,
        currency: str = "IDR",
        max_items_per_order: int = 4,
        max_quantity: int = 3,
        flash_sale_discount_range: tuple[float, float] = (0.10, 0.30),
        promo_probability: float = 0.30,
    ) -> None:
        self.rng = rng
        self.currency = currency
        self.max_items_per_order = max_items_per_order
        self.max_quantity = max_quantity
        self.flash_sale_discount_range = flash_sale_discount_range
        self.promo_probability = promo_probability

    def _unit_price(self, price: Decimal, *, flash_sale: bool) -> Decimal:
        if flash_sale:
            discount = self.rng.uniform(*self.flash_sale_discount_range)
        elif self.rng.random() < self.promo_probability:
            discount = self.rng.uniform(0.05, 0.15)
        else:
            discount = 0.0
        if discount == 0:
            return price
        factor = Decimal(str(1 - discount))
        return max((price * factor).quantize(CENTS, rounding=ROUND_HALF_UP), CENTS)

    def create_order(
        self,
        *,
        user_id: int,
        shipping_city: str,
        products: list[ProductRef],
        flash_sale: bool = False,
    ) -> OrderDraft:
        if not products:
            raise ValueError("need at least one product to build an order")

        count = min(self.rng.randint(1, self.max_items_per_order), len(products))
        chosen = self.rng.sample(products, count)
        items = tuple(
            OrderItemDraft(
                product_id=product.product_id,
                quantity=self.rng.randint(1, self.max_quantity),
                unit_price=self._unit_price(product.price, flash_sale=flash_sale),
            )
            for product in chosen
        )
        total = sum((item.line_total for item in items), start=Decimal("0.00"))

        # Most checkouts are paid immediately; the rest start pending and settle later.
        paid_now = self.rng.random() < 0.75
        status = "paid" if paid_now else "pending"
        method = self.rng.choices(
            [name for name, _ in PAYMENT_METHODS],
            weights=[weight for _, weight in PAYMENT_METHODS],
            k=1,
        )[0]
        payment_status = "captured" if paid_now else self.rng.choice(["pending", "authorized"])

        return OrderDraft(
            user_id=user_id,
            shipping_city=shipping_city,
            status=status,
            currency=self.currency,
            items=items,
            total_amount=total.quantize(CENTS, rounding=ROUND_HALF_UP),
            payment_method=method,
            payment_status=payment_status,
        )
