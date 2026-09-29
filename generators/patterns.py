"""Traffic shaping shared by the live generators and the historical seeder.

Hours are evaluated in the marketplace's local timezone (Asia/Jakarta), while the
timestamps we hand out stay timezone-aware. Simulated history therefore has the
same shape as live traffic: quiet nights, a lunch bump, an evening peak, and a
5x spike on the 9.9 / 11.11 / 12.12 flash-sale days.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Asia/Jakarta")

# Relative click volume per local hour of day (index 0 = midnight).
HOUR_WEIGHTS: tuple[float, ...] = (
    0.15,
    0.08,
    0.05,
    0.04,
    0.05,
    0.10,  # 00-05  quiet
    0.30,
    0.55,
    0.75,
    0.85,
    0.95,
    1.00,  # 06-11  morning ramp into lunch
    1.05,
    0.95,
    0.90,
    0.95,
    1.00,
    1.10,  # 12-17  lunch bump, afternoon
    1.25,
    1.45,
    1.55,
    1.40,
    1.00,
    0.55,  # 18-23  evening peak
)
assert len(HOUR_WEIGHTS) == 24

# Shopee/Lazada-style double-digit campaigns.
FLASH_SALE_DATES: frozenset[tuple[int, int]] = frozenset({(9, 9), (11, 11), (12, 12)})
FLASH_SALE_MULTIPLIER = 5.0

# Guard rail: refuse to sample over absurdly wide ranges (keeps the weight table small).
MAX_SAMPLE_HOURS = 24 * 400


def is_flash_sale(ts: datetime) -> bool:
    local = ts.astimezone(LOCAL_TZ)
    return (local.month, local.day) in FLASH_SALE_DATES


def flash_sale_multiplier(ts: datetime) -> float:
    return FLASH_SALE_MULTIPLIER if is_flash_sale(ts) else 1.0


def traffic_multiplier(ts: datetime) -> float:
    """Expected traffic relative to the daily average at ``ts`` (around 1.0)."""
    local = ts.astimezone(LOCAL_TZ)
    return HOUR_WEIGHTS[local.hour] * flash_sale_multiplier(ts)


def sample_poisson(rng: random.Random, mean: float) -> int:
    """Draw a non-negative integer with the given mean (Knuth's algorithm)."""
    if mean <= 0:
        return 0
    if mean > 30:  # Knuth is slow for large means; normal approximation is fine here.
        return max(0, round(rng.gauss(mean, mean**0.5)))
    limit = pow(2.718281828459045, -mean)
    k, product = 0, 1.0
    while True:
        product *= rng.random()
        if product <= limit:
            return k
        k += 1


def sample_timestamp(rng: random.Random, start: datetime, end: datetime) -> datetime:
    """Pick an instant in ``[start, end)`` weighted by the diurnal/flash-sale curve."""
    if end <= start:
        raise ValueError("end must be after start")

    cursor = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    if cursor < start:
        cursor += timedelta(hours=1)
    slots: list[datetime] = []
    while cursor < end:
        slots.append(cursor)
        cursor += timedelta(hours=1)
        if len(slots) > MAX_SAMPLE_HOURS:
            raise ValueError("range too wide to sample by hour; narrow it down")

    chosen = rng.choices(slots, weights=[traffic_multiplier(s) for s in slots], k=1)[0]
    result = chosen + timedelta(seconds=rng.randrange(3600))
    return min(result, end - timedelta(microseconds=1))
