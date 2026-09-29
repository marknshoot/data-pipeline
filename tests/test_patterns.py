"""Tests for traffic shaping (diurnal curve, flash sales, sampling)."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from generators.patterns import (
    FLASH_SALE_MULTIPLIER,
    HOUR_WEIGHTS,
    LOCAL_TZ,
    flash_sale_multiplier,
    is_flash_sale,
    sample_poisson,
    sample_timestamp,
    traffic_multiplier,
)


def _local(year: int, month: int, day: int, hour: int) -> datetime:
    return datetime(year, month, day, hour, tzinfo=LOCAL_TZ).astimezone(UTC)


def test_hour_weights_cover_a_full_day() -> None:
    assert len(HOUR_WEIGHTS) == 24
    assert all(weight > 0 for weight in HOUR_WEIGHTS)


def test_evening_is_busier_than_night() -> None:
    night = traffic_multiplier(_local(2026, 3, 10, 3))
    evening = traffic_multiplier(_local(2026, 3, 10, 20))
    assert evening > night


def test_flash_sale_days_spike() -> None:
    normal = _local(2026, 3, 10, 20)
    flash = _local(2026, 11, 11, 20)
    assert not is_flash_sale(normal)
    assert is_flash_sale(flash)
    assert flash_sale_multiplier(flash) == FLASH_SALE_MULTIPLIER
    assert traffic_multiplier(flash) == pytest.approx(
        traffic_multiplier(normal) * FLASH_SALE_MULTIPLIER
    )


def test_sample_poisson_mean_and_non_negative() -> None:
    rng = random.Random(1)
    draws = [sample_poisson(rng, 5.0) for _ in range(20_000)]
    assert all(value >= 0 for value in draws)
    assert 4.8 < sum(draws) / len(draws) < 5.2


def test_sample_timestamp_stays_in_range() -> None:
    rng = random.Random(2)
    end = datetime(2026, 3, 10, tzinfo=UTC)
    start = end - timedelta(days=30)
    for _ in range(500):
        ts = sample_timestamp(rng, start, end)
        assert start <= ts < end
        assert ts.tzinfo is not None


def test_sample_timestamp_rejects_empty_range() -> None:
    now = datetime(2026, 3, 10, tzinfo=UTC)
    with pytest.raises(ValueError):
        sample_timestamp(random.Random(3), now, now)


def test_sample_timestamp_prefers_evening_over_night() -> None:
    rng = random.Random(4)
    end = datetime(2026, 3, 10, tzinfo=UTC)
    start = end - timedelta(days=60)
    evening = night = 0
    for _ in range(4_000):
        hour = sample_timestamp(rng, start, end).astimezone(LOCAL_TZ).hour
        if 19 <= hour <= 21:
            evening += 1
        elif 2 <= hour <= 4:
            night += 1
    assert evening > night * 3
