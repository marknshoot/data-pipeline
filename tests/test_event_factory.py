"""Tests for the deliberately messy clickstream factory."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta

from generators.event_factory import EVENT_TYPES, ClickstreamFactory

NOW = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
USER_IDS = list(range(1, 101))
PRODUCT_IDS = list(range(1, 51))


def _factory(*, v2_start: datetime | None = None, seed: int = 7) -> ClickstreamFactory:
    return ClickstreamFactory(
        rng=random.Random(seed),
        schema_v2_start=v2_start or NOW - timedelta(days=365),
    )


def test_v2_events_add_platform_version_and_rename_device() -> None:
    factory = _factory(v2_start=NOW - timedelta(days=1))
    event = factory.build_event(NOW, user_id=1, product_id=2)
    assert event["schema_version"] == 2
    assert "device_type" in event
    assert "device" not in event
    assert "platform_version" in event


def test_events_before_v2_start_use_v1_shape() -> None:
    factory = _factory(v2_start=NOW + timedelta(days=1))
    event = factory.build_event(NOW, user_id=1, product_id=2)
    assert event["schema_version"] == 1
    assert "device" in event
    assert "device_type" not in event
    assert "platform_version" not in event


def test_event_types_are_from_the_known_mix() -> None:
    factory = _factory()
    known = {name for name, _ in EVENT_TYPES}
    seen = {factory.build_event(NOW, user_id=i, product_id=i).get("event_type") for i in range(200)}
    assert seen <= known
    assert seen == known


def test_batch_has_duplicates_late_events_and_malformed_records() -> None:
    factory = _factory(seed=11)
    records = factory.build_records(NOW, 20_000, user_ids=USER_IDS, product_ids=PRODUCT_IDS)

    assert len(records) == 20_000 + sum(r.duplicate for r in records)
    duplicate_share = sum(r.duplicate for r in records) / 20_000
    assert 0.015 < duplicate_share < 0.025

    malformed_share = sum(r.malformed for r in records) / len(records)
    assert 0.003 < malformed_share < 0.008

    well_formed = []
    for record in records:
        if record.malformed:
            continue
        well_formed.append(json.loads(record.value))
    late = sum(
        1
        for event in well_formed
        if datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")) < NOW
    )
    late_share = late / len(well_formed)
    assert 0.025 < late_share < 0.035


def test_malformed_records_are_unparseable_or_missing_ids() -> None:
    factory = _factory(seed=13)
    records = factory.build_records(NOW, 5_000, user_ids=USER_IDS, product_ids=PRODUCT_IDS)
    malformed = [r for r in records if r.malformed]
    assert malformed, "expected some malformed records"
    for record in malformed:
        try:
            parsed = json.loads(record.value)
        except json.JSONDecodeError:
            continue
        assert "event_id" not in parsed or "user_id" not in parsed


def test_duplicates_reuse_the_same_event_id() -> None:
    factory = _factory(seed=17)
    records = factory.build_records(NOW, 5_000, user_ids=USER_IDS, product_ids=PRODUCT_IDS)
    duplicate_values = [r.value for r in records if r.duplicate]
    assert duplicate_values
    all_values = [r.value for r in records]
    for value in duplicate_values[:50]:
        assert all_values.count(value) >= 2


def test_key_is_the_user_id_and_session_is_sticky() -> None:
    factory = _factory(seed=19)
    records = factory.build_records(NOW, 200, user_ids=USER_IDS, product_ids=PRODUCT_IDS)
    for record in records:
        assert record.key is not None
        assert record.key.decode().isdigit()
    # Consecutive events for a user mostly reuse one session id.
    sessions = [
        json.loads(r.value)["session_id"]
        for r in records
        if not r.malformed and json.loads(r.value)["user_id"] == 1
    ]
    if len(sessions) >= 3:
        assert len(set(sessions)) < len(sessions)
