"""Pure clickstream event generation.

No Kafka, no disk: given a seeded ``random.Random`` the output is reproducible, so
the messy-data guarantees (duplicates, late events, schema v2, malformed records)
can be unit-tested directly.

Messiness is injected on purpose, matching PLAN.md section 3:
  * ~2% of records are exact duplicates (same ``event_id``, delivered twice)
  * ~3% of events are late (``event_time`` up to 2h before send time)
  * from ``schema_v2_start`` onward: adds ``platform_version``, renames
    ``device`` -> ``device_type``
  * ~0.5% of records are malformed JSON / missing fields, for the dead-letter path
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

from .reference import CITIES, DEVICES

# Event mix, weighted so the funnel has plausible drop-off (views >> purchases).
EVENT_TYPES: tuple[tuple[str, float], ...] = (
    ("page_view", 0.70),
    ("add_to_cart", 0.18),
    ("checkout", 0.07),
    ("purchase", 0.05),
)


@dataclass(frozen=True)
class EventRecord:
    """One Kafka message. ``value`` may be intentionally malformed."""

    key: bytes | None
    value: bytes
    malformed: bool
    duplicate: bool = False


def _weighted(rng: random.Random, choices: tuple[tuple[str, float], ...]) -> str:
    names = [name for name, _ in choices]
    weights = [weight for _, weight in choices]
    return rng.choices(names, weights=weights, k=1)[0]


@dataclass
class ClickstreamFactory:
    """Builds event payloads, including the deliberate imperfections."""

    rng: random.Random
    schema_v2_start: datetime
    duplicate_rate: float = 0.02
    late_rate: float = 0.03
    malformed_rate: float = 0.005
    max_lateness: timedelta = timedelta(hours=2)
    new_session_probability: float = 0.3
    _sessions: dict[int, str] = field(default_factory=dict, repr=False)

    # ------------------------------------------------------------- helpers
    def session_for(self, user_id: int) -> str:
        """Reuse a user's session most of the time so funnels stay coherent."""
        session = self._sessions.get(user_id)
        if session is None or self.rng.random() < self.new_session_probability:
            session = str(uuid.uuid4())
            self._sessions[user_id] = session
        return session

    def event_time(self, now: datetime) -> datetime:
        if self.rng.random() < self.late_rate:
            offset = self.rng.uniform(0, self.max_lateness.total_seconds())
            return now - timedelta(seconds=offset)
        return now

    def build_event(
        self,
        now: datetime,
        *,
        user_id: int,
        product_id: int,
        event_time: datetime | None = None,
    ) -> dict:
        when = event_time if event_time is not None else self.event_time(now)
        is_v2 = when >= self.schema_v2_start
        event: dict = {
            "event_id": str(uuid.uuid4()),
            "schema_version": 2 if is_v2 else 1,
            "event_type": _weighted(self.rng, EVENT_TYPES),
            "user_id": user_id,
            "session_id": self.session_for(user_id),
            "product_id": product_id,
            "event_time": when.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "city": self.rng.choice(CITIES),
        }
        if is_v2:
            event["device_type"] = self.rng.choice(DEVICES)
            event["platform_version"] = f"{self.rng.randint(2, 4)}.{self.rng.randint(0, 9)}"
        else:
            event["device"] = self.rng.choice(DEVICES)
        return event

    # ------------------------------------------------------------ corrupt
    def corrupt(self, event: dict) -> bytes:
        """Return a payload a naive consumer would choke on."""
        flavor = self.rng.random()
        if flavor < 0.4:
            # Truncated mid-JSON, e.g. a partial network flush.
            return json.dumps(event)[: self.rng.randint(8, 24)].encode()
        if flavor < 0.8:
            # Structurally valid JSON but missing mandatory fields.
            broken = {k: v for k, v in event.items() if k not in {"event_id", "user_id"}}
            return json.dumps(broken).encode()
        return b'{"event_type": "page_view", "oops": '

    # ------------------------------------------------------------- batch
    def build_records(
        self,
        now: datetime,
        count: int,
        *,
        user_ids: list[int],
        product_ids: list[int],
    ) -> list[EventRecord]:
        """Build ``count`` events, then layer duplicates and corruption on top."""
        records: list[EventRecord] = []
        for _ in range(count):
            user_id = self.rng.choice(user_ids)
            event = self.build_event(now, user_id=user_id, product_id=self.rng.choice(product_ids))
            malformed = self.rng.random() < self.malformed_rate
            value = self.corrupt(event) if malformed else json.dumps(event).encode()
            record = EventRecord(key=str(user_id).encode(), value=value, malformed=malformed)
            records.append(record)
            if self.rng.random() < self.duplicate_rate:
                # Same bytes => same event_id, delivered twice.
                records.append(replace(record, duplicate=True))
        return records
