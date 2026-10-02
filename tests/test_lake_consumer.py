"""Tests for the Kafka -> lake consumer.

The interesting behaviour is the delivery guarantee, so the loop is exercised with
fakes: a fake consumer that records every ``commit`` call and a fake sink that can
be made to fail. The contract under test is *write, then commit* -- offsets for data
that never reached S3 must never be committed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
import pytest

from streaming.lake_consumer import (
    EVENT_SCHEMA,
    Buffer,
    dlq_key,
    events_key,
    parse_event,
    run,
)

TOPIC = "clickstream.events"


# ------------------------------------------------------------------ fixtures


class FakeMessage:
    def __init__(self, value, *, partition=0, offset=0, error=None, topic=TOPIC):
        self._value = value
        self._partition = partition
        self._offset = offset
        self._error = error
        self._topic = topic

    def error(self):
        return self._error

    def key(self):
        return None

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, messages=()):
        self._messages = list(messages)
        self.commits: list = []

    def poll(self, timeout=None):
        return self._messages.pop(0) if self._messages else None

    def commit(self, offsets=None, asynchronous=True):
        self.commits.append(list(offsets) if offsets is not None else None)


class RecordingSink:
    """Collects writes; optionally fails on a key containing ``fail_on``."""

    def __init__(self, fail_on: str | None = None):
        self.writes: list[tuple[str, object]] = []
        self.fail_on = fail_on

    def __call__(self, table, key):
        if self.fail_on and self.fail_on in key:
            raise RuntimeError(f"sink refused {key}")
        self.writes.append((key, table))
        return key

    @property
    def keys(self) -> list[str]:
        return [key for key, _ in self.writes]


class Clock:
    """Manually advanced clock so batch timing is deterministic."""

    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def payload(**overrides) -> bytes:
    event = {
        "event_id": "evt-1",
        "schema_version": 1,
        "event_type": "page_view",
        "user_id": 42,
        "session_id": "sess-1",
        "product_id": 7,
        "event_time": "2026-09-27T10:00:00.123456Z",
        "city": "Jakarta",
        "device": "mobile",
    }
    event.update(overrides)
    return json.dumps(event).encode()


# ------------------------------------------------------------------- parsing


def test_parse_event_v1_maps_device():
    event = parse_event(payload())
    assert event["schema_version"] == 1
    assert event["device"] == "mobile"
    assert event["device_type"] is None
    assert event["user_id"] == 42
    assert event["event_time"] == datetime(2026, 9, 27, 10, 0, 0, 123456, tzinfo=UTC)
    assert event["event_time"].tzinfo is not None


def test_parse_event_v2_maps_device_type():
    event = parse_event(
        payload(schema_version=2, device_type="ios", platform_version="3.4", device=None)
    )
    assert event["schema_version"] == 2
    assert event["device_type"] == "ios"
    assert event["platform_version"] == "3.4"


def test_parse_event_normalises_case_and_defaults():
    event = parse_event(payload(event_type="  PURCHASE ", product_id=None, city=None))
    assert event["event_type"] == "purchase"
    assert event["product_id"] == 0
    assert event["city"] == ""


def test_parse_event_records_consumed_at_from_clock():
    seen = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
    assert parse_event(payload(), seen_at=seen)["consumed_at"] == seen


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (b"", "empty payload"),
        (b'{"event_id": "x", "event_typ', "invalid json"),
        (b'["not", "an", "object"]', "expected an object"),
        (b"\xff\xfe not utf-8", "not valid utf-8"),
    ],
)
def test_parse_event_rejects_unusable_payloads(value, reason):
    with pytest.raises(ValueError, match=reason):
        parse_event(value)


def test_parse_event_reports_missing_fields():
    broken = json.loads(payload())
    del broken["event_id"]
    del broken["session_id"]
    with pytest.raises(ValueError, match="missing required field"):
        parse_event(json.dumps(broken).encode())


def test_parse_event_rejects_unparseable_timestamp():
    with pytest.raises(ValueError, match="bad field value"):
        parse_event(payload(event_time="not-a-date"))


# -------------------------------------------------------------- lake key shape


def test_event_keys_are_deterministic_for_the_same_batch():
    when = datetime(2026, 9, 27, 10, 30, tzinfo=UTC)
    first = events_key(3, 100, 199, when)
    assert first == events_key(3, 100, 199, when)
    assert first == "raw/events/dt=2026-09-27/hr=10/part=3-100-199.parquet"
    assert dlq_key(3, 100, 199, when).startswith("dlq/events/dt=2026-09-27/")


def test_event_keys_differ_across_hours_which_is_why_dedup_exists():
    """A redelivery in a later hour writes a new object; event_id dedup covers it."""
    offsets = (3, 100, 199)
    early = events_key(*offsets, datetime(2026, 9, 27, 10, 59, tzinfo=UTC))
    late = events_key(*offsets, datetime(2026, 9, 27, 11, 1, tzinfo=UTC))
    assert early != late
    assert "/hr=10/" in early and "/hr=11/" in late


def test_event_keys_distinguish_partitions_and_ranges():
    when = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)
    assert events_key(0, 0, 9, when) != events_key(1, 0, 9, when)
    assert events_key(0, 0, 9, when) != events_key(0, 10, 19, when)


# ------------------------------------------------- the at-least-once contract


def test_flush_writes_before_committing():
    """Order matters: the commit must never precede the successful write."""
    order: list[str] = []
    when = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)

    class OrderedSink(RecordingSink):
        def __call__(self, table, key):
            order.append("write")
            return super().__call__(table, key)

    class OrderedConsumer(FakeConsumer):
        def commit(self, offsets=None, asynchronous=True):
            order.append("commit")
            super().commit(offsets=offsets, asynchronous=asynchronous)

    buffer = Buffer(TOPIC)
    buffer.add(0, 5, row=parse_event(payload()))
    consumer = OrderedConsumer()
    objects, commits = buffer.flush(OrderedSink(), consumer, when)

    assert order == ["write", "commit"]
    assert (objects, commits) == (1, 1)
    # Committed offset is the next one to read, not the last one read.
    assert consumer.commits[0][0].offset == 6


def test_failed_write_never_commits():
    """A batch that cannot be persisted must stay uncommitted for redelivery."""
    buffer = Buffer(TOPIC)
    buffer.add(0, 5, row=parse_event(payload()))
    consumer = FakeConsumer()

    with pytest.raises(RuntimeError, match="sink refused"):
        buffer.flush(RecordingSink(fail_on="raw/events"), consumer, datetime.now(UTC))

    assert consumer.commits == []


def test_malformed_record_goes_to_dlq_and_still_commits():
    buffer = Buffer(TOPIC)
    buffer.add(0, 7, row=parse_event(payload()))
    buffer.add(
        0,
        8,
        dead={
            "topic": TOPIC,
            "partition": 0,
            "offset": 8,
            "consumed_at": datetime.now(UTC),
            "error": "invalid json",
            "raw_value": '{"broken":',
        },
    )
    sink = RecordingSink()
    consumer = FakeConsumer()
    objects, commits = buffer.flush(sink, consumer, datetime(2026, 9, 27, 10, 0, tzinfo=UTC))

    assert objects == 2
    assert commits == 1
    assert any(key.startswith("raw/events/") for key in sink.keys)
    assert any(key.startswith("dlq/events/") for key in sink.keys)
    # Both objects are valid Parquet with the expected schemas.
    written = {key: table for key, table in sink.writes}
    valid = next(table for key, table in written.items() if key.startswith("raw/events/"))
    dead = next(table for key, table in written.items() if key.startswith("dlq/events/"))
    assert valid.schema.equals(EVENT_SCHEMA)
    assert valid.num_rows == 1
    assert dead.num_rows == 1
    assert dead.column("error")[0].as_py() == "invalid json"


def test_buffer_is_cleared_after_flush():
    buffer = Buffer(TOPIC)
    buffer.add(0, 1, row=parse_event(payload()))
    assert buffer.size == 1
    buffer.flush(RecordingSink(), FakeConsumer(), datetime.now(UTC))
    assert buffer.size == 0
    assert buffer.parts == {}


# ------------------------------------------------------------------- the loop


def test_run_consumes_a_batch_and_commits_the_final_offsets():
    messages = [FakeMessage(payload(event_id=f"evt-{i}"), partition=0, offset=i) for i in range(3)]
    consumer = FakeConsumer(messages)
    sink = RecordingSink()
    clock = Clock(datetime(2026, 9, 27, 10, 0, tzinfo=UTC))

    stats = run(
        consumer,
        sink,
        topic=TOPIC,
        batch_size=100,
        batch_seconds=60,
        max_messages=3,
        now_fn=clock,
        log=lambda _: None,
    )

    assert stats.consumed == 3
    assert stats.valid == 3
    assert stats.malformed == 0
    assert stats.batches == 1
    assert stats.objects == 1
    assert consumer.commits[0][0].offset == 3


def test_run_splits_partitions_into_separate_objects():
    messages = [
        FakeMessage(payload(event_id="a"), partition=0, offset=0),
        FakeMessage(payload(event_id="b"), partition=1, offset=0),
        FakeMessage(payload(event_id="c"), partition=0, offset=1),
    ]
    consumer = FakeConsumer(messages)
    sink = RecordingSink()

    stats = run(
        consumer,
        sink,
        topic=TOPIC,
        max_messages=3,
        now_fn=lambda: datetime(2026, 9, 27, 10, 0, tzinfo=UTC),
        log=lambda _: None,
    )

    assert stats.objects == 2
    assert {key.rsplit("/", 1)[-1] for key in sink.keys} == {
        "part=0-0-1.parquet",
        "part=1-0-0.parquet",
    }
    committed = {tp.partition: tp.offset for tp in consumer.commits[0]}
    assert committed == {0: 2, 1: 1}


def test_run_flushes_on_the_time_trigger_not_only_on_size():
    """A trickle of records must still land: batch_seconds is the other trigger."""
    messages = [FakeMessage(payload(event_id="slow"), partition=0, offset=0)]
    consumer = FakeConsumer(messages)
    sink = RecordingSink()
    clock = Clock(datetime(2026, 9, 27, 10, 0, tzinfo=UTC))
    original_poll = consumer.poll

    def poll(timeout=None):
        # Advance the clock as if ten seconds passed between messages.
        clock.tick(10)
        return original_poll(timeout)

    consumer.poll = poll  # type: ignore[method-assign]
    stats = run(
        consumer,
        sink,
        topic=TOPIC,
        batch_size=1000,
        batch_seconds=5,
        max_messages=1,
        now_fn=clock,
        log=lambda _: None,
    )

    assert stats.batches == 1
    assert sink.keys and sink.keys[0].endswith("part=0-0-0.parquet")


def test_run_routes_malformed_records_to_the_dlq():
    messages = [
        FakeMessage(payload(event_id="good"), partition=0, offset=0),
        FakeMessage(b'{"truncated": ', partition=0, offset=1),
        FakeMessage(payload(event_id="also-good"), partition=0, offset=2),
    ]
    consumer = FakeConsumer(messages)
    sink = RecordingSink()

    stats = run(
        consumer,
        sink,
        topic=TOPIC,
        max_messages=3,
        now_fn=lambda: datetime(2026, 9, 27, 10, 0, tzinfo=UTC),
        log=lambda _: None,
    )

    assert stats.consumed == 3
    assert stats.valid == 2
    assert stats.malformed == 1
    # One object holds both good events, one holds the bad record.
    assert len(sink.keys) == 2
    dlq = next(table for key, table in sink.writes if key.startswith("dlq/events/"))
    assert dlq.num_rows == 1
    assert "invalid json" in dlq.column("error")[0].as_py()
    assert dlq.column("raw_value")[0].as_py() == '{"truncated": '


def test_run_propagates_write_failure_without_committing():
    messages = [FakeMessage(payload(), partition=0, offset=0)]
    consumer = FakeConsumer(messages)

    with pytest.raises(RuntimeError, match="sink refused"):
        run(
            consumer,
            RecordingSink(fail_on="raw/events"),
            topic=TOPIC,
            max_messages=1,
            now_fn=lambda: datetime(2026, 9, 27, 10, 0, tzinfo=UTC),
            log=lambda _: None,
        )

    # Uncommitted => the record is redelivered after a restart.
    assert consumer.commits == []


def test_run_ignores_message_errors():
    messages = [
        FakeMessage(None, error="Broker: not available"),
        FakeMessage(payload(), partition=0, offset=1),
    ]
    consumer = FakeConsumer(messages)
    logged: list[str] = []
    clock = Clock(datetime(2026, 9, 27, 10, 0, tzinfo=UTC))
    original_poll = consumer.poll

    def poll(timeout=None):
        # Bound the run by wall clock: an errored message never counts towards
        # max_messages, so max_seconds is the only stop condition that applies.
        clock.tick(1)
        return original_poll(timeout)

    consumer.poll = poll  # type: ignore[method-assign]
    stats = run(
        consumer,
        RecordingSink(),
        topic=TOPIC,
        max_seconds=3,
        now_fn=clock,
        log=logged.append,
    )

    assert stats.consumed == 1
    assert stats.valid == 1
    assert any("consumer error" in line for line in logged)


def test_written_parquet_round_trips_through_a_buffer():
    """The object we hand to the lake must be readable Parquet with UTC timestamps."""
    import io

    table = Buffer(TOPIC)
    table.add(0, 0, row=parse_event(payload()))
    sink = RecordingSink()
    table.flush(sink, FakeConsumer(), datetime(2026, 9, 27, 10, 0, tzinfo=UTC))

    written = sink.writes[0][1]
    buffer = io.BytesIO()
    pq.write_table(written, buffer)
    buffer.seek(0)
    restored = pq.read_table(buffer)

    assert restored.num_rows == 1
    assert restored.schema.field("event_time").type.tz == "UTC"
    assert restored.column("event_id")[0].as_py() == "evt-1"
