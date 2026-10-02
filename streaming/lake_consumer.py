"""Streaming ingestion: Kafka to the data lake, built for at-least-once delivery.

The consumer reads ``clickstream.events``, buffers a micro-batch per partition, and
writes it to the lake as Parquet. The ordering that makes the guarantee is:

    poll -> buffer -> write object -> *then* commit offsets

If the process dies anywhere before the commit, those offsets are simply redelivered
on restart, so nothing is lost. The cost is that a record can be written twice --
which is the honest trade, and the reason ``raw.events`` is a ReplacingMergeTree
keyed by ``event_id`` rather than a plain table.

Two details worth keeping:

* Offsets are stored manually (``enable.auto.offset.store=false``). Letting the
  client auto-store would commit offsets that were never persisted to S3.
* Object keys are derived from ``(partition, first_offset, last_offset)`` plus the
  batch's hour. A restart that redelivers within the same hour therefore overwrites
  the same object with the same bytes instead of piling up copies. Across an hour
  boundary the key differs, which does leave duplicates -- those are the ones
  ``event_id`` deduplication exists for.

Malformed payloads (the generator emits ~0.5%) never block the batch: they are
written to ``dlq/events/`` with the reason, and counted.

Usage:
    uv run python -m streaming.lake_consumer --max-seconds 30 --from-beginning
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

import psycopg
import pyarrow as pa
from confluent_kafka import Consumer, TopicPartition

from generators import db
from generators.config import Settings, get_settings
from ingestion import lake

# Fields an event must have to be usable; anything else has a documented default.
REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id",
    "user_id",
    "event_type",
    "session_id",
    "event_time",
)

# Must match clickhouse/init/02_events.sql and warehouse.load_raw.EVENTS_COLUMNS.
EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("schema_version", pa.int32()),
        ("event_type", pa.string()),
        ("user_id", pa.int64()),
        ("session_id", pa.string()),
        ("product_id", pa.int64()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("city", pa.string()),
        ("device", pa.string()),
        ("device_type", pa.string()),
        ("platform_version", pa.string()),
        ("consumed_at", pa.timestamp("us", tz="UTC")),
    ]
)

DLQ_SCHEMA = pa.schema(
    [
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("consumed_at", pa.timestamp("us", tz="UTC")),
        ("error", pa.string()),
        ("raw_value", pa.string()),
    ]
)

WriteFn = Callable[[pa.Table, str], str]


class Message(Protocol):
    """The slice of ``confluent_kafka.Message`` this module depends on."""

    def error(self) -> Any: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...


# --------------------------------------------------------------------- parsing


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        # The generator always sends UTC; treat a naive value as UTC rather than
        # silently assuming local time.
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_event(value: bytes | None, *, seen_at: datetime | None = None) -> dict:
    """Normalise one Kafka payload into a raw.events row.

    Raises ``ValueError`` with a human-readable reason for anything unusable, which
    is what routes the record to the dead-letter location.
    """
    if not value:
        raise ValueError("empty payload")
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"not valid utf-8 ({exc.reason} at byte {exc.start})") from exc

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json ({exc.msg} at position {exc.pos})") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"payload is {type(payload).__name__}, expected an object")

    missing = [name for name in REQUIRED_FIELDS if payload.get(name) in (None, "")]
    if missing:
        raise ValueError(f"missing required field(s): {','.join(missing)}")

    try:
        schema_version = int(payload.get("schema_version") or 1)
        product_id = payload.get("product_id")
        return {
            "event_id": str(payload["event_id"]),
            "schema_version": schema_version,
            "event_type": str(payload["event_type"]).strip().lower(),
            "user_id": int(payload["user_id"]),
            "session_id": str(payload["session_id"]),
            "product_id": int(product_id) if product_id not in (None, "") else 0,
            "event_time": _parse_time(payload["event_time"]),
            "city": str(payload.get("city") or "").strip(),
            # v1 sends `device`; v2 sends `device_type` + `platform_version`.
            "device": _optional_str(payload.get("device")),
            "device_type": _optional_str(payload.get("device_type")),
            "platform_version": _optional_str(payload.get("platform_version")),
            "consumed_at": seen_at or datetime.now(UTC),
        }
    except (TypeError, ValueError) as exc:
        raise ValueError(f"bad field value ({exc})") from exc


# ------------------------------------------------------------------ lake keys


def _hour_prefix(when: datetime) -> str:
    when = when.astimezone(UTC)
    return f"dt={when.date().isoformat()}/hr={when.hour:02d}"


def events_key(partition: int, first_offset: int, last_offset: int, when: datetime) -> str:
    """Object key for one partition's micro-batch (deterministic in the offsets)."""
    return f"raw/events/{_hour_prefix(when)}/part={partition}-{first_offset}-{last_offset}.parquet"


def dlq_key(partition: int, first_offset: int, last_offset: int, when: datetime) -> str:
    return f"dlq/events/{_hour_prefix(when)}/part={partition}-{first_offset}-{last_offset}.parquet"


# -------------------------------------------------------------------- stats


@dataclass
class Stats:
    polls: int = 0
    consumed: int = 0
    valid: int = 0
    malformed: int = 0
    batches: int = 0
    objects: int = 0
    commits: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "records": self.consumed,
            "malformed": self.malformed,
            "batches": self.batches,
            "objects": self.objects,
        }


# ------------------------------------------------------------------ buffering


@dataclass
class _PartitionBatch:
    rows: list[dict] = field(default_factory=list)
    dead: list[dict] = field(default_factory=list)
    first_offset: int | None = None
    last_offset: int = -1

    def add(self, offset: int, *, row: dict | None = None, dead: dict | None = None) -> None:
        if self.first_offset is None:
            self.first_offset = offset
        self.last_offset = max(self.last_offset, offset)
        if row is not None:
            self.rows.append(row)
        if dead is not None:
            self.dead.append(dead)

    @property
    def size(self) -> int:
        return len(self.rows) + len(self.dead)


class Buffer:
    """Per-partition micro-batch buffer.

    Kept separate from the poll loop (and from Kafka) so the delivery guarantee can
    be tested with fakes: the ordering "write succeeds, *then* commit" is the whole
    contract.
    """

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.parts: dict[int, _PartitionBatch] = {}

    @property
    def size(self) -> int:
        return sum(batch.size for batch in self.parts.values())

    def add(
        self,
        partition: int,
        offset: int,
        *,
        row: dict | None = None,
        dead: dict | None = None,
    ) -> None:
        self.parts.setdefault(partition, _PartitionBatch()).add(offset, row=row, dead=dead)

    def flush(self, write: WriteFn, consumer: Any, when: datetime) -> tuple[int, int]:
        """Write every buffered partition, then commit. Returns (objects, commits).

        The commit happens only after *all* writes succeed. If a write raises, the
        exception propagates with offsets uncommitted, so the records are redelivered
        on restart -- a duplicate at worst, never a loss.
        """
        commits: list[TopicPartition] = []
        objects = 0

        for partition in sorted(self.parts):
            batch = self.parts[partition]
            if batch.size == 0 or batch.first_offset is None:
                continue
            first, last = batch.first_offset, batch.last_offset
            if batch.rows:
                write(
                    pa.Table.from_pylist(batch.rows, schema=EVENT_SCHEMA),
                    events_key(partition, first, last, when),
                )
                objects += 1
            if batch.dead:
                write(
                    pa.Table.from_pylist(batch.dead, schema=DLQ_SCHEMA),
                    dlq_key(partition, first, last, when),
                )
                objects += 1
            # Kafka commits the *next* offset to read.
            commits.append(TopicPartition(self.topic, partition, last + 1))

        if commits:
            consumer.commit(offsets=commits, asynchronous=False)

        self.parts.clear()
        return objects, len(commits)


# --------------------------------------------------------------------- loop


def run(
    consumer: Any,
    write: WriteFn,
    *,
    topic: str,
    batch_size: int = 500,
    batch_seconds: float = 10.0,
    max_messages: int | None = None,
    max_seconds: float | None = None,
    poll_timeout: float = 0.5,
    now_fn: Callable[[], datetime] | None = None,
    log: Callable[[str], None] = print,
) -> Stats:
    """Poll, buffer and flush until a stop condition is met.

    ``write`` is injected so the loop runs against fakes in tests.
    """
    now_fn = now_fn or (lambda: datetime.now(UTC))
    stats = Stats()
    buffer = Buffer(topic)
    started = now_fn()
    last_flush = started

    while True:
        if max_seconds is not None and (now_fn() - started).total_seconds() >= max_seconds:
            break
        if max_messages is not None and stats.consumed >= max_messages:
            break

        message = consumer.poll(poll_timeout)
        stats.polls += 1

        if message is not None:
            error = message.error()
            if error is not None:
                # Transient broker/partition errors are reported and skipped; the
                # offsets of real records are what matter.
                log(f"consumer error: {error}")
            else:
                stats.consumed += 1
                seen_at = now_fn()
                try:
                    row = parse_event(message.value(), seen_at=seen_at)
                except ValueError as exc:
                    stats.malformed += 1
                    buffer.add(
                        message.partition(),
                        message.offset(),
                        dead={
                            "topic": message.topic(),
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "consumed_at": seen_at,
                            "error": str(exc),
                            "raw_value": (message.value() or b"").decode("utf-8", "replace"),
                        },
                    )
                else:
                    stats.valid += 1
                    buffer.add(message.partition(), message.offset(), row=row)

        due = buffer.size > 0 and (
            buffer.size >= batch_size or (now_fn() - last_flush).total_seconds() >= batch_seconds
        )
        if due:
            objects, commits = buffer.flush(write, consumer, now_fn())
            stats.objects += objects
            stats.commits += commits
            stats.batches += 1
            last_flush = now_fn()
            log(
                f"flushed batch: {objects} object(s) from {commits} partition(s) "
                f"({stats.consumed} records consumed, {stats.malformed} malformed)"
            )

    if buffer.size:
        objects, commits = buffer.flush(write, consumer, now_fn())
        stats.objects += objects
        stats.commits += commits
        stats.batches += 1
        log(f"flushed final batch: {objects} object(s), {commits} partition(s) committed")

    return stats


# --------------------------------------------------------------------- glue


def make_writer(settings: Settings) -> WriteFn:
    """Bind the lake writer once so each object does not rebuild the S3 filesystem."""
    filesystem = lake.build_filesystem(settings)
    bucket = settings.s3_bucket

    def write(table: pa.Table, key: str) -> str:
        return lake.write_parquet(table, key, bucket=bucket, filesystem=filesystem)

    return write


def build_consumer(settings: Settings, *, group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": group_id,
            "client.id": "lake-consumer",
            # Never let the client commit an offset for data that is not yet in S3.
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )


def record_run(
    settings: Settings,
    *,
    run_id: str,
    group_id: str,
    topic: str,
    started_at: datetime,
    finished_at: datetime,
    stats: Stats,
) -> None:
    """Report the run to Postgres so DLQ and throughput are queryable.

    Deliberately non-fatal: losing a metrics row must not fail an ingestion run.
    """
    try:
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_meta.stream_run
                    (run_id, group_id, topic, started_at, finished_at,
                     records, malformed, batches, objects, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ok')
                """,
                (
                    run_id,
                    group_id,
                    topic,
                    started_at,
                    finished_at,
                    stats.consumed,
                    stats.malformed,
                    stats.batches,
                    stats.objects,
                ),
            )
    except psycopg.Error as exc:
        print(f"warning: could not record run in pipeline_meta: {exc}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Consume Kafka clickstream into the lake")
    parser.add_argument("--group-id", default="lake-consumer", help="Kafka consumer group")
    parser.add_argument("--batch-size", type=int, default=500, help="records per flush")
    parser.add_argument("--batch-seconds", type=float, default=10.0, help="max seconds per batch")
    parser.add_argument("--max-seconds", type=float, default=None, help="stop after N seconds")
    parser.add_argument("--max-messages", type=int, default=None, help="stop after N records")
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="read the topic from the start (default: only new records)",
    )
    parser.add_argument("--no-metrics", action="store_true", help="skip the Postgres run record")
    args = parser.parse_args(argv)

    started_at = datetime.now(UTC)
    run_id = uuid.uuid4().hex[:12]
    consumer = build_consumer(settings, group_id=args.group_id, from_beginning=args.from_beginning)
    consumer.subscribe([settings.kafka_topic_events])
    print(
        f"lake_consumer: group={args.group_id} topic={settings.kafka_topic_events} "
        f"batch_size={args.batch_size} batch_seconds={args.batch_seconds} "
        f"offset_reset={'earliest' if args.from_beginning else 'latest'}",
        flush=True,
    )

    try:
        stats = run(
            consumer,
            make_writer(settings),
            topic=settings.kafka_topic_events,
            batch_size=args.batch_size,
            batch_seconds=args.batch_seconds,
            max_messages=args.max_messages,
            max_seconds=args.max_seconds,
        )
    except KeyboardInterrupt:
        # Ctrl-C is a clean stop: the buffer is flushed and committed before exit.
        print("\ninterrupted", file=sys.stderr)
        return 130
    finally:
        consumer.close()

    finished_at = datetime.now(UTC)
    print(
        f"done: consumed={stats.consumed} valid={stats.valid} malformed={stats.malformed} "
        f"batches={stats.batches} objects={stats.objects} commits={stats.commits} "
        f"in {(finished_at - started_at).total_seconds():.1f}s"
    )
    if not args.no_metrics:
        record_run(
            settings,
            run_id=run_id,
            group_id=args.group_id,
            topic=settings.kafka_topic_events,
            started_at=started_at,
            finished_at=finished_at,
            stats=stats,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
