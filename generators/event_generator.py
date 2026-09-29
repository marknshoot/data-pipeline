"""Publish simulated clickstream events to Kafka (topic: ``clickstream.events``).

Order generation lives in ``generators.order_generator``; this job only produces
the clickstream. The pacing and batching loop is separated from the Kafka client
so it can be unit-tested with a fake ``emit`` callable.

Usage:
    uv run python -m generators.event_generator --rate 40 --duration 60
    uv run python -m generators.event_generator --dry-run --rate 20 --duration 3
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg
from confluent_kafka import Producer

from . import db
from .config import get_settings
from .event_factory import ClickstreamFactory, EventRecord
from .patterns import sample_poisson, traffic_multiplier


@dataclass
class PublishStats:
    ticks: int = 0
    produced: int = 0
    malformed: int = 0
    duplicates: int = 0


def pump(
    factory: ClickstreamFactory,
    emit,
    *,
    user_ids: list[int],
    product_ids: list[int],
    base_rate: float,
    duration: float | None,
    tick_seconds: float = 1.0,
    now_fn=datetime.now,
    sleep=time.sleep,
) -> PublishStats:
    """Emit events at ``base_rate`` scaled by the traffic curve until ``duration``."""
    stats = PublishStats()
    start = now_fn(UTC)
    while True:
        now = now_fn(UTC)
        if duration is not None and (now - start).total_seconds() >= duration:
            break

        mean = base_rate * traffic_multiplier(now) * tick_seconds
        records = factory.build_records(
            now,
            sample_poisson(factory.rng, mean),
            user_ids=user_ids,
            product_ids=product_ids,
        )
        for record in records:
            emit(record)
        stats.produced += len(records)
        stats.malformed += sum(record.malformed for record in records)
        stats.duplicates += sum(record.duplicate for record in records)
        stats.ticks += 1
        sleep(tick_seconds)

    return stats


def _load_ids(conn: psycopg.Connection) -> tuple[list[int], list[int]]:
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT product_id FROM products WHERE is_active")
        product_ids = [row[0] for row in cur.fetchall()]
    return user_ids, product_ids


def _default_v2_start() -> datetime:
    now = datetime.now(UTC)
    return now.replace(minute=0, second=0, microsecond=0)


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Publish simulated clickstream events")
    parser.add_argument("--rate", type=float, default=20.0, help="events/s at average traffic")
    parser.add_argument(
        "--duration", type=float, default=None, help="seconds to run (default: forever)"
    )
    parser.add_argument("--tick", type=float, default=1.0, help="loop interval in seconds")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed (default: random)")
    parser.add_argument("--dry-run", action="store_true", help="print payloads, do not publish")
    parser.add_argument(
        "--schema-v2-start",
        default=None,
        help="ISO-8601 instant from which events use schema v2 (default: this hour)",
    )
    args = parser.parse_args(argv)

    v2_start = (
        datetime.fromisoformat(args.schema_v2_start)
        if args.schema_v2_start
        else _default_v2_start()
    )
    if v2_start.tzinfo is None:
        v2_start = v2_start.replace(tzinfo=UTC)

    rng = random.Random(args.seed)
    factory = ClickstreamFactory(rng=rng, schema_v2_start=v2_start)

    conn = None
    producer = None
    try:
        try:
            conn = db.connect()
            user_ids, product_ids = _load_ids(conn)
        except psycopg.Error:
            if not args.dry_run:
                raise
            print("database unavailable; using synthetic ids for dry run", file=sys.stderr)
            user_ids = list(range(1, 10_001))
            product_ids = list(range(1, 5_001))
        if not user_ids or not product_ids:
            print("shop database is empty - run `make seed` first", file=sys.stderr)
            return 1

        if args.dry_run:

            def emit(record: EventRecord) -> None:
                print(record.value.decode(errors="replace"))

            def flush() -> None:
                return None

        else:
            producer = Producer(
                {
                    "bootstrap.servers": settings.kafka_bootstrap,
                    "client.id": "event-generator",
                    "linger.ms": 50,
                    "message.timeout.ms": 10_000,
                    "enable.idempotence": False,
                }
            )
            topic = settings.kafka_topic_events
            failures = {"count": 0}

            def on_delivery(err, msg) -> None:
                if err is not None:
                    failures["count"] += 1
                    print(f"delivery failed: {err}", file=sys.stderr)

            def emit(record: EventRecord) -> None:
                producer.produce(topic, key=record.key, value=record.value, on_delivery=on_delivery)
                producer.poll(0)

            def flush() -> None:
                remaining = producer.flush(30)
                if remaining:
                    print(f"{remaining} messages still pending after flush", file=sys.stderr)
                if failures["count"]:
                    print(f"{failures['count']} messages failed delivery", file=sys.stderr)

        print(
            f"event_generator: rate={args.rate}/s topic={settings.kafka_topic_events} "
            f"v2_start={v2_start.isoformat()} "
            f"users={len(user_ids)} products={len(product_ids)}"
            f"{' (dry run)' if args.dry_run else ''}",
            flush=True,
        )
        stats = pump(
            factory,
            emit,
            user_ids=user_ids,
            product_ids=product_ids,
            base_rate=args.rate,
            duration=args.duration,
            tick_seconds=args.tick,
        )
        flush()
        print(
            f"done: ticks={stats.ticks} produced={stats.produced} "
            f"malformed={stats.malformed} duplicates={stats.duplicates}"
        )
        return 0
    except KeyboardInterrupt:
        print("\ninterrupted")
        if producer is not None:
            producer.flush(10)
        return 0
    except psycopg.Error as exc:
        print(f"database error: {exc}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
