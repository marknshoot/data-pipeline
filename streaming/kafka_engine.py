"""Provision ClickHouse's own Kafka consumption, for the "live" dashboard panel.

This is the second, independent path from Kafka: the lake consumer is the durable,
replayable route into the warehouse, while this Kafka-engine table feeds a dashboard
that can show activity *now* without waiting for a load. Both read the same topic
under different consumer groups, which is the point of keeping the raw log around:
one consumer can be slow and batchy, the other fast and lossy, and neither interferes
with the other.

The target table is **event-grain** (ReplacingMergeTree by ``event_id``), not a
pre-aggregated per-minute counter, and the per-minute views aggregate on read. That
choice is deliberate: ClickHouse's Kafka engine is at-least-once, so a rebalance
re-reads from the last committed offset. With a SummingMergeTree minute counter those
redeliveries are added twice and the panel over-reports (observed: 11,427 events from
8,724 messages). Keying on ``event_id`` makes redelivery idempotent instead, exactly
as ``raw.events`` does for the lake path, and ``FINAL`` in the views collapses the
duplicates.

Everything here is DROP-then-CREATE in dependency order, so the file is safe to
re-run. Recreating the table resets its consumer group, so the panel replays the
topic before going live.

Usage:
    uv run python -m streaming.kafka_engine
    uv run python -m streaming.kafka_engine --verify
"""

from __future__ import annotations

import argparse
import sys

from clickhouse_connect.driver.client import Client

from generators.config import Settings, get_settings
from warehouse.clickhouse import get_client

DATABASE = "analytics"
KAFKA_TABLE = "events_stream"
EVENTS_TABLE = "rt_events"
EVENTS_MV = "rt_events_mv"
MINUTES_VIEW = "rt_events_per_minute"
ORDERS_VIEW = "rt_orders_per_minute"

# One consumer per partition: ClickHouse reads them in parallel within this group.
NUM_CONSUMERS = 6
# Corrupted payloads are skipped rather than killing the consumer; the lake consumer
# is the one that preserves them in dlq/events/.
SKIP_BROKEN_MESSAGES = 1000

# The funnel steps the live panel cares about; anything else is dropped at the MV.
EVENT_TYPES = ("page_view", "add_to_cart", "checkout", "purchase")

# NOTE: there is deliberately no `kafka_auto_offset_reset` -- ClickHouse 26.3 removed
# that table setting. A fresh consumer group starts at the beginning of the topic, so
# the materialized view catches up and then goes live. That suits a demo (the panel is
# populated immediately), and recreating the table replays the topic by design.


def ddl(settings: Settings) -> list[str]:
    """Statements in dependency order: drop views before their sources."""
    broker = settings.kafka_internal_broker
    topic = settings.kafka_topic_events
    event_types = ", ".join(f"'{name}'" for name in EVENT_TYPES)
    return [
        # --- drops, in reverse dependency order ---
        f"DROP VIEW IF EXISTS {DATABASE}.{ORDERS_VIEW}",
        f"DROP VIEW IF EXISTS {DATABASE}.{MINUTES_VIEW}",
        f"DROP TABLE IF EXISTS {DATABASE}.{EVENTS_MV}",
        f"DROP TABLE IF EXISTS {DATABASE}.{KAFKA_TABLE}",
        # The target must be dropped too, or a second run fails with
        # TABLE_ALREADY_EXISTS *after* having dropped the MV -- leaving the live
        # path half-built.
        f"DROP TABLE IF EXISTS {DATABASE}.{EVENTS_TABLE}",
        # --- the Kafka source ---
        # Every column is String: the topic mixes schema v1 and v2, and one missing
        # field must not fail the insert. Typing happens in the materialized view.
        f"""
        CREATE TABLE {DATABASE}.{KAFKA_TABLE}
        (
            event_id String,
            schema_version String,
            event_type String,
            user_id String,
            session_id String,
            product_id String,
            event_time String,
            city String,
            device String,
            device_type String,
            platform_version String
        )
        ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '{broker}',
            kafka_topic_list = '{topic}',
            kafka_group_name = 'ch_live_events',
            kafka_format = 'JSONEachRow',
            kafka_num_consumers = {NUM_CONSUMERS},
            kafka_skip_broken_messages = {SKIP_BROKEN_MESSAGES}
        """,
        # --- event-grain target, deduplicated by event_id ---
        f"""
        CREATE TABLE {DATABASE}.{EVENTS_TABLE}
        (
            event_id String,
            event_time DateTime64(3, 'UTC'),
            event_type LowCardinality(String),
            user_id Int64,
            session_id String,
            city String,
            device_type String,
            ingested_at DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY event_id
        """,
        # The generator sends ISO-8601 UTC with a trailing 'Z'; the second parse
        # covers a value with the suffix stripped, and the fallback keeps a record
        # with an unparseable timestamp out of 1970.
        f"""
        CREATE MATERIALIZED VIEW {DATABASE}.{EVENTS_MV}
        TO {DATABASE}.{EVENTS_TABLE}
        AS
        SELECT
            event_id,
            coalesce(
                parseDateTime64BestEffortOrNull(event_time),
                parseDateTime64BestEffortOrNull(replaceAll(event_time, 'Z', '')),
                now64(3, 'UTC')
            ) AS event_time,
            event_type,
            toInt64OrZero(user_id) AS user_id,
            session_id,
            city,
            if(device_type != '', device_type, device) AS device_type,
            now64(3, 'UTC') AS ingested_at
        FROM {DATABASE}.{KAFKA_TABLE}
        WHERE event_id != '' AND event_type IN ({event_types})
        """,
        # --- read-time aggregation; FINAL collapses any redelivered events ---
        f"""
        CREATE VIEW {DATABASE}.{MINUTES_VIEW}
        AS
        SELECT
            toStartOfMinute(event_time) AS minute,
            event_type,
            toUInt64(count()) AS events
        FROM {DATABASE}.{EVENTS_TABLE} FINAL
        GROUP BY minute, event_type
        """,
        # The plan's "orders per minute": the purchase step of the clickstream. It is
        # a view, not a table, so it can never drift from the numbers above.
        f"""
        CREATE VIEW {DATABASE}.{ORDERS_VIEW}
        AS
        SELECT
            toStartOfMinute(event_time) AS minute,
            toUInt64(count()) AS orders
        FROM {DATABASE}.{EVENTS_TABLE} FINAL
        WHERE event_type = 'purchase'
        GROUP BY minute
        """,
    ]


def apply(client: Client, settings: Settings) -> None:
    for statement in ddl(settings):
        client.command(statement)


def verify(client: Client) -> bool:
    """Report whether the live path is wired up; return False if it is not."""
    tables = {
        row[0]
        for row in client.query(
            "SELECT name FROM system.tables WHERE database = {db:String}",
            parameters={"db": DATABASE},
        ).result_rows
    }
    missing = {KAFKA_TABLE, EVENTS_TABLE, EVENTS_MV, MINUTES_VIEW, ORDERS_VIEW} - tables
    if missing:
        print(f"missing objects: {', '.join(sorted(missing))}", file=sys.stderr)
        return False

    engine = client.query(
        "SELECT engine FROM system.tables WHERE database = {db:String} AND name = {name:String}",
        parameters={"db": DATABASE, "name": KAFKA_TABLE},
    ).first_row
    print(f"{DATABASE}.{KAFKA_TABLE}: engine={engine[0]}")

    errors: list[str] = []
    try:
        consumers = client.query(
            "SELECT table, num_messages_read, exceptions.text, exceptions.time "
            "FROM system.kafka_consumers WHERE database = {db:String}",
            parameters={"db": DATABASE},
        ).result_rows
        total_read = 0
        for table, messages, texts, times in consumers:
            total_read += messages or 0
            failures = [t for t in (texts or []) if t]
            if failures:
                errors.append(f"{table}: {failures[-1]}")
                when = (times or [None])[-1]
                print(f"  consumer {table}: read={messages}, last error at {when}: {failures[-1]}")
        print(f"  consumers={len(consumers)} messages read={total_read}")
    except Exception as exc:  # noqa: BLE001 - the table's columns are version-dependent
        print(f"  (could not read system.kafka_consumers: {exc})")

    # FINAL so a redelivered event is counted once, matching what the views return.
    counts = client.query(
        f"SELECT count(), uniqExact(event_id), uniqExact(session_id) "
        f"FROM {DATABASE}.{EVENTS_TABLE} FINAL"
    ).first_row
    print(f"{DATABASE}.{EVENTS_TABLE}: {counts[0]} row(s), {counts[1]} distinct event(s)")
    print(f"{DATABASE}.{EVENTS_TABLE} sessions: {counts[2]}")
    return not errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create ClickHouse Kafka engine + live views")
    parser.add_argument("--verify", action="store_true", help="check instead of applying")
    args = parser.parse_args(argv)

    settings = get_settings()
    try:
        client = get_client(settings)
        if args.verify:
            return 0 if verify(client) else 1
        apply(client, settings)
        print(
            f"created {DATABASE}.{KAFKA_TABLE} engine=Kafka topic={settings.kafka_topic_events} "
            f"broker={settings.kafka_internal_broker} consumers={NUM_CONSUMERS}"
        )
        print(f"created {DATABASE}.{EVENTS_TABLE} (ReplacingMergeTree by event_id) + {EVENTS_MV}")
        print(f"created {DATABASE}.{MINUTES_VIEW} and {DATABASE}.{ORDERS_VIEW}")
        return 0
    except Exception as exc:  # noqa: BLE001 - surface any client/HTTP error to the CLI
        print(f"failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
