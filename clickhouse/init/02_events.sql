-- Clickstream raw layer: one row per event, loaded from the lake by
-- `warehouse.load_raw --tables events`.
--
-- The Kafka consumer is at-least-once (offsets are committed only after the S3
-- write succeeds), and the generator deliberately re-sends ~2% of events with an
-- identical `event_id`. The same event can therefore appear in several lake
-- objects. ReplacingMergeTree(consumed_at) keyed by `event_id` converges to one
-- row per event, exactly as raw.orders does for repeated `updated_at` versions.
-- Merges are asynchronous, so exact queries use FINAL (staging does this).
--
-- Schema v1 and v2 coexist in the same topic: v1 carries `device`, v2 carries
-- `device_type` plus `platform_version`. All three columns exist and are nullable;
-- staging coalesces them into a single `device_type`.
--
-- Types mirror the Parquet schema written by streaming/lake_consumer.py.

CREATE TABLE IF NOT EXISTS raw.events
(
    event_id String,
    schema_version Int32,
    event_type String,
    user_id Int64,
    session_id String,
    product_id Int64,
    event_time DateTime64(6, 'UTC'),
    city String,
    device Nullable(String),
    device_type Nullable(String),
    platform_version Nullable(String),
    consumed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(consumed_at)
ORDER BY event_id;
