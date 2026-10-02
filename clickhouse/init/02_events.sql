-- Clickstream raw layer: one row per event, loaded from the lake's *clean* layer by
-- `warehouse.load_raw --tables events`.
--
-- The Spark job (spark/clean_events.py) owns deduplication by `event_id`, the v1/v2
-- schema merge and event-time partitioning, so this table reads a layer that is
-- already deduplicated. ReplacingMergeTree is kept anyway: the load appends every
-- object under clean/events/, and re-running it must stay idempotent.
--
-- Types mirror the Parquet schema written by spark/clean_events.py.

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
    device_type Nullable(String),
    platform_version Nullable(String),
    consumed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(consumed_at)
ORDER BY event_id;

-- `device` belonged to the raw layer: schema v1 sent it, v2 sent `device_type`, and
-- the clean layer now resolves the two into `device_type`. Dropping it here keeps an
-- existing installation convergent with a fresh one (`make ch-schema` is re-runnable).
ALTER TABLE raw.events DROP COLUMN IF EXISTS device;
