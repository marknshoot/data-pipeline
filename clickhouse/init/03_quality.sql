-- Data-quality results, written by monitoring/quality.py and read by the
-- "Pipeline Health" Metabase dashboard.
--
-- These tables are append-only snapshots: every run adds a `checked_at` batch, so the
-- panel can show both the current state and the trend. Nothing is updated in place,
-- which means a run that fails to write leaves the previous snapshot intact rather
-- than blanking the panel.
--
-- The DDL lives here as the single source of truth, and monitoring/quality.py applies
-- it (idempotently) before writing, so the job works on a fresh warehouse without
-- `make ch-schema` having been run first.

-- One row per entity per day (or per entity, for whole-table checks), comparing what
-- the source holds against what the warehouse holds.
CREATE TABLE IF NOT EXISTS analytics.dq_row_counts
(
    checked_at DateTime64(3, 'UTC'),
    entity LowCardinality(String),
    grain LowCardinality(String),
    -- NULL for whole-table checks, which have no meaningful date.
    check_date Nullable(Date),
    source_count Int64,
    warehouse_count Int64,
    difference Int64,
    status LowCardinality(String)
)
ENGINE = MergeTree
-- check_date is deliberately not in the sorting key: it is Nullable for whole-table
-- checks, and ClickHouse rejects a nullable sorting key unless `allow_nullable_key`
-- is enabled. Every row in a batch shares `checked_at`, so the finer order buys
-- nothing here.
ORDER BY (checked_at, entity);

-- The health snapshot, one row per (component, entity, metric). Narrow rather than
-- wide because the metrics are heterogeneous -- an age in hours, a row count, a
-- duration in seconds -- and a wide table would need a column per metric plus a
-- status per metric. `metric_value` rather than `value`, which is a SQL keyword.
CREATE TABLE IF NOT EXISTS analytics.dq_health
(
    checked_at DateTime64(3, 'UTC'),
    component LowCardinality(String),
    entity String,
    metric LowCardinality(String),
    metric_value Float64,
    status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (checked_at, component, entity, metric);

-- What a panel actually reads: the newest snapshot only. Both tables are aliased
-- because the correlated subquery reads the same table as the outer query.
CREATE OR REPLACE VIEW analytics.dq_health_latest AS
SELECT
    h.component,
    h.entity,
    h.metric,
    h.metric_value,
    h.status,
    h.checked_at
FROM analytics.dq_health AS h
WHERE h.checked_at = (SELECT MAX(h2.checked_at) FROM analytics.dq_health AS h2);

CREATE OR REPLACE VIEW analytics.dq_row_counts_latest AS
SELECT
    r.entity,
    r.grain,
    r.check_date,
    r.source_count,
    r.warehouse_count,
    r.difference,
    r.status,
    r.checked_at
FROM analytics.dq_row_counts AS r
WHERE r.checked_at = (SELECT MAX(r2.checked_at) FROM analytics.dq_row_counts AS r2);
