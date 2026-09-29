-- Pipeline bookkeeping that is not part of the business data model.
-- Kept in its own schema so it never shows up as an OLTP source table.

CREATE SCHEMA IF NOT EXISTS pipeline_meta;

-- Last `updated_at` successfully extracted per source table. Used by the
-- standalone `make extract` (incremental) mode; Airflow drives the same job with
-- explicit data intervals instead, which is what makes backfills repeatable.
CREATE TABLE IF NOT EXISTS pipeline_meta.extract_state (
    table_name text        PRIMARY KEY,
    watermark  timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One row per extract run, for observability ("when did table X last load?").
CREATE TABLE IF NOT EXISTS pipeline_meta.extract_run (
    run_id      text        NOT NULL,
    table_name  text        NOT NULL,
    window_start timestamptz NOT NULL,
    window_end  timestamptz NOT NULL,
    row_count   bigint      NOT NULL,
    object_path text        NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, table_name)
);
