-- Streaming bookkeeping. The consumer reports one row per run so the lake's DLQ
-- count and consumer throughput are queryable without reading S3 (Phase 7's
-- pipeline-health panel reads this table).

CREATE TABLE IF NOT EXISTS pipeline_meta.stream_run (
    run_id       text        PRIMARY KEY,
    group_id     text        NOT NULL,
    topic        text        NOT NULL,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL DEFAULT now(),
    records      bigint      NOT NULL DEFAULT 0,
    malformed    bigint      NOT NULL DEFAULT 0,
    batches      integer     NOT NULL DEFAULT 0,
    objects      integer     NOT NULL DEFAULT 0,
    status       text        NOT NULL DEFAULT 'ok'
);

CREATE INDEX IF NOT EXISTS stream_run_started_at_idx
    ON pipeline_meta.stream_run (started_at DESC);
