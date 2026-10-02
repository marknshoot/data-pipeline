-- Spark job bookkeeping, so a run's window and row counts are queryable next to the
-- streaming runs (Phase 7's pipeline-health panel reads both).

CREATE TABLE IF NOT EXISTS pipeline_meta.clean_run (
    run_id             text        PRIMARY KEY,
    window_start       timestamptz NOT NULL,
    window_end         timestamptz NOT NULL,
    input_rows         bigint      NOT NULL,
    output_rows        bigint      NOT NULL,
    duplicates_removed bigint      NOT NULL,
    partitions_written integer     NOT NULL,
    objects_read       integer     NOT NULL,
    seconds            numeric(10, 2) NOT NULL,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS clean_run_recorded_at_idx
    ON pipeline_meta.clean_run (recorded_at DESC);
