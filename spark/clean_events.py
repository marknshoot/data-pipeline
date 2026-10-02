"""Spark job that turns the lake's raw clickstream into its clean layer.

    s3://lake/raw/events/dt=.../hr=.../*.parquet   (what the consumer wrote)
        -> s3://lake/clean/events/dt=.../hr=.../   (deduplicated, typed, by event time)

Four things happen here that the consumer deliberately does not do:

1. **Deduplication by ``event_id``.** The consumer is at-least-once, so a restart can
   rewrite a batch, and the generator re-sends ~2% of events on purpose. Keeping the
   row with the newest ``consumed_at`` collapses both.
2. **Schema merge.** v1 carries ``device``, v2 carries ``device_type`` and
   ``platform_version``. The clean layer resolves them into one ``device_type``, so
   nothing downstream has to know the topic ever changed shape.
3. **Types as a contract.** The casts are defensive: the consumer already writes typed
   Parquet, but this is the layer that promises the types, so a producer that starts
   sending strings is absorbed here rather than in a mart.
4. **Event-time partitioning.** Raw objects are partitioned by *consume* time (a
   batch's flush hour), which is right for ingestion and useless for analytics. The
   clean layer partitions by ``event_time``, so a query for a day reads a day.

**Late events and the lookback window.** A run reprocesses the last N whole hours and
*replaces* those partitions. Two properties make that correct rather than merely
convenient:

* The window starts at an hour boundary, so the oldest partition it touches is
  recomputed from a complete set of events rather than a partial hour.
* Every object whose events could fall in the window is read. Because an event cannot
  be consumed before it happens, an event with ``event_time >= window_start`` must
  live in an object whose consume-hour is also ``>= window_start`` -- so selecting
  objects by their path is sufficient, and no raw file has to be opened to decide.

The lookback must exceed the producer's maximum lateness (2 hours by default), or an
event could arrive for a partition that has already been published and never be
reconsidered. ``run`` refuses to start otherwise.

Usage:
    python -m spark.clean_events --lookback-hours 3
    python -m spark.clean_events --full          # first run: every raw partition
"""

from __future__ import annotations

import argparse
import contextlib
import re
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from generators import db
from generators.config import Settings, get_settings
from ingestion import lake

# The consumer's raw schema. Stated explicitly rather than inferred: v1 files have no
# `device_type` and v2 files have no `device`, and an explicit schema makes Spark fill
# the absent column with null instead of depending on parquet schema merging.
RAW_EVENT_SCHEMA = (
    "event_id string, schema_version int, event_type string, user_id long, "
    "session_id string, product_id long, event_time timestamp, city string, "
    "device string, device_type string, platform_version string, consumed_at timestamp"
)

# What the clean layer guarantees, in this order (it matches
# warehouse.load_raw.EVENTS_COLUMNS).
CLEAN_COLUMNS: tuple[str, ...] = (
    "event_id",
    "schema_version",
    "event_type",
    "user_id",
    "session_id",
    "product_id",
    "event_time",
    "city",
    "device_type",
    "platform_version",
    "consumed_at",
)

RAW_PREFIX = "raw/events"
CLEAN_PREFIX = "clean/events"

# The producer's maximum lateness (generators.event_factory.ClickstreamFactory).
MAX_LATENESS_HOURS = 2

# raw/events/dt=2026-10-02/hr=11/part=3-100-199.parquet
_PARTITION_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hr=(\d{2})")


@dataclass(frozen=True)
class CleanResult:
    window_start: datetime
    window_end: datetime
    objects_read: int
    input_rows: int
    output_rows: int
    duplicates_removed: int
    partitions_written: int
    seconds: float

    def as_dict(self) -> dict:
        return asdict(self)


# ------------------------------------------------------------------- planning


def object_partition(key: str) -> datetime | None:
    """The consume-time hour encoded in a raw object's key, or None if absent."""
    match = _PARTITION_RE.search(key)
    if match is None:
        return None
    day, hour = match.groups()
    return datetime.fromisoformat(f"{day}T{hour}:00:00+00:00")


def window_start_for(now: datetime, lookback_hours: float) -> datetime:
    """Start of the reprocessing window: a whole hour, ``lookback_hours`` back."""
    return (now - timedelta(hours=lookback_hours)).replace(minute=0, second=0, microsecond=0)


def select_objects(keys: list[str], since: datetime) -> list[str]:
    """Raw objects that can hold an event with ``event_time >= since``.

    An event cannot be consumed before it happens, so its object's consume-hour is
    never earlier than its own hour. Selecting on the path is therefore enough, and no
    file has to be opened to make the decision.
    """
    floor = since.replace(minute=0, second=0, microsecond=0)
    selected = []
    for key in keys:
        partition = object_partition(key)
        if partition is None or partition >= floor:
            # An unparseable key is kept: correctness first, and the filter on
            # event_time below still excludes anything out of window.
            selected.append(key)
    return sorted(selected)


# --------------------------------------------------------------- transforms


def clean_frame(frame: DataFrame) -> DataFrame:
    """Merge the two schema versions, cast the types, and deduplicate by event_id."""
    typed = frame.select(
        F.col("event_id").cast("string").alias("event_id"),
        F.col("schema_version").cast("int").alias("schema_version"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type"),
        F.col("user_id").cast("long").alias("user_id"),
        F.col("session_id").cast("string").alias("session_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("event_time").cast("timestamp").alias("event_time"),
        F.trim(F.col("city")).alias("city"),
        # The schema merge: v1's `device` and v2's `device_type` become one column.
        F.coalesce(
            F.nullif(F.col("device_type"), F.lit("")),
            F.nullif(F.col("device"), F.lit("")),
        ).alias("device_type"),
        F.nullif(F.col("platform_version"), F.lit("")).alias("platform_version"),
        F.col("consumed_at").cast("timestamp").alias("consumed_at"),
    ).where(F.col("event_id").isNotNull() & (F.col("event_type") != ""))

    # One row per event, keeping the newest delivery. Ties cannot matter: duplicate
    # deliveries of the same event_id are byte-identical.
    newest = Window.partitionBy("event_id").orderBy(F.col("consumed_at").desc())
    return (
        typed.withColumn("_delivery_rank", F.row_number().over(newest))
        .where(F.col("_delivery_rank") == 1)
        .drop("_delivery_rank")
    )


def with_event_partitions(frame: DataFrame) -> DataFrame:
    """Add the event-time partition columns the clean layer is laid out by."""
    return frame.withColumn("dt", F.date_format("event_time", "yyyy-MM-dd")).withColumn(
        "hr", F.lpad(F.hour("event_time").cast("string"), 2, "0")
    )


# ------------------------------------------------------------------- spark


def build_spark(
    app_name: str = "shopstream-clean-events",
    master: str = "local[2]",
    driver_memory: str = "1g",
    shuffle_partitions: int = 4,
) -> SparkSession:
    """A local SparkSession sized for a laptop.

    The UI is off because this runs inside the Airflow container, where binding a
    dashboard port is noise; the timezone is pinned so ``date_format`` and ``hour``
    agree with the UTC timestamps in the data.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# -------------------------------------------------------------------- lake io


def download_objects(keys: list[str], target: Path, settings: Settings, filesystem=None) -> Path:
    """Copy raw objects to local disk for Spark to read.

    Spark's own S3 connector (s3a) would mean shipping hadoop-aws and the AWS SDK
    into the image -- a few hundred megabytes of jars to move a handful of small
    files. The project already has a lake client (pyarrow, used by the extractor and
    the loader), so the transfer stays in one place and Spark does the processing.
    """
    fs = filesystem or lake.build_filesystem(settings)
    target.mkdir(parents=True, exist_ok=True)
    for key in keys:
        source = f"{settings.s3_bucket}/{key}"
        # Flatten the key into a filename: the partitions are in the path, and Spark
        # reads the directory as one dataset.
        destination = target / key.replace("/", "__")
        with fs.open_input_stream(source) as reader, destination.open("wb") as writer:
            shutil.copyfileobj(reader, writer)
    return target


def publish_partitions(
    local_dir: Path,
    settings: Settings,
    filesystem=None,
) -> list[str]:
    """Replace each cleaned partition in the lake and return the keys written.

    A partition is deleted before it is written, not merely overwritten: if a rerun
    produces fewer files, a merge would leave the previous run's files behind and
    silently double-count.
    """
    fs = filesystem or lake.build_filesystem(settings)
    bucket = settings.s3_bucket
    written: list[str] = []

    for dt_dir in sorted(local_dir.glob("dt=*")):
        for hr_dir in sorted(dt_dir.glob("hr=*")):
            prefix = f"{CLEAN_PREFIX}/{dt_dir.name}/{hr_dir.name}"
            # The prefix may not exist yet on a first run.
            with contextlib.suppress(FileNotFoundError):
                fs.delete_dir(f"{bucket}/{prefix}")
            for part in sorted(hr_dir.glob("*.parquet")):
                key = f"{prefix}/{part.name}"
                lake.write_parquet(pq.read_table(part), key, bucket=bucket, filesystem=fs)
                written.append(key)
    return written


# --------------------------------------------------------------------- run


def run(
    *,
    lookback_hours: float = 3.0,
    full: bool = False,
    now: datetime | None = None,
    settings: Settings | None = None,
    spark: SparkSession | None = None,
    work_dir: Path | None = None,
    filesystem=None,
    log=print,
) -> CleanResult:
    """Clean the raw clickstream into the lake's clean layer.

    Reprocesses the last ``lookback_hours`` whole hours and replaces those partitions;
    ``full=True`` reprocesses everything, which is what the first run needs.
    """
    if not full and lookback_hours <= MAX_LATENESS_HOURS:
        raise ValueError(
            f"lookback_hours ({lookback_hours}) must exceed the producer's maximum "
            f"lateness ({MAX_LATENESS_HOURS}h), or a late event could land in a "
            "partition that was already published"
        )

    settings = settings or get_settings()
    fs = filesystem or lake.build_filesystem(settings)
    now = now or datetime.now(UTC)
    started = time.monotonic()

    if full:
        window_start = datetime(1970, 1, 1, tzinfo=UTC)
    else:
        window_start = window_start_for(now, lookback_hours)

    keys = select_objects(
        lake.list_keys(RAW_PREFIX, bucket=settings.s3_bucket, filesystem=fs), window_start
    )
    if not keys:
        log(f"no raw objects at or after {window_start.isoformat()}; nothing to clean")
        return CleanResult(window_start, now, 0, 0, 0, 0, 0, time.monotonic() - started)

    own_spark = spark is None
    spark = spark or build_spark()
    work = Path(work_dir or tempfile.mkdtemp(prefix="spark-clean-"))
    try:
        download_objects(keys, work / "in", settings, fs)
        log(f"read {len(keys)} raw object(s) into {work / 'in'}")

        frame = spark.read.schema(RAW_EVENT_SCHEMA).parquet(str(work / "in"))
        # Filter on the partition boundary, not on `window_start` itself: the oldest
        # hour in the window must be rebuilt from all of its events, not just those
        # after the minute the job happened to run.
        frame = frame.where(F.col("event_time") >= F.lit(window_start))
        input_rows = frame.count()

        cleaned = with_event_partitions(clean_frame(frame))
        output_rows = cleaned.count()
        duplicates_removed = input_rows - output_rows

        out = work / "out"
        # Spark does the partitioning and the write; the lake upload below decides
        # which partitions get replaced.
        cleaned.write.mode("overwrite").partitionBy("dt", "hr").parquet(str(out))
        written = publish_partitions(out, settings, fs)

        partitions = len({key.rsplit("/", 1)[0] for key in written})
        result = CleanResult(
            window_start=window_start,
            window_end=now,
            objects_read=len(keys),
            input_rows=input_rows,
            output_rows=output_rows,
            duplicates_removed=duplicates_removed,
            partitions_written=partitions,
            seconds=time.monotonic() - started,
        )
        log(
            f"clean: {input_rows:,} in -> {output_rows:,} out "
            f"({duplicates_removed:,} duplicate deliveries removed), "
            f"{partitions} partition(s), {result.seconds:.1f}s"
        )
        return result
    finally:
        if work_dir is None:
            shutil.rmtree(work, ignore_errors=True)
        if own_spark:
            spark.stop()


def record_run(settings: Settings, run_id: str, result: CleanResult) -> None:
    """Report the run to Postgres. Non-fatal: losing a metrics row is not a failure."""
    try:
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_meta.clean_run
                    (run_id, window_start, window_end, input_rows, output_rows,
                     duplicates_removed, partitions_written, objects_read, seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    result.window_start,
                    result.window_end,
                    result.input_rows,
                    result.output_rows,
                    result.duplicates_removed,
                    result.partitions_written,
                    result.objects_read,
                    round(result.seconds, 2),
                ),
            )
    except psycopg.Error as exc:
        print(f"warning: could not record run in pipeline_meta: {exc}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Spark: raw clickstream -> clean layer")
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=3.0,
        help=f"whole hours to reprocess (must exceed {MAX_LATENESS_HOURS}h)",
    )
    parser.add_argument("--full", action="store_true", help="reprocess every partition")
    parser.add_argument("--driver-memory", default="1g", help="Spark driver memory")
    parser.add_argument("--master", default="local[2]", help="Spark master URL")
    parser.add_argument("--no-metrics", action="store_true", help="skip the Postgres record")
    args = parser.parse_args(argv)

    settings = get_settings()
    spark = build_spark(driver_memory=args.driver_memory, master=args.master)
    try:
        result = run(
            lookback_hours=args.lookback_hours,
            full=args.full,
            settings=settings,
            spark=spark,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        spark.stop()

    if not args.no_metrics:
        record_run(settings, uuid.uuid4().hex[:12], result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
