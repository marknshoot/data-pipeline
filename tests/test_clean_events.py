"""Tests for the Spark clean layer.

These need a JVM, so they are marked ``spark`` and excluded from the default pytest
run (the host has no Java). Run them in the container:

    make spark-test

The lake is faked with pyarrow's local filesystem, so the job's real code path --
listing objects, downloading, cleaning, replacing partitions -- runs without S3.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from generators.config import get_settings
from ingestion import lake
from spark.clean_events import (
    CLEAN_PREFIX,
    MAX_LATENESS_HOURS,
    build_spark,
    clean_frame,
    object_partition,
    run,
    select_objects,
    window_start_for,
    with_event_partitions,
)

pytestmark = pytest.mark.spark

RAW_SCHEMA = pa.schema(
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


@pytest.fixture(scope="session")
def spark():
    session = build_spark(master="local[1]", driver_memory="1g", shuffle_partitions=2)
    yield session
    session.stop()


@pytest.fixture
def fake_lake(tmp_path, monkeypatch):
    """A local stand-in for the S3 lake: returns (filesystem, settings).

    pyarrow's LocalFileSystem rejects absolute paths, so the fake bucket is relative
    and the test runs from a temporary working directory.
    """
    import pyarrow.fs as pafs

    monkeypatch.chdir(tmp_path)
    settings = dataclasses.replace(get_settings(), s3_bucket="lake")
    return pafs.LocalFileSystem(), settings


def raw_row(**overrides):
    row = {
        "event_id": "evt-1",
        "schema_version": 1,
        "event_type": "page_view",
        "user_id": 42,
        "session_id": "sess-1",
        "product_id": 7,
        "event_time": datetime(2026, 10, 2, 10, 30, tzinfo=UTC),
        "city": "Jakarta",
        "device": "mobile",
        "device_type": None,
        "platform_version": None,
        "consumed_at": datetime(2026, 10, 2, 10, 31, tzinfo=UTC),
    }
    row.update(overrides)
    return row


def write_raw_object(settings, filesystem, key, rows):
    lake.write_parquet(
        pa.Table.from_pylist(rows, schema=RAW_SCHEMA),
        key,
        bucket=settings.s3_bucket,
        filesystem=filesystem,
    )


def read_clean(settings, filesystem, prefix=CLEAN_PREFIX):
    """Every row under clean/events, with its partition path."""
    import pyarrow.parquet as pq

    out = []
    for key in lake.list_keys(prefix, bucket=settings.s3_bucket, filesystem=filesystem):
        table = pq.read_table(f"{settings.s3_bucket}/{key}", filesystem=filesystem)
        for row in table.to_pylist():
            out.append({**row, "_key": key})
    return out


# ------------------------------------------------------------ window planning


def test_object_partition_parses_the_consume_hour():
    key = "raw/events/dt=2026-10-02/hr=11/part=3-100-199.parquet"
    assert object_partition(key) == datetime(2026, 10, 2, 11, 0, tzinfo=UTC)
    assert object_partition("something/else.parquet") is None


def test_window_start_snaps_to_a_whole_hour():
    now = datetime(2026, 10, 2, 11, 47, 13, tzinfo=UTC)
    assert window_start_for(now, 3) == datetime(2026, 10, 2, 8, 0, tzinfo=UTC)


def test_select_objects_drops_partitions_before_the_window():
    keys = [
        "raw/events/dt=2026-10-02/hr=07/part=0-0-9.parquet",
        "raw/events/dt=2026-10-02/hr=08/part=0-10-19.parquet",
        "raw/events/dt=2026-10-02/hr=11/part=0-20-29.parquet",
    ]
    selected = select_objects(keys, datetime(2026, 10, 2, 8, 30, tzinfo=UTC))
    # hr=07 cannot hold an event at or after 08:30 (an event is never consumed before
    # it happens), and hr=08 is included because the window is a whole hour.
    assert selected == keys[1:]


def test_select_objects_keeps_unparseable_keys():
    keys = ["raw/events/weird-name.parquet"]
    assert select_objects(keys, datetime(2026, 10, 2, tzinfo=UTC)) == keys


# --------------------------------------------------------------- transforms


def test_clean_frame_merges_schema_v1_and_v2(spark):
    frame = spark.createDataFrame(
        [
            raw_row(event_id="v1", schema_version=1, device="android", device_type=None),
            raw_row(
                event_id="v2",
                schema_version=2,
                device=None,
                device_type="ios",
                platform_version="3.4",
            ),
        ],
        schema="event_id string, schema_version int, event_type string, user_id long, "
        "session_id string, product_id long, event_time timestamp, city string, "
        "device string, device_type string, platform_version string, consumed_at timestamp",
    )
    rows = {row["event_id"]: row for row in clean_frame(frame).collect()}

    assert rows["v1"]["device_type"] == "android"
    assert rows["v2"]["device_type"] == "ios"
    assert rows["v2"]["platform_version"] == "3.4"


def test_clean_frame_dedups_keeping_the_newest_delivery(spark):
    first = datetime(2026, 10, 2, 10, 31, tzinfo=UTC)
    frame = spark.createDataFrame(
        [
            raw_row(event_id="dup", city="Jakarta", consumed_at=first),
            raw_row(
                event_id="dup",
                city="Bandung",
                consumed_at=first + timedelta(minutes=5),
            ),
        ],
        schema="event_id string, schema_version int, event_type string, user_id long, "
        "session_id string, product_id long, event_time timestamp, city string, "
        "device string, device_type string, platform_version string, consumed_at timestamp",
    )
    rows = clean_frame(frame).collect()

    assert len(rows) == 1
    assert rows[0]["city"] == "Bandung"


def test_clean_frame_drops_unusable_rows(spark):
    frame = spark.createDataFrame(
        [
            raw_row(event_id=None),
            raw_row(event_id="blank-type", event_type="   "),
            raw_row(event_id="good"),
        ],
        schema="event_id string, schema_version int, event_type string, user_id long, "
        "session_id string, product_id long, event_time timestamp, city string, "
        "device string, device_type string, platform_version string, consumed_at timestamp",
    )
    assert [row["event_id"] for row in clean_frame(frame).collect()] == ["good"]


def test_clean_frame_normalises_event_type_and_casts(spark):
    frame = spark.createDataFrame(
        [raw_row(event_id="x", event_type="  PURCHASE ", user_id="42")],
        schema="event_id string, schema_version int, event_type string, user_id string, "
        "session_id string, product_id long, event_time timestamp, city string, "
        "device string, device_type string, platform_version string, consumed_at timestamp",
    )
    row = clean_frame(frame).collect()[0]
    assert row["event_type"] == "purchase"
    assert row["user_id"] == 42
    assert isinstance(row["user_id"], int)


def test_with_event_partitions_formats_dt_and_hr(spark):
    frame = spark.createDataFrame(
        [raw_row(event_time=datetime(2026, 10, 2, 7, 5, tzinfo=UTC))],
        schema="event_id string, schema_version int, event_type string, user_id long, "
        "session_id string, product_id long, event_time timestamp, city string, "
        "device string, device_type string, platform_version string, consumed_at timestamp",
    )
    row = with_event_partitions(frame).collect()[0]
    assert row["dt"] == "2026-10-02"
    assert row["hr"] == "07"


# ------------------------------------------------------------------ end to end


def test_run_writes_event_time_partitions(spark, fake_lake, tmp_path):
    filesystem, settings = fake_lake
    write_raw_object(
        settings,
        filesystem,
        "raw/events/dt=2026-10-02/hr=11/part=0-0-9.parquet",
        [
            raw_row(event_id="a", event_time=datetime(2026, 10, 2, 10, 5, tzinfo=UTC)),
            raw_row(event_id="b", event_time=datetime(2026, 10, 2, 11, 5, tzinfo=UTC)),
        ],
    )

    result = run(
        full=True,
        settings=settings,
        spark=spark,
        work_dir=tmp_path / "work",
        filesystem=filesystem,
        log=lambda _: None,
    )

    assert result.input_rows == 2
    assert result.output_rows == 2
    assert result.partitions_written == 2

    rows = read_clean(settings, filesystem)
    # Partitioned by event_time, not by the consume hour both rows arrived in.
    # Assert on the partition path: Spark's part-file names carry a UUID suffix.
    assert {row["_key"].rsplit("/", 1)[0] for row in rows} == {
        "clean/events/dt=2026-10-02/hr=10",
        "clean/events/dt=2026-10-02/hr=11",
    }


def test_run_removes_duplicate_deliveries(spark, fake_lake, tmp_path):
    filesystem, settings = fake_lake
    later = datetime(2026, 10, 2, 10, 35, tzinfo=UTC)
    write_raw_object(
        settings,
        filesystem,
        "raw/events/dt=2026-10-02/hr=11/part=0-0-9.parquet",
        [
            raw_row(event_id="dup", consumed_at=datetime(2026, 10, 2, 10, 31, tzinfo=UTC)),
            raw_row(event_id="dup", consumed_at=later, city="Bandung"),
        ],
    )

    result = run(
        full=True,
        settings=settings,
        spark=spark,
        work_dir=tmp_path / "work",
        filesystem=filesystem,
        log=lambda _: None,
    )

    assert result.input_rows == 2
    assert result.output_rows == 1
    assert result.duplicates_removed == 1
    rows = read_clean(settings, filesystem)
    assert len(rows) == 1
    assert rows[0]["city"] == "Bandung"


def test_run_replaces_a_partition_instead_of_appending(spark, fake_lake, tmp_path):
    """A second run must not leave the first run's files behind."""
    filesystem, settings = fake_lake
    key = "raw/events/dt=2026-10-02/hr=11/part=0-0-9.parquet"
    write_raw_object(settings, filesystem, key, [raw_row(event_id="a")])

    def clean():
        return run(
            full=True,
            settings=settings,
            spark=spark,
            work_dir=tmp_path / f"work-{len(read_clean(settings, filesystem))}",
            filesystem=filesystem,
            log=lambda _: None,
        )

    clean()
    objects_after_first = len(read_clean(settings, filesystem))
    clean()
    objects_after_second = len(read_clean(settings, filesystem))

    assert objects_after_first == objects_after_second == 1


def test_run_only_touches_partitions_inside_the_window(spark, fake_lake, tmp_path):
    filesystem, settings = fake_lake
    old = datetime(2026, 10, 2, 2, 0, tzinfo=UTC)
    recent = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    write_raw_object(
        settings,
        filesystem,
        "raw/events/dt=2026-10-02/hr=02/part=0-0-9.parquet",
        [raw_row(event_id="old", event_time=old)],
    )
    write_raw_object(
        settings,
        filesystem,
        "raw/events/dt=2026-10-02/hr=11/part=0-10-19.parquet",
        [raw_row(event_id="recent", event_time=recent)],
    )

    result = run(
        lookback_hours=3,
        now=datetime(2026, 10, 2, 11, 30, tzinfo=UTC),
        settings=settings,
        spark=spark,
        work_dir=tmp_path / "work",
        filesystem=filesystem,
        log=lambda _: None,
    )

    # The 02:00 event is outside the window: its partition is neither read nor written,
    # so a previous run's output for it survives untouched.
    assert result.objects_read == 1
    assert result.output_rows == 1
    assert {row["_key"].rsplit("/", 1)[0] for row in read_clean(settings, filesystem)} == {
        "clean/events/dt=2026-10-02/hr=10"
    }


def test_lookback_must_exceed_the_producers_lateness(spark, fake_lake):
    filesystem, settings = fake_lake
    with pytest.raises(ValueError, match="maximum\\s+lateness"):
        run(
            lookback_hours=MAX_LATENESS_HOURS,
            settings=settings,
            spark=spark,
            filesystem=filesystem,
        )


def test_run_reports_nothing_to_do_on_an_empty_lake(spark, fake_lake):
    filesystem, settings = fake_lake
    result = run(
        full=True,
        settings=settings,
        spark=spark,
        filesystem=filesystem,
        log=lambda _: None,
    )
    assert result.objects_read == 0
    assert result.output_rows == 0
