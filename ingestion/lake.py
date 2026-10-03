"""Helpers for the S3-compatible data lake (SeaweedFS locally, S3 in the cloud).

Nothing here knows about a specific job: it builds a pyarrow filesystem from the
env-driven settings and renders the lake's object layout, so the same code works
against SeaweedFS or real S3 by changing ``S3_ENDPOINT`` alone.

Layout:
    s3://<bucket>/raw/oltp/<table>/dt=YYYY-MM-DD/<run_id>.parquet
    s3://<bucket>/raw/events/dt=YYYY-MM-DD/hr=HH/<batch>.parquet      (Phase 5)
    s3://<bucket>/clean/events/dt=YYYY-MM-DD/hr=HH/part-*.parquet     (Phase 6)
    s3://<bucket>/dlq/events/dt=YYYY-MM-DD/                           (Phase 5)
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, date, datetime
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from generators.config import Settings, get_settings


def parse_endpoint(endpoint: str) -> tuple[str, str]:
    """Split ``http://host:port`` into ``("host:port", "http")`` for pyarrow."""
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    scheme = parsed.scheme or "http"
    if scheme not in {"http", "https"}:
        raise ValueError(f"unsupported S3 endpoint scheme: {scheme!r}")
    host = parsed.netloc or parsed.path
    return host, scheme


def build_filesystem(settings: Settings | None = None) -> pafs.S3FileSystem:
    settings = settings or get_settings()
    host, scheme = parse_endpoint(settings.s3_endpoint)
    return pafs.S3FileSystem(
        endpoint_override=host,
        scheme=scheme,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
        # SeaweedFS (and MinIO) serve path-style requests.
        force_virtual_addressing=False,
    )


def raw_oltp_key(table: str, partition_date: date, run_id: str) -> str:
    """Object key (including bucket) for one OLTP extract."""
    return f"raw/oltp/{table}/dt={partition_date.isoformat()}/{run_id}.parquet"


def write_parquet(
    table: pa.Table,
    key: str,
    *,
    bucket: str,
    filesystem: pafs.FileSystem | None = None,
) -> str:
    """Write ``table`` to ``bucket/key``, overwriting any existing object."""
    fs = filesystem or build_filesystem()
    target = f"{bucket}/{key}"
    if isinstance(fs, pafs.LocalFileSystem):
        # Object stores have no real directories; a local FS needs them made first.
        fs.create_dir(target.rsplit("/", 1)[0], recursive=True)
    pq.write_table(table, target, filesystem=fs, compression="snappy")
    return f"s3://{bucket}/{key}"


def partition_date_for(ts: datetime) -> date:
    """Lake partitions are keyed by the UTC calendar day of the window end."""
    return ts.astimezone(UTC).date()


def list_keys(prefix: str = "", *, bucket: str | None = None, filesystem=None) -> list[str]:
    """List object keys under ``prefix`` (debugging helper).

    Returns an empty list when the prefix has never been written: callers treat
    "no objects" as a skip, and a missing prefix is not an error condition.
    """
    fs = filesystem or build_filesystem()
    # Only consult the environment when the bucket was not supplied: a caller that
    # passes both a filesystem and a bucket should not need a populated .env (the
    # Spark tests fake the lake and must not depend on one).
    target_bucket = bucket if bucket is not None else get_settings().s3_bucket
    selector = pafs.FileSelector(f"{target_bucket}/{prefix}".rstrip("/"), recursive=True)
    try:
        infos = fs.get_file_info(selector)
    except FileNotFoundError:
        return []
    return [
        info.path.removeprefix(f"{target_bucket}/")
        for info in infos
        if info.type == pafs.FileType.File
    ]


def lake_stats(prefix: str = "", *, bucket: str | None = None, filesystem=None) -> tuple[int, int]:
    """Return ``(object_count, row_count)`` for every Parquet object under ``prefix``."""
    import pyarrow.parquet as pq

    fs = filesystem or build_filesystem()
    target_bucket = bucket if bucket is not None else get_settings().s3_bucket
    keys = list_keys(prefix, bucket=target_bucket, filesystem=fs)
    rows = sum(pq.read_table(f"{target_bucket}/{key}", filesystem=fs).num_rows for key in keys)
    return len(keys), rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List objects in the data lake")
    parser.add_argument("prefix", nargs="?", default="", help="key prefix (default: everything)")
    parser.add_argument("--stats", action="store_true", help="print object and row counts")
    args = parser.parse_args(argv)
    if args.stats:
        objects, rows = lake_stats(args.prefix)
        print(f"{args.prefix or '(all)'}: {objects} object(s), {rows:,} rows")
        return 0
    keys = list_keys(args.prefix)
    for key in keys:
        print(key)
    print(f"{len(keys)} object(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
