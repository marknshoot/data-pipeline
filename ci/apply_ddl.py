"""Apply the warehouse DDL to a ClickHouse instance.

CI cannot use `docker exec` against a service container portably, and
`clickhouse-client` is not installed on a GitHub runner, so the schema is applied over
the HTTP interface with the client the project already depends on.

Usage:
    python -m ci.apply_ddl
    python -m ci.apply_ddl --include-databases   # also create raw/staging/marts
"""

from __future__ import annotations

import argparse
from pathlib import Path

from warehouse.clickhouse import apply_sql_file

INIT_DIR = Path(__file__).resolve().parents[1] / "clickhouse" / "init"


def ddl_files(*, include_databases: bool) -> list[Path]:
    """Every init file, in order, optionally including the database creation."""
    files = sorted(INIT_DIR.glob("0[1-9]_*.sql"))
    if include_databases:
        files = sorted(INIT_DIR.glob("00_*.sql")) + files
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply clickhouse/init/*.sql")
    parser.add_argument(
        "--include-databases",
        action="store_true",
        help="also apply 00_databases.sql (raw, staging, marts)",
    )
    parser.add_argument(
        "--database",
        default="analytics",
        help="database to create for the dbt target and ops tables (default: analytics)",
    )
    args = parser.parse_args(argv)

    # Imported here so `--help` works without a reachable server.
    from ci.seed_sample_data import connect

    client = connect()
    print(f"applying DDL to {client.server_version}")

    # The dbt profile's default database and the dq/Kafka-engine tables live here; the
    # official image only creates it when CLICKHOUSE_DB is set, which is not something
    # CI should have to rely on.
    client.command(f"CREATE DATABASE IF NOT EXISTS {args.database}")

    for path in ddl_files(include_databases=args.include_databases):
        count = apply_sql_file(client, path)
        print(f"  {path.name:<24} {count} statement(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
