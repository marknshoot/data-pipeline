"""ClickHouse client helper for the warehouse layer."""

from __future__ import annotations

from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from generators.config import Settings, get_settings


def get_client(settings: Settings | None = None) -> Client:
    """Open a ClickHouse HTTP client from the environment-driven settings."""
    settings = settings or get_settings()
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_http_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def scalar(client: Client, sql: str) -> object:
    """Run a single-value query and return the value."""
    return client.query(sql).first_row[0]


def split_statements(sql: str) -> list[str]:
    """Split a multi-statement SQL file into statements.

    ``--`` line comments are dropped first, because a semicolon inside a comment would
    otherwise cut a statement in half -- which is exactly what happened to
    ``00_databases.sql``, whose header comment reads "dbt builds staging/marts later;
    raw is loaded from the lake".

    Still deliberately naive: it does not understand ``/* */`` blocks, semicolons
    inside string literals, or a trailing ``--`` after code on the same line. None
    occur in this repo's DDL, and a real SQL parser is not worth the dependency for a
    case that does not arise. This is the function to fix if one ever does.
    """
    without_comments = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    return [statement.strip() for statement in without_comments.split(";") if statement.strip()]


def apply_sql_file(client: Client, path: Path) -> int:
    """Apply every statement in ``path``; returns how many were run.

    Statements are expected to be idempotent (``CREATE ... IF NOT EXISTS``,
    ``CREATE OR REPLACE``), which is what makes the warehouse schema re-appliable.
    """
    statements = split_statements(path.read_text())
    for statement in statements:
        client.command(statement)
    return len(statements)
