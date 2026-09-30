"""ClickHouse client helper for the warehouse layer."""

from __future__ import annotations

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
