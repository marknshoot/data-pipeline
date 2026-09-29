"""Postgres connection helper for the generators and ingestion jobs."""

from __future__ import annotations

import psycopg

from .config import get_settings


def connect(*, autocommit: bool = False) -> psycopg.Connection:
    """Open a connection to the ``shop`` OLTP database, pinned to UTC."""
    conn = psycopg.connect(get_settings().postgres_dsn, autocommit=autocommit)
    # Deterministic timestamps for extraction and generation; storage is unaffected.
    conn.execute("SET TIME ZONE 'UTC'")
    return conn
