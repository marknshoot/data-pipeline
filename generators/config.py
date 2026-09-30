"""Runtime configuration, loaded from the repo-root ``.env``.

Real environment variables always win over file values, so containerised runs and
one-off overrides keep working. Nothing here holds a secret of its own.
"""

from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo

ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    """Load ``.env`` as the project's source of truth.

    ``override=True`` on purpose: this project's ``.env`` should win over whatever
    happens to be exported in the ambient shell (other projects on a dev machine
    often export ``POSTGRES_*``). Containers do not mount ``.env`` and receive their
    configuration through Compose ``environment``, so nothing is clobbered there.
    """
    with contextlib.suppress(OSError):
        load_dotenv(ROOT / ".env", override=True)


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set; copy .env.example to .env and fill it in")
    return value


@dataclass(frozen=True)
class Settings:
    postgres_dsn: str
    kafka_bootstrap: str
    kafka_topic_events: str
    s3_endpoint: str
    s3_endpoint_internal: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    s3_region: str
    clickhouse_host: str
    clickhouse_http_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_env()
    return Settings(
        postgres_dsn=make_conninfo(
            host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=_require("POSTGRES_DB"),
            user=_require("POSTGRES_USER"),
            password=_require("POSTGRES_PASSWORD"),
            connect_timeout="5",
        ),
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9094"),
        kafka_topic_events=os.environ.get("KAFKA_TOPIC_EVENTS", "clickstream.events"),
        s3_endpoint=os.environ.get("S3_ENDPOINT", "http://127.0.0.1:8333"),
        # ClickHouse runs in the compose network and must reach SeaweedFS/S3 by
        # service name, not localhost.
        s3_endpoint_internal=os.environ.get("S3_ENDPOINT_INTERNAL")
        or os.environ.get("S3_ENDPOINT", "http://127.0.0.1:8333"),
        s3_access_key=_require("S3_ACCESS_KEY"),
        s3_secret_key=_require("S3_SECRET_KEY"),
        s3_bucket=os.environ.get("S3_BUCKET", "lake"),
        s3_region=os.environ.get("S3_REGION", "us-east-1"),
        clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        clickhouse_http_port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        clickhouse_user=_require("CLICKHOUSE_USER"),
        clickhouse_password=_require("CLICKHOUSE_PASSWORD"),
        clickhouse_db=os.environ.get("CLICKHOUSE_DB", "analytics"),
    )
