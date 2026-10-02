"""Provision Metabase reproducibly: read-only ClickHouse user, admin setup, the
ClickHouse connection, and the dashboards defined in ``metabase/dashboards.py``.

Re-runnable: everything is looked up by name and updated in place, so running it
twice converges rather than duplicating. This is what makes the BI layer
reproducible from a fresh clone instead of hand-clicked in the UI.

Usage:
    uv run python -m metabase.provision
"""

from __future__ import annotations

import argparse
import re
import sys
import time

import requests
from clickhouse_connect.driver.client import Client

from generators.config import Settings, get_settings
from warehouse.clickhouse import get_client

from .dashboards import COLLECTION, DASHBOARDS

SITE_NAME = "ShopStream"
DATABASE_NAME = "ClickHouse (marts)"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class MetabaseError(RuntimeError):
    pass


class Metabase:
    """Minimal Metabase API client (session auth)."""

    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session_id: str | None = None

    def _headers(self) -> dict[str, str]:
        if self.session_id is None:
            return {}
        return {"X-Metabase-Session": self.session_id}

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        response = self.session.request(
            method, f"{self.base}{path}", headers=self._headers(), timeout=60, **kwargs
        )
        if response.status_code >= 400:
            raise MetabaseError(f"{method} {path} -> {response.status_code}: {response.text[:300]}")
        return response

    def get(self, path: str, **kwargs):
        return self._request("GET", path, **kwargs).json()

    def post(self, path: str, **kwargs):
        return self._request("POST", path, **kwargs).json()

    def put(self, path: str, **kwargs):
        return self._request("PUT", path, **kwargs).json()

    # ---------------------------------------------------------------- setup
    def properties(self) -> dict:
        return requests.get(f"{self.base}/api/session/properties", timeout=30).json()

    def ensure_setup(self, settings: Settings) -> None:
        props = self.properties()
        if not props.get("has-user-setup"):
            token = props.get("setup-token")
            if not token:
                raise MetabaseError("Metabase needs setup but exposed no setup token")
            self.post(
                "/api/setup",
                json={
                    "token": token,
                    "user": {
                        "email": settings.metabase_admin_email,
                        "first_name": settings.metabase_admin_first_name,
                        "last_name": settings.metabase_admin_last_name,
                        "password": settings.metabase_admin_password,
                        "site_name": SITE_NAME,
                    },
                    "prefs": {"site_name": SITE_NAME, "allow_tracking": False},
                    "database": None,
                },
            )
            print(f"completed first-run setup for {settings.metabase_admin_email}")

        session = self.post(
            "/api/session",
            json={
                "username": settings.metabase_admin_email,
                "password": settings.metabase_admin_password,
            },
        )
        self.session_id = session["id"]
        print("authenticated with Metabase")

    # ------------------------------------------------------------- database
    def ensure_database(self, settings: Settings) -> int:
        databases = self.get("/api/database")
        rows = databases["data"] if isinstance(databases, dict) else databases
        for database in rows:
            if database["name"] == DATABASE_NAME:
                print(f"database connection already exists (id={database['id']})")
                return database["id"]

        created = self.post(
            "/api/database",
            json={
                "name": DATABASE_NAME,
                "engine": "clickhouse",
                "details": {
                    # Metabase dials ClickHouse from inside the compose network.
                    "host": settings.clickhouse_host_internal,
                    "port": settings.clickhouse_http_port,
                    "dbname": "marts",
                    "user": settings.clickhouse_ro_user,
                    "password": settings.clickhouse_ro_password,
                    "ssl": False,
                },
                "is_full_sync": True,
            },
        )
        print(f"created database connection id={created['id']}")
        return created["id"]

    def sync_and_wait(self, database_id: int, seconds: int = 20) -> None:
        self.post(f"/api/database/{database_id}/sync_schema")
        time.sleep(seconds)


def ensure_clickhouse_ro_user(settings: Settings) -> None:
    """Create/refresh a read-only ClickHouse user for Metabase."""
    if not _IDENTIFIER.match(settings.clickhouse_ro_user):
        raise ValueError(f"unsafe ClickHouse username: {settings.clickhouse_ro_user!r}")

    user = settings.clickhouse_ro_user
    password = settings.clickhouse_ro_password.replace("'", "''")
    admin: Client = get_client(settings)
    admin.command(f"CREATE USER IF NOT EXISTS {user} IDENTIFIED BY '{password}'")
    # Converge the password to whatever .env holds, so rotating it is just an edit.
    admin.command(f"ALTER USER {user} IDENTIFIED BY '{password}'")
    admin.command(f"GRANT SELECT ON marts.* TO {user}")
    # The live dashboard reads ClickHouse's own Kafka-fed tables, which live in
    # `analytics` rather than `marts`.
    admin.command(f"GRANT SELECT ON analytics.* TO {user}")
    # readonly=2: read queries only, but session settings may still be changed.
    # readonly=1 forbids setting changes, which breaks clients like Metabase that
    # set per-session options (e.g. async_insert) on connect.
    admin.command(f"ALTER USER {user} SETTINGS readonly = 2")
    print(f"clickhouse read-only user '{user}' ready (SELECT on marts.*, analytics.*)")


def _find(items: list[dict], name: str) -> dict | None:
    return next((item for item in items if item.get("name") == name), None)


def ensure_collection(metabase: Metabase, name: str) -> int:
    collections = metabase.get("/api/collection/")
    rows = collections["data"] if isinstance(collections, dict) else collections
    existing = _find([c for c in rows if not c.get("personal_owner_id")], name)
    if existing:
        return existing["id"]
    created = metabase.post("/api/collection", json={"name": name, "parent_id": None})
    print(f"created collection '{name}' (id={created['id']})")
    return created["id"]


def ensure_card(metabase: Metabase, card: dict, database_id: int, collection_id: int) -> int:
    payload = {
        "name": card["name"],
        "dataset_query": {
            "type": "native",
            "native": {"query": card["sql"], "template-tags": {}},
            "database": database_id,
        },
        "display": card["display"],
        "visualization_settings": card.get("visualization_settings", {}),
        "collection_id": collection_id,
    }
    cards = metabase.get("/api/card")
    rows = cards["data"] if isinstance(cards, dict) else cards
    existing = _find(rows, card["name"])
    if existing:
        metabase.put(f"/api/card/{existing['id']}", json=payload)
        return existing["id"]
    created = metabase.post("/api/card", json=payload)
    return created["id"]


def ensure_dashboard(
    metabase: Metabase,
    spec: dict,
    collection_id: int,
    card_ids: dict[str, int],
) -> int:
    dashboards = metabase.get("/api/dashboard")
    rows = dashboards["data"] if isinstance(dashboards, dict) else dashboards
    existing = _find(rows, spec["name"])
    if existing:
        dashboard_id = existing["id"]
    else:
        created = metabase.post(
            "/api/dashboard",
            json={
                "name": spec["name"],
                "description": spec.get("description", ""),
                "collection_id": collection_id,
            },
        )
        dashboard_id = created["id"]
        print(f"created dashboard '{spec['name']}' (id={dashboard_id})")

    # Reuse existing dashcard ids where the card is already placed, so the PUT is a
    # no-op for unchanged cards instead of a delete/recreate.
    current = metabase.get(f"/api/dashboard/{dashboard_id}")
    existing_ids = {dc["card_id"]: dc["id"] for dc in current.get("dashcards", [])}

    dashcards = []
    for index, card in enumerate(spec["cards"], start=1):
        card_id = card_ids[card["name"]]
        size = card["size"]
        dashcards.append(
            {
                "id": existing_ids.get(card_id, -index),
                "card_id": card_id,
                "row": size["row"],
                "col": size["col"],
                "size_x": size["size_x"],
                "size_y": size["size_y"],
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )
    body: dict = {"dashcards": dashcards}
    # Metabase auto-refresh is a dashboard-level setting, in seconds.
    if spec.get("auto_refresh_interval"):
        body["auto_refresh_interval"] = spec["auto_refresh_interval"]
    metabase.put(f"/api/dashboard/{dashboard_id}", json=body)
    return dashboard_id


def provision(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    ensure_clickhouse_ro_user(settings)

    metabase = Metabase(settings.metabase_url)
    metabase.ensure_setup(settings)
    database_id = metabase.ensure_database(settings)
    metabase.sync_and_wait(database_id)

    collection_id = ensure_collection(metabase, COLLECTION)
    for spec in DASHBOARDS:
        card_ids = {
            card["name"]: ensure_card(metabase, card, database_id, collection_id)
            for card in spec["cards"]
        }
        dashboard_id = ensure_dashboard(metabase, spec, collection_id, card_ids)
        print(f"dashboard '{spec['name']}' ready with {len(card_ids)} cards (id={dashboard_id})")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Provision Metabase from code")
    parser.parse_args(argv)
    try:
        provision()
    except (MetabaseError, requests.RequestException, ValueError) as exc:
        print(f"provisioning failed: {exc}", file=sys.stderr)
        return 1
    print(f"\nMetabase is ready: {get_settings().metabase_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
