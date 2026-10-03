"""Tests for the CI helpers and the CI fixture.

The fixture is not throwaway code: if it stopped satisfying the invariants the dbt
tests check, CI would go green for the wrong reason or fail for a reason that has
nothing to do with the change under test. So the fixture's properties are asserted
here, independently of dbt.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from ci.apply_ddl import ddl_files
from ci.seed_sample_data import (
    CATEGORIES,
    EVENTS,
    ORDERS,
    PAYMENTS,
    PRODUCTS,
    SELLERS,
    USERS,
    event_rows,
    order_rows,
    payment_rows,
)
from warehouse.clickhouse import apply_sql_file, split_statements

# ------------------------------------------------------------ split_statements


def test_split_statements_ignores_a_semicolon_inside_a_comment():
    """Regression: 00_databases.sql's header comment contains a semicolon.

    Splitting naively cut the first CREATE DATABASE in half and sent an empty query.
    """
    sql = "-- staging/marts later; raw is loaded from the lake\nCREATE DATABASE x"
    assert split_statements(sql) == ["CREATE DATABASE x"]


def test_split_statements_drops_blank_and_comment_only_chunks():
    sql = "SELECT 1;\n-- just a comment\n;\n\nSELECT 2;"
    assert split_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_split_statements_keeps_multi_line_statements_intact():
    sql = "CREATE TABLE t\n(\n    a Int32\n)\nENGINE = MergeTree\nORDER BY a;\nSELECT 1;"
    statements = split_statements(sql)
    assert len(statements) == 2
    assert statements[0].startswith("CREATE TABLE t")
    assert "\n" in statements[0]
    assert statements[1] == "SELECT 1"


def test_split_statements_returns_nothing_for_an_empty_file():
    assert split_statements("") == []
    assert split_statements("\n\n-- nothing here\n") == []


def test_every_repo_ddl_statement_looks_like_sql():
    """A comment-only chunk would be sent to ClickHouse as an empty query."""
    for path in ddl_files(include_databases=True):
        for statement in split_statements(path.read_text()):
            code = "\n".join(
                line
                for line in statement.splitlines()
                if line.strip() and not line.lstrip().startswith("--")
            ).strip()
            assert code, f"{path.name} produced a comment-only statement"
            assert code.upper().startswith(("CREATE", "ALTER", "DROP")), (
                f"{path.name}: unexpected statement {code[:40]!r}"
            )


def test_ddl_files_are_ordered_and_include_databases_only_on_request():
    without = ddl_files(include_databases=False)
    with_databases = ddl_files(include_databases=True)

    assert [path.name for path in without] == [
        "01_raw_tables.sql",
        "02_events.sql",
        "03_quality.sql",
    ]
    assert with_databases[0].name == "00_databases.sql"
    assert with_databases[1:] == without


class RecordingClient:
    def __init__(self) -> None:
        self.commands: list[str] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)


def test_apply_sql_file_sends_every_statement(tmp_path: Path):
    path = tmp_path / "ddl.sql"
    path.write_text(
        "-- a comment; with a semicolon\nCREATE TABLE a (x Int32);\nCREATE TABLE b (y Int32);"
    )

    client = RecordingClient()
    count = apply_sql_file(client, path)

    assert count == 2
    assert client.commands == ["CREATE TABLE a (x Int32)", "CREATE TABLE b (y Int32)"]


# ------------------------------------------------------------- fixture shape


def test_order_totals_equal_the_sum_of_their_items():
    """The invariant assert_order_total_matches_items checks in dbt."""
    orders, items = order_rows()
    totals = {order[0]: order[5] for order in orders}

    summed: dict[int, Decimal] = {}
    for _item_id, order_id, _product_id, quantity, unit_price, _c, _u in items:
        summed[order_id] = summed.get(order_id, Decimal("0.00")) + unit_price * quantity

    assert set(totals) == set(summed)
    for order_id, total in totals.items():
        assert total == summed[order_id], order_id


def test_every_order_has_at_least_one_item():
    orders, items = order_rows()
    with_items = {item[1] for item in items}
    assert {order[0] for order in orders} == with_items


def test_payment_amounts_match_their_orders():
    amounts = {order[0]: order[5] for order in order_rows()[0]}
    for _payment_id, order_id, _method, amount, _status, _c, _u in payment_rows():
        assert amount == amounts[order_id], order_id


def test_every_order_item_references_a_known_product():
    known = {product[0] for product in PRODUCTS}
    for _item_id, _order_id, product_id, *_rest in order_rows()[1]:
        assert product_id in known


def test_products_reference_known_sellers_and_categories():
    sellers = {row[0] for row in SELLERS}
    categories = {row[0] for row in CATEGORIES}
    for product in PRODUCTS:
        assert product[1] in sellers
        assert product[2] in categories


def test_orders_and_events_reference_known_users():
    users = {row[0] for row in USERS}
    for order in ORDERS:
        assert order[1] in users
    for event in event_rows():
        assert event[3] in users


def test_statuses_stay_within_the_values_dbt_accepts():
    order_statuses = {"pending", "paid", "shipped", "delivered", "cancelled", "refunded"}
    payment_statuses = {"pending", "authorized", "captured", "failed", "refunded"}
    payment_methods = {"card", "bank_transfer", "va", "ewallet", "cod"}
    event_types = {"page_view", "add_to_cart", "checkout", "purchase"}

    assert {order[2] for order in ORDERS} <= order_statuses
    assert {row[3] for row in PAYMENTS} <= payment_statuses
    assert {row[2] for row in PAYMENTS} <= payment_methods
    assert {event[2] for event in EVENTS} <= event_types


def test_the_fixture_includes_non_revenue_orders():
    """Otherwise the marts' revenue filters would pass without excluding anything."""
    revenue = {"paid", "shipped", "delivered"}
    statuses = {order[2] for order in ORDERS}
    assert statuses - revenue, "no cancelled/pending/refunded order to filter out"


def test_the_funnel_narrows_at_every_step():
    """The shape assert_funnel_monotonic validates."""
    sessions: dict[str, set[str]] = {}
    for _event_id, _version, event_type, _user_id, session_id, *_rest in EVENTS:
        sessions.setdefault(event_type, set()).add(session_id)

    counts = [
        len(sessions.get(step, set()))
        for step in ("page_view", "add_to_cart", "checkout", "purchase")
    ]
    assert counts == sorted(counts, reverse=True), counts
    assert counts[0] > 0 and counts[-1] > 0


def test_the_fixture_has_both_event_schema_versions():
    versions = {event[1] for event in EVENTS}
    assert versions == {1, 2}


def test_the_fixture_supports_cohort_retention_beyond_month_zero():
    """A user must order in two different months, or retention has only the 1.0 row."""
    revenue = {"paid", "shipped", "delivered"}
    months: dict[int, set[tuple[int, int]]] = {}
    for order in ORDERS:
        if order[2] in revenue:
            user_id, created_at = order[1], order[5]
            months.setdefault(user_id, set()).add((created_at.year, created_at.month))

    assert any(len(user_months) > 1 for user_months in months.values()), months


def test_the_fixture_is_deterministic():
    """Two calls must produce identical rows: CI should not be flaky."""
    assert order_rows() == order_rows()
    assert event_rows() == event_rows()
    assert payment_rows() == payment_rows()
