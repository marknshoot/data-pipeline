"""Tests for the data-quality checks.

The comparison and classification logic is pure, so it is tested directly: these are
the functions that decide whether a pipeline is reported healthy, and getting them
wrong is worse than having no check at all.
"""

from __future__ import annotations

from datetime import date

import pytest

from monitoring.quality import (
    CHECKS,
    RowCountResult,
    compare_series,
    status_for,
)

# ------------------------------------------------------------------ status_for


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.0, "ok"),
        (2.9, "ok"),
        (3.0, "warn"),
        (5.9, "warn"),
        (6.0, "error"),
        (99.0, "error"),
    ],
)
def test_status_for_higher_is_worse(value, expected):
    assert status_for(value, warn_at=3.0, error_at=6.0) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1.0, "ok"),
        (0.05, "ok"),
        (0.02, "warn"),
        (0.01, "warn"),
        (0.001, "error"),
    ],
)
def test_status_for_lower_is_worse(value, expected):
    """For a rate, *low* is good: e.g. a fall in a success ratio."""
    assert status_for(value, warn_at=0.02, error_at=0.001, higher_is_worse=False) == expected


def test_status_for_treats_the_threshold_as_breached():
    # A metric exactly at the threshold is a warning, not "still fine".
    assert status_for(3.0, warn_at=3.0, error_at=6.0) == "warn"


# -------------------------------------------------------------- compare_series


def test_compare_series_matches_identical_series():
    series = {date(2026, 10, 1): 10, date(2026, 10, 2): 20}
    results = compare_series("orders", "day", series, dict(series))

    assert len(results) == 2
    assert all(result.difference == 0 for result in results)
    assert all(result.status == "ok" for result in results)


def test_compare_series_reports_a_missing_day():
    """A day in the source but not the warehouse is the shape of a dropped extract."""
    results = compare_series(
        "orders",
        "day",
        {date(2026, 10, 1): 10, date(2026, 10, 2): 20},
        {date(2026, 10, 1): 10},
    )
    by_date = {result.check_date: result for result in results}

    assert by_date[date(2026, 10, 2)].warehouse_count == 0
    assert by_date[date(2026, 10, 2)].difference == 20
    assert by_date[date(2026, 10, 2)].status == "error"
    assert by_date[date(2026, 10, 1)].status == "ok"


def test_compare_series_reports_an_extra_day():
    """The reverse -- rows in the warehouse with no source -- means duplication."""
    results = compare_series("orders", "day", {}, {date(2026, 10, 2): 5})

    assert len(results) == 1
    assert results[0].source_count == 0
    assert results[0].difference == -5
    assert results[0].status == "error"


def test_compare_series_sorts_days_and_keeps_total_last():
    results = compare_series(
        "users",
        "total",
        {None: 100, date(2026, 10, 2): 3},
        {None: 100, date(2026, 10, 2): 3},
    )
    # `None` (the whole-table grain) sorts last, after any real dates.
    assert [result.check_date for result in results] == [date(2026, 10, 2), None]


def test_compare_series_is_empty_for_two_empty_series():
    assert compare_series("orders", "day", {}, {}) == []


# -------------------------------------------------------------------- results


def test_row_count_result_difference_and_status():
    assert RowCountResult("orders", "day", date(2026, 10, 1), 10, 10).status == "ok"
    assert RowCountResult("orders", "day", date(2026, 10, 1), 10, 8).status == "error"
    assert RowCountResult("orders", "day", date(2026, 10, 1), 10, 12).difference == -2


# --------------------------------------------------------------------- checks


def test_every_check_bounds_the_source_by_a_watermark():
    """Without the bound the check compares against live Postgres and always differs."""
    for check in CHECKS:
        assert "%(watermark)s" in check.source_sql, check.entity
        assert check.watermark_table, check.entity


def test_every_check_uses_the_declared_grain():
    for check in CHECKS:
        assert check.grain in {"day", "total"}, check.entity
        if check.grain == "total":
            # A whole-table check returns one count, with no date column.
            assert "group by" not in check.source_sql.lower(), check.entity
        else:
            assert "group by 1" in check.source_sql.lower(), check.entity


def test_checks_cover_the_facts_and_dimensions():
    entities = {check.entity for check in CHECKS}
    assert {"orders", "order_items", "payments"} <= entities
    assert {"users", "products", "sellers", "categories"} <= entities
