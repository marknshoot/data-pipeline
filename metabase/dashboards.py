"""Declarative definitions of the Metabase collection, questions and dashboards.

Kept as data so `provision.py` stays generic: it walks these structures and
creates (or updates) whatever is missing. Every question is native SQL against the
`marts` schema, so the dashboards depend only on the tested dbt models.
"""

from __future__ import annotations

COLLECTION = "ShopStream"

# Revenue recognised in every question below: the same status filter the dbt marts use.
REVENUE_STATUSES = "('paid', 'shipped', 'delivered')"

SPARKLINE_SETTINGS = {}  # placeholder for future visual tweaks

DASHBOARDS: list[dict] = [
    {
        "name": "Marketplace Overview",
        "description": (
            "GMV, orders and AOV over time, plus where the revenue comes from "
            "(categories, sellers, payment methods)."
        ),
        "cards": [
            {
                "name": "Total GMV",
                "display": "scalar",
                "sql": "select sum(gmv) as total_gmv from marts.mart_daily_gmv",
                "size": {"col": 0, "row": 0, "size_x": 6, "size_y": 3},
            },
            {
                "name": "Total orders",
                "display": "scalar",
                "sql": "select sum(orders) as total_orders from marts.mart_daily_gmv",
                "size": {"col": 6, "row": 0, "size_x": 6, "size_y": 3},
            },
            {
                "name": "Average order value",
                "display": "scalar",
                "sql": (
                    "select round(sum(gmv) / nullIf(sum(orders), 0), 2) as aov "
                    "from marts.mart_daily_gmv"
                ),
                "size": {"col": 12, "row": 0, "size_x": 6, "size_y": 3},
            },
            {
                "name": "Active buyers",
                "display": "scalar",
                "sql": (
                    f"select uniqExact(user_id) as buyers from marts.fct_orders "
                    f"where order_status in {REVENUE_STATUSES}"
                ),
                "size": {"col": 18, "row": 0, "size_x": 6, "size_y": 3},
            },
            {
                "name": "GMV by day",
                "display": "line",
                "sql": "select date_key, gmv from marts.mart_daily_gmv order by date_key",
                "size": {"col": 0, "row": 3, "size_x": 24, "size_y": 8},
            },
            {
                "name": "Orders and buyers by day",
                "display": "bar",
                "sql": (
                    "select date_key, orders, buyers from marts.mart_daily_gmv order by date_key"
                ),
                "size": {"col": 0, "row": 11, "size_x": 12, "size_y": 8},
            },
            {
                "name": "Top categories by revenue",
                "display": "bar",
                "sql": (
                    "select c.category_name as category, sum(f.line_amount) as revenue\n"
                    "from marts.fct_order_items as f\n"
                    "inner join marts.dim_categories as c on f.category_id = c.category_id\n"
                    f"where f.order_status in {REVENUE_STATUSES}\n"
                    "group by category\n"
                    "order by revenue desc\n"
                    "limit 10"
                ),
                "size": {"col": 12, "row": 11, "size_x": 12, "size_y": 8},
            },
            {
                "name": "Top sellers",
                "display": "table",
                "sql": (
                    "select seller_name, orders, units_sold, revenue\n"
                    "from marts.mart_seller_performance\n"
                    "order by revenue desc\n"
                    "limit 10"
                ),
                "size": {"col": 0, "row": 19, "size_x": 12, "size_y": 8},
            },
            {
                "name": "Revenue by payment method",
                "display": "bar",
                "sql": (
                    "select payment_method, sum(amount) as revenue, count() as payments\n"
                    "from marts.fct_payments\n"
                    "where payment_status in ('captured', 'refunded')\n"
                    "group by payment_method\n"
                    "order by revenue desc"
                ),
                "size": {"col": 12, "row": 19, "size_x": 12, "size_y": 8},
            },
        ],
    },
    {
        "name": "Customer",
        "description": (
            "Cohort retention, geography and repeat behaviour. The acquisition "
            "funnel lives in a separate panel once clickstream events land in the "
            "warehouse (Phase 5)."
        ),
        "cards": [
            {
                "name": "Cohort retention",
                # NOTE: Metabase only allows the 'pivot' display for questions built in
                # the query builder, not native SQL, so this is a grouped bar chart of
                # retention decay per cohort. The raw triangle is the table below.
                "display": "bar",
                "visualization_settings": {
                    "graph.dimensions": ["month_number", "cohort_month"],
                    "graph.metrics": ["retention_rate"],
                },
                "sql": (
                    "select month_number, cohort_month, retention_rate\n"
                    "from marts.mart_cohort_retention\n"
                    "order by cohort_month, month_number"
                ),
                "size": {"col": 0, "row": 0, "size_x": 12, "size_y": 9},
            },
            {
                "name": "Cohort retention (table)",
                "display": "table",
                "sql": (
                    "select\n"
                    "    cohort_month,\n"
                    "    month_number,\n"
                    "    active_users,\n"
                    "    retention_rate\n"
                    "from marts.mart_cohort_retention\n"
                    "order by cohort_month, month_number"
                ),
                "size": {"col": 12, "row": 0, "size_x": 12, "size_y": 9},
            },
            {
                "name": "New customers per cohort",
                "display": "bar",
                "sql": (
                    "select cohort_month, cohort_size\n"
                    "from marts.mart_cohort_retention\n"
                    "where month_number = 0\n"
                    "order by cohort_month"
                ),
                "size": {"col": 0, "row": 9, "size_x": 12, "size_y": 8},
            },
            {
                "name": "Repeat purchase rate",
                "display": "scalar",
                "sql": (
                    "select round(countIf(orders > 1) / nullIf(count(), 0), 4) as repeat_rate\n"
                    "from (\n"
                    "    select user_id, count() as orders\n"
                    "    from marts.fct_orders\n"
                    f"    where order_status in {REVENUE_STATUSES}\n"
                    "    group by user_id\n"
                    ")"
                ),
                "size": {"col": 12, "row": 9, "size_x": 12, "size_y": 4},
            },
            {
                "name": "Revenue by shipping city",
                "display": "bar",
                "sql": (
                    "select shipping_city, sum(total_amount) as revenue, count() as orders\n"
                    "from marts.fct_orders\n"
                    f"where order_status in {REVENUE_STATUSES}\n"
                    "group by shipping_city\n"
                    "order by revenue desc"
                ),
                "size": {"col": 12, "row": 13, "size_x": 12, "size_y": 8},
            },
        ],
    },
]
