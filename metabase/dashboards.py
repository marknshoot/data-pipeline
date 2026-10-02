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
            "Cohort retention, geography, repeat behaviour and the acquisition "
            "funnel built from clickstream events."
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
            {
                "name": "Acquisition funnel",
                "display": "funnel",
                "visualization_settings": {
                    "graph.dimensions": ["step"],
                    "graph.metrics": ["sessions"],
                },
                "sql": ("select step, sessions\nfrom marts.mart_funnel_steps\norder by step_order"),
                "size": {"col": 0, "row": 17, "size_x": 12, "size_y": 9},
            },
            {
                "name": "Funnel conversion by step",
                "display": "table",
                "sql": (
                    "select\n"
                    "    step,\n"
                    "    sessions,\n"
                    "    share_of_entry,\n"
                    "    step_conversion_rate\n"
                    "from marts.mart_funnel_steps\n"
                    "order by step_order"
                ),
                "size": {"col": 12, "row": 21, "size_x": 12, "size_y": 9},
            },
            {
                "name": "Step-to-step conversion by day",
                "display": "line",
                "sql": (
                    "select\n"
                    "    event_date,\n"
                    "    view_to_cart_rate,\n"
                    "    cart_to_checkout_rate,\n"
                    "    checkout_to_purchase_rate\n"
                    "from marts.mart_funnel_daily\n"
                    "order by event_date"
                ),
                "size": {"col": 0, "row": 26, "size_x": 24, "size_y": 8},
            },
        ],
    },
    {
        # The one panel that is not reading dbt models. ClickHouse consumes the Kafka
        # topic itself, so these numbers move as events arrive rather than waiting for
        # the hourly batch; the dashboard refreshes itself every 30 seconds.
        "name": "Live Traffic",
        "description": (
            "Streaming panel, auto-refreshing every 30s. ClickHouse reads the Kafka "
            "topic directly, so this is live rather than batch: the same events reach "
            "the batch dashboards after they land in the warehouse."
        ),
        "auto_refresh_interval": 30,
        "cards": [
            {
                "name": "Live: purchases per minute",
                "display": "line",
                "sql": (
                    "select minute, orders\n"
                    "from analytics.rt_orders_per_minute\n"
                    "where minute >= now() - interval 30 minute\n"
                    "order by minute"
                ),
                "size": {"col": 0, "row": 0, "size_x": 12, "size_y": 8},
            },
            {
                "name": "Live: events per minute by step",
                "display": "bar",
                "visualization_settings": {
                    "graph.dimensions": ["minute", "event_type"],
                    "graph.metrics": ["events"],
                },
                "sql": (
                    "select minute, event_type, events\n"
                    "from analytics.rt_events_per_minute\n"
                    "where minute >= now() - interval 30 minute\n"
                    "order by minute, event_type"
                ),
                "size": {"col": 12, "row": 0, "size_x": 12, "size_y": 8},
            },
            {
                "name": "Live funnel",
                "display": "funnel",
                "visualization_settings": {
                    "graph.dimensions": ["step"],
                    "graph.metrics": ["sessions"],
                },
                # FINAL because ClickHouse's Kafka engine is at-least-once: a
                # rebalance re-reads from the last commit, and this collapses the
                # redelivered events instead of double-counting them.
                "sql": (
                    "select 'page_view' as step, uniqExact(session_id) as sessions\n"
                    "from analytics.rt_events final where event_type = 'page_view'\n"
                    "union all\n"
                    "select 'add_to_cart', uniqExact(session_id)\n"
                    "from analytics.rt_events final where event_type = 'add_to_cart'\n"
                    "union all\n"
                    "select 'checkout', uniqExact(session_id)\n"
                    "from analytics.rt_events final where event_type = 'checkout'\n"
                    "union all\n"
                    "select 'purchase', uniqExact(session_id)\n"
                    "from analytics.rt_events final where event_type = 'purchase'"
                ),
                "size": {"col": 0, "row": 8, "size_x": 12, "size_y": 8},
            },
            {
                "name": "Live: sessions (last 15 min)",
                "display": "scalar",
                "sql": (
                    "select uniqExact(session_id) as sessions\n"
                    "from analytics.rt_events final\n"
                    "where event_time >= now() - interval 15 minute"
                ),
                "size": {"col": 12, "row": 8, "size_x": 12, "size_y": 8},
            },
        ],
    },
]
