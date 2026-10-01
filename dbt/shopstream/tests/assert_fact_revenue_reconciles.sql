-- Reconciliation across two facts: revenue computed from order headers must equal
-- revenue computed from order lines, for the same status filter. Catches a broken
-- join, a mis-scoped filter, or lines silently dropped from one fact.
--
-- Fails (returns a row) when the two totals differ by more than a cent.

with by_order as (
    select sum(total_amount) as revenue
    from {{ ref('fct_orders') }}
    where order_status in ('paid', 'shipped', 'delivered')
),

by_line as (
    select sum(line_amount) as revenue
    from {{ ref('fct_order_items') }}
    where order_status in ('paid', 'shipped', 'delivered')
)

select
    by_order.revenue  as revenue_from_orders,
    by_line.revenue   as revenue_from_lines
from by_order, by_line
where abs(toFloat64(by_order.revenue) - toFloat64(by_line.revenue)) > 0.01
