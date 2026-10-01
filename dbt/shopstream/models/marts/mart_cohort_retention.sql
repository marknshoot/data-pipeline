{{ config(materialized='table', order_by='(cohort_month, month_number)') }}

-- Monthly cohort retention. A user's cohort is the month of their first
-- revenue-generating order; `month_number` counts months since then, so the
-- month_number = 0 row is the cohort size (retention = 1.0 by definition).
with revenue_orders as (
    select user_id, order_date
    from {{ ref('fct_orders') }}
    where order_status in ('paid', 'shipped', 'delivered')
),

first_order as (
    select
        user_id,
        min(order_date) as first_order_date
    from revenue_orders
    group by user_id
),

activity as (
    select
        toStartOfMonth(fo.first_order_date) as cohort_month,
        dateDiff(
            'month',
            toStartOfMonth(fo.first_order_date),
            toStartOfMonth(r.order_date)
        )                                   as month_number,
        r.user_id
    from revenue_orders as r
    inner join first_order as fo on r.user_id = fo.user_id
),

retention as (
    select
        cohort_month,
        month_number,
        uniqExact(user_id) as active_users
    from activity
    group by cohort_month, month_number
)

select
    r.cohort_month,
    s.cohort_size,
    r.month_number,
    r.active_users,
    round(r.active_users / s.cohort_size, 4) as retention_rate
from retention as r
inner join (
    select cohort_month, active_users as cohort_size
    from retention
    where month_number = 0
) as s on r.cohort_month = s.cohort_month
order by r.cohort_month, r.month_number
