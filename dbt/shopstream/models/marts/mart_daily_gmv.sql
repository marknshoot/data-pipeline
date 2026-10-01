{{ config(materialized='table', order_by='date_key') }}

-- Daily GMV. "Revenue" excludes pending, cancelled and refunded orders — the same
-- definition every revenue mart uses, so the numbers reconcile across dashboards.
select
    o.order_date                                        as date_key,
    count()                                             as orders,
    sum(o.total_amount)                                 as gmv,
    round(sum(o.total_amount) / nullIf(count(), 0), 2)  as average_order_value,
    uniqExact(o.user_id)                                as buyers
from {{ ref('fct_orders') }} as o
where o.order_status in ('paid', 'shipped', 'delivered')
group by o.order_date
order by o.order_date
