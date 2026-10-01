{{ config(materialized='table', order_by='seller_id') }}

-- Seller scorecard. Revenue is summed from order lines (already carrying
-- seller_id) and restricted to the same revenue-generating statuses as the other
-- marts, so a seller total reconciles with mart_daily_gmv.
select
    s.seller_id                 as seller_id,
    s.seller_name               as seller_name,
    s.city                      as city,
    s.rating                    as rating,
    uniqExact(f.order_id)       as orders,
    sum(f.quantity)             as units_sold,
    sum(f.line_amount)          as revenue,
    round(avg(f.unit_price), 2) as average_unit_price,
    uniqExact(f.product_id)     as distinct_products
from {{ ref('fct_order_items') }} as f
inner join {{ ref('dim_sellers') }} as s
    on f.seller_id = s.seller_id
where f.order_status in ('paid', 'shipped', 'delivered')
group by s.seller_id, s.seller_name, s.city, s.rating
order by revenue desc
