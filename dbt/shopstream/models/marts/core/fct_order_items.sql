{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='delete+insert',
    engine='MergeTree()',
    order_by='order_item_id',
) }}

-- Order-line grain fact. Carries seller/category so revenue can be sliced without
-- re-joining the product dimension, and keeps `order_status` so cancelled and
-- refunded revenue can be excluded consistently with fct_orders.
with items as (
    select * from {{ ref('stg_order_items') }}
    where {{ incremental_lookback() }}
)

select
    i.order_item_id   as order_item_id,
    i.order_id        as order_id,
    o.user_id         as user_id,
    i.product_id      as product_id,
    p.seller_id       as seller_id,
    p.category_id     as category_id,
    i.quantity        as quantity,
    i.unit_price      as unit_price,
    i.line_amount     as line_amount,
    o.order_status    as order_status,
    o.order_date      as order_date,
    i.created_at      as created_at,
    i.updated_at      as updated_at
from items as i
inner join {{ ref('stg_orders') }} as o
    on i.order_id = o.order_id
left join {{ ref('dim_products') }} as p
    on i.product_id = p.product_id
   and p.is_current
