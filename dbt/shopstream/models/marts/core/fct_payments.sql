{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='delete+insert',
    engine='MergeTree()',
    order_by='payment_id',
) }}

-- Payment grain fact. Kept separate from orders because a payment can fail or be
-- refunded independently of the order's status.
with payments as (
    select * from {{ ref('stg_payments') }}
    where {{ incremental_lookback() }}
)

select
    p.payment_id       as payment_id,
    p.order_id         as order_id,
    o.user_id          as user_id,
    p.payment_method   as payment_method,
    p.amount           as amount,
    p.payment_status   as payment_status,
    o.order_status     as order_status,
    o.order_date       as order_date,
    p.created_at       as created_at,
    p.updated_at       as updated_at
from payments as p
inner join {{ ref('stg_orders') }} as o
    on p.order_id = o.order_id
