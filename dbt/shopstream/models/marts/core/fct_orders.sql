{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    engine='MergeTree()',
    order_by='order_id',
) }}

-- Order-grain fact. `user_sk` is the customer version that was current when the
-- order was placed (SCD2 as-of join), falling back to the current version when the
-- order predates the earliest tracked version. `shipping_city` is the source's own
-- point-in-time snapshot, kept so city analysis never depends on SCD2 coverage.
with orders as (
    select * from {{ ref('stg_orders') }}
    where {{ incremental_lookback() }}
)

select
    o.order_id                        as order_id,
    o.user_id                         as user_id,
    coalesce(uh.user_sk, uc.user_sk)   as user_sk,
    o.order_status                    as order_status,
    o.currency                        as currency,
    o.shipping_city                   as shipping_city,
    o.total_amount                    as total_amount,
    o.order_date                      as order_date,
    o.created_at                      as created_at,
    o.updated_at                      as updated_at,
    o.is_terminal                     as is_terminal
from orders as o
left join {{ ref('dim_users') }} as uh
    on o.user_id = uh.user_id
   and o.created_at >= uh.valid_from
   and (uh.valid_to is null or o.created_at < uh.valid_to)
left join {{ ref('dim_users') }} as uc
    on o.user_id = uc.user_id
   and uc.is_current
