{{ config(materialized='table', order_by='seller_id') }}

-- SCD1 seller dimension: current attributes only (no tracked history).
select
    cityHash64(seller_id) as seller_sk,
    seller_id,
    seller_name,
    email,
    city,
    country_code,
    rating
from {{ ref('stg_sellers') }}
