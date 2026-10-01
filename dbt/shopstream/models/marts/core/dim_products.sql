{{ config(materialized='table', order_by='product_id') }}

-- SCD2 product dimension (price history). The price actually paid lives on
-- fct_order_items.unit_price; this dimension answers "what did it cost then".
select
    dbt_scd_id            as product_sk,
    product_id,
    seller_id,
    category_id,
    product_name,
    sku,
    price,
    cost,
    stock,
    is_active,
    gross_margin_ratio,
    dbt_valid_from        as valid_from,
    dbt_valid_to          as valid_to,
    dbt_valid_to is null  as is_current
from {{ ref('products_snapshot') }}
