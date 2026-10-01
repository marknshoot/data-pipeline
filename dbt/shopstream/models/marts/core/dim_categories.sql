{{ config(materialized='table', order_by='category_id') }}

-- Category dimension, flattened one level so a product rolls up to its parent.
with categories as (
    select * from {{ ref('stg_categories') }}
)

select
    cityHash64(c.category_id) as category_sk,
    c.category_id             as category_id,
    c.category_name           as category_name,
    c.slug                    as slug,
    c.parent_category_id      as parent_category_id,
    p.category_name           as parent_category_name
from categories as c
left join categories as p on c.parent_category_id = p.category_id
