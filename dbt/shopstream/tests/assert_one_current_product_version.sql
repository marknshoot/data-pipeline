-- SCD2 integrity: a dimension must have at most one current row per business key.
-- Fails (returns rows) when a key has more than one open version.

select
    product_id,
    count() as current_versions
from {{ ref('dim_products') }}
where is_current
group by product_id
having count() > 1
