-- SCD2 integrity: a dimension must have at most one current row per business key.
-- Fails (returns rows) when a key has more than one open version.

select
    user_id,
    count() as current_versions
from {{ ref('dim_users') }}
where is_current
group by user_id
having count() > 1
