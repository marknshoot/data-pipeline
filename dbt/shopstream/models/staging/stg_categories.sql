select
    category_id,
    trim(name)          as category_name,
    lower(trim(slug))   as slug,
    parent_category_id,
    created_at,
    updated_at
from {{ source('raw', 'categories') }} final
