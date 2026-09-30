select
    product_id,
    seller_id,
    category_id,
    trim(name)                              as product_name,
    upper(trim(sku))                        as sku,
    price,
    cost,
    stock,
    is_active,
    -- Margin as a ratio; nullIf guards a zero price (not expected, but cheap).
    round((price - cost) / nullIf(price, 0), 4) as gross_margin_ratio,
    created_at,
    updated_at
from {{ source('raw', 'products') }} final
