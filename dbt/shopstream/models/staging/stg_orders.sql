select
    order_id,
    user_id,
    lower(trim(status))     as order_status,
    upper(trim(currency))   as currency,
    trim(shipping_city)     as shipping_city,
    total_amount,
    toDate(created_at)      as order_date,
    created_at,
    updated_at,
    -- Terminal states no longer change; used to filter "in flight" orders.
    status in ('delivered', 'cancelled', 'refunded') as is_terminal
from {{ source('raw', 'orders') }} final
