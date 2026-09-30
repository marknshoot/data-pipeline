select
    payment_id,
    order_id,
    lower(trim(method))   as payment_method,
    amount,
    lower(trim(status))   as payment_status,
    created_at,
    updated_at
from {{ source('raw', 'payments') }} final
