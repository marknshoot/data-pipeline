select
    seller_id,
    trim(name)                            as seller_name,
    lower(trim(email))                    as email,
    trim(city)                            as city,
    upper(trim(country))                  as country_code,
    rating,
    created_at,
    updated_at
from {{ source('raw', 'sellers') }} final
