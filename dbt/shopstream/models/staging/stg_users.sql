-- One row per user. FINAL collapses ReplacingMergeTree versions to the newest
-- updated_at, so staging is the first place duplicates are resolved explicitly.
select
    user_id,
    lower(trim(email))                     as email,
    trim(full_name)                        as full_name,
    nullIf(trim(coalesce(phone, '')), '')  as phone,
    trim(city)                             as city,
    upper(trim(country))                   as country_code,
    created_at,
    updated_at
from {{ source('raw', 'users') }} final
