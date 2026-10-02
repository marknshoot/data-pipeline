select
    event_id,
    toInt32(schema_version)             as schema_version,
    lower(trim(event_type))             as event_type,
    user_id,
    session_id,
    product_id,
    event_time,
    toDate(event_time)                  as event_date,
    toStartOfHour(event_time)           as event_hour,
    trim(city)                          as city,
    -- Schema v1 carries `device`, v2 carries `device_type` (+ platform_version).
    -- Coalescing here keeps the version split out of every downstream model.
    coalesce(nullIf(device_type, ''), nullIf(device, '')) as device_type,
    nullIf(platform_version, '')        as platform_version,
    consumed_at
from {{ source('raw', 'events') }} final
where event_type != ''
