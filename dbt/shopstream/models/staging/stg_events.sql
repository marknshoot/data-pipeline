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
    -- Schema v1's `device` and v2's `device_type` were merged by the Spark clean
    -- layer (spark/clean_events.py), so there is nothing to coalesce here.
    nullIf(device_type, '')             as device_type,
    nullIf(platform_version, '')        as platform_version,
    consumed_at
from {{ source('raw', 'events') }} final
where event_type != ''
