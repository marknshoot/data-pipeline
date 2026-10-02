{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    engine='MergeTree()',
    order_by='(event_date, event_id)',
) }}

-- Event-grain fact: one row per clickstream event, deduplicated by event_id.
--
-- The streaming path is at-least-once, so the same event can reach the lake more
-- than once; raw.events (ReplacingMergeTree by event_id) plus FINAL in staging
-- collapses those before they get here.
--
-- The incremental window looks back three hours because the generator makes ~3% of
-- events late (up to two hours behind send time). A late event belongs to an
-- already-processed hour, so the lookback re-reads those hours and delete+insert on
-- event_id updates them without duplicating anything.
with events as (
    select * from {{ ref('stg_events') }}
    {% if is_incremental() %}
    where event_time > (select max(event_time) - toIntervalHour(3) from {{ this }})
    {% endif %}
)

select
    e.event_id         as event_id,
    e.user_id          as user_id,
    e.session_id       as session_id,
    e.product_id       as product_id,
    e.event_type       as event_type,
    e.event_time       as event_time,
    e.event_date       as event_date,
    e.event_hour       as event_hour,
    e.city             as city,
    e.device_type      as device_type,
    e.platform_version as platform_version,
    e.schema_version   as schema_version,
    e.consumed_at      as consumed_at
from events as e
