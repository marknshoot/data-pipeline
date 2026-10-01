{{ config(materialized='table', order_by='date_key') }}

-- Calendar dimension spanning the order history. Generated from the data range so
-- it never drifts out of sync with the facts.
with bounds as (
    select
        min(order_date) as min_date,
        max(order_date) as max_date
    from {{ ref('stg_orders') }}
),

days as (
    select toDate(b.min_date + toIntervalDay(number)) as date_key
    from bounds as b
    array join range(toUInt32(dateDiff('day', b.min_date, b.max_date)) + 1) as number
)

select
    date_key,
    toYear(date_key)                as year,
    toQuarter(date_key)             as quarter,
    toMonth(date_key)               as month,
    formatDateTime(date_key, '%M')  as month_name,
    toDayOfMonth(date_key)          as day_of_month,
    toDayOfWeek(date_key)           as day_of_week,
    formatDateTime(date_key, '%W')  as day_name,
    toDayOfWeek(date_key) in (6, 7) as is_weekend
from days
