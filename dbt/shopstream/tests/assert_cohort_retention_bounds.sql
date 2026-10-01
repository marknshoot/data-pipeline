-- Cohort retention sanity: a cohort's month_number = 0 row is its own size, so
-- retention must be exactly 1.0 there, and can never exceed 1 (or go negative).
-- Fails (returns rows) when a row violates those bounds.

select
    cohort_month,
    month_number,
    active_users,
    cohort_size,
    retention_rate
from {{ ref('mart_cohort_retention') }}
where retention_rate < 0
   or retention_rate > 1.0001
   or (month_number = 0 and retention_rate != 1)
   or active_users > cohort_size
