-- Cohort mart grain: one row per (cohort_month, month_number).
-- Fails (returns rows) when the grain is violated.

select
    cohort_month,
    month_number,
    count() as rows_for_grain
from {{ ref('mart_cohort_retention') }}
group by cohort_month, month_number
having count() > 1
