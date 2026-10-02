-- A funnel that widens is a bug, never a result: every step must be a subset of the
-- step before it. Steps are counted independently in mart_funnel_steps, so a
-- widening step means the clickstream itself is inconsistent (for example an
-- `add_to_cart` arriving with no session that ever viewed a page).
--
-- Returns one row per offending step; zero rows is a pass.
with steps as (
    select
        step_order,
        sessions
    from {{ ref('mart_funnel_steps') }}
)

select
    current_step.step_order     as step_order,
    current_step.sessions       as sessions,
    previous_step.sessions      as previous_sessions
from steps as current_step
inner join steps as previous_step
    on previous_step.step_order = current_step.step_order - 1
where current_step.sessions > previous_step.sessions
