{{ config(materialized='table', order_by='step_order') }}

-- The funnel as a single ordered series, which is the shape a funnel chart wants:
-- one row per step with its size, share of the entry step and drop-off from the
-- step before. Long format also means a test can assert monotonicity directly.
--
-- Steps are counted independently (a session that added to cart without a page
-- view still counts), so a "widening" step is a real signal of upstream data
-- problems rather than something the query smoothed over.
with steps as (
    select
        1                          as step_order,
        'page_view'                as step,
        uniqExactIf(session_id, event_type = 'page_view') as sessions
    from {{ ref('fct_events') }}

    union all

    select
        2,
        'add_to_cart',
        uniqExactIf(session_id, event_type = 'add_to_cart')
    from {{ ref('fct_events') }}

    union all

    select
        3,
        'checkout',
        uniqExactIf(session_id, event_type = 'checkout')
    from {{ ref('fct_events') }}

    union all

    select
        4,
        'purchase',
        uniqExactIf(session_id, event_type = 'purchase')
    from {{ ref('fct_events') }}
),

-- The entry step (the largest) is the funnel's denominator; the previous step comes
-- from a self-join rather than a window function so the logic stays obvious.
funnel_context as (
    select
        s.step_order              as step_order,
        s.step                    as step,
        s.sessions                as sessions,
        e.entry_sessions          as entry_sessions,
        p.sessions                as previous_sessions
    from steps as s
    cross join (select max(sessions) as entry_sessions from steps) as e
    left join steps as p
        on p.step_order = s.step_order - 1
)

select
    step_order,
    step,
    sessions,
    round(sessions / nullIf(entry_sessions, 0), 4)      as share_of_entry,
    round(sessions / nullIf(previous_sessions, 0), 4)   as step_conversion_rate,
    round(1 - sessions / nullIf(previous_sessions, 0), 4) as drop_off_rate
from funnel_context
order by step_order
