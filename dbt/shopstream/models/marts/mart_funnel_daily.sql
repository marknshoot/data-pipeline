{{ config(materialized='table', order_by='event_date') }}

-- Daily acquisition funnel, session-grain.
--
-- A session counts at a step if it produced at least one event of that type that
-- day. Sessions are the unit because a funnel is about journeys, and the generator
-- keeps a user's session_id stable across events so consecutive steps belong to the
-- same journey. (A stricter, strictly-sequential funnel would order events inside
-- the session; see the note in mart_funnel_steps.)
with daily as (
    select
        event_date,
        uniqExact(session_id)                                       as sessions,
        uniqExactIf(session_id, event_type = 'page_view')            as page_view_sessions,
        uniqExactIf(session_id, event_type = 'add_to_cart')          as cart_sessions,
        uniqExactIf(session_id, event_type = 'checkout')             as checkout_sessions,
        uniqExactIf(session_id, event_type = 'purchase')             as purchase_sessions
    from {{ ref('fct_events') }}
    group by event_date
)

select
    event_date,
    sessions,
    page_view_sessions,
    cart_sessions,
    checkout_sessions,
    purchase_sessions,
    -- Step-to-step conversion. nullIf keeps a zero denominator from producing inf
    -- or a divide-by-zero error on a day with no traffic at an earlier step.
    round(cart_sessions / nullIf(page_view_sessions, 0), 4)      as view_to_cart_rate,
    round(checkout_sessions / nullIf(cart_sessions, 0), 4)       as cart_to_checkout_rate,
    round(purchase_sessions / nullIf(checkout_sessions, 0), 4)   as checkout_to_purchase_rate,
    round(purchase_sessions / nullIf(page_view_sessions, 0), 4)  as view_to_purchase_rate
from daily
order by event_date
