-- Daily funnel invariants that generic tests cannot express.
--
--   1. Every rate is a proportion, so it must sit in [0, 1]. A rate above 1 means a
--      denominator was counted wrongly (or that two days' sessions were mixed).
--   2. Each step must be no larger than the one above it, on every day. This is the
--      per-day version of assert_funnel_monotonic, and it catches a case the overall
--      funnel hides: one day's data being wrong while the totals still look sane.
--
-- Returns offending rows; zero rows is a pass.
select
    event_date,
    page_view_sessions,
    cart_sessions,
    checkout_sessions,
    purchase_sessions,
    view_to_cart_rate,
    cart_to_checkout_rate,
    checkout_to_purchase_rate,
    view_to_purchase_rate
from {{ ref('mart_funnel_daily') }}
where view_to_cart_rate < 0
    or view_to_cart_rate > 1
    or cart_to_checkout_rate < 0
    or cart_to_checkout_rate > 1
    or checkout_to_purchase_rate < 0
    or checkout_to_purchase_rate > 1
    or view_to_purchase_rate < 0
    or view_to_purchase_rate > 1
    or cart_sessions > page_view_sessions
    or checkout_sessions > cart_sessions
    or purchase_sessions > checkout_sessions
