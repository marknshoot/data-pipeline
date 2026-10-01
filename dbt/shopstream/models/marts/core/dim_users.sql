{{ config(materialized='table', order_by='user_id') }}

-- SCD2 customer dimension. One row per version; `is_current` marks the live one.
-- `user_sk` (the snapshot's scd id) is the surrogate key facts join on.
select
    dbt_scd_id            as user_sk,
    user_id,
    email,
    full_name,
    phone,
    city,
    country_code,
    dbt_valid_from        as valid_from,
    dbt_valid_to          as valid_to,
    dbt_valid_to is null  as is_current
from {{ ref('users_snapshot') }}
