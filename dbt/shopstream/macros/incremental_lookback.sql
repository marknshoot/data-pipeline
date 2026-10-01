{#
  Incremental lookback filter.

  Facts are incremental on `updated_at`, but re-read a short overlapping window so a
  row that changed just after the previous run (or arrived late) is not missed.
  Combined with `incremental_strategy='delete+insert'` on the business key, the
  overlap is idempotent: the row is deleted and re-inserted, not duplicated.
#}
{% macro incremental_lookback(column='updated_at', lookback_days=1) -%}
    {%- if is_incremental() -%}
        {{ column }} > (
            select max({{ column }}) - toIntervalDay({{ lookback_days }}) from {{ this }}
        )
    {%- else -%}
        1 = 1
    {%- endif -%}
{%- endmacro %}
