{% snapshot products_snapshot %}
{#
  SCD2 history for listings. Prices drift, so the price a customer actually paid
  must come from the fact table (`order_items.unit_price`), while the dimension
  keeps every price a product has had and when.

  Same caveat as users_snapshot: raw keeps only the newest version, so history is
  built forward from the first snapshot run.

  Nuance: `strategy='timestamp'` versions on ANY change to the row, so a stock-only
  update also creates a version (its price is unchanged in both rows). The
  alternative, `strategy='check'` with `check_cols=['price','cost','is_active']`,
  would suppress those but would set `dbt_valid_from` to the snapshot run time
  instead of business time.
#}
{{ config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True,
) }}

select * from {{ ref('stg_products') }}

{% endsnapshot %}
