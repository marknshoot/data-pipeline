{% snapshot users_snapshot %}
{#
  SCD2 history for customers.

  strategy='timestamp' uses the row's own `updated_at` as `dbt_valid_from`, so the
  history starts from each user's last known change rather than from the moment
  snapshotting began. NOTE: the raw layer keeps only the newest version of a row
  (ReplacingMergeTree), so versions older than the first snapshot run cannot be
  reconstructed — history is built forward from here. Point-in-time facts do not
  depend on this: orders already carry `shipping_city` as of order time.

  invalidate_hard_deletes closes the current version when a user disappears at source.
#}
{{ config(
    target_schema='snapshots',
    unique_key='user_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True,
) }}

select * from {{ ref('stg_users') }}

{% endsnapshot %}
