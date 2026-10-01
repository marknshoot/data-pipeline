"""Assets shared between DAGs.

`oltp_extract` declares ``RAW_OLTP`` as an outlet; ``warehouse_build`` is scheduled
on it. That makes the warehouse rebuild data-triggered: the moment new raw Parquet
lands, the load + dbt build fires, instead of relying on a time-based guess.
"""

from airflow.sdk import Asset

# The lake prefix the hourly extract writes to.
RAW_OLTP = Asset("s3://lake/raw/oltp")
