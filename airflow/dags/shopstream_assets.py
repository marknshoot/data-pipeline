"""Assets shared between DAGs.

`oltp_extract` declares ``RAW_OLTP`` as an outlet; ``warehouse_build`` is scheduled
on it. That makes the warehouse rebuild data-triggered: the moment new raw Parquet
lands, the load + dbt build fires, instead of relying on a time-based guess.

The streaming path adds a second producer: ``clean_events`` publishes
``CLEAN_EVENTS`` once Spark has rewritten the clickstream's clean layer, and
``warehouse_build`` is scheduled on both. A list of assets means "any of these
updated", so either source can refresh the warehouse.
"""

from airflow.sdk import Asset

# The lake prefix the hourly extract writes to.
RAW_OLTP = Asset("s3://lake/raw/oltp")

# The lake prefix the Spark clean job writes to (deduplicated, by event time).
CLEAN_EVENTS = Asset("s3://lake/clean/events")
