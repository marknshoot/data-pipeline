"""Data-lake and ingestion jobs.

`oltp_extract` reads increments from Postgres and lands Parquet in the S3-compatible
lake; later phases add the streaming consumer and Spark jobs here.
"""
