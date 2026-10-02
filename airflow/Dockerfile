# Airflow runs our DAGs, so it needs the same warehouse/DB/lake dependencies as the
# repo: dbt (transformation), clickhouse-connect (raw load), pyarrow+boto3 (lake),
# psycopg (source DB). Pinned to match pyproject.toml.
#
# It also carries a JVM and PySpark, because the `clean_events` DAG runs the Spark job
# in this container exactly as `warehouse_build` runs dbt. One image, one place for job
# dependencies -- and the standalone `spark` compose service reuses it, so the job and
# its tests run against identical versions.
FROM apache/airflow:3.3.2-python3.12

USER root
# Spark needs a JVM. Debian's default-jre-headless is Java 17, which is what Spark 4.1
# requires (it supports 17 and 21).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    "boto3==1.43.104" \
    "clickhouse-connect==1.9.0" \
    "dbt-clickhouse==1.10.3" \
    "dbt-core==1.12.5" \
    "psycopg[binary]==3.3.6" \
    "pyarrow==25.0.1" \
    "pyspark==4.1.3" \
    "pytest==9.1.1" \
    "python-dotenv==1.2.3"
