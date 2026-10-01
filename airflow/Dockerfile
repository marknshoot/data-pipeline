# Airflow runs our DAGs, so it needs the same warehouse/DB/lake dependencies as the
# repo: dbt (transformation), clickhouse-connect (raw load), pyarrow+boto3 (lake),
# psycopg (source DB). Pinned to match pyproject.toml.
FROM apache/airflow:3.3.2-python3.12

RUN pip install --no-cache-dir \
    "boto3==1.43.104" \
    "clickhouse-connect==1.9.0" \
    "dbt-clickhouse==1.10.3" \
    "dbt-core==1.12.5" \
    "psycopg[binary]==3.3.6" \
    "pyarrow==25.0.1" \
    "python-dotenv==1.2.3"
