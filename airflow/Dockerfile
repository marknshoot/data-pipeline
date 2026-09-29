# Airflow runs our DAGs, so it needs the same lake/DB dependencies as the repo.
# Pinned to match pyproject.toml runtime dependencies.
FROM apache/airflow:3.3.2-python3.12

RUN pip install --no-cache-dir \
    "boto3==1.43.104" \
    "pyarrow==25.0.1" \
    "psycopg[binary]==3.3.6" \
    "python-dotenv==1.2.3"
