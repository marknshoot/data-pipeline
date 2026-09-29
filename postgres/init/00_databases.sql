-- Extra databases on the same Postgres instance (runs once, on first volume init).
-- "shop" (the OLTP source) is created by POSTGRES_DB.
CREATE DATABASE metabase;
CREATE DATABASE airflow;
