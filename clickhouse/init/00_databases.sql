-- Warehouse layers. dbt builds staging/marts later; raw is loaded from the lake.
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS marts;
