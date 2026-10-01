# ShopStream: End-to-End Marketplace Data Platform (Self-Hosted)

A self-hosted, Docker-based data platform for a simulated Southeast Asian e-commerce marketplace.
It takes in data two ways: batch extracts from the app database and a live clickstream through Kafka.
The data lands in an S3-compatible data lake, is cleaned with Spark, modelled with dbt in ClickHouse,
run on a schedule by Airflow, and shown in Metabase.

Target: a portfolio project for data engineering internship applications.
Emphasis (from job-post research): SQL and data modelling > ETL reliability > Spark/cloud > Airflow/Kafka.

---

## 1. Stack

| Layer | Tool | Notes |
|---|---|---|
| Source app DB | PostgreSQL 16 | `shop` database. Also hosts the Airflow metadata DB (separate database). |
| Event streaming | Apache Kafka 4.x (KRaft mode, single broker) | No ZooKeeper. Heap capped at 512 MB. |
| Object storage / data lake | SeaweedFS (S3 gateway) | S3-compatible. Endpoint is set by config so the same code can point to AWS S3. |
| Processing | PySpark (runs on demand in `local[2]` mode, not a long-running cluster) | Cleans raw data into the `clean` layer. |
| Warehouse | ClickHouse | Analytics. Reads Parquet from the lake with the `s3()` table function. Kafka engine feeds the real-time panel. |
| Transformation | dbt-core + dbt-clickhouse | Staging → intermediate → marts, snapshots, tests, docs. |
| Orchestration | Apache Airflow 3.x (LocalExecutor) | Schedule-based and data-triggered (asset) DAGs. |
| BI | Metabase (with ClickHouse driver) | Dashboards. |
| Infra / CI | Docker Compose (with profiles), Makefile, GitHub Actions, optional Terraform (AWS S3) | |
| Code quality | ruff, pytest, sqlfluff, pre-commit | |

Pin every image tag and Python package to an exact version when you set things up. Don't use `latest`.

### RAM budget (this machine: 7.7 GB total, about 4.5 GB free)

| Service | Limit |
|---|---|
| Postgres | 256 MB |
| Kafka | 768 MB |
| SeaweedFS | 256 MB |
| ClickHouse | 1.2 GB |
| Airflow (api-server + scheduler + dag-processor) | 1.5 GB |
| Metabase | 1 GB |
| Spark job (short-lived) | 1.2 GB |

All together this is more than the machine has free, so split the services with Compose profiles:
- `core`: postgres, seaweedfs, clickhouse (always on)
- `stream`: kafka, event-generator, lake-consumer
- `orch`: airflow
- `bi`: metabase

Build and test one profile at a time. Run the full demo with other apps closed, or with a larger swap file.
Set `mem_limit` on every service.

---

## 2. Architecture

```
                ┌──────────────────────┐
                │  order-generator     │  inserts/updates orders, users, products
                └─────────┬────────────┘
                          ▼
                ┌──────────────────────┐   Airflow: oltp_extract (hourly, incremental by updated_at)
                │ Postgres  (shop DB)  │ ─────────────────────────────────────┐
                └──────────────────────┘                                      │
                                                                              ▼
┌──────────────────┐   ┌─────────┐   ┌────────────────┐        ┌──────────────────────────────┐
│ event-generator  │ → │  Kafka  │ → │ lake-consumer  │ ─────▶ │ SeaweedFS  s3://lake          │
│ (clickstream)    │   │ topic:  │   │ micro-batch →  │        │  raw/   (Parquet, as-is)      │
└──────────────────┘   │ events  │   │ Parquet        │        │  clean/ (deduped, typed)      │
                       └────┬────┘   └────────────────┘        └──────────────┬───────────────┘
                            │                                   Spark job (hourly) raw → clean
                            │  ClickHouse Kafka engine                        │
                            ▼  (real-time path)                               ▼  s3() load
                     ┌──────────────────────────────────────────────────────────────────┐
                     │ ClickHouse: raw_* → dbt staging → intermediate → marts            │
                     └──────────────────────────────┬───────────────────────────────────┘
                                                    ▼
                                           Metabase dashboards
```

The design has two paths:
- Batch path (accurate): Kafka → lake → Spark → ClickHouse → dbt. It's the source of truth.
- Real-time path (fast, approximate): Kafka → ClickHouse Kafka engine → materialized view. It feeds only the "live" panel.
- The README should explain why the two paths exist (this is known as a Lambda-style architecture).

---

## 3. Data design

### Source (Postgres `shop` database)
`users`, `sellers`, `products`, `categories`, `orders`, `order_items`, `payments`.
Every table has `created_at` and `updated_at`.

### Events (Kafka topic `clickstream.events`, JSON)
```json
{"event_id": "uuid", "schema_version": 1, "event_type": "page_view|add_to_cart|checkout|purchase",
 "user_id": 123, "session_id": "uuid", "product_id": 456, "event_time": "ISO-8601",
 "device": "android|ios|web", "city": "Jakarta"}
```
The generator makes the data messy on purpose, so the pipeline has to handle it:
- [ ] about 2% duplicate `event_id`s
- [ ] about 3% late events (`event_time` up to 2 hours before the send time)
- [ ] schema v2 from a chosen date onward: adds `platform_version`, renames `device` to `device_type`
- [ ] a few malformed records, which go to a dead-letter location

### Lake layout
```
s3://lake/raw/oltp/<table>/dt=YYYY-MM-DD/<run_id>.parquet
s3://lake/raw/events/dt=YYYY-MM-DD/hr=HH/<batch>.parquet
s3://lake/clean/events/dt=YYYY-MM-DD/hr=HH/part-*.parquet   (partitioned by event_time, not arrival time)
s3://lake/dlq/events/dt=YYYY-MM-DD/
```

### Warehouse (star schema)
- Facts: `fct_orders`, `fct_order_items`, `fct_events`, `fct_payments`
- Dimensions: `dim_users` (keeps change history, SCD2, via a dbt snapshot), `dim_products` (SCD2 for price), `dim_sellers`, `dim_categories`, `dim_date`
- Marts: `mart_daily_gmv`, `mart_funnel_daily`, `mart_cohort_retention`, `mart_seller_performance`
- Real-time: `rt_orders_per_minute` (ClickHouse materialized view fed from Kafka)

---

## 4. Repo layout

```
dateng/
├── docker-compose.yml
├── Makefile                  # up, down, seed, demo, test, lint, backfill
├── .env.example              # never commit .env
├── generators/               # order_generator.py, event_generator.py
├── ingestion/                # lake_consumer.py, oltp_extract.py
├── spark/jobs/               # clean_events.py
├── airflow/dags/             # oltp_extract, clean_events, warehouse_build
├── clickhouse/init/          # raw tables, Kafka engine, materialized views
├── dbt/shopstream/           # models/, snapshots/, tests/, macros/
├── metabase/                 # dashboard export / setup script
├── infra/terraform/aws/      # optional: S3 bucket + IAM
├── tests/                    # pytest (generators, consumer, spark job)
├── docs/                     # architecture.png, data_model.png, screenshots
└── .github/workflows/ci.yml
```

---

## 5. Build checklist

### Phase 0: Setup (days 1–2) ✅
- [x] `git init`, `.gitignore` (.env, data/, target/, logs/), README stub
- [x] Python project (uv), with ruff, pytest, sqlfluff and pre-commit configured
- [x] `docker-compose.yml` with profiles, pinned image tags, `mem_limit`, healthchecks
- [x] `.env.example` with all credentials as placeholders
- [x] `Makefile`: `up`, `down`, `logs`, `ps`, `clean`

### Phase 1: Sources (days 3–6) ✅
- [x] Postgres schema and seed data (Faker): about 10k users, 500 sellers, 5k products
- [x] `order_generator`: creates new orders continuously, plus updates (order status changes, price changes, users moving city)
- [x] `event_generator`: writes to Kafka with duplicates, late events, schema v2 and malformed records
- [x] Traffic patterns: busier in the evening, and a flash-sale spike (e.g. 9.9 or 11.11)
- [x] Unit tests for the generators

### Phase 2: Lake and batch ingestion (week 2) ✅
- [x] SeaweedFS S3 gateway, `lake` bucket, access keys from env
- [x] `oltp_extract.py`: incremental by `updated_at` watermark → Parquet in `raw/oltp/...`
- [x] Safe to re-run: re-running the same interval overwrites its partition and doesn't duplicate data
- [x] Airflow up (LocalExecutor, metadata stored in Postgres)
- [x] DAG `oltp_extract` (hourly, catchup enabled, retries and a timeout on each task)
- [x] Test: run a backfill for 3 past days twice and check row counts stay the same

### Phase 3: Warehouse and dbt (weeks 2–3). This is the most important phase.
- [x] ClickHouse `raw` tables. Load from the lake with `INSERT ... SELECT FROM s3(...)`
- [x] dbt sources with freshness checks
- [x] Staging models (rename, cast, one model per source table)
- [x] Snapshots: `users`, `products` (SCD2)
- [x] Facts and dimensions (star schema), with incremental models for facts
- [~] Marts: GMV, funnel, cohort retention, seller performance
      — GMV, cohort retention and seller performance are built; `mart_funnel_daily`
        moves to Phase 5 because it needs the clickstream (events reach the
        warehouse only after the Kafka → lake → ClickHouse path exists)
- [x] Tests: unique, not_null, relationships, accepted_values, plus custom checks
      (order total = sum of items, cross-fact revenue reconciliation, SCD2
      one-current-version, cohort grain/bounds, no prefixed column names)
- [x] `dbt docs generate` (screenshot of the lineage graph still to take in Phase 9)
- [x] DAG `warehouse_build`: load → `dbt build`, triggered when the extract finishes (Airflow assets)

### Phase 4: Dashboard. Minimum viable product at the end of this phase (week 3)
- [x] Metabase connected to ClickHouse with a read-only user (`readonly = 2`)
- [x] Dashboard "Marketplace Overview": GMV trend, orders, AOV, top categories, top sellers
- [~] Dashboard "Customer": cohort retention heatmap, city breakdown done; funnel
      panel deferred to Phase 5 with `mart_funnel_daily` (needs clickstream events)
- [x] Setup reproducible from a script (`metabase/provision.py` creates the connection, 13 questions and 2 dashboards; re-runnable)
- [x] Checkpoint: this is already a complete batch project. Commit, and write the first version of the README.

### Phase 5: Streaming (week 4)
- [ ] Kafka topic `clickstream.events` (6 partitions, keyed by `user_id`)
- [ ] `lake_consumer.py`: consumer group, writes a micro-batch every N seconds or M records, commits offsets *after* the S3 write succeeds (at-least-once delivery)
- [ ] Malformed records → dead-letter location, with a count metric
- [ ] ClickHouse Kafka engine table → `rt_orders_per_minute` materialized view
- [ ] `fct_events` + `mart_funnel_daily` (moved here from Phase 3; needs the clickstream in the warehouse)
- [ ] "Live" Metabase panel with auto-refresh
- [ ] Test: kill the consumer while it's running, restart it, and confirm no events are lost (duplicates are fine and get removed later)

### Phase 6: Spark (week 5)
- [ ] `clean_events.py`: remove duplicates by `event_id`, merge schema v1 and v2, cast types, partition by `event_time`
- [ ] Late events: re-process a lookback window (e.g. the last 3 hours) and overwrite those partitions
- [ ] Run from Airflow (DockerOperator or spark-submit) as DAG `clean_events` (hourly)
- [ ] pytest on the Spark job with a small local SparkSession and fixtures
- [ ] Record the run time and row counts for the README

### Phase 7: Data quality and monitoring (week 5)
- [ ] Row-count check: Postgres vs warehouse, per day
- [ ] Airflow `on_failure_callback` → Discord/Telegram webhook (or a log alert)
- [ ] dbt source freshness failing → DAG fails
- [ ] Simple "pipeline health" Metabase panel (last load time, row counts, DLQ count)

### Phase 8: CI and infra (week 6)
- [ ] GitHub Actions: ruff, sqlfluff, pytest, `dbt parse`/`dbt build` against a ClickHouse service container on sample data
- [ ] pre-commit hooks
- [ ] Optional: Terraform `infra/terraform/aws` (S3 bucket, IAM user with least privilege). `plan` output only, or apply then destroy right away. Set a billing alert first.
- [ ] Optional: switch the lake to real AWS S3 just by changing env config, as a demo

### Phase 9: Presentation (week 6–7)
- [ ] `make demo`: fresh clone → running dashboard in under 10 minutes
- [ ] README: problem, architecture diagram, stack and *why* each tool, data model diagram, how to run, design decisions, results and metrics, limitations and future work
- [ ] Screenshots: Airflow DAGs, dbt lineage, Kafka and consumer logs, dashboards
- [ ] 2–3 minute demo video (linked at the top of the README)
- [ ] Résumé bullets with real numbers (see below)
- [ ] Pin the repo on GitHub and add it to LinkedIn

---

## 6. Interview talking points (make sure each one is in the README)

- Safe re-runs: partitions are overwritten and backfills are safe
- Incremental loads: watermark approach and its edge cases (clock skew, updates to the same row)
- Delivery guarantees: why at-least-once plus downstream de-duplication instead of exactly-once
- Late data: partitioning by event time and re-processing a lookback window
- Schema evolution: v1 → v2 handling
- SCD2: why history matters (e.g. revenue by the city the user lived in *at order time*)
- Batch vs real-time paths: trade-off between accuracy and speed
- Why ClickHouse (column storage, analytics) vs Postgres (app database, transactions)
- Running with limited resources: RAM budget and Compose profiles

## 7. Résumé bullet templates (fill in real numbers)

- Built an end-to-end marketplace data platform (Kafka, Spark, Airflow, dbt, ClickHouse, S3-compatible data lake) processing **X M events/day**, fully containerized and started with one command.
- Designed a star-schema warehouse with **N dbt models and M data-quality tests**, including SCD2 dimensions and incremental facts.
- Built backfill-safe, re-runnable Airflow pipelines. Handled duplicate, late and schema-changing events, and reduced hourly processing to **T minutes**.

## 8. Risks

| Risk | Mitigation |
|---|---|
| Running out of RAM | Compose profiles, `mem_limit`, Spark only when needed, swap |
| Scope creep | Finish Phase 4 (MVP) before starting streaming. Terraform is optional. |
| Simulated data looks fake | Realistic distributions and deliberate problems in the data, explained in the README |
| Tool versions drift | Pin image tags and package versions |
| Exposing services if you host it | Expose only the Metabase public dashboard. Keep everything else private. |
