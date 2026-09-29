# data-pipeline: ShopStream Marketplace Data Platform

A self-hosted, end-to-end data engineering project for a simulated Southeast Asian e-commerce marketplace.
Batch extracts from the app database and a live Kafka clickstream flow into an S3-compatible data lake.
Spark cleans the data, dbt models it in ClickHouse, Airflow schedules the pipeline, and Metabase shows the dashboards.

> Status: **Phase 0 (setup)**. See [PLAN.md](PLAN.md) for the full roadmap and checklist.

## Stack

| Layer | Tool |
|---|---|
| Source app DB | PostgreSQL 16 |
| Streaming | Apache Kafka 4 (KRaft, no ZooKeeper) |
| Data lake | SeaweedFS (S3-compatible API) |
| Processing | PySpark |
| Warehouse | ClickHouse |
| Transformation | dbt |
| Orchestration | Apache Airflow 3 |
| BI | Metabase |

## Quick start

Requirements: Docker with Compose v2, [uv](https://docs.astral.sh/uv/), GNU make. About 8 GB of RAM.

```bash
make env          # creates .env from .env.example; replace the change_me values
make install      # Python dev tools + pre-commit hooks
make up-core      # postgres, seaweedfs (S3), clickhouse
make up-stream    # kafka + topic creation
make up-bi        # metabase at http://localhost:3000
make ps           # status
make down         # stop (data is kept)
```

Services are split into Compose profiles (`core`, `stream`, `bi`) so the stack fits on a laptop.
Every container has a memory limit. All ports bind to `127.0.0.1` only.

| Service | URL / port |
|---|---|
| Postgres | `localhost:5432` |
| S3 (SeaweedFS) | `http://localhost:8333` |
| ClickHouse | `http://localhost:8123` (HTTP), `localhost:9000` (native) |
| Kafka | `localhost:9094` |
| Metabase | `http://localhost:3000` |

## Development

```bash
make lint   # ruff + sqlfluff
make test   # pytest
```
