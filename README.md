# data-pipeline: ShopStream Marketplace Data Platform

A self-hosted, end-to-end data engineering project for a simulated Southeast Asian e-commerce marketplace.
Batch extracts from the app database and a live Kafka clickstream flow into an S3-compatible data lake.
Spark cleans the data, dbt models it in ClickHouse, Airflow schedules the pipeline, and Metabase shows the dashboards.

> Status: **Phase 1 (sources)**. See [PLAN.md](PLAN.md) for the full roadmap and checklist.

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
make seed         # Faker users/sellers/products + 30 days of order history
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

## Simulated source data

Phase 1 ships the Postgres schema and the two generators that keep the source
systems busy. With `make up-core` running:

```bash
make seed          # 10k users, 500 sellers, 5k products, 30 days of order history
make gen-orders    # new orders + status/price/city mutations, runs until Ctrl-C
make gen-events    # clickstream into Kafka (needs `make up-stream`)
```

Every generator takes `--rate`, `--duration`, `--tick`, `--seed` and `--dry-run`
(see `--help`). Dry-run mode exercises the whole loop without touching Postgres or
Kafka, which is also how the unit tests drive it.

The data is messy on purpose, because the pipeline has to cope with it:

| Imperfection | Produced by | Roughly |
|---|---|---|
| duplicate `event_id`, delivered twice | `event_generator` | 2% |
| late events (`event_time` up to 2h back) | `event_generator` | 3% |
| schema v2: `device` → `device_type` + `platform_version` | `event_generator` (`--schema-v2-start`) | — |
| malformed JSON / missing fields | `event_generator` → dead-letter later | 0.5% |
| orders stalled mid-lifecycle | `seed`, `order_generator` | — |
| price drift, stock decrements, users changing city | `order_generator` | — |

Order and event timestamps follow an evening-peaked daily curve with a 5× spike on
the 9.9 / 11.11 / 12.12 flash-sale days (local time `Asia/Jakarta`). Because late
events can cross the schema-v2 boundary, a single live run naturally contains both
schema versions.

## Development

```bash
make schema       # (re)apply the OLTP schema to a running Postgres
make seed         # seed reference data + history
make seed-reset   # wipe and re-seed
make lint         # ruff + sqlfluff
make test         # pytest
make check        # lint + test
```
