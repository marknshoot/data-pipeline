# data-pipeline: ShopStream Marketplace Data Platform

A self-hosted, end-to-end data engineering project for a simulated Southeast Asian e-commerce marketplace.
Batch extracts from the app database and a live Kafka clickstream flow into an S3-compatible data lake.
Spark cleans the data, dbt models it in ClickHouse, Airflow schedules the pipeline, and Metabase shows the dashboards.

> Status: **Phase 3 (warehouse)** — in progress. See [PLAN.md](PLAN.md) for the full roadmap and checklist.

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
make up-orch      # airflow (http://localhost:8080)
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
| Airflow | `http://localhost:8080` |
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

## Batch ingestion and orchestration

`make extract` copies rows changed since a stored watermark from Postgres into the
lake as Parquet:

```
s3://lake/raw/oltp/<table>/dt=YYYY-MM-DD/<window>.parquet
```

The object key is derived from the extract window, so **re-running an interval
overwrites its object instead of appending duplicates** — that is what makes
backfills safe. A row updated after its interval's end is picked up again by a
later interval and deduplicated downstream by newest `updated_at`.

Airflow schedules the same job hourly:

```bash
make up-orch                 # postgres + seaweedfs + airflow, UI on :8080
make extract ARGS="--history-days 400"   # one-off incremental run
make lake-ls ARGS="raw/oltp/orders"      # inspect the lake
uv run python -m ingestion.lake --stats raw/oltp
```

The `oltp_extract` DAG runs hourly with `catchup=True`, so unpausing it fills in
every missed hour; each task retries twice and has a 20-minute timeout. To re-run a
window (safe, idempotent):

```bash
docker compose --profile core --profile orch exec airflow-scheduler \
  airflow backfill create --dag-id oltp_extract \
  --from-date 2026-09-26T00:00:00+00:00 --to-date 2026-09-29T00:00:00+00:00 \
  --reprocess-behavior completed
```

### Airflow 3 notes

Three non-obvious settings are required when the components run in separate
containers (all are set in `docker-compose.yml`):

* `AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=true` — Airflow 3 otherwise gives
  cron DAGs a zero-width data interval, so `data_interval_start == data_interval_end`.
* `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` — tasks call the execution API, which
  defaults to `localhost:8080` (the wrong container once components are split).
* `SimpleAuthManager` with `all_admins`, since Airflow 3 has no login by default for
  a local deployment and the API is bound to `127.0.0.1` only. Switch to a real auth
  manager before exposing this anywhere.

## Warehouse (ClickHouse)

`make ch-schema` creates one `raw.<table>` per OLTP source table. `make load-raw`
fills them by reading the lake Parquet directly with ClickHouse's `s3()` table
function — no data passes through Python.

```bash
make ch-schema                    # create raw tables (idempotent)
make load-raw ARGS="--truncate"   # lake -> ClickHouse, clean reload
make load-raw                     # append; dedup handles repeats
```

Design notes:

* Raw tables are `ReplacingMergeTree(updated_at)` keyed by the business key. A row
  updated after its extract interval lands in several lake files, so repeated loads
  converge to one row per key with the newest `updated_at`. Merges are asynchronous,
  so exact queries use `FINAL` (dbt staging applies the same rule explicitly).
* `s3()` credentials are passed as server-side query parameters rather than
  interpolated into SQL, keeping them out of `system.query_log`.
* Intervals with no changes write no Parquet object: ClickHouse's Parquet reader
  rejects row groups with zero rows, and an empty file carries no information.
* ClickHouse reads the lake over the compose network, so it uses
  `S3_ENDPOINT_INTERNAL` (`http://seaweedfs:8333`) instead of the host endpoint.

Verified: raw row counts match Postgres one-for-one, and re-loading without
`--truncate` doubles the physical row count while `FINAL` stays constant.

## Development

```bash
make schema       # (re)apply the OLTP schema + pipeline metadata to a running Postgres
make seed         # seed reference data + history
make seed-reset   # wipe and re-seed
make extract      # incremental OLTP extract to the lake
make lake-ls      # list lake objects (make lake-ls ARGS="raw/oltp")
make lint         # ruff + sqlfluff
make test         # pytest
make check        # lint + test
```

### Configuration precedence

The Makefile exports `.env`, and the Python entry points load it with
`override=True`, so this project's `.env` always wins over variables exported in
your shell — other projects on a dev machine commonly export `POSTGRES_*` and would
otherwise silently hijack the connection. Compose services receive their
overrides from `docker-compose.yml` instead, and `.env` is never mounted into a
container, so no secret is copied in and container config cannot drift.
