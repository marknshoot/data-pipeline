SHELL := /bin/bash
COMPOSE := docker compose

# Make this project's .env authoritative over the ambient shell. Another project may
# export POSTGRES_* etc., which would otherwise silently override our config when
# Compose interpolates ${...} at `up` time.
-include .env
export

.DEFAULT_GOAL := help
.PHONY: help env install up-core up-stream up-bi up up-orch down ps logs clean lint fmt test check \
	schema seed seed-reset gen-orders gen-events extract lake-ls ch-schema load-raw dbt dbt-build \
	metabase-setup screenshots py shell

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

env: ## Create .env from .env.example (edit the change_me values afterwards)
	@test -f .env || (cp .env.example .env && echo "Created .env - replace the change_me values")

install: ## Install Python dev tooling and git hooks
	uv sync
	uv run pre-commit install

up-core: env ## Start postgres, seaweedfs (S3), clickhouse
	$(COMPOSE) --profile core up -d --wait

up-stream: env ## Start kafka and create topics
	$(COMPOSE) --profile stream up -d --wait kafka
	$(COMPOSE) --profile stream run --rm kafka-init

up-bi: env ## Start metabase (needs core)
	$(COMPOSE) --profile core --profile bi up -d --wait postgres metabase

metabase-setup: env ## Provision Metabase: admin, ClickHouse connection, dashboards
	uv run python -m metabase.provision

screenshots: env ## Capture UI screenshots into docs/img (needs up-bi + up-orch)
	uv run --with playwright==1.63.0 python -m scripts.capture_screenshots

schema: env ## Apply the OLTP schema + pipeline metadata to a running Postgres
	@for f in postgres/init/0[1-9]_*.sql; do \
		echo "applying $$f"; \
		$(COMPOSE) --profile core exec -T postgres sh -c 'psql -q -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"' < $$f || exit 1; \
	done

seed: env ## Seed reference data + order history (fails if already seeded)
	uv run python -m generators.seed $(ARGS)

seed-reset: env ## Wipe and re-seed the OLTP database
	uv run python -m generators.seed --reset $(ARGS)

gen-orders: env ## Continuously generate orders (make gen-orders ARGS="--rate 10")
	uv run python -m generators.order_generator $(ARGS)

gen-events: env ## Publish clickstream events to Kafka (make gen-events ARGS="--rate 40")
	uv run python -m generators.event_generator $(ARGS)

extract: env ## Incremental OLTP extract to the lake (make extract ARGS="--tables orders")
	uv run python -m ingestion.oltp_extract --incremental $(ARGS)

lake-ls: env ## List objects in the lake (make lake-ls ARGS="raw/oltp/orders")
	uv run python -m ingestion.lake $(ARGS)

ch-schema: env ## Apply clickhouse/init/*.sql to a running ClickHouse (idempotent)
	@for f in clickhouse/init/0[1-9]_*.sql; do \
		echo "applying $$f"; \
		docker compose --profile core exec -T clickhouse clickhouse-client --multiquery < $$f || exit 1; \
	done

load-raw: env ## Load lake Parquet into ClickHouse raw tables (make load-raw ARGS="--truncate")
	uv run python -m warehouse.load_raw $(ARGS)

DBT_DIR := dbt/shopstream
# dbt 1.12 moved --project-dir/--profiles-dir onto each subcommand; the env vars are
# equivalent and apply uniformly.
export DBT_PROJECT_DIR := $(DBT_DIR)
export DBT_PROFILES_DIR := $(DBT_DIR)

dbt: env ## Run dbt (make dbt ARGS="build")
	uv run dbt $(ARGS)

dbt-build: env ## dbt build (run + test everything)
	uv run dbt build

py: env ## Run Python with this project's .env (make py ARGS="-c 'print(1)'")
	uv run python $(ARGS)

shell: env ## Interactive shell with this project's .env exported
	@exec bash

up: up-core up-stream ## Start core + stream

up-orch: env ## Start Airflow (api-server, scheduler, dag-processor) + its core deps
	$(COMPOSE) --profile core --profile orch up -d --wait postgres seaweedfs \
		airflow-api-server airflow-scheduler airflow-dag-processor

down: ## Stop all services (keeps volumes)
	$(COMPOSE) --profile '*' down

ps: ## Show service status
	$(COMPOSE) --profile '*' ps -a

logs: ## Tail logs (make logs s=clickhouse)
	$(COMPOSE) --profile '*' logs -f --tail=100 $(s)

clean: ## Stop everything AND delete all data volumes (destructive)
	@read -p "Delete all local data volumes? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) --profile '*' down -v

lint: ## Run ruff and sqlfluff
	uv run ruff check .
	uv run ruff format --check .
	uv run sqlfluff lint clickhouse/

fmt: ## Auto-format Python
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run pytest
	uv run pytest

check: lint test ## Lint + test
