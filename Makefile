SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help
.PHONY: help env install up-core up-stream up-bi up down ps logs clean lint fmt test check

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

up: up-core up-stream ## Start core + stream

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
