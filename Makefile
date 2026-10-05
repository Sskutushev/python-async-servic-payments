.DEFAULT_GOAL := help
COMPOSE ?= docker compose
API_KEY ?= demo-api-key-change-me-please

.PHONY: help install check lint type test test-unit test-integration test-e2e cov up down logs demo migrate clean

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## install dependencies (uv)
	uv sync --all-groups

lint: ## ruff format check + lint
	uv run ruff format --check src tests alembic tools
	uv run ruff check src tests alembic tools

type: ## mypy (strict)
	uv run mypy

security: ## bandit + pip-audit
	uv run bandit -q -c pyproject.toml -r src
	uv run pip-audit --strict

complexity: ## no function may be harder to read than grade B
	uv run radon cc src --min C --total-average

hygiene: ## forbidden files, parseable configs, line endings
	uv run python tools/check_repo.py

check: lint type security complexity hygiene ## all static gates

test-unit: ## unit + property tests (no external services)
	uv run pytest tests/unit -q

test-integration: ## real PostgreSQL + RabbitMQ (docker compose up postgres rabbitmq)
	uv run pytest tests/integration tests/rabbit -q

test-e2e: ## full compose stack (make up demo first)
	uv run pytest tests/e2e -q -m e2e

test: test-unit test-integration ## unit + integration

cov: ## coverage report for unit + integration
	uv run pytest tests/unit tests/integration tests/rabbit --cov --cov-report=term-missing --cov-fail-under=85

up: ## build and start the whole stack (+ demo webhook receiver)
	$(COMPOSE) --profile demo up --build -d

down: ## stop everything, keep volumes
	$(COMPOSE) --profile demo down

clean: ## stop everything and drop volumes
	$(COMPOSE) --profile demo down -v

logs: ## follow api + consumer + receiver logs
	$(COMPOSE) --profile demo logs -f api consumer webhook-receiver

migrate: ## run migrations against the compose database
	$(COMPOSE) run --rm migrate

demo: ## create a payment and poll it until it reaches a final state
	@uv run python tools/demo.py
