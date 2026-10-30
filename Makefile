.PHONY: help init setup test lint format run backfill dashboard docker-up docker-down clean

help:
	@echo "Production Data Lakehouse & ETL Platform"
	@echo "========================================="
	@echo "Available commands:"
	@echo "  make init          Initialize environment and storage folders"
	@echo "  make setup         Install Python dependencies in virtualenv"
	@echo "  make test          Run complete test suite (unit, integration, e2e)"
	@echo "  make lint          Run ruff code quality linter"
	@echo "  make format        Format code using ruff"
	@echo "  make run           Execute end-to-end lakehouse pipeline"
	@echo "  make backfill      Execute historical backfill"
	@echo "  make dashboard     Launch Streamlit observability dashboard"
	@echo "  make docker-up     Start Docker Compose platform (Postgres, Airflow, Spark)"
	@echo "  make docker-down   Stop Docker Compose environment"
	@echo "  make clean         Clean temporary caches, logs, and staging files"

init:
	python -c "from src.common.config import config; config.ensure_directories()"

setup:
	pip install -r requirements.txt

test:
	pytest tests/ -v --durations=10

lint:
	ruff check .

format:
	ruff format .

run:
	python scripts/run_pipeline.py --batch-type INCREMENTAL

backfill:
	python scripts/run_backfill.py --start-date 2026-01-01 --end-date 2026-01-07

dashboard:
	streamlit run dashboard/app.py

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov
