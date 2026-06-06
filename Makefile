.PHONY: help install demo demo-lite test lint format clean

help:
	@echo "FleetPulse - Makefile Commands"
	@echo "==============================="
	@echo "make install      - Install Python dependencies"
	@echo "make demo         - Start full pipeline with Docker Compose"
	@echo "make demo-lite    - Start lite mode (no Kafka, Windows-friendly)"
	@echo "make test         - Run pytest tests"
	@echo "make lint         - Run ruff and mypy"
	@echo "make format       - Format code with black and ruff"
	@echo "make clean        - Clean generated files and stop containers"
	@echo "make dbt-docs     - Generate and serve dbt documentation"

install:
	pip install -r requirements.txt
	pre-commit install

demo:
	docker compose up --build

demo-lite:
	docker compose -f docker-compose.lite.yml up --build

test:
	pytest tests/ -v --cov=fleetpulse --cov-report=html --cov-report=term-missing

lint:
	ruff check fleetpulse/ tests/ dags/ app/
	mypy fleetpulse/ tests/ dags/ app/

format:
	black fleetpulse/ tests/ dags/ app/
	ruff check --fix fleetpulse/ tests/ dags/ app/

clean:
	docker compose down -v
	rm -rf data/ logs/ alerts/ *.duckdb *.duckdb.wal
	rm -rf dbt_project/target/ dbt_project/logs/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	rm -rf htmlcov/ .coverage

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve
