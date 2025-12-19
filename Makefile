.PHONY: help setup start stop restart logs test lint format clean

help:
	@echo "BEES Brewery Pipeline - Available Commands"
	@echo ""
	@echo "  make setup    - Initial setup (copy .env and create directories)"
	@echo "  make start    - Start all Docker services"
	@echo "  make stop     - Stop all Docker services"
	@echo "  make restart  - Restart all Docker services"
	@echo "  make logs     - Follow Docker logs"
	@echo "  make test     - Run test suite inside container"
	@echo "  make lint     - Run linting checks"
	@echo "  make format   - Format code with black and isort"
	@echo "  make clean    - Clean generated files and stop services"

setup:
	@echo "Setting up environment..."
	cp -n .env.example .env || true
	mkdir -p data/bronze data/silver data/gold logs
	@echo "Setup complete! Edit .env if needed, then run 'make start'"

start:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Services started! Access Airflow at http://localhost:8080"
	@echo "Default credentials: airflow / airflow"

stop:
	@echo "Stopping services..."
	docker-compose down

restart:
	@echo "Restarting services..."
	docker-compose down
	docker-compose up -d

logs:
	docker-compose logs -f

test:
	@echo "Running tests..."
	docker-compose exec airflow-webserver pytest -v

lint:
	@echo "Running linting..."
	docker-compose exec airflow-webserver flake8 src/ tests/
	docker-compose exec airflow-webserver mypy src/

format:
	@echo "Formatting code..."
	docker-compose exec airflow-webserver black src/ tests/ dags/
	docker-compose exec airflow-webserver isort src/ tests/ dags/

clean:
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf data/bronze/* data/silver/* data/gold/*
	rm -rf logs/*
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Cleanup complete!"
