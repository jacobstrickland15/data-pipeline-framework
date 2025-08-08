# Data Pipeline Makefile

.PHONY: help install install-dev install-spark setup test lint format clean docker-build docker-run generate-python-analysis generate-scala-analysis generate-all-analysis

# Default target
help:
	@echo "Available commands:"
	@echo "  install      - Install basic dependencies"
	@echo "  install-dev  - Install development dependencies"
	@echo "  install-spark - Install with Spark support"
	@echo "  setup        - Complete setup with environment"
	@echo "  test         - Run tests"
	@echo "  lint         - Run linting"
	@echo "  format       - Format code"
	@echo "  clean        - Clean up temporary files"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run in Docker container"
	@echo "  generate-python-analysis - Generate Python analysis code"
	@echo "  generate-scala-analysis - Generate Scala analysis code"
	@echo "  generate-all-analysis - Generate analysis code for all tables"

# Installation targets
install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt
	pip install -e ".[dev]"

install-spark:
	pip install -r requirements.txt
	pip install -e ".[spark]"

install-all:
	pip install -e ".[all]"

# Setup environment
setup: install-dev
	@echo "Setting up environment..."
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env file - please edit with your configuration"; fi
	@mkdir -p data/{raw,processed,external}
	@mkdir -p logs
	@mkdir -p reports
	@echo "Environment setup complete!"

# Development targets
test:
	pytest tests/ -v --cov=src/data_pipeline --cov-report=html --cov-report=term-missing

lint:
	flake8 src/ tests/
	mypy src/data_pipeline

format:
	black src/ tests/
	isort src/ tests/

# Maintenance
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage

# Docker targets
docker-build:
	docker build -t data-pipeline:latest .

docker-run:
	docker run -it --rm \
		-v $(PWD)/data:/app/data \
		-v $(PWD)/config:/app/config \
		-v $(PWD)/reports:/app/reports \
		--env-file .env \
		data-pipeline:latest

# Pipeline commands
run-sample:
	data-pipeline run config/pipelines/sample_csv_pipeline.yaml --verbose

profile-sample:
	data-pipeline profile data/raw/sample.csv --format html

init-pipeline:
	data-pipeline init --name my_pipeline --source-type csv

# Analysis code generation
generate-python-analysis:
	data-pipeline generate analysis sample_table --language python --output analysis_sample.py

generate-scala-analysis:
	data-pipeline generate analysis sample_table --language scala --output analysis_sample.scala

generate-all-analysis:
	data-pipeline generate analysis --all-tables --language python --output-dir ./analysis/