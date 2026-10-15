.PHONY: help install run-generator run-pipeline run-dashboard test clean docker-up docker-down docker-logs

PYTHON ?= python
PIP ?= pip
STREAMLIT ?= streamlit
DOCKER_COMPOSE ?= docker compose

help:
	@echo "======================================================================"
	@echo " Real-Time Data Analytics Pipeline: Development & Operations Tasks"
	@echo "======================================================================"
	@echo " make install         - Install dependencies from requirements.txt"
	@echo " make run-pipeline    - Run real-time stream processor ingestion"
	@echo " make run-generator   - Run synthetic event stream generator"
	@echo " make run-dashboard   - Launch Streamlit analytics UI"
	@echo " make test            - Run pytest suite with coverage"
	@echo " make docker-up       - Start full stack via Docker Compose"
	@echo " make docker-down     - Stop container stack"
	@echo " make docker-logs     - Stream live logs from containers"
	@echo " make clean           - Remove temporary caches and database files"
	@echo "======================================================================"

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

run-pipeline:
	$(PYTHON) -m src.stream_processor --rate 15 --count 0

run-generator:
	$(PYTHON) -m src.data_generator --rate 10 --count 0

run-dashboard:
	$(STREAMLIT) run src/dashboard.py --server.port=8501

test:
	$(PYTHON) -m pytest tests/ -v --tb=short

docker-up:
	$(DOCKER_COMPOSE) up -d --build

docker-down:
	$(DOCKER_COMPOSE) down -v

docker-logs:
	$(DOCKER_COMPOSE) logs -f

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -f *.db *.sqlite3 *.log
