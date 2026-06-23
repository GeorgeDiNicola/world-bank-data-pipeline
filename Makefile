PROJECT_NAME=world-bank-data-pipeline
PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
SPARK_MASTER_URL ?= spark://spark-master:7077
WORKERS := $(if $(filter-out run,$(MAKECMDGOALS)),$(filter-out run,$(MAKECMDGOALS)),3)

.PHONY: install run test test-coverage stop

install:
	@if [ -f requirements.txt ]; then \
		$(PYTHON) -m pip install --upgrade pip; \
		$(PYTHON) -m pip install -r requirements.txt; \
	else \
		echo "requirements.txt not found, skipping pip install."; \
	fi

run:
	docker compose up -d --scale spark-worker=$(WORKERS)
	docker compose exec spark-master spark-submit --master $(SPARK_MASTER_URL) /src/main.py

test:
	$(PYTHON) -m pytest -v -rs tests/

test-coverage:
	$(PYTHON) -m pytest -v -rs --cov=src tests/

stop:
	docker compose down

%:
	@:
