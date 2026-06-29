PROJECT_NAME=world-bank-data-pipeline
PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
DOCKERHUB_NAMESPACE ?= georgedinicola
IMAGE_TAG ?= latest
DOCKER_PLATFORMS ?= linux/amd64,linux/arm64
IMAGE_OUTPUT_DIR ?= dist/docker
BUILDX_BUILDER ?= world-bank-builder
PIPELINE_JOB_IMAGE ?= $(DOCKERHUB_NAMESPACE)/world-bank-pipeline-job
PACKAGE_JOB_IMAGE ?= $(DOCKERHUB_NAMESPACE)/world-bank-package-job
SPARK_MASTER_URL ?= spark://spark-master:7077
SPARK_SQL_SHUFFLE_PARTITIONS ?= 8
SPARK_WORKER_CORES ?= 1
SPARK_WORKER_MEMORY ?= 2g
SPARK_DRIVER_MEMORY ?= 1g
SPARK_EXECUTOR_MEMORY ?= 1g
SPARK_EXECUTOR_CORES ?= 1
WORKERS := $(if $(filter-out run,$(MAKECMDGOALS)),$(filter-out run,$(MAKECMDGOALS)),1)

.PHONY: install run docker-use-buildx-builder docker-build-images docker-push-images test test-coverage stop

install:
	@if [ -f requirements.txt ]; then \
		$(PYTHON) -m pip install --upgrade pip; \
		$(PYTHON) -m pip install -r requirements.txt; \
	else \
		echo "requirements.txt not found, skipping pip install."; \
	fi

run:
	PIPELINE_JOB_IMAGE=$(PIPELINE_JOB_IMAGE) PACKAGE_JOB_IMAGE=$(PACKAGE_JOB_IMAGE) IMAGE_TAG=$(IMAGE_TAG) docker compose pull pipeline-job package-job
	SPARK_WORKER_CORES=$(SPARK_WORKER_CORES) SPARK_WORKER_MEMORY=$(SPARK_WORKER_MEMORY) docker compose up -d --scale spark-worker=$(WORKERS)
	SPARK_MASTER_URL=$(SPARK_MASTER_URL) SPARK_SQL_SHUFFLE_PARTITIONS=$(SPARK_SQL_SHUFFLE_PARTITIONS) SPARK_DRIVER_MEMORY=$(SPARK_DRIVER_MEMORY) SPARK_EXECUTOR_MEMORY=$(SPARK_EXECUTOR_MEMORY) SPARK_EXECUTOR_CORES=$(SPARK_EXECUTOR_CORES) docker compose run --rm --no-deps pipeline-job
	docker compose run --rm --no-deps package-job

docker-use-buildx-builder:
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || docker buildx create --name $(BUILDX_BUILDER) --driver docker-container --use
	docker buildx inspect --bootstrap $(BUILDX_BUILDER)

docker-build-images: docker-use-buildx-builder
	mkdir -p $(IMAGE_OUTPUT_DIR)
	docker buildx build --builder $(BUILDX_BUILDER) --platform $(DOCKER_PLATFORMS) --file Dockerfile.job --tag $(PIPELINE_JOB_IMAGE):$(IMAGE_TAG) --output type=oci,dest=$(IMAGE_OUTPUT_DIR)/world-bank-pipeline-job-$(IMAGE_TAG).oci .
	docker buildx build --builder $(BUILDX_BUILDER) --platform $(DOCKER_PLATFORMS) --file Dockerfile.package --tag $(PACKAGE_JOB_IMAGE):$(IMAGE_TAG) --output type=oci,dest=$(IMAGE_OUTPUT_DIR)/world-bank-package-job-$(IMAGE_TAG).oci .

docker-push-images: docker-use-buildx-builder
	docker buildx build --builder $(BUILDX_BUILDER) --platform $(DOCKER_PLATFORMS) --file Dockerfile.job --tag $(PIPELINE_JOB_IMAGE):$(IMAGE_TAG) --push .
	docker buildx build --builder $(BUILDX_BUILDER) --platform $(DOCKER_PLATFORMS) --file Dockerfile.package --tag $(PACKAGE_JOB_IMAGE):$(IMAGE_TAG) --push .

test:
	$(PYTHON) -m pytest -v -rs tests/

test-coverage:
	$(PYTHON) -m pytest -v -rs --cov=src tests/

stop:
	docker compose down

%:
	@:
