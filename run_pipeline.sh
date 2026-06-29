#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

WORKERS="${WORKERS:-1}"
SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-1}"
SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-8}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-1g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
PIPELINE_JOB_IMAGE="${PIPELINE_JOB_IMAGE:-georgedinicola/world-bank-pipeline-job}"
PACKAGE_JOB_IMAGE="${PACKAGE_JOB_IMAGE:-georgedinicola/world-bank-package-job}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

cleanup() {
    exit_code="$?"
    docker compose down || true
    exit "${exit_code}"
}

trap cleanup EXIT

echo "Pulling pipeline images..."
PIPELINE_JOB_IMAGE="${PIPELINE_JOB_IMAGE}" \
PACKAGE_JOB_IMAGE="${PACKAGE_JOB_IMAGE}" \
IMAGE_TAG="${IMAGE_TAG}" \
docker compose pull pipeline-job package-job

echo "Starting Spark cluster..."
SPARK_WORKER_CORES="${SPARK_WORKER_CORES}" \
SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY}" \
docker compose up -d --scale spark-worker="${WORKERS}"

echo "Running Spark pipeline..."
SPARK_MASTER_URL="${SPARK_MASTER_URL}" \
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS}" \
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY}" \
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY}" \
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES}" \
docker compose run --rm --no-deps pipeline-job

echo "Packaging and publishing data..."
docker compose run --rm --no-deps package-job
