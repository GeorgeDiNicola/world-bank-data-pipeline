#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

cleanup() {
    exit_code="$?"
    docker compose down || true
    exit "${exit_code}"
}

trap cleanup EXIT

echo "Pulling pipeline images..."
docker compose pull pipeline-job package-job

echo "Starting Spark cluster..."
docker compose up -d --scale spark-worker="${WORKERS:-2}"

echo "Running Spark pipeline..."
docker compose run --rm --no-deps pipeline-job

echo "Packaging and publishing data..."
docker compose run --rm --no-deps package-job
