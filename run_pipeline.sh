#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

started_at="$(date +%s)"

format_elapsed_time() {
    local total_seconds="$1"
    local hours=$((total_seconds / 3600))
    local minutes=$(((total_seconds % 3600) / 60))
    local seconds=$((total_seconds % 60))

    if ((hours > 0)); then
        printf "%dh %dm %ds" "${hours}" "${minutes}" "${seconds}"
    elif ((minutes > 0)); then
        printf "%dm %ds" "${minutes}" "${seconds}"
    else
        printf "%ds" "${seconds}"
    fi
}

cleanup() {
    exit_code="$?"
    docker compose down || true
    finished_at="$(date +%s)"
    echo "run_pipeline.sh elapsed time: $(format_elapsed_time "$((finished_at - started_at))")"
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
