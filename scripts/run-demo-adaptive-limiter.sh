#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="adaptive-limiter-demo"
INITIAL_LIMIT="${INITIAL_LIMIT:-16}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-240}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PHASE_FILE="${PROJECT_ROOT}/docs/tasks/evidence/DEMO1202-adaptive-limiter-phases.tsv"
WINDOW_LOG="${PROJECT_ROOT}/docs/tasks/evidence/DEMO1202-adaptive-limiter-windows.log"
HANDLER_LOG="${PROJECT_ROOT}/docs/tasks/evidence/DEMO1202-adaptive-limiter-handlers.log"
SUMMARY_FILE="${PROJECT_ROOT}/docs/tasks/evidence/DEMO1202-adaptive-limiter-summary.txt"
GRAPH_FILE="${PROJECT_ROOT}/docs/diagrams/demo-adaptive-limiter.svg"
WORKER_LOG="$(mktemp /tmp/adaptive-limiter-worker.XXXXXX)"
trap 'rm -f "$WORKER_LOG"' EXIT

cd "$PROJECT_ROOT"
export KAFKA_TOPIC_PARTITIONS=12
export KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-19092}"
export MAIN_WORKER_A_CONCURRENCY="$INITIAL_LIMIT"
export MAIN_WORKER_B_CONCURRENCY=8
export WORKER_MAX_RETAINED_RECORDS=250
export DEMO_RANDOM_SEED=1202
compose=(docker compose --project-name "$PROJECT_NAME")

if (( INITIAL_LIMIT < 8 || INITIAL_LIMIT > 24 )); then
    echo "INITIAL_LIMIT must be between 8 and 24 for this controlled workload" >&2
    exit 2
fi

mkdir -p docs/tasks/evidence docs/diagrams
printf 'phase\trun_id\tstart_epoch_ms\tend_epoch_ms\trecords\tunique_handlers\tfinal_lag_rows\tdelay_ms\n' \
    > "$PHASE_FILE"
: > "$WINDOW_LOG"
: > "$HANDLER_LOG"

"${compose[@]}" --profile tools build
"${compose[@]}" up -d --wait kafka main-worker-a

run_phase() {
    local phase="$1"
    local records="$2"
    local delay_ms="$3"
    local run_id="adaptive-$(date -u +%Y%m%dT%H%M%SZ)-${phase}-$$"
    local start_ms
    local end_ms
    local deadline
    local handled
    local lag_rows

    start_ms="$(date +%s%3N)"
    export DEMO_RUN_ID="$run_id"
    export DEMO_HANDLER_DELAY_MILLIS="$delay_ms"
    echo "Starting phase=$phase records=$records handlerDelayMillis=$delay_ms runId=$run_id"
    "${compose[@]}" --profile tools run --rm sample-producer --load-count "$records"

    deadline=$((SECONDS + MAX_WAIT_SECONDS))
    while (( SECONDS < deadline )); do
        "${compose[@]}" logs --no-color main-worker-a > "$WORKER_LOG" 2>&1
        handled="$(grep -F "demoRunId=$run_id" "$WORKER_LOG" \
            | grep -F "loadMode=success" \
            | sed -n 's/.*loadSequence=\([0-9][0-9]*\).*/\1/p' \
            | sort -n -u | wc -l || true)"
        lag_rows="$("${compose[@]}" exec -T kafka \
            /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 \
            --describe --group job-main-v1 2>/dev/null \
            | awk '$1 == "job-main-v1" && $6 != "0" { count++ } END { print count+0 }')"
        if (( handled == records && lag_rows == 0 )); then
            break
        fi
        sleep 1
    done
    if (( handled != records || lag_rows != 0 )); then
        echo "Phase failed: phase=$phase handled=${handled:-0}/$records lagRows=${lag_rows:-unknown}" >&2
        "${compose[@]}" logs --no-color main-worker-a >&2
        return 1
    fi

    end_ms="$(date +%s%3N)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$phase" "$run_id" "$start_ms" "$end_ms" "$records" "$handled" "$lag_rows" "$delay_ms" \
        >> "$PHASE_FILE"
    echo "Completed phase=$phase uniqueHandlers=$handled lagRows=$lag_rows"
}

run_phase warmup 1200 500
run_phase pressure 160 1300
run_phase recovery 1200 500

"${compose[@]}" logs --no-color main-worker-a > "$WORKER_LOG" 2>&1
grep -F 'Adaptive limiter window role=main' "$WORKER_LOG" > "$WINDOW_LOG" || true
while IFS=$'\t' read -r phase run_id start_ms end_ms records handled lag_rows delay_ms; do
    [[ "$phase" == "phase" ]] && continue
    grep -F "demoRunId=$run_id" "$WORKER_LOG" >> "$HANDLER_LOG" || true
done < "$PHASE_FILE"

python3 scripts/generate-adaptive-limiter-graph.py \
    "$WINDOW_LOG" "$PHASE_FILE" "$HANDLER_LOG" "$GRAPH_FILE" "$SUMMARY_FILE"

echo "Adaptive limiter graph: $GRAPH_FILE"
echo "Window evidence: $WINDOW_LOG"
echo "Handler evidence: $HANDLER_LOG"
echo "Summary: $SUMMARY_FILE"
echo "Compose project remains running; stop it with: docker compose -p $PROJECT_NAME down"
