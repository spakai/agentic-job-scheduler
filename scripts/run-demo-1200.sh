#!/usr/bin/env bash
set -euo pipefail

DEMO_RUN_ID="${DEMO_RUN_ID:-demo-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
DEMO_HANDLER_DELAY_MILLIS="${DEMO_HANDLER_DELAY_MILLIS:-100}"
DEMO_RANDOM_SEED="${DEMO_RANDOM_SEED:-1200}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"

handler_log="docs/tasks/evidence/DEMO1200-concurrency-success-handler-log.txt"
summary_file="docs/tasks/evidence/DEMO1200-concurrency-summary.txt"
graph_file="docs/diagrams/demo1200-concurrency.svg"

export DEMO_RUN_ID DEMO_HANDLER_DELAY_MILLIS DEMO_RANDOM_SEED

mkdir -p docs/tasks/evidence docs/diagrams

docker compose --profile tools build
docker compose up --scale main-worker=2 -d

docker compose --profile tools run --rm sample-producer --load-1200 |
  tee /tmp/demo-1200-producer.log

deadline=$((SECONDS + MAX_WAIT_SECONDS))
while (( SECONDS < deadline)); do
    docker compose logs main-worker > /tmp/demo-1200-main.log
    handled=$(grep "loadMode=success.*demoRunId=${DEMO_RUN_ID}" \
        /tmp/demo-1200-main.log | wc -l)
    lag_rows=$(docker compose exec -T kafka \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --describe --group job-main-v1 2>/dev/null |
        awk 'NR > 1 && $6 != "0" { count++ } END { print count+0 }')
    if (( handled == 1200 && lag_rows == 0)); then
        break
    fi
    sleep "$POLL_INTERVAL_SECONDS"
done

docker compose logs main-worker > /tmp/demo-1200-main.log
grep "loadMode=success.*demoRunId=${DEMO_RUN_ID}" \
    /tmp/demo-1200-main.log > "$handler_log"

handler_lines=$(wc -l < "$handler_log")
unique_sequences=$(sed -n 's/.*loadSequence=\([0-9]*\).*/\1/p' "$handler_log" |
    sort -n -u | wc -l)
handled=$unique_sequences
duplicate_lines=$((handler_lines - unique_sequences))
peak_active=$(sed -n 's/.*peakActive=\([0-9]*\).*/\1/p' "$handler_log" |
    sort -n | tail -n 1)
partition_counts=$(sed -n 's/.*loadSequence=\([0-9]*\).*partition=\([0-9]*\).*offset=.*/\1 \2/p' "$handler_log" |
    awk '!seen[$1 FS $2]++ {count[$2]++} END {for(i=0;i<12;i++) print i,count[i]+0}' |
    awk '{print $2 " P"$1}' |
    paste -sd ', ' -)
if [[ -z "$peak_active" ]]; then
    peak_active=0
fi

docker compose exec -T kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group job-main-v1 \
    > docs/tasks/evidence/DEMO1200-concurrency-group-offsets.txt

lag_rows=$(awk 'NR > 1 && $6 != "0" { count++ } END { print count+0 }' \
    docs/tasks/evidence/DEMO1200-concurrency-group-offsets.txt)

if (( handled != 1200 || unique_sequences != 1200 || lag_rows != 0 || peak_active < 2)); then
    echo "Demo did not meet success criteria:" >&2
    echo "handled=$handled unique_sequences=$unique_sequences lag_rows=$lag_rows peak_active=$peak_active" >&2
    exit 1
fi

/usr/bin/python3 scripts/generate-demo-concurrency-graph.py \
    "$handler_log" "$DEMO_RUN_ID" "$graph_file"

main_pod_ids=$(docker compose ps -q main-worker)
pod_index=1
pod_log_files=()
for pod_id in $main_pod_ids; do
    pod_log="docs/tasks/evidence/DEMO1200-main-pod-${pod_index}-concurrency.log"
    docker logs "$pod_id" > "$pod_log" 2>&1
    pod_log_files+=("$pod_log")
    pod_index=$((pod_index + 1))
done

if (( ${#pod_log_files[@]} == 2)); then
    /usr/bin/python3 scripts/generate-two-main-pod-concurrency-graph.py \
        "${pod_log_files[0]}" "${pod_log_files[1]}" \
        docs/diagrams/demo1200-two-main-pods.svg
fi

{
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) live twelve-partition 1,200-record concurrency and commit run"
    echo "Run ID: ${DEMO_RUN_ID}"
    echo "Producer: 1,200 accepted; randomized across P0-P11"
    echo "Random seed: ${DEMO_RANDOM_SEED:-1200}"
    echo "Handler delay: ${DEMO_HANDLER_DELAY_MILLIS} ms"
    echo "Main concurrency: 16"
    echo "Execution verticle pool: 4"
    echo "Demo retained window: 250 records"
    echo "Load concurrency summary loadCompleted=${handled} peakActive=${peak_active}"
    echo "Success handler lines: ${handled}"
    echo "Handler log lines including replay: ${handler_lines}"
    echo "Duplicate replay lines: ${duplicate_lines}"
    echo "Unique load sequences: ${unique_sequences}"
    echo "Partition counts:"
    echo "${partition_counts}"
} > "$summary_file"

echo "Graph generated: $graph_file"
echo "Evidence summary: $summary_file"
