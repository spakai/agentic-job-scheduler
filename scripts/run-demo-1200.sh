#!/usr/bin/env bash
set -euo pipefail

DEMO_RUN_ID="${DEMO_RUN_ID:-demo-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
DEMO_HANDLER_DELAY_MILLIS="${DEMO_HANDLER_DELAY_MILLIS:-100}"
DEMO_RANDOM_SEED="${DEMO_RANDOM_SEED:-1200}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
TRANSPORT="${DEMO_TRANSPORT:-direct-kafka}"

export DEMO_RUN_ID DEMO_HANDLER_DELAY_MILLIS DEMO_RANDOM_SEED

usage() {
    echo "Usage: $0 [--transport direct-kafka|http]" >&2
}

while (( $# > 0)); do
    case "$1" in
        --transport)
            if (( $# < 2)); then
                usage
                exit 2
            fi
            TRANSPORT="$2"
            shift 2
            ;;
        --transport=*)
            TRANSPORT="${1#*=}"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 2
            ;;
    esac
done

if [[ "$TRANSPORT" != "direct-kafka" && "$TRANSPORT" != "http" ]]; then
    echo "Unsupported transport: $TRANSPORT (expected direct-kafka or http)" >&2
    exit 2
fi

if [[ "$TRANSPORT" == "http" ]]; then
    artifact_prefix="DEMO1200-http"
    graph_suffix="http"
else
    artifact_prefix="DEMO1200"
    graph_suffix="direct-kafka"
fi
handler_log="docs/tasks/evidence/${artifact_prefix}-concurrency-success-handler-log.txt"
summary_file="docs/tasks/evidence/${artifact_prefix}-concurrency-summary.txt"
group_offsets_file="docs/tasks/evidence/${artifact_prefix}-concurrency-group-offsets.txt"
graph_file="docs/diagrams/demo1200-${graph_suffix}-concurrency.svg"
pod_a_log="docs/tasks/evidence/${artifact_prefix}-main-pod-a-concurrency.log"
pod_b_log="docs/tasks/evidence/${artifact_prefix}-main-pod-b-concurrency.log"
two_pod_graph="docs/diagrams/demo1200-${graph_suffix}-two-main-pods.svg"

mkdir -p docs/tasks/evidence docs/diagrams

docker compose --profile tools build
docker compose up -d

if [[ "$TRANSPORT" == "http" ]]; then
    readiness_deadline=$((SECONDS + 60))
    until curl --fail --silent --show-error \
        "http://127.0.0.1:8080/health/live" >/dev/null; do
        if (( SECONDS >= readiness_deadline)); then
            echo "HTTP admission API did not become live within 60 seconds" >&2
            docker compose logs http-api >&2
            exit 1
        fi
        sleep 1
    done
fi

docker compose --profile tools run --rm sample-producer \
    --load-1200 --transport "$TRANSPORT" |
  tee /tmp/demo-1200-producer.log

deadline=$((SECONDS + MAX_WAIT_SECONDS))
while (( SECONDS < deadline)); do
    docker compose logs main-worker-a main-worker-b > /tmp/demo-1200-main.log
    handled=$(grep "loadMode=success.*demoRunId=${DEMO_RUN_ID}" \
        /tmp/demo-1200-main.log | wc -l)
    lag_rows=$(docker compose exec -T kafka \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --describe --group job-main-v1 2>/dev/null |
        awk '$1 == "job-main-v1" && $6 != "0" { count++ } END { print count+0 }')
    if (( handled == 1200 && lag_rows == 0)); then
        break
    fi
    sleep "$POLL_INTERVAL_SECONDS"
done

docker compose logs main-worker-a main-worker-b > /tmp/demo-1200-main.log
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
    > "$group_offsets_file"

lag_rows=$(awk '$1 == "job-main-v1" && $6 != "0" { count++ } END { print count+0 }' \
    "$group_offsets_file")

if (( handled != 1200 || unique_sequences != 1200 || lag_rows != 0 || peak_active < 2)); then
    echo "Demo did not meet success criteria:" >&2
    echo "handled=$handled unique_sequences=$unique_sequences lag_rows=$lag_rows peak_active=$peak_active" >&2
    exit 1
fi

/usr/bin/python3 scripts/generate-demo-concurrency-graph.py \
    "$handler_log" "$DEMO_RUN_ID" "$graph_file"

docker logs agentic-job-scheduler-main-worker-a-1 2>&1 |
    grep "loadMode=success.*demoRunId=${DEMO_RUN_ID}" > "$pod_a_log" || true
docker logs agentic-job-scheduler-main-worker-b-1 2>&1 |
    grep "loadMode=success.*demoRunId=${DEMO_RUN_ID}" > "$pod_b_log" || true

if [[ -s "$pod_a_log" && -s "$pod_b_log" ]]; then
    /usr/bin/python3 scripts/generate-two-main-pod-concurrency-graph.py \
        "$pod_a_log" "$pod_b_log" \
        "$two_pod_graph"
fi

{
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) live twelve-partition 1,200-record concurrency and commit run"
    echo "Run ID: ${DEMO_RUN_ID}"
    echo "Transport: ${TRANSPORT}"
    echo "Admission: 1,200 accepted; randomized across P0-P11"
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
if [[ -s "$two_pod_graph" ]]; then
    echo "Per-pod graph generated: $two_pod_graph"
fi
echo "Evidence summary: $summary_file"
