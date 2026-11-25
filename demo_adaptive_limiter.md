# Main-worker adaptive limiter demo

This isolated Compose demo makes the main worker's adaptive concurrency response
observable without changing the limiter policy. It runs one main-worker pod
through three Kafka workloads:

1. **Warmup:** 1,200 jobs held for 500 ms; healthy backlog windows should raise
   the configured starting limit.
2. **Pressure:** 160 jobs held for 1,300 ms; windows with at least 20 completed
   attempts and p95 above the 1,000 ms target should halve the limit.
3. **Recovery:** 1,200 jobs held for 500 ms; healthy backlog windows should
   increase the reduced limit again.

The demo starts from the default limit of 16; `INITIAL_LIMIT` can be set from 8
through 24 to tune the demonstration. The limit is per worker process. It
changes admission of new handlers and does not cancel in-flight work. The graph
shows the configured per-process limit, measured p95 handler latency, and
handler-start `activeAtStart` observations separately.

## Run

From the repository root:

```sh
bash scripts/run-demo-adaptive-limiter.sh
```

The runner builds and starts a separate Compose project named
`adaptive-limiter-demo`; it does not stop or reuse the regular demo stack. It
publishes its broker on host port `19092` by default (override with
`KAFKA_HOST_PORT` if needed). It waits for each phase's unique handler sequences
and zero main-topic lag. It fails unless the recorded five-second windows prove
a healthy increase, a slow-window decrease with at least 20 samples, and a
healthy recovery increase.

Outputs:

- `docs/diagrams/demo-adaptive-limiter.svg`
- `docs/tasks/evidence/DEMO1202-adaptive-limiter-summary.txt`
- `docs/tasks/evidence/DEMO1202-adaptive-limiter-windows.log`
- `docs/tasks/evidence/DEMO1202-adaptive-limiter-handlers.log`
- `docs/tasks/evidence/DEMO1202-adaptive-limiter-phases.tsv`

To stop the isolated containers and network after reviewing the evidence:

```sh
docker compose -p adaptive-limiter-demo down
```

The command leaves the named Kafka volume intact. Remove persistent demo data
only when you explicitly want to discard its Kafka backlog and offsets.
