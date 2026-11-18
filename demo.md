# 1,200-record Kafka worker demo

This demo verifies the main Kafka worker with enough records to exceed the
1,000-record processing-window high watermark.

## A. What is tested

The demo checks the following observable behavior:

- The producer creates 1,200 distinct success jobs.
- The Kafka partitioner routes exactly 100 jobs to each of P0 through P11.
- The main worker handles all 1,200 jobs and writes an ISO timestamp for each
  successful execution.
- Load sequence fields are present from 1 through 1,200 and are unique.
- Every handler success completes normally with `remainingAttempts=3`.
- The main consumer group commits independently completed partition prefixes.
- P0 through P11 reach `LAG=0` in Kafka's consumer-group description.
- The handler remains in flight for a controlled delay, allowing concurrent
  executions to overlap across execution verticles.
- The log reports `peakActive` and a final
  `Load concurrency summary` for the successful load.
- The 100 ms handler hold allows overlap up to the configured main
  concurrency limit. The 1,200 run measured a live peak of 16 in-flight
  handlers; it did not and should not attempt 1,000 simultaneous handlers.
- The 1,200 records exercise retained backlog above the 1,000-record window.
  They do not create 1,200 simultaneous handler executions: the worker's
  adaptive/start limits and execution verticle pool bound true concurrency.
- Processing continues while retained work exceeds the normal 1,000-record
  window boundary. This run exercises backpressure behavior; it does not prove
  every individual pause/resume transition.

The demo does not claim durable deduplication, global ordering, or
external-effect fencing. It also does not test retry/DLQ behavior.

## B. Test parameters

| Parameter | Value |
| --- | --- |
| Main topic | `jobs.main.v1` |
| Consumer group | `job-main-v1` |
| Topic partitions | 12: P0-P11 |
| Records | 1,200 |
| Records per partition | 100 |
| Producer key | Canonical `jobId` UTF-8 bytes |
| Partition routing | `toPositive(murmur2(key)) % 12` |
| Job type | `demo.echo` |
| Remaining attempts | 3 |
| Demo mode | `success` |
| Processing-window bound | 1,000 records or 16 MiB |
| Main worker concurrency | `16` by default |
| Execution verticles | `min(WORKER_CONCURRENCY, 4)` = 4 by default |
| Handler delay | `100` ms by default |
| Concurrency evidence | `peakActive` and final `Load concurrency summary` |
| Retry worker | Running, but no failures should be produced |

## C. How to run

### 1. Start or rebuild the Compose demo

From the repository root:

```sh
docker compose --profile tools build
docker compose up -d
```

The main topic and retry topic default to 12 partitions. Kafka topic partition
counts cannot be changed safely on an existing persistent backlog: use the
existing topics consistently and do not publish a key with a mismatched
partition count.

### 2. Publish the 1,200-record load

```sh
docker compose --profile tools run --rm sample-producer --load-1200
```

Expected producer output includes:

```text
Load complete accepted=1200 topic=jobs.main.v1
partitionCounts={0=100, 1=100, 2=100, 3=100, 4=100, 5=100, 6=100, 7=100, 8=100, 9=100, 10=100, 11=100}
```

Each publish is acknowledged by Kafka before the producer reports progress.

### 3. Capture timestamped main-worker logs

```sh
docker compose logs main-worker | tee /tmp/demo-1200-main.log
```

Wait until processing settles, then count successful load executions:

```sh
grep 'loadMode=success' /tmp/demo-1200-main.log | wc -l
grep 'loadMode=success' /tmp/demo-1200-main.log |
  sed -n 's/.*loadSequence=\([0-9]*\).*/\1/p' |
  sort -n -u |
  wc -l
```

Both commands should report `1200`.

Verify that the same log reports overlapping execution:

```sh
grep 'Load concurrency summary' /tmp/demo-1200-main.log
```

The summary should report `loadCompleted=1200` and a `peakActive` greater than
1. The validated default run reported `peakActive=16`. This demonstrates
concurrent handler execution in verticle contexts; it does not claim that all
1,200 or 1,000 records ran simultaneously.

Count successful executions per partition:

```sh
grep 'loadMode=success' /tmp/demo-1200-main.log |
  sed -n 's/.*partition=\([0-9]*\).*offset=.*/\1/p' |
  sort -n |
  uniq -c
```

The output should contain `100` for each partition P0-P11.

### 4. Verify Kafka committed offsets and lag

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group job-main-v1
```

Success requires:

- P0-P11 all appear in the description.
- `CURRENT-OFFSET=LOG-END-OFFSET` for every partition.
- `LAG=0` for every partition.

Existing persistent data can make initial offsets and log-end offsets
nonzero; this demo verifies that all newly published records are processed and
that lag reaches zero, not that offsets start at 0.

## D. Success criteria

The run passes only when all of these are true:

1. The producer reports `accepted=1200`.
2. It reports 100 records for P0-P11.
3. The main worker logs 1,200 `loadMode=success` executions.
4. The success log contains 1,200 unique `loadSequence` values.
5. Each P0-P11 log count is exactly 100.
6. Kafka reports lag 0 for P0-P11.
7. The handler reports `peakActive>1` and
   `Load concurrency summary loadCompleted=1200`.

## E. Recorded evidence

The validated live run from 2026-11-18 recorded:

- [Concurrency success summary](docs/tasks/evidence/DEMO1200-concurrency-summary.txt)
- [Timestamped handler log](docs/tasks/evidence/DEMO1200-concurrency-success-handler-log.txt)
- [Per-partition handler counts](docs/tasks/evidence/DEMO1200-concurrency-handler-partition-counts.txt)
- [Kafka consumer-group offsets](docs/tasks/evidence/DEMO1200-concurrency-group-offsets.txt)
