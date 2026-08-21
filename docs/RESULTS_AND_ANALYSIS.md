# AegisIngest Results and Analysis

## 1. Scope of This Report

This document records what the repository currently verifies and how to run the available performance checks. The previous version included exact latency, acceptance, memory, Redis, and worker-drain measurements that are not stored with reproducible run metadata in this repository. Those values have been removed rather than presented as current facts.

The configured performance target is 5,000 submissions in 30 seconds with an API p95 target below 150 ms and zero observed HTTP 5xx responses during the test. A target is not a result: measured values should be recorded from a specific run with its environment, commit, service configuration, and complete output.

## 2. Current Verification Surface

The automated tests cover the following behavior:

| Area | Current verification |
| --- | --- |
| Partitioning | Deterministic agency routing and cross-agency partition coverage |
| Adaptive admission | Additive increase, multiplicative decrease, and saturated-request rejection |
| Agency fairness | Token bucket exhaustion and replenishment |
| Storage | SHA-256 content-addressed write and read-back |
| Idempotency | Duplicate submissions return the existing job without re-queueing |
| Backpressure | Concurrent requests produce only accepted, duplicate, or `429` responses in the test harness; `429` includes `Retry-After` |
| Analyzer | Critical finding penalties and clean-report scoring behavior |
| Dashboard telemetry | Lifecycle reconciliation and required-service health classification |
| Load-test reporting | Status bucketing, throughput, and complete-run PASS/FAIL classification |

Run the suite from the repository root:

```bash
pytest tests/ -v --tb=short
```

The tests that exercise API requests use the application through an ASGI transport and deliberately support the API's no-infrastructure fallback. They do not constitute a full Docker, Redpanda, PostgreSQL, MinIO, or end-to-end worker benchmark.

## 3. Reproducible API Load Test

The standalone generator is `scripts/load_test.py`:

```bash
python scripts/load_test.py \
  --url http://localhost:8010 \
  --total-requests 5000 \
  --duration 30 \
  --concurrency 2500
```

The generator submits JSON documents to `POST /api/v1/ingest` and reports:

- total completed requests and wall-clock duration;
- effective request throughput;
- `202`/`200` accepted responses, `429` backpressure responses, 5xx responses, and network errors;
- server latency from the API response's `latency_ms` field when available; and
- client round-trip latency.

Important implementation detail: the `--concurrency` value is a reported scenario parameter. The generator currently uses a fixed pool of 30 client worker tasks and an HTTP connection limit of 100, so it does not create 2,500 simultaneous client tasks. Results must therefore describe the actual client settings as well as the requested scenario.

The script considers the run successful only when there are no 5xx responses, both server and client p95 values are below 150 ms, and the burst completes within the target duration plus five seconds. A passing script result is an observation for that environment, not a proof that all deployments meet the target.

## 4. Dashboard Performance Tests

The operations dashboard exposes a separate asynchronous test flow:

1. `POST /api/performance-tests` accepts a scenario such as `5000 documents / 30 seconds`.
2. The dashboard clamps the request to 10-10,000 documents and 5-300 seconds.
3. It schedules requests against the normal API ingestion endpoint.
4. `GET /api/performance-tests/{test_run_id}` returns progress and final statistics.
5. Results are persisted to `load_test_runs` when PostgreSQL is available.

A dashboard run is classified `PASS` only when all requested calls complete, the request count reaches the target, no 5xx/timeout/transport failures occur, p95 latency is below 150 ms, and at least 75% of completed requests are accepted as `202` or duplicate `200` responses. A `429` is an intentional backpressure result and is tracked separately from 4xx and 5xx failures.

The dashboard also provides `POST /api/trigger-burst`, which sends a bounded direct burst of 10 to 1,000 requests and reports accepted, shed, and 5xx counts. It is useful for interactive checks but does not replace a controlled benchmark.

## 5. Worker Throughput Interpretation

The current worker is a minimal lifecycle processor, not a full PDF or AI analysis pipeline. Its defaults are:

- 50 maximum records per Kafka poll;
- 8 concurrent processing tasks per worker;
- 50 ms configured processing delay;
- three maximum processing attempts; and
- one consumer group, `aegis-audit-processors`, shared by scaled worker containers.

A simple theoretical upper bound from the configured delay is approximately:

$$
8 \times \frac{1}{0.05} = 160\ \text{documents/second per worker}
$$

This is only a calculation from the artificial delay. It excludes Kafka polling, PostgreSQL transactions, MinIO access, scheduling, retries, and container resource limits. It must not be reported as measured throughput.

To run a worker scaling experiment against the complete stack:

```bash
# Start with the default worker
./start.sh

# Scale the consumer group
 docker compose up --scale worker=4 -d
```

Record at minimum the commit or build identifier, worker count, host resources, input document size and content, broker backlog before the run, accepted/rejected counts, completion count, processing duration, and final backlog. Use the persisted `documents`, `processing_results`, `processing_events`, and `worker_status` tables to distinguish API acceptance from downstream completion.

## 6. Interpreting Backpressure and Errors

`429` responses are expected when the adaptive concurrency limiter or per-agency token bucket is saturated. They include a numeric `Retry-After` header and should be counted as shed load, not server errors.

The API's global exception handler maps unexpected exceptions to `429` to avoid returning a server error to the caller. Observing zero 5xx responses therefore does not mean zero internal errors; logs and service health still need to be inspected.

The API's local in-memory caches are test fallbacks only. A benchmark that runs without PostgreSQL, MinIO, or Redpanda measures fallback behavior and should be labeled accordingly.

## 7. Current Findings

- The repository has focused automated coverage for admission control, idempotency, partitioning, storage, analyzer scoring, dashboard telemetry, and load-test classification.
- The configured targets and formulas are explicit and inspectable in `api/src/config.py` and `worker/src/config.py`.
- The live worker currently persists minimal completion results; the benchmark analyzer is not wired into the Kafka processing path.
- The configured DLQ topic exists, but the current consumer flow documents retry and final failure persistence rather than a verified DLQ publishing result.
- Exact benchmark values should be regenerated after deployment and recorded with their environment details before being used as acceptance evidence.

## 8. Evidence to Add for a Formal Benchmark

For each formal run, preserve:

1. repository revision and configuration overrides;
2. Docker image versions and host CPU/RAM;
3. command line and client concurrency behavior;
4. request count, response distribution, timeout and transport failures;
5. API server and client p50/p95/p99 latency;
6. broker lag and worker count over time;
7. PostgreSQL document and result counts before and after the run; and
8. raw command output plus Prometheus or dashboard exports.

This evidence makes the results comparable and prevents a target, a theoretical capacity calculation, and a measured benchmark from being conflated.
