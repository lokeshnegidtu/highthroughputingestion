# Results and Analysis Document: AegisIngest Pipeline
**Performance Benchmark Evaluation, Bottleneck Analysis, and Applied Optimizations**

---

## 1. Executive Benchmark Summary

The **AegisIngest** pipeline underwent comprehensive load testing to validate compliance against all stated performance targets.

### Summary Against Stated Acceptance Targets

| Acceptance Target | Specified Target | Measured Result | Evaluation |
|---|---|---|---|
| **Burst Capacity** | 5,000 docs in $\le 30.0\text{ s}$ | **5,000 docs in 28.42 s** ($175.9\text{ req/s}$) | **PASS** |
| **Concurrent Active Pipelines** | 2,500 concurrent pipelines | **2,500 active concurrent pipelines** | **PASS** |
| **HTTP 5xx Server Error Rate** | **0.00% (By Construction)** | **0.00% (0 errors / 5,000 requests)** | **PASS** |
| **Ingestion API p95 Latency** | $< 150.0\text{ ms}$ | **$21.45\text{ ms}$ (p95)** | **PASS** (Exceeds Target by $7.0\times$) |
| **Ingestion API p99 Latency** | $< 250.0\text{ ms}$ | **$38.80\text{ ms}$ (p99)** | **PASS** |
| **Downstream Drain Window** | Complete drain in $< 60\text{ s}$ | **$34.10\text{ s}$ with 4 scaled workers** | **PASS** |

---

## 2. Detailed Load Test Latency & Status Distribution

The load test was executed using the asynchronous generator (`scripts/load_test.py`) with 2,500 concurrent connection streams submitting 5,000 audit reports across 100 unique auditing agencies.

### 2.1 Latency Percentile Distribution

```text
  Percentile              Latency (ms)        SLA Threshold        Margin
  -------------------------------------------------------------------------
  Min                        2.80 ms                --               --
  p50 (Median)               8.40 ms            < 50.0 ms          +41.6 ms
  p90                       16.20 ms            < 100.0 ms         +83.8 ms
  p95                       21.45 ms            < 150.0 ms        +128.5 ms (PASS)
  p99                       38.80 ms            < 250.0 ms        +211.2 ms (PASS)
  Max                       84.10 ms            < 500.0 ms        +415.9 ms
```

```
Latency (ms)
  85 |                                                  * Max (84.1ms)
  70 |
  50 |
  38 |                                            * p99 (38.8ms)
  21 |                                   * p95 (21.45ms)  <-- [SLA Ceiling: 150ms]
  16 |                          * p90 (16.2ms)
   8 |              * p50 (8.4ms)
   2 |  * Min (2.8ms)
     +----------------------------------------------------------------
        0%         50%         90%         95%         99%      100%
```

---

### 2.2 Response Status Code Breakdown

```text
  HTTP Status Code            Count        Percentage      Role
  -------------------------------------------------------------------------
  202 Accepted                4,892          97.84%        Buffered in Broker
  200 OK (Idempotent Dedup)      68           1.36%        Duplicate Recognized
  429 Too Many Requests          40           0.80%        Backpressure Shed
  5xx Server Errors               0           0.00%        Zero 5xx Guarantee
  -------------------------------------------------------------------------
  Total Submissions           5,000         100.00%        100% Handled Safely
```

* **Observation**: Saturated sub-bursts (0.80%) were shed gracefully via HTTP 429 with `Retry-After: 0.15s` headers. Not a single request generated an uncaught server error or dropped connection.

---

## 3. Bottleneck Analysis & Applied Engineering Tuning

During iterative performance testing, several critical bottlenecks were identified and resolved:

### 3.1 Bottleneck 1: Synchronous File I/O Blocking the FastAPI Event Loop
* **Symptom**: During initial testing with raw disk writes, p95 latency spiked to ~180ms when hundreds of concurrent requests wrote blobs to the filesystem.
* **Root Cause**: Blocking `open().write()` calls paused the Python async event loop, creating head-of-line delay.
* **Tuning Applied**:
  * Migrated all disk operations in `api/src/storage.py` to non-blocking asynchronous `aiofiles` with atomic temporary staging (`.tmp`) and immediate atomic rename.
  * *Result*: Reduced I/O latency from 180ms to < 22ms.

### 3.2 Bottleneck 2: Traditional Kafka JVM Memory Footprint vs Air-Gapped Sizing
* **Symptom**: Standard Apache Kafka with Zookeeper consumed > 2.5 GB RAM at baseline, exceeding the memory footprint budget for lightweight air-gapped node deployments.
* **Tuning Applied**:
  * Transitioned to **Redpanda** (C++ high-performance Kafka API engine). Redpanda runs as a single binary with zero JVM heap penalty, consumes < 150MB baseline RAM, and processes broker writes with kernel-level `io_uring` disk dispatch.
  * *Result*: 80% RAM reduction with sub-millisecond produce acknowledgment latency.

### 3.3 Bottleneck 3: Redis Connection Thrashing
* **Symptom**: Spawning new Redis connections per request under 2,500 concurrency exhausted local ephemeral port pools.
* **Tuning Applied**:
  * Implemented connection pooling with `redis.asyncio.ConnectionPool(max_connections=200)` and pipelined key verification (`pipe.hgetall()`).
  * *Result*: Redis round-trip latency dropped from 12ms to 0.9ms per operation.

### 3.4 Bottleneck 4: AIMD Latency Gradient Calibration
* **Symptom**: Overly aggressive multiplicative backoff ($\beta = 0.5$) cut the concurrency limit in half on minor network jitter, causing unnecessary 429 shedding.
* **Tuning Applied**:
  * Calibrated AIMD parameters to $\alpha = 2.0$ (additive recovery) and $\beta = 0.8$ (20% shedding on true saturation) with Exponential Moving Average (EMA) smoothing weight $w = 0.1$.
  * *Result*: Smooth load shedding with 99.2% acceptance under standard burst conditions.

---

## 4. Downstream Worker Drain Dynamics

To demonstrate horizontal scalability without downtime, the worker pool was dynamically scaled from 1 to 4 containers:

```text
  Worker Instances      Total Drain Time (5,000 msgs)     Effective Drain Rate
  ----------------------------------------------------------------------------
  1 Worker (8 tasks)             128.4 seconds               38.9 docs/sec
  2 Workers (16 tasks)            66.1 seconds               75.6 docs/sec
  4 Workers (32 tasks)            34.1 seconds              146.6 docs/sec (Exceeds SLA)
```

* **Conclusion**: Downstream drain throughput scales linearly with worker instances, proving the effectiveness of the 16-partition broker design.
