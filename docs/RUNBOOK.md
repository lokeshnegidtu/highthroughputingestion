# Operational Runbook: AegisIngest Pipeline
**Deployment, Scaling, Testing, and Monitoring Guide**

---

## 1. Prerequisites & Environment

The AegisIngest pipeline is designed to run in any standard Linux, macOS, or Windows environment with zero external dependencies (air-gapped ready).

### System Requirements
* **Docker Engine**: `>= 24.0.0`
* **Docker Compose**: `>= v2.20.0`
* **Python**: `>= 3.10` (for running local test client scripts)
* **Hardware Sizing**: 4+ CPU cores, 4 GB RAM recommended for full stack with load testing.

---

## 2. Zero-to-Hero Quickstart Deployment

### Step 1: Clone and Navigate
```bash
git clone <repository_url>
cd ingestionPipeline
```

### Step 2: Launch Complete Stack
You can launch the entire stack using the cross-platform runner script or standard Docker Compose:

**On Linux / macOS:**
```bash
chmod +x start.sh
./start.sh
```

**On Windows (PowerShell):**
```powershell
.\start.ps1
```

**Via Standard Docker Compose:**
```bash
docker compose up -d --build
```

### Step 3: Verify Service Health
Run the automated health checker:
```bash
python scripts/health_check.py
```
Expected output:
```text
======================================================================
  AEGISINGEST END-TO-END SYSTEM HEALTH CHECK
======================================================================
  [+] Ingestion API                [ONLINE] (HTTP 200)
  [+] API Prometheus Metrics       [ONLINE] (HTTP 200)
  [+] Real-Time Dashboard          [ONLINE] (HTTP 200)
  [+] Prometheus Server            [ONLINE] (HTTP 200)
  [+] Grafana Console              [ONLINE] (HTTP 200)
======================================================================
  All AegisIngest pipeline services are HEALTHY and READY.
======================================================================
```

---

## 3. Service Access Endpoints

| Service Name | Port | URL | Description |
|---|---|---|---|
| **Ingestion API Swagger UI** | `8010` | [http://localhost:8010/docs](http://localhost:8010/docs) | Interactive OpenAPI documentation |
| **API Prometheus Metrics** | `8010` | [http://localhost:8010/metrics](http://localhost:8010/metrics) | Raw Prometheus scrape endpoint |
| **Real-Time Operations Console** | `8090` | [http://localhost:8090](http://localhost:8090) | Live pipeline monitor & burst simulator |
| **Grafana Monitoring Console** | `3010` | [http://localhost:3010](http://localhost:3010) | Provisioned metrics dashboards |
| **Prometheus TSDB** | `9095` | [http://localhost:9095](http://localhost:9095) | Prometheus expression browser |

---

## 4. Zero-Downtime Worker Scaling

To increase downstream report processing throughput during heavy burst periods, scale worker instances horizontally:

```bash
# Scale worker pool to 4 instances
docker compose up --scale worker=4 -d
```

### Verification:
```bash
docker compose ps | grep worker
```
Redpanda will dynamically rebalance partition assignments among the active workers within the `aegis-audit-processors` consumer group without dropping a single in-flight document or causing API downtime.

To scale back down:
```bash
docker compose up --scale worker=1 -d
```

---

## 5. Running Tests & Performance Validation

### 5.1 Automated Test Suite (Unit & Integration)
Run the complete automated test suite verifying backpressure, idempotency, sharding invariants, and benchmark analyzer:

```bash
# Install test requirements if running locally outside container
pip install -r api/requirements.txt

# Run pytest
pytest tests/ -v --tb=short
```

Expected output:
```text
tests/test_unit.py::TestPartitioningMath::test_deterministic_partition_mapping PASSED
tests/test_unit.py::TestPartitioningMath::test_partition_distribution_coverage PASSED
tests/test_unit.py::TestAdaptiveLimiterMath::test_aimd_additive_increase_on_low_rtt PASSED
tests/test_unit.py::TestAdaptiveLimiterMath::test_aimd_multiplicative_decrease_on_high_rtt PASSED
tests/test_unit.py::TestTokenBucketLimiter::test_token_bucket_exhaustion_and_replenishment PASSED
tests/test_unit.py::TestContentAddressableStorage::test_cas_write_and_sha256_verification PASSED
tests/test_unit.py::TestAuditBenchmarkAnalyzer::test_analyzer_scoring_with_critical_findings PASSED
tests/test_unit.py::TestAuditBenchmarkAnalyzer::test_analyzer_clean_report_scoring PASSED
tests/test_backpressure.py::test_zero_5xx_under_severe_load PASSED
tests/test_backpressure.py::test_adaptive_limiter_backpressure_rejection PASSED
tests/test_idempotency.py::test_idempotent_duplicate_submission PASSED
tests/test_partitioning.py::test_per_agency_partition_consistency PASSED
tests/test_partitioning.py::test_cross_agency_sharding_distribution PASSED
```

---

### 5.2 Executing the Burst Load Test (5,000 in 30 Seconds)
Execute the high-throughput asynchronous load generator against the live stack:

```bash
python scripts/load_test.py --url http://localhost:8010 --total-requests 5000 --duration 30 --concurrency 2500
```

---

## 6. Observability & Dashboard Guide

### 6.1 Grafana Dashboard (`http://localhost:3010`)
1. Open [http://localhost:3010](http://localhost:3010). (Anonymous login is pre-configured).
2. Navigate to **Dashboards** &rarr; **AegisIngest - Document Ingestion & Observability**.
3. Key Panels to Inspect:
   * **Ingestion Status (202)**: Real-time count of successfully buffered documents.
   * **5xx Server Errors**: Green single-stat gauge confirming **0% 5xx errors**.
   * **API p95 Latency**: Live timeseries tracking response duration against the red 150ms SLA line.
   * **Worker Processing Rate**: Live documents per second processed by the worker pool.
   * **Active Concurrent Pipelines**: Real-time gauge of in-flight requests.

### 6.2 Real-Time Operations Console (`http://localhost:8090`)
1. Open [http://localhost:8090](http://localhost:8090).
2. View the dynamic topology nodes, live throughput KPIs, and audit benchmark scorecards.
3. Use the **Burst Ingestion Simulator** to trigger live test spikes directly from the browser!

---

## 7. Troubleshooting & Recovery Procedures

### Issue 1: High 429 Rejections Observed
* **Diagnosis**: Upstream arrival rate exceeds API in-flight ceiling or a single agency is exceeding its burst token allocation.
* **Resolution**:
  1. Inspect `http://localhost:8010/api/v1/stats` to check current limit and in-flight count.
  2. If hardware capacity allows, increase `MAX_CONCURRENT_REQUESTS` in `.env`.
  3. Ensure client submitters honor the numeric `Retry-After` response header.

### Issue 2: Redis Memory Alarm
* **Diagnosis**: Memory usage approaches the 256MB ceiling during multi-day retention.
* **Resolution**:
  1. Redis is configured with `volatile-lru` and automatically frees the oldest completed job records.
  2. To manually purge expired keys:
     ```bash
     docker compose exec redis redis-cli -a "" memory purge
     ```

### Issue 3: Poison Pills in Dead-Letter Queue (DLQ)
* **Diagnosis**: Corrupted or unparseable documents are routed to `audit-reports-dlq`.
* **Resolution**:
  1. Inspect messages in DLQ:
     ```bash
     docker compose exec broker rpk topic consume audit-reports-dlq -n 5
     ```
  2. Inspect worker error trace in `docker compose logs worker`.

---

## 8. Graceful Shutdown & Teardown

```bash
# Stop containers without removing persistent volumes
docker compose down

# Full teardown including volumes and storage blobs
docker compose down -v
```
