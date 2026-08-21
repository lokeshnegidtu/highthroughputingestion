# AegisIngest

A containerized, asynchronous document ingestion pipeline for cybersecurity audit reports. The API applies adaptive concurrency and per-agency rate limits, stores accepted documents in PostgreSQL and MinIO, publishes work to Redpanda, and processes it through a horizontally scalable worker pool. A FastAPI operations dashboard and Prometheus/Grafana provide live telemetry.

## Architecture

```text
Clients
  |
  v
Ingestion API (FastAPI :8010)
  |-- PostgreSQL  -- durable document metadata, idempotency, events, results
  |-- MinIO       -- content-addressed document blobs
  '-- Redpanda    -- audit-reports-ingest, 16 partitions
                         |
                         v
                 Worker consumer group
                 aegis-audit-processors

Operations Dashboard (FastAPI :8090) --> API, PostgreSQL, Prometheus
Prometheus (:9095) --> API and worker metrics
Grafana (:3010) --> Prometheus dashboards
```

### Ingestion behavior

- New submissions return `202 Accepted` and are processed asynchronously.
- Duplicate idempotency keys or document hashes return `200` with the existing job.
- Saturated concurrency or per-agency rate limits return `429` with `Retry-After`.
- Documents larger than 25 MiB return `413`.
- Accepted bytes are stored using SHA-256 content-addressed storage.
- Agency IDs are deterministically assigned to one of 16 Kafka-compatible partitions, preserving agency sequence ordering.
- The worker uses the `aegis-audit-processors` consumer group, batches up to 50 records, and supports up to 8 concurrent processing tasks per worker.

## Requirements

- Docker Engine with Docker Compose v2
- Python 3.10+ for the health check, load generator, and tests
- At least 4 GB RAM recommended for the complete stack

## Quickstart

Start the complete stack from the repository root:

```bash
# Linux/macOS
./start.sh

# Windows PowerShell
.\start.ps1

# Any platform with Docker Compose
docker compose up -d --build
```

The startup scripts wait for the services and run `scripts/health_check.py`. The health check verifies the API, API metrics, dashboard, Prometheus, and Grafana endpoints.

Stop the stack while retaining named volumes:

```bash
docker compose down
```

Remove containers, volumes, and local storage data:

```bash
docker compose down -v
```

## Service URLs

| Service | URL | Purpose |
| --- | --- | --- |
| Ingestion API docs | [http://localhost:8010/docs](http://localhost:8010/docs) | Interactive OpenAPI documentation |
| API health | [http://localhost:8010/healthz](http://localhost:8010/healthz) | Dependency health status |
| API metrics | [http://localhost:8010/metrics](http://localhost:8010/metrics) | Prometheus scrape endpoint |
| Operations dashboard | [http://localhost:8090](http://localhost:8090) | Telemetry, uploads, and load testing |
| Prometheus | [http://localhost:9095](http://localhost:9095) | Metrics and queries |
| Grafana | [http://localhost:3010](http://localhost:3010) | Provisioned monitoring dashboards |

Grafana is configured for anonymous administrator access in the local Compose deployment.

## API Endpoints

The API accepts JSON, multipart form data, or a raw request body at `POST /api/v1/ingest`.

Example JSON submission:

```bash
curl -X POST http://localhost:8010/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "agency_id": "agency-001",
    "audit_type": "ISO_27001",
    "report_year": 2026,
    "auditor_org": "Example Audit Group",
    "idempotency_key": "example-report-001",
    "content_raw": "audit report content"
  }'
```

The response contains a `job_id`, checksum, partition, and relative `status_url`. Retrieve processing state with:

```bash
curl http://localhost:8010/api/v1/status/<job_id>
```

Additional API endpoints:

- `GET /livez` - alias for the health endpoint.
- `GET /api/v1/stats` - limiter state and capacity configuration.
- `GET /api/v1/console` - read-only document, worker, event, and admission snapshot.

Default capacity settings are a 5,000-document burst target over 30 seconds, a maximum of 250 concurrent requests, a 100-token per-agency burst, and a 20-token-per-second per-agency refill rate. These settings can be overridden through the API service environment; the maximum request payload is 25 MiB.

## Dashboard Features

The dashboard at `http://localhost:8090` provides:

- Live pipeline health, document lifecycle counts, throughput, latency, Kafka lag, and backpressure telemetry.
- Document upload through `POST /api/documents`.
- Per-document processing events through `GET /api/documents/{document_id}/events`.
- Generated sample PDF reports through `POST /api/generate-sample-reports`.
- Configurable performance tests through `POST /api/performance-tests` and `GET /api/performance-tests/{test_run_id}`.
- A direct burst trigger through `POST /api/trigger-burst`.

## Testing and Load Generation

Run the automated suite from the repository root:

```bash
pytest tests/ -v --tb=short
```

The tests cover deterministic partitioning, adaptive limiter behavior, token buckets, content-addressed storage, analyzer scoring, backpressure classification, idempotency, dashboard telemetry, and load-test result classification.

Run the standalone burst generator against a running API:

```bash
python scripts/load_test.py \
  --url http://localhost:8010 \
  --total-requests 5000 \
  --duration 30 \
  --concurrency 2500
```

The `Makefile` provides shortcuts:

```bash
make up
make health
make test
make load-test
make scale-workers
make down
```

Scale the worker service horizontally:

```bash
docker compose up --scale worker=4 -d
```

## Configuration and Data

Compose provisions Redpanda, PostgreSQL 16, MinIO, Prometheus, and Grafana. Persistent service data uses named Docker volumes. API document staging and blob data are also mounted at `data/storage` in the repository.

Important defaults are defined in [api/src/config.py](api/src/config.py) and [worker/src/config.py](worker/src/config.py). The PostgreSQL schema, including documents, processing results, processing events, worker status, and load-test runs, is initialized from [database/init.sql](database/init.sql).

## Project Documentation

- [Design document](docs/DESIGN_DOCUMENT.md) - architecture, capacity assumptions, and contracts.
- [Results and analysis](docs/RESULTS_AND_ANALYSIS.md) - benchmark methodology and results.
- [Operational runbook](docs/RUNBOOK.md) - deployment, scaling, monitoring, and recovery procedures.
