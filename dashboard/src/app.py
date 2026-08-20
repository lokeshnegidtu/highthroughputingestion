"""
AegisIngest - Real-Time Operations Dashboard Backend
Provides telemetry aggregation, live monitoring feeds, and interactive burst simulation.
"""

import os
import time
import json
import asyncio
import math
import logging
from typing import Dict, Any, List, Optional
from uuid import uuid4
import uuid as uuidlib

import httpx
import asyncpg
from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("aegis.dashboard")

API_URL = os.getenv("API_URL", "http://api:8000")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://aegis:aegis@postgres:5432/aegis")

app = FastAPI(title="AegisIngest Operations Console", version="1.0.0")

batches: Dict[str, Dict[str, Any]] = {}
performance_tests: Dict[str, Dict[str, Any]] = {}
db_pool: Optional[asyncpg.Pool] = None


def reconcile_document_lifecycle(counts: Optional[Dict[str, Any]]) -> Dict[str, int]:
    """Reconcile document lifecycle totals from the durable PostgreSQL state."""
    if not isinstance(counts, dict):
        return {"submitted": 0, "processing": 0, "completed": 0, "failed": 0}

    normalized = {str(key).upper(): int(value or 0) for key, value in counts.items()}
    submitted = sum(normalized.values())
    processing = sum(normalized.get(key, 0) for key in ("QUEUED", "PROCESSING", "RETRYING"))
    completed = normalized.get("COMPLETED", 0)
    failed = normalized.get("FAILED", 0) + normalized.get("REJECTED", 0)
    return {
        "submitted": max(0, submitted),
        "processing": max(0, processing),
        "completed": max(0, completed),
        "failed": max(0, failed),
    }


def evaluate_pipeline_health(services: Dict[str, Any]) -> str:
    """Classify overall pipeline status using only required service dependencies."""
    required = {
        "api": bool(services.get("api", False)),
        "database": bool(services.get("database", False)),
        "broker": bool(services.get("broker", False)),
        "workers": bool(services.get("workers", False)),
    }
    if all(required.values()):
        return "HEALTHY"
    if any(required.values()):
        return "DEGRADED"
    return "UNHEALTHY"


async def query_prometheus(client: httpx.AsyncClient, query: str) -> Optional[float]:
    """Query Prometheus and return the first numeric value for a metric expression."""
    try:
        response = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=3.0)
        if response.status_code != 200:
            return None
        payload = response.json()
        result = payload.get("data", {}).get("result", [])
        if not result:
            return None
        value = result[0].get("value", [None, None])
        if value[1] is None:
            return None
        numeric_value = float(value[1])
        return numeric_value if math.isfinite(numeric_value) else None
    except Exception:
        return None


async def init_db():
    """Initialize database connection pool."""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        logger.info("Dashboard connected to PostgreSQL")
    except Exception as e:
        logger.warning("Failed to connect to PostgreSQL: %s", e)


async def close_db():
    """Close database connection pool."""
    global db_pool
    if db_pool:
        await db_pool.close()


def create_simple_pdf(title: str, lines: List[str]) -> bytes:
    """Build a compact, dependency-free PDF suitable for sample report data."""
    def pdf_text(value: str) -> str:
        return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    content_lines = ["BT", "/F1 18 Tf", "72 750 Td", f"({pdf_text(title)}) Tj", "/F1 10 Tf"]
    for line in lines:
        content_lines.extend(["0 -22 Td", f"({pdf_text(line)}) Tj"])
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", "replace")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(output))
        output.extend(f"{index} 0 obj\n".encode() + obj + b"\nendobj\n")
    xref = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode())
    output.extend(b"".join(f"{offset:010} 00000 n \n".encode() for offset in offsets[1:]))
    output.extend(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
    return bytes(output)


async def send_reports(batch_id: str, reports: List[Dict[str, Any]]):
    """Submit reports serially so the UI can show each file entering the pipeline."""
    batch = batches[batch_id]
    batch["status"] = "RUNNING"
    async with httpx.AsyncClient(timeout=20.0) as client:
        for report in reports:
            batch["current_file"] = report["name"]
            try:
                response = await client.post(
                    f"{API_URL}/api/v1/ingest",
                    data={"agency_id": "dashboard-upload", "audit_type": "ISO_27001", "report_year": "2026"},
                    files={"file": (report["name"], report["content"], report.get("content_type", "application/pdf"))},
                )
                if response.status_code in (200, 202):
                    batch["accepted"] += 1
                    # The API has durably accepted this report. Worker completion remains observable via telemetry.
                    batch["completed"] += 1
                else:
                    batch["errors"] += 1
            except Exception as exc:
                logger.warning("Dashboard report submission failed for %s: %s", report["name"], exc)
                batch["errors"] += 1
    batch["current_file"] = ""
    batch["status"] = "COMPLETED" if batch["errors"] == 0 else "COMPLETED_WITH_ERRORS"


def start_batch(reports: List[Dict[str, Any]]) -> Dict[str, str]:
    batch_id = uuid4().hex
    batches[batch_id] = {"batch_id": batch_id, "status": "PREPARING", "total": len(reports), "accepted": 0,
                         "completed": 0, "errors": 0, "current_file": "", "created_at": time.time()}
    asyncio.create_task(send_reports(batch_id, reports))
    return {"batch_id": batch_id}


@app.on_event("startup")
async def startup():
    await init_db()
    logger.info("Dashboard initialized with API and Prometheus integration.")


@app.on_event("shutdown")
async def shutdown():
    await close_db()


@app.get("/api/telemetry")
async def get_telemetry():
    """Aggregates telemetry from the API, Prometheus, and PostgreSQL-backed state."""
    telemetry = {
        "timestamp": time.time(),
        "api_status": "OFFLINE",
        "limiter": {"current_limit": 250, "in_flight": 0, "smoothed_rtt_ms": 0},
        "stats": {
            "total_ingested": 0,
            "total_processed": 0,
            "active_pipelines": 0,
            "submission_rate": 0.0,
            "processing_rate": 0.0,
            "rate_shed_429": 0,
            "error_5xx": 0,
            "http_5xx_total": 0,
            "kafka_lag": 0,
            "api_p95_ms": 0,
        },
        "system_health": {"api": False, "database": False, "broker": False, "workers": False, "minio": False},
    }

    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            health_res = await client.get(f"{API_URL}/healthz")
            if health_res.status_code == 200:
                health = health_res.json()
                telemetry["api_status"] = health.get("status", "OFFLINE")
                telemetry["system_health"]["api"] = bool(health.get("status") in {"HEALTHY", "DEGRADED"})
                telemetry["system_health"]["database"] = bool(health.get("database_connected"))
                telemetry["system_health"]["broker"] = bool(health.get("broker_connected"))
                telemetry["system_health"]["minio"] = bool(health.get("object_storage_connected"))
        except Exception:
            pass

        try:
            res = await client.get(f"{API_URL}/api/v1/stats")
            if res.status_code == 200:
                data = res.json()
                telemetry["limiter"] = data.get("limiter", telemetry["limiter"])
        except Exception:
            pass

        try:
            console_res = await client.get(f"{API_URL}/api/v1/console")
            if console_res.status_code == 200:
                console = console_res.json()
                counts = console.get("counts", {})
                lifecycle = reconcile_document_lifecycle(counts)
                telemetry["stats"]["total_ingested"] = lifecycle["submitted"]
                telemetry["stats"]["total_processed"] = lifecycle["completed"]
                telemetry["stats"]["active_pipelines"] = lifecycle["processing"]
                telemetry["system_health"]["workers"] = bool(console.get("workers") or [])
        except Exception:
            pass

        try:
            telemetry["stats"]["submission_rate"] = await query_prometheus(client, 'sum(rate(http_requests_total{status_code="202"}[5m])) or 0') or 0.0
            telemetry["stats"]["processing_rate"] = await query_prometheus(client, 'sum(rate(worker_documents_processed_total[5m])) or 0') or 0.0
            p95_seconds = await query_prometheus(client, 'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{endpoint="/api/v1/ingest",method="POST"}[5m])) by (le))') or 0.0
            telemetry["stats"]["api_p95_ms"] = p95_seconds * 1000.0
            telemetry["stats"]["http_5xx_total"] = int((await query_prometheus(client, 'sum(increase(http_requests_total{status_code=~"5.."}[5m])) or 0')) or 0)
            telemetry["stats"]["kafka_lag"] = int((await query_prometheus(client, 'sum(worker_consumer_lag) or 0')) or 0)
            telemetry["stats"]["rate_shed_429"] = int((await query_prometheus(client, 'sum(increase(http_requests_total{status_code="429"}[5m])) or 0')) or 0)
            telemetry["stats"]["error_5xx"] = telemetry["stats"]["http_5xx_total"]
        except Exception:
            pass

        telemetry["system_health"]["status"] = evaluate_pipeline_health(telemetry["system_health"])

    return telemetry


@app.get("/api/recent-audits")
async def get_recent_audits():
    """Recent audit state is exposed by the API's PostgreSQL-backed read model."""
    return {"count": 0, "audits": []}


@app.get("/api/console")
async def console_snapshot():
    """Proxy the API's read-only PostgreSQL-backed operations snapshot."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(f"{API_URL}/api/v1/console")
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        logger.warning("Console snapshot unavailable: %s", exc)
        return {"counts": {}, "documents": [], "workers": [], "events": [], "admission": {}, "broker_connected": False}


@app.get("/api/documents/{document_id}/events")
async def get_document_events(document_id: str):
    """Retrieve chronological processing events for a specific document."""
    if not db_pool:
        return {"events": []}
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT event_type, severity, worker_id, document_id, agency_id, kafka_partition, kafka_offset, message, created_at "
                "FROM processing_events WHERE document_id = $1 ORDER BY created_at ASC",
                uuidlib.UUID(document_id)
            )
            events = []
            for r in rows:
                event = dict(r)
                if event.get("created_at"):
                    event["created_at"] = event["created_at"].isoformat()
                if event.get("document_id"):
                    event["document_id"] = str(event["document_id"])
                events.append(event)
            return {"events": events}
    except Exception as exc:
        logger.warning("Failed to fetch events for document %s: %s", document_id, exc)
        return {"events": []}



@app.post("/api/documents")
async def submit_document(agency_id: str = Form(...), case_id: str = Form(""),
                          document_type: str = Form("VAPT Report"), file: UploadFile = File(...)):
    """Console upload proxy; durable admission remains exclusively in the ingestion API."""
    payload = await file.read()
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{API_URL}/api/v1/ingest",
            data={"agency_id": agency_id, "audit_type": document_type, "auditor_org": case_id or "Console submission", "report_year": "2026"},
            files={"file": (file.filename or "audit-report", payload, file.content_type or "application/octet-stream")},
        )
    return JSONResponse(status_code=response.status_code, content=response.json(),
                        headers={"Retry-After": response.headers.get("Retry-After", "")} if response.status_code == 429 else None)


async def run_performance_test(test_run_id: str, target_documents: int, target_duration_seconds: int, audit_type: str):
    """Execute a time-based load test and store results in the database."""
    test = performance_tests[test_run_id]
    test["status"] = "RUNNING"
    test["started_at"] = time.time()
    
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE load_test_runs SET status='RUNNING', started_at=now() WHERE test_run_id=$1",
                    uuidlib.UUID(test_run_id)
                )
        except Exception as e:
            logger.warning("Failed to update test status in DB: %s", e)
    
    try:
        result = await _run_timed_burst(target_documents, target_duration_seconds, audit_type)
        test.update(result)
        test["actual_duration"] = time.time() - test["started_at"]
        
        # Determine PASS/FAIL based on comprehensive criteria
        test_result = determine_test_result(result, target_documents, target_duration_seconds)
        test["test_result"] = test_result
        test["status"] = "COMPLETED"
        
        # Persist to database
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE load_test_runs 
                        SET status='COMPLETED', 
                            requested_documents=$2, accepted_202=$3, rejected_429=$4, rejected_4xx=$5, 
                            failed_5xx=$6, failed_timeout=$7,
                            p50_latency_ms=$8, p95_latency_ms=$9, p99_latency_ms=$10,
                            test_result=$11, actual_duration_seconds=$12, throughput_docs_per_sec=$13,
                            completed_at=now()
                        WHERE test_run_id=$1
                    """, uuidlib.UUID(test_run_id), 
                    result.get("requested_documents", 0),
                    result.get("accepted_202", 0),
                    result.get("rejected_429", 0),
                    result.get("rejected_4xx", 0),
                    result.get("failed_5xx", 0),
                    result.get("failed_timeout", 0),
                    result.get("p50_latency_ms", 0),
                    result.get("p95_latency_ms", 0),
                    result.get("p99_latency_ms", 0),
                    test_result,
                    test["actual_duration"],
                    result.get("throughput_docs_per_sec", 0)
                    )
            except Exception as e:
                logger.warning("Failed to persist test results to DB: %s", e)
    except Exception as exc:
        test.update({"status": "FAILED", "error": str(exc), "test_result": "FAIL"})
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE load_test_runs SET status='FAILED', test_result='FAIL', completed_at=now() WHERE test_run_id=$1",
                        uuidlib.UUID(test_run_id)
                    )
            except Exception as e:
                logger.warning("Failed to update failed test in DB: %s", e)


def classify_load_test_status(status_code: Optional[int]) -> Dict[str, Any]:
    """Map an HTTP status code to the correct load-test bucket."""
    if status_code in (200, 202):
        return {"bucket": "accepted_202", "success": True, "label": "Accepted"}
    if status_code == 429:
        return {"bucket": "rejected_429", "success": False, "label": "Rejected / Backpressure"}
    if status_code is not None and 400 <= status_code < 500:
        return {"bucket": "rejected_4xx", "success": False, "label": "Failed 4xx"}
    if status_code is not None and 500 <= status_code < 600:
        return {"bucket": "failed_5xx", "success": False, "label": "Failed 5xx"}
    if status_code in (None, 0):
        return {"bucket": "failed_transport", "success": False, "label": "Connection / timeout"}
    return {"bucket": "failed_transport", "success": False, "label": "Connection / timeout"}


def calculate_throughput_docs_per_sec(result: Dict[str, Any]) -> float:
    """Return accepted docs per second over the elapsed test duration."""
    successful = int(result.get("accepted_202", 0) or 0)
    elapsed = float(result.get("actual_duration", 0) or result.get("elapsed_seconds", 0) or 0)
    if successful <= 0:
        return 0.0
    if elapsed <= 0:
        return 0.0
    return round(successful / elapsed, 2)


def determine_test_result(result: Dict[str, Any], target_documents: int, target_duration_seconds: int) -> str:
    """Evaluate PASS/FAIL based on complete, real results only."""
    accepted = int(result.get("accepted_202", 0) or 0)
    failed_5xx = int(result.get("failed_5xx", 0) or 0)
    p95_ms = result.get("p95_latency_ms")
    requested = int(result.get("requested_documents", 0) or 0)
    completed_requests = int(result.get("completed_requests", 0) or 0)
    failed_timeout = int(result.get("failed_timeout", 0) or 0)
    failed_transport = int(result.get("failed_transport", 0) or 0)

    if requested <= 0 or completed_requests <= 0:
        return "FAIL"

    if completed_requests < requested:
        return "FAIL"

    if requested < target_documents:
        return "FAIL"

    if failed_5xx > 0:
        return "FAIL"

    if failed_timeout > 0 or failed_transport > 0:
        return "FAIL"

    if p95_ms is None or p95_ms >= 150:
        return "FAIL"

    acceptance_rate = accepted / requested if requested > 0 else 0
    if acceptance_rate < 0.75:
        return "FAIL"

    return "PASS"


async def _run_timed_burst(target_documents: int, target_duration_seconds: int, audit_type: str) -> Dict[str, Any]:
    """
    Run a time-based load test.
    Dynamically adjusts request rate to attempt exactly target_documents in target_duration_seconds.
    """
    # Calculate target request rate
    target_rate = target_documents / target_duration_seconds  # requests per second
    
    latencies = []
    status_codes = {202: 0, 429: 0, 400: 0, 404: 0, 500: 0, 503: 0}
    timeouts = 0
    transport_failures = 0
    tasks = []
    
    async def send_single(client: httpx.AsyncClient, i: int):
        nonlocal timeouts, transport_failures
        try:
            started = time.perf_counter()
            response = await client.post(
                f"{API_URL}/api/v1/ingest",
                json={
                    "agency_id": f"load-{i % 100:03d}",
                    "audit_type": audit_type,
                    "report_year": 2026,
                    "auditor_org": "Load Test",
                },
                timeout=10.0
            )
            latency = (time.perf_counter() - started) * 1000
            code = response.status_code
            # Normalize any non-202 success to the accepted bucket, while keeping
            # 429/4xx/5xx semantics explicit for reporting. Timeouts and transport
            # failures are recorded separately and excluded from the 5xx bucket.
            if code in (200, 202):
                code = 202
            elif code == 429:
                code = 429
            elif 400 <= code < 500:
                code = 400
            elif code >= 500:
                code = 500
            else:
                code = 202

            status_codes[code] = status_codes.get(code, 0) + 1
            latencies.append(latency)
            return code, latency
        except (httpx.TimeoutException, asyncio.TimeoutError):
            timeouts += 1
            return None, 0
        except httpx.HTTPError as exc:
            transport_failures += 1
            logger.warning("Load-test transport failure: %s", exc)
            return None, 0
    
    start_time = time.perf_counter()
    async with httpx.AsyncClient(timeout=12.0) as client:
        # Schedule requests at the target rate
        for i in range(target_documents):
            # Calculate when this request should be sent
            elapsed = time.perf_counter() - start_time
            ideal_time = i / target_rate
            
            if elapsed < ideal_time:
                await asyncio.sleep(ideal_time - elapsed)
            
            # Fire the request without waiting (concurrent)
            tasks.append(asyncio.create_task(send_single(client, i)))

        # Do not close the client or persist a result while scheduled requests
        # are still running; await every request to a response or timeout.
        await asyncio.gather(*tasks)
    
    # Calculate statistics
    latencies.sort()
    actual_duration = max(time.perf_counter() - start_time, 0.001)
    result = {
        "requested_documents": len(tasks),
        "completed_requests": len(tasks),
        "accepted_202": status_codes.get(202, 0),
        "rejected_429": status_codes.get(429, 0),
        "rejected_4xx": sum(v for k, v in status_codes.items() if 400 <= k < 500 and k != 429),
        "failed_5xx": sum(v for k, v in status_codes.items() if k >= 500),
        "failed_timeout": timeouts,
        "failed_transport": transport_failures,
        "p50_latency_ms": round(latencies[int(len(latencies) * 0.50)], 2) if latencies else None,
        "p95_latency_ms": round(latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))], 2) if latencies else None,
        "p99_latency_ms": round(latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))], 2) if latencies else None,
        "actual_duration": actual_duration,
    }
    result["throughput_docs_per_sec"] = calculate_throughput_docs_per_sec(result)
    return result



@app.post("/api/performance-tests")
async def start_performance_test(request: Request):
    """Start a new load test with specified target documents and duration."""
    body = await request.json()
    test_run_id = uuid4().hex
    
    # Parse scenario: "5000 documents / 30 seconds" → 5000 docs, 30 sec
    scenario = body.get("scenario", "100 documents / 10 seconds").strip()
    parts = scenario.split("/")
    target_documents = int(parts[0].split()[0]) if parts else 100
    target_duration = int(parts[1].split()[0]) if len(parts) > 1 else 10
    
    # Clamp reasonable limits
    target_documents = min(10000, max(10, target_documents))
    target_duration = min(300, max(5, target_duration))
    
    audit_type = body.get("audit_type", "ISO_27001")
    
    # Create test record in memory first
    performance_tests[test_run_id] = {
        "id": test_run_id,
        "status": "PREPARING",
        "target_documents": target_documents,
        "target_duration_seconds": target_duration,
        "audit_type": audit_type,
        "created_at": time.time(),
        "test_result": "PENDING",
    }
    
    # Create persistent record in database
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO load_test_runs 
                    (test_run_id, status, target_documents, target_duration_seconds)
                    VALUES ($1, $2, $3, $4)
                """, uuidlib.UUID(test_run_id), "PREPARING", target_documents, target_duration)
        except Exception as e:
            logger.warning("Failed to create test run in DB: %s", e)
    
    # Start the test in the background
    asyncio.create_task(run_performance_test(test_run_id, target_documents, target_duration, audit_type))
    
    return {
        "test_run_id": test_run_id,
        "status": "PREPARING",
        "target_documents": target_documents,
        "target_duration_seconds": target_duration,
        "created_at": performance_tests[test_run_id]["created_at"],
    }


@app.get("/api/performance-tests/{test_run_id}")
async def get_performance_test(test_run_id: str):
    """Get current status and results of a load test."""
    # Try to get from memory first (current/recent test)
    if test_run_id in performance_tests:
        test = performance_tests[test_run_id]
        return {
            "test_run_id": test_run_id,
            "status": test.get("status"),
            "target_documents": test.get("target_documents", 0),
            "target_duration_seconds": test.get("target_duration_seconds", 0),
            "requested_documents": test.get("requested_documents", 0),
            "completed_requests": test.get("completed_requests", 0),
            "accepted_202": test.get("accepted_202", 0),
            "rejected_429": test.get("rejected_429", 0),
            "rejected_4xx": test.get("rejected_4xx", 0),
            "failed_5xx": test.get("failed_5xx", 0),
            "failed_timeout": test.get("failed_timeout", 0),
            "failed_transport": test.get("failed_transport", 0),
            "p50_latency_ms": test.get("p50_latency_ms", 0),
            "p95_latency_ms": test.get("p95_latency_ms", 0),
            "p99_latency_ms": test.get("p99_latency_ms", 0),
            "throughput_docs_per_sec": test.get("throughput_docs_per_sec", 0),
            "test_result": test.get("test_result", "PENDING"),
            "actual_duration": test.get("actual_duration", 0),
        }
    
    # Try to get from database (completed test)
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM load_test_runs WHERE test_run_id=$1",
                    uuidlib.UUID(test_run_id)
                )
                if row:
                    return dict(row)
        except Exception as e:
            logger.warning("Failed to fetch test from DB: %s", e)
    
    raise HTTPException(status_code=404, detail="Performance test not found")


@app.post("/api/trigger-burst")
async def trigger_burst(request: Request):
    """Triggers an automated burst test directly from dashboard."""
    body = await request.json()
    count = min(1000, max(10, int(body.get("count", 100))))
    framework = body.get("audit_type", "ISO_27001")

    async def send_single(client: httpx.AsyncClient, i: int):
        agency = f"agency-console-{i % 25:03d}"
        payload = {
            "agency_id": agency,
            "audit_type": framework,
            "report_year": 2026,
            "auditor_org": "National Cybersecurity Inspection Team",
        }
        try:
            start = time.perf_counter()
            r = await client.post(f"{API_URL}/api/v1/ingest", json=payload)
            lat = (time.perf_counter() - start) * 1000
            return r.status_code, lat
        except Exception:
            return 500, 0

    results = {"202": 0, "429": 0, "5xx": 0, "latencies": []}
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [send_single(client, i) for i in range(count)]
        responses = await asyncio.gather(*tasks)

        for code, lat in responses:
            if code == 202:
                results["202"] += 1
                results["latencies"].append(lat)
            elif code == 429:
                results["429"] += 1
            elif code >= 500:
                results["5xx"] += 1

    latencies = sorted(results["latencies"])
    p50 = latencies[int(len(latencies) * 0.50)] if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0

    return {
        "requested_burst": count,
        "accepted_202": results["202"],
        "shed_429": results["429"],
        "server_errors_5xx": results["5xx"],
        "p50_latency_ms": round(p50, 2),
        "p95_latency_ms": round(p95, 2),
        "success_rate_pct": round((results["202"] / count) * 100, 1),
    }


@app.post("/api/generate-sample-reports")
async def generate_sample_reports(request: Request):
    """Create realistic PDF audit samples and stream them through the normal ingestion API."""
    try:
        count = int((await request.json()).get("count", 10))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="count must be a number")
    count = min(100, max(1, count))
    reports = []
    frameworks = ["ISO 27001", "SOC 2 Type II", "NIST CSF", "PCI DSS v4"]
    for index in range(count):
        report_number = index + 1
        framework = frameworks[index % len(frameworks)]
        name = f"sample-audit-report-{report_number:03d}.pdf"
        reports.append({
            "name": name,
            "content_type": "application/pdf",
            "content": create_simple_pdf(
                f"Sample Cybersecurity Audit Report #{report_number}",
                [
                    f"Framework: {framework}",
                    f"Organization: Example Agency {report_number:03d}",
                    "Reporting period: 2026",
                    "Assessment summary: Controls were reviewed against the selected framework.",
                    "This report was generated by AegisIngest for pipeline validation.",
                ],
            ),
        })
    return start_batch(reports)


@app.post("/api/upload-reports")
async def upload_reports(files: List[UploadFile] = File(...)):
    """Receive a folder selection from the console and ingest its files as one observable batch."""
    if not files:
        raise HTTPException(status_code=400, detail="Select at least one report")
    if len(files) > 100:
        raise HTTPException(status_code=400, detail="Please upload no more than 100 reports at a time")
    reports = []
    for upload in files:
        content = await upload.read()
        if not content:
            continue
        reports.append({"name": upload.filename or "uploaded-report", "content": content,
                        "content_type": upload.content_type or "application/octet-stream"})
    if not reports:
        raise HTTPException(status_code=400, detail="The selected folder contains no readable files")
    return start_batch(reports)


@app.get("/api/batches/{batch_id}")
async def get_batch(batch_id: str):
    batch = batches.get(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Ingestion batch not found")
    return batch


# Static Assets
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def serve_index():
    index_file = os.path.join(static_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return JSONResponse(content={"message": "AegisIngest Dashboard Backend Running"})
