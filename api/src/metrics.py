"""
AegisIngest - Prometheus Instrumentation Module for Ingestion API
Tracks throughput, latency percentiles (with 150ms SLA boundary), and backpressure rejections.
"""

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# HTTP Request Count by Endpoint, Method, and Status Code
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests received by Ingestion API",
    ["endpoint", "method", "status_code"],
)

# HTTP Latency Histogram with fine-grained buckets around 150ms target
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["endpoint", "method"],
    buckets=[
        0.005, 0.010, 0.025, 0.050, 0.075, 0.100, 
        0.125, 0.150, 0.200, 0.250, 0.500, 1.000, 2.500, 5.000
    ],
)

# Backpressure Shedding Rejections Counter (429s)
BACKPRESSURE_REJECTIONS_TOTAL = Counter(
    "backpressure_rejections_total",
    "Total requests shed under backpressure / rate limits",
    ["reason"],
)

# Active Concurrent Pipelines
ACTIVE_PIPELINES_GAUGE = Gauge(
    "active_pipelines_gauge",
    "Number of active concurrent document ingestion pipelines",
)

# Total Ingested Document Bytes
INGESTED_BYTES_TOTAL = Counter(
    "ingested_document_bytes_total",
    "Total raw document bytes ingested",
)

# Dynamic Concurrency Limit Gauge
CURRENT_CONCURRENCY_LIMIT_GAUGE = Gauge(
    "adaptive_concurrency_limit",
    "Current dynamic concurrency ceiling from AIMD limiter",
)


def get_metrics_payload() -> bytes:
    return generate_latest()


def get_content_type() -> str:
    return CONTENT_TYPE_LATEST
