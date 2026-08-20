"""Prometheus metrics for the worker's Kafka lifecycle processing."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

WORKER_DOCUMENTS_PROCESSED_TOTAL = Counter("worker_documents_processed_total", "Total documents completed by workers", ["worker_id"])
WORKER_DOCUMENTS_FAILED_TOTAL = Counter("worker_documents_failed_total", "Total documents that exhausted worker retries", ["worker_id"])
WORKER_PROCESSING_DURATION_SECONDS = Histogram(
    "worker_processing_duration_seconds", "Time taken to run the minimal document processing step", ["worker_id"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)
WORKER_PROCESSING_RATE = Gauge("worker_processing_rate", "Documents completed by a worker per second", ["worker_id"])
WORKER_ACTIVE_JOBS = Gauge("worker_active_jobs", "Number of documents currently being processed", ["worker_id"])
WORKER_RETRY_TOTAL = Counter("worker_retry_total", "Total processing retries attempted by workers", ["worker_id"])
WORKER_HEARTBEAT = Gauge("worker_heartbeat", "Unix timestamp of the worker's latest heartbeat", ["worker_id"])
WORKER_CONSUMER_LAG_GAUGE = Gauge("worker_consumer_lag", "Estimated consumer lag per partition", ["topic", "partition"])


def start_worker_metrics_server(port: int = 9100):
    start_http_server(port)
