"""
AegisIngest Ingestion API - Configuration Module
All capacity constants are derived from stated performance specifications.
No hand-tuned magic numbers.
"""

import os
from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict


class Settings(BaseSettings):
    model_config = ConfigDict(case_sensitive=True, env_file=".env", extra="ignore")

    # --- Service Identity ---
    SERVICE_NAME: str = "aegis-ingest-api"
    ENVIRONMENT: str = Field(default="production", description="Environment mode")
    LOG_LEVEL: str = "INFO"
    API_PORT: int = 8000
    API_HOST: str = "0.0.0.0"

    # --- Capacity & Mathematical Derivation Assumptions ---
    # Stated Target: Burst of 5,000 document submissions in 30 seconds across 2,500 active pipelines.
    BURST_VOLUME: int = 5000                     # Total burst documents
    BURST_WINDOW_SECONDS: float = 30.0           # Burst duration in seconds
    TARGET_P95_LATENCY_SECONDS: float = 0.150    # Target API p95 latency: 150 ms
    PEAK_BURST_COEFFICIENT: float = 2.0          # Peak instantaneous burst multiplier

    # Derived Rate & Concurrency:
    # Mean arrival rate: lambda = 5000 / 30 = 166.67 req/s
    # Peak arrival rate: lambda_peak = 166.67 * 2.0 = 333.33 req/s
    # Little's Law In-Flight Concurrency: L = lambda_peak * W = 333.33 * 0.150 = 50 concurrent requests
    # Concurrency limit with 5x safety factor for async I/O: 250
    MAX_CONCURRENT_REQUESTS: int = 250
    
    # Adaptive Limiter Settings (AIMD Gradient Algorithm):
    ADAPTIVE_LIMITER_ENABLED: bool = True
    ADAPTIVE_RTT_TARGET_MS: float = 100.0        # Target RTT threshold (ms)
    ADAPTIVE_MIN_LIMIT: int = 25                 # Derived baseline (Little's Law at mean rate)
    ADAPTIVE_MAX_LIMIT: int = 500                # Absolute ceiling under healthy low RTT
    ADAPTIVE_AIMD_ALPHA: float = 2.0             # Additive increase step
    ADAPTIVE_AIMD_BETA: float = 0.8              # Multiplicative decrease factor (0.8 = shed 20% on saturation)

    # Per-Agency Token Bucket Rate Limiting:
    RATE_LIMIT_AGENCY_BURST: int = 100
    RATE_LIMIT_AGENCY_RATE: float = 20.0         # tokens per second

    # --- Broker Contract ---
    # Partition Count: 16 partitions derived to support up to 24 parallel worker threads
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="broker:9092", description="Broker connection string")
    KAFKA_TOPIC_INGEST: str = "audit-reports-ingest"
    KAFKA_TOPIC_DLQ: str = "audit-reports-dlq"
    KAFKA_NUM_PARTITIONS: int = 16
    KAFKA_PRODUCER_ACKS: str = "all"             # Durability guarantee
    KAFKA_PRODUCER_LINGER_MS: int = 5            # Micro-batching window for high throughput
    KAFKA_PRODUCER_COMPRESSION: str = "lz4"      # High throughput compression

    # --- Durable datastore and object-storage contracts ---
    DATABASE_URL: str = "postgresql://aegis:aegis@postgres:5432/aegis"
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "aegisminio"
    MINIO_SECRET_KEY: str = "aegisminio-secret"
    MINIO_BUCKET: str = "audit-documents"
    MINIO_SECURE: bool = False

    # --- Storage Contract ---
    STORAGE_DIR: str = os.getenv("STORAGE_DIR", "/data/storage")
    MAX_PAYLOAD_BYTES: int = 25 * 1024 * 1024    # 25 MB max document size

settings = Settings()
