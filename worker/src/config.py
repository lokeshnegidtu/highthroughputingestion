"""
AegisIngest Worker - Configuration Module
Worker concurrency and batch sizing derived to meet the 5,000 burst drain target.
"""

import os
from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict


class WorkerSettings(BaseSettings):
    model_config = ConfigDict(case_sensitive=True, env_file=".env", extra="ignore")

    SERVICE_NAME: str = "aegis-processor-worker"
    WORKER_ID: str = "worker-01"
    LOG_LEVEL: str = "INFO"
    PROMETHEUS_METRICS_PORT: int = 9100

    # --- Broker & Consumer Group ---
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="broker:9092", description="Broker connection string")
    KAFKA_TOPIC_INGEST: str = "audit-reports-ingest"
    KAFKA_TOPIC_DLQ: str = "audit-reports-dlq"
    KAFKA_CONSUMER_GROUP: str = "aegis-audit-processors"
    DATABASE_URL: str = "postgresql://aegis:aegis@postgres:5432/aegis"
    
    # Batching and Concurrency:
    KAFKA_MAX_POLL_RECORDS: int = 50
    KAFKA_FETCH_MIN_BYTES: int = 1024
    KAFKA_FETCH_MAX_WAIT_MS: int = 100
    WORKER_CONCURRENCY: int = 8
    PROCESSING_DELAY_SECONDS: float = 0.05
    PROCESSING_MAX_RETRIES: int = 3
    HEARTBEAT_INTERVAL_SECONDS: float = 5.0


worker_settings = WorkerSettings()
