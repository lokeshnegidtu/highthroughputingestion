"""Worker-side PostgreSQL result persistence with document-id idempotency."""
import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional
try:
    import asyncpg
except ImportError:
    asyncpg = None
from worker.src.config import worker_settings


class ResultRepository:
    def __init__(self): self.pool: Optional[asyncpg.Pool] = None
    async def start(self): self.pool = await asyncpg.create_pool(worker_settings.DATABASE_URL, min_size=1, max_size=10)
    async def stop(self):
        if self.pool: await self.pool.close(); self.pool = None
    async def completed(self, document_id: str) -> bool:
        assert self.pool
        return bool(await self.pool.fetchval("SELECT EXISTS(SELECT 1 FROM processing_results WHERE document_id=$1)", uuid.UUID(document_id)))
    async def save_result(self, document_id: str, result: Dict[str, Any], duration_ms: int, *, worker_id: str, kafka_topic: str, kafka_partition: int, kafka_offset: int, processing_started_at: datetime, processing_completed_at: datetime) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""INSERT INTO processing_results
                    (result_id,document_id,classification_status,processing_started_at,processing_completed_at,processing_duration_ms,model_version,result_json)
                    VALUES ($1,$2,'COMPLETED',$3,$4,$5,'v1',$6::jsonb)
                    ON CONFLICT (document_id) DO NOTHING""",
                    uuid.uuid4(), uuid.UUID(document_id), processing_started_at, processing_completed_at, duration_ms, json.dumps(result))
                await conn.execute("""UPDATE documents SET
                    status='COMPLETED',
                    kafka_topic=$2,
                    kafka_partition=$3,
                    kafka_offset=$4,
                    worker_id=$5,
                    processing_started_at=$6,
                    processing_completed_at=$7,
                    processing_duration_ms=$8,
                    updated_at=now()
                    WHERE document_id=$1""",
                    uuid.UUID(document_id), kafka_topic, kafka_partition, kafka_offset, worker_id,
                    processing_started_at, processing_completed_at, duration_ms)

                await conn.execute("""INSERT INTO processing_events (event_id, document_id, agency_id, sequence_number, event_type, worker_id, kafka_partition, kafka_offset, severity, message)
                    SELECT gen_random_uuid(), d.document_id, d.agency_id, d.sequence_number, 'PROCESSING_COMPLETED', $2, d.kafka_partition, d.kafka_offset, 'INFO', 'Worker processed and completed Kafka message'
                    FROM documents d WHERE d.document_id=$1
                    ON CONFLICT DO NOTHING""", uuid.UUID(document_id), worker_id)

    async def mark_processing_started(self, document_id: str, *, worker_id: str, kafka_topic: str, kafka_partition: int, kafka_offset: int, started_at: datetime) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""UPDATE documents SET
                    status='PROCESSING',
                    kafka_topic=$2,
                    kafka_partition=$3,
                    kafka_offset=$4,
                    worker_id=$5,
                    processing_started_at=$6,
                    updated_at=now()
                    WHERE document_id=$1""",
                    uuid.UUID(document_id), kafka_topic, kafka_partition, kafka_offset, worker_id, started_at)
                await conn.execute("""INSERT INTO processing_events (event_id, document_id, agency_id, sequence_number, event_type, worker_id, kafka_partition, kafka_offset, severity, message)
                    SELECT gen_random_uuid(), d.document_id, d.agency_id, d.sequence_number, 'PROCESSING_STARTED', $2, d.kafka_partition, d.kafka_offset, 'INFO', 'Worker started processing Kafka message'
                    FROM documents d WHERE d.document_id=$1""",
                    uuid.UUID(document_id), worker_id)

    async def record_retry(self, document_id: str, *, worker_id: str, retry_count: int, kafka_partition: int, kafka_offset: int) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            await conn.execute("""UPDATE documents SET retry_count=$2, status='RETRYING', updated_at=now() WHERE document_id=$1""",
                uuid.UUID(document_id), retry_count)
            await conn.execute("""INSERT INTO processing_events (event_id, document_id, agency_id, sequence_number, event_type, worker_id, kafka_partition, kafka_offset, severity, message)
                SELECT gen_random_uuid(), d.document_id, d.agency_id, d.sequence_number, 'RETRYING', $2, $3, $4, 'WARNING', 'Worker retry scheduled due to processing failure'
                FROM documents d WHERE d.document_id=$1""",
                uuid.UUID(document_id), worker_id, kafka_partition, kafka_offset)

    async def register_worker(self, worker_id: str, *, hostname: str, process_id: int, status: str = 'REGISTERED',
                             current_state: str = 'IDLE', current_document_id: str = None, cpu_usage_pct: float = 0.0,
                             memory_usage_pct: float = 0.0, processing_rate: float = 0.0, failed_jobs: int = 0,
                             retry_count: int = 0) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            await conn.execute("""INSERT INTO worker_status
                (worker_id, hostname, process_id, status, current_state, current_document_id, cpu_usage_pct, memory_usage_pct,
                 processing_rate, failed_jobs, retry_count, last_heartbeat, registered_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now())
                ON CONFLICT (worker_id) DO UPDATE SET
                    hostname = EXCLUDED.hostname,
                    process_id = EXCLUDED.process_id,
                    status = EXCLUDED.status,
                    current_state = EXCLUDED.current_state,
                    current_document_id = EXCLUDED.current_document_id,
                    cpu_usage_pct = EXCLUDED.cpu_usage_pct,
                    memory_usage_pct = EXCLUDED.memory_usage_pct,
                    processing_rate = EXCLUDED.processing_rate,
                    failed_jobs = EXCLUDED.failed_jobs,
                    retry_count = EXCLUDED.retry_count,
                    last_heartbeat = now()""",
                worker_id, hostname, process_id, status, current_state, uuid.UUID(current_document_id) if current_document_id else None,
                cpu_usage_pct, memory_usage_pct, processing_rate, failed_jobs, retry_count)

    async def heartbeat_worker(self, worker_id: str, *, status: str = 'ACTIVE', current_state: str = 'IDLE',
                              current_document_id: str = None, cpu_usage_pct: float = 0.0,
                              memory_usage_pct: float = 0.0, processing_rate: float = 0.0,
                              failed_jobs: int = 0, retry_count: int = 0) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            await conn.execute("""UPDATE worker_status SET
                status = $2,
                current_state = $3,
                current_document_id = $4,
                cpu_usage_pct = $5,
                memory_usage_pct = $6,
                processing_rate = $7,
                failed_jobs = $8,
                retry_count = $9,
                last_heartbeat = now()
                WHERE worker_id = $1""",
                worker_id, status, current_state, uuid.UUID(current_document_id) if current_document_id else None,
                cpu_usage_pct, memory_usage_pct, processing_rate, failed_jobs, retry_count)

    async def append_event(self, *, event_type: str, worker_id: str, severity: str = 'INFO', message: str = '',
                          document_id: Optional[str] = None, agency_id: Optional[str] = None,
                          sequence_number: Optional[int] = None, kafka_partition: Optional[int] = None,
                          kafka_offset: Optional[int] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute("""INSERT INTO processing_events
                (event_id, document_id, agency_id, sequence_number, event_type, worker_id, kafka_partition, kafka_offset, severity, message, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                uuid.uuid4(),
                uuid.UUID(document_id) if document_id else None,
                agency_id,
                sequence_number,
                event_type,
                worker_id,
                kafka_partition,
                kafka_offset,
                severity,
                message,
                json.dumps(metadata) if metadata else '{}')

repository = ResultRepository()
