"""PostgreSQL durable metadata and idempotency repository."""
import json
import uuid
from typing import Any, Dict, Optional, Tuple
try:
    import asyncpg
except ImportError:
    asyncpg = None
from api.src.config import settings


class DocumentRepository:
    def __init__(self):
        self.pool = None

    async def start(self) -> None:
        if not asyncpg:
            raise RuntimeError("asyncpg is not installed")
        self.pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=1, max_size=20)
        await self.ensure_schema()

    async def ensure_schema(self) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            columns = {
                row["column_name"] for row in await conn.fetch("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'documents'
                """)
            }
            missing = [
                ("kafka_topic", "TEXT"),
                ("kafka_partition", "INTEGER"),
                ("kafka_offset", "BIGINT"),
                ("worker_id", "TEXT"),
                ("processing_started_at", "TIMESTAMPTZ"),
                ("processing_completed_at", "TIMESTAMPTZ"),
                ("processing_duration_ms", "INTEGER"),
            ]
            for column_name, column_type in missing:
                if column_name not in columns:
                    await conn.execute(f"ALTER TABLE documents ADD COLUMN IF NOT EXISTS {column_name} {column_type}")
            worker_columns = {
                row["column_name"] for row in await conn.fetch("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'worker_status'
                """)
            }
            for column_name, column_type in [("last_heartbeat", "TIMESTAMPTZ"), ("processing_rate", "FLOAT"), ("failed_jobs", "INTEGER"), ("retry_count", "INTEGER")]:
                if column_name not in worker_columns:
                    await conn.execute(f"ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS {column_name} {column_type}")

    async def stop(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def create_or_get(self, *, idempotency_key: str, agency_id: str, filename: str,
                            object_key: str, sha256: str, file_size: int, audit_type: str,
                            report_year: int, auditor_org: str) -> Tuple[Dict[str, Any], bool]:
        assert self.pool
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow("SELECT * FROM documents WHERE idempotency_key=$1", idempotency_key)
                if existing:
                    return dict(existing), True
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", agency_id)
                sequence = await conn.fetchval("SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM documents WHERE agency_id=$1", agency_id)
                document_id = uuid.uuid4()
                row = await conn.fetchrow("""INSERT INTO documents
                  (document_id,idempotency_key,agency_id,filename,object_key,sha256,file_size,status,sequence_number,audit_type,report_year,auditor_org)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,'QUEUED',$8,$9,$10,$11) RETURNING *""",
                  document_id, idempotency_key, agency_id, filename, object_key, sha256, file_size, sequence, audit_type, report_year, auditor_org)
                await conn.execute("""INSERT INTO processing_events (event_id, document_id, agency_id, sequence_number, event_type, severity, message)
                    VALUES ($1, $2, $3, $4, 'DOCUMENT_SUBMITTED', 'INFO', 'Document accepted and queued for Kafka publish')""",
                    uuid.uuid4(), document_id, agency_id, sequence)
                return dict(row), False

    async def get(self, document_id: str) -> Optional[Dict[str, Any]]:
        assert self.pool
        row = await self.pool.fetchrow("SELECT d.*, r.result_json FROM documents d LEFT JOIN processing_results r USING(document_id) WHERE document_id=$1", uuid.UUID(document_id))
        return dict(row) if row else None

    async def update_document_metadata(self, document_id: str, *, kafka_topic: str = None, kafka_partition: int = None,
                                      kafka_offset: int = None, worker_id: str = None, status: str = None,
                                      processing_started_at=None, processing_completed_at=None,
                                      processing_duration_ms: int = None, retry_count: int = None,
                                      error_code: str = None) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                update_sql = ["updated_at = now()"]
                params = [uuid.UUID(document_id)]
                index = 1
                if kafka_topic is not None:
                    index += 1; params.append(kafka_topic); update_sql.append(f"kafka_topic = ${index}")
                if kafka_partition is not None:
                    index += 1; params.append(kafka_partition); update_sql.append(f"kafka_partition = ${index}")
                if kafka_offset is not None:
                    index += 1; params.append(kafka_offset); update_sql.append(f"kafka_offset = ${index}")
                if worker_id is not None:
                    index += 1; params.append(worker_id); update_sql.append(f"worker_id = ${index}")
                if status is not None:
                    index += 1; params.append(status); update_sql.append(f"status = ${index}")
                if processing_started_at is not None:
                    index += 1; params.append(processing_started_at); update_sql.append(f"processing_started_at = ${index}")
                if processing_completed_at is not None:
                    index += 1; params.append(processing_completed_at); update_sql.append(f"processing_completed_at = ${index}")
                if processing_duration_ms is not None:
                    index += 1; params.append(processing_duration_ms); update_sql.append(f"processing_duration_ms = ${index}")
                if retry_count is not None:
                    index += 1; params.append(retry_count); update_sql.append(f"retry_count = ${index}")
                if error_code is not None:
                    index += 1; params.append(error_code); update_sql.append(f"error_code = ${index}")
                if not update_sql:
                    return
                await conn.execute(f"UPDATE documents SET {', '.join(update_sql)} WHERE document_id = $1", *params)

    async def append_event(self, *, document_id: str = None, agency_id: str = None, sequence_number: int = None,
                          event_type: str, worker_id: str = None, kafka_partition: int = None,
                          kafka_offset: int = None, severity: str = 'INFO', message: str = '',
                          metadata: Optional[Dict[str, Any]] = None) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            await conn.execute("""INSERT INTO processing_events
                (event_id, document_id, agency_id, sequence_number, event_type, worker_id, kafka_partition, kafka_offset, severity, message, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)""",
                uuid.uuid4(), uuid.UUID(document_id) if document_id else None, agency_id, sequence_number,
                event_type, worker_id, kafka_partition, kafka_offset, severity, message,
                json.dumps(metadata or {}))

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

    async def console_snapshot(self) -> Dict[str, Any]:
        assert self.pool
        counts = await self.pool.fetch("SELECT status, count(*)::int AS total FROM documents GROUP BY status")
        documents = await self.pool.fetch("""SELECT document_id, agency_id, filename, status, sequence_number,
            kafka_topic, kafka_partition, kafka_offset, worker_id, processing_started_at,
            processing_completed_at, processing_duration_ms, created_at, updated_at
            FROM documents ORDER BY created_at DESC LIMIT 25""")
        workers = await self.pool.fetch("""SELECT worker_id, status, current_state, cpu_usage_pct, memory_usage_pct,
            processing_rate, current_document_id, failed_jobs, retry_count, last_heartbeat
            FROM worker_status ORDER BY last_heartbeat DESC LIMIT 25""")
        events = await self.pool.fetch("""SELECT event_type, severity, worker_id, document_id, agency_id,
            kafka_partition, kafka_offset, message, created_at FROM processing_events ORDER BY created_at DESC LIMIT 25""")
        return {"counts": {row["status"]: row["total"] for row in counts},
                "documents": [dict(row) for row in documents],
                "workers": [dict(row) for row in workers],
                "events": [dict(row) for row in events]}


repository = DocumentRepository()
