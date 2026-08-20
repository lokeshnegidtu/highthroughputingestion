"""Minimal worker processing logic for the Kafka lifecycle processor."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from worker.src.config import worker_settings
from worker.src.metrics import (
    WORKER_DOCUMENTS_FAILED_TOTAL,
    WORKER_DOCUMENTS_PROCESSED_TOTAL,
    WORKER_PROCESSING_DURATION_SECONDS,
    WORKER_RETRY_TOTAL,
)
from worker.src.repository import repository

logger = logging.getLogger("aegis.processor")


class MinimalProcessingHandler:
    """Very small status processor: just mark lifecycle transitions, sleep, and finish."""

    def __init__(self, processing_delay: Optional[float] = None):
        self.processing_delay = processing_delay if processing_delay is not None else worker_settings.PROCESSING_DELAY_SECONDS

    async def has_completed(self, document_id: Optional[str]) -> bool:
        if not document_id or not repository.pool:
            return False
        return await repository.completed(document_id)

    async def mark_processing(self, document_id: str, *, worker_id: str, kafka_topic: Optional[str], kafka_partition: Optional[int], kafka_offset: Optional[int], agency_id: Optional[str], sequence_number: Optional[int]) -> None:
        if not repository.pool:
            return
        await repository.mark_processing_started(
            document_id,
            worker_id=worker_id,
            kafka_topic=kafka_topic or worker_settings.KAFKA_TOPIC_INGEST,
            kafka_partition=kafka_partition or 0,
            kafka_offset=kafka_offset or 0,
            started_at=datetime.now(timezone.utc),
        )
        await repository.append_event(
            document_id=document_id,
            agency_id=agency_id,
            sequence_number=sequence_number,
            event_type="PROCESSING",
            worker_id=worker_id,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            severity="INFO",
            message="Document transitioned to PROCESSING.",
            metadata={"topic": kafka_topic, "partition": kafka_partition, "offset": kafka_offset},
        )

    async def mark_completed(self, document_id: str, *, worker_id: str, kafka_topic: Optional[str], kafka_partition: Optional[int], kafka_offset: Optional[int], processing_started_at: datetime) -> None:
        if not repository.pool:
            return
        processing_completed_at = datetime.now(timezone.utc)
        duration_ms = max(0, int((processing_completed_at - processing_started_at).total_seconds() * 1000))
        await repository.save_result(
            document_id,
            result={"status": "COMPLETED", "worker_id": worker_id, "processing_mode": "minimal"},
            duration_ms=duration_ms,
            worker_id=worker_id,
            kafka_topic=kafka_topic or worker_settings.KAFKA_TOPIC_INGEST,
            kafka_partition=kafka_partition or 0,
            kafka_offset=kafka_offset or 0,
            processing_started_at=processing_started_at,
            processing_completed_at=processing_completed_at,
        )
        await repository.append_event(
            document_id=document_id,
            event_type="COMPLETED",
            worker_id=worker_id,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            severity="INFO",
            message="Document completed by minimal worker processing step.",
            metadata={"processing_duration_ms": duration_ms},
        )

    async def mark_retry(self, document_id: str, *, worker_id: str, retry_count: int, kafka_partition: Optional[int], kafka_offset: Optional[int]) -> None:
        if not repository.pool:
            return
        await repository.record_retry(
            document_id,
            worker_id=worker_id,
            retry_count=retry_count,
            kafka_partition=kafka_partition or 0,
            kafka_offset=kafka_offset or 0,
        )

    async def mark_failed(self, document_id: str, *, worker_id: str, kafka_topic: Optional[str], kafka_partition: Optional[int], kafka_offset: Optional[int], error: str, retry_count: int) -> None:
        if not repository.pool:
            return
        async with repository.pool.acquire() as conn:
            await conn.execute(
                """UPDATE documents
                    SET status='FAILED', retry_count=$2, error_code='WORKER_PROCESSING_FAILED', updated_at=now()
                    WHERE document_id=$1""",
                document_id,
                retry_count,
            )
        await repository.append_event(
            document_id=document_id,
            event_type="FAILED",
            worker_id=worker_id,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            severity="ERROR",
            message=str(error),
            metadata={"retry_count": retry_count, "topic": kafka_topic},
        )

    async def process_event(self, payload: Dict[str, Any], *, worker_id: str) -> bool:
        document_id = str(payload.get("document_id") or payload.get("job_id") or "")
        if not document_id:
            raise ValueError("Kafka payload missing document_id/job_id")

        if await self.has_completed(document_id):
            logger.info("Skipping already-completed document_id=%s worker_id=%s", document_id, worker_id)
            return True

        started_at = datetime.now(timezone.utc)
        retry_count = int(payload.get("retry_count") or 0)
        kafka_topic = payload.get("kafka_topic")
        kafka_partition = payload.get("kafka_partition")
        kafka_offset = payload.get("kafka_offset")
        agency_id = payload.get("agency_id")
        sequence_number = payload.get("sequence_number")

        try:
            await self.mark_processing(
                document_id,
                worker_id=worker_id,
                kafka_topic=kafka_topic,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                agency_id=agency_id,
                sequence_number=sequence_number,
            )
            await asyncio.sleep(self.processing_delay)
            await self.mark_completed(
                document_id,
                worker_id=worker_id,
                kafka_topic=kafka_topic,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                processing_started_at=started_at,
            )
            WORKER_DOCUMENTS_PROCESSED_TOTAL.labels(worker_id=worker_id).inc()
            WORKER_PROCESSING_DURATION_SECONDS.labels(worker_id=worker_id).observe(
                max(0.0, (datetime.now(timezone.utc) - started_at).total_seconds())
            )
            return True
        except Exception as exc:
            retry_count += 1
            if retry_count < worker_settings.PROCESSING_MAX_RETRIES:
                await self.mark_retry(
                    document_id,
                    worker_id=worker_id,
                    retry_count=retry_count,
                    kafka_partition=kafka_partition,
                    kafka_offset=kafka_offset,
                )
                WORKER_RETRY_TOTAL.labels(worker_id=worker_id).inc()
                logger.warning("Document %s failed; retry %s/%s worker_id=%s err=%s", document_id, retry_count, worker_settings.PROCESSING_MAX_RETRIES, worker_id, exc)
                raise

            await self.mark_failed(
                document_id,
                worker_id=worker_id,
                kafka_topic=kafka_topic,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                error=str(exc),
                retry_count=retry_count,
            )
            WORKER_DOCUMENTS_FAILED_TOTAL.labels(worker_id=worker_id).inc()
            logger.exception("Document %s exhausted retries and failed worker_id=%s", document_id, worker_id)
            raise


DocumentProcessor = MinimalProcessingHandler
