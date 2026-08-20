"""
AegisIngest - Worker Kafka Batch Consumer
Reads audit report events from topic partitions with parallel concurrency and offset management.
"""

import json
import asyncio
import logging
import os
import socket
import time
from typing import Optional
import aiokafka

from worker.src.config import worker_settings
from worker.src.processor import DocumentProcessor
from worker.src.metrics import (
    WORKER_ACTIVE_JOBS,
    WORKER_CONSUMER_LAG_GAUGE,
    WORKER_DOCUMENTS_FAILED_TOTAL,
    WORKER_DOCUMENTS_PROCESSED_TOTAL,
    WORKER_HEARTBEAT,
    WORKER_PROCESSING_DURATION_SECONDS,
    WORKER_PROCESSING_RATE,
    WORKER_RETRY_TOTAL,
)
from worker.src.repository import repository

logger = logging.getLogger("aegis.consumer")


class WorkerBatchConsumer:
    def __init__(self):
        self.consumer: Optional[aiokafka.AIOKafkaConsumer] = None
        self.processor: Optional[DocumentProcessor] = None
        self.running = False
        self.worker_id = os.getenv("WORKER_ID") or worker_settings.WORKER_ID
        self.current_state = "IDLE"
        self.current_document_id = None
        self.processing_rate = 0.0
        self.failed_jobs = 0
        self.retry_count = 0
        self.processed_jobs = 0
        self.active_jobs = 0
        self.last_processing_started = None
        self.heartbeat_task: Optional[asyncio.Task[None]] = None

    async def start(self):
        self.running = True
        logger.info("Initializing worker consumer with worker_id=%s", self.worker_id)

        try:
            await repository.start()
            logger.info("Worker connected to PostgreSQL.")
        except Exception as e:
            logger.warning("Worker PostgreSQL connection failed: %s", e)

        if repository.pool:
            try:
                await repository.register_worker(
                    self.worker_id,
                    hostname=socket.gethostname(),
                    process_id=os.getpid(),
                    status="ACTIVE",
                    current_state="IDLE",
                    cpu_usage_pct=0.0,
                    memory_usage_pct=0.0,
                    processing_rate=0.0,
                )
                await repository.append_event(
                    event_type="WORKER_REGISTERED",
                    worker_id=self.worker_id,
                    severity="INFO",
                    message="Worker registered and ready for Kafka processing.",
                    metadata={"hostname": socket.gethostname(), "process_id": os.getpid()},
                )
            except Exception as exc:
                logger.warning("Worker registration failed: %s", exc)

        self.processor = DocumentProcessor(processing_delay=worker_settings.PROCESSING_DELAY_SECONDS)
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        retry_delay = 2.0
        while self.running:
            try:
                self.consumer = aiokafka.AIOKafkaConsumer(
                    worker_settings.KAFKA_TOPIC_INGEST,
                    bootstrap_servers=worker_settings.KAFKA_BOOTSTRAP_SERVERS,
                    group_id=worker_settings.KAFKA_CONSUMER_GROUP,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    max_poll_records=worker_settings.KAFKA_MAX_POLL_RECORDS,
                    fetch_min_bytes=worker_settings.KAFKA_FETCH_MIN_BYTES,
                    fetch_max_wait_ms=worker_settings.KAFKA_FETCH_MAX_WAIT_MS,
                )
                await self.consumer.start()
                logger.info("Kafka consumer started. Subscribed to topic: %s", worker_settings.KAFKA_TOPIC_INGEST)
                break
            except Exception as e:
                logger.warning("Kafka broker not ready (%s). Retrying in %.1fs...", e, retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(10.0, retry_delay * 1.5)

        await self._consume_loop()

    async def _heartbeat_loop(self):
        while self.running:
            try:
                cpu, mem = (0.0, 0.0)
                if repository.pool:
                    await repository.heartbeat_worker(
                        self.worker_id,
                        status="ACTIVE",
                        current_state=self.current_state,
                        current_document_id=self.current_document_id,
                        cpu_usage_pct=cpu,
                        memory_usage_pct=mem,
                        processing_rate=self.processing_rate,
                        failed_jobs=self.failed_jobs,
                        retry_count=self.retry_count,
                    )
                WORKER_HEARTBEAT.labels(worker_id=self.worker_id).set(time.time())
                WORKER_PROCESSING_RATE.labels(worker_id=self.worker_id).set(self.processing_rate)
                WORKER_ACTIVE_JOBS.labels(worker_id=self.worker_id).set(self.active_jobs)
            except Exception as exc:
                logger.warning("Worker heartbeat failed: %s", exc)
            await asyncio.sleep(worker_settings.HEARTBEAT_INTERVAL_SECONDS)

    async def _consume_loop(self):
        semaphore = asyncio.Semaphore(worker_settings.WORKER_CONCURRENCY)

        while self.running:
            try:
                # Poll message batches
                msg_batch = await self.consumer.getmany(
                    timeout_ms=500,
                    max_records=worker_settings.KAFKA_MAX_POLL_RECORDS
                )

                if not msg_batch:
                    await asyncio.sleep(0.05)
                    continue

                total_messages = sum(len(msgs) for msgs in msg_batch.values())
                logger.debug("Received batch with %d messages across %d partitions", total_messages, len(msg_batch))

                tasks = []
                for tp, messages in msg_batch.items():
                    for msg in messages:
                        async def handle_record(record, topic=tp.topic, partition=tp.partition):
                            async with semaphore:
                                self.current_state = "PROCESSING"
                                self.active_jobs += 1
                                payload = None
                                started_at = time.monotonic()
                                self.last_processing_started = started_at
                                try:
                                    payload = json.loads(record.value.decode("utf-8"))
                                    payload.setdefault("kafka_topic", topic)
                                    payload.setdefault("kafka_partition", partition)
                                    payload.setdefault("kafka_offset", record.offset)
                                    document_id = payload.get("document_id") or payload.get("job_id")
                                    self.current_document_id = document_id
                                    if repository.pool:
                                        await repository.append_event(
                                            document_id=document_id,
                                            agency_id=payload.get("agency_id"),
                                            sequence_number=payload.get("sequence_number"),
                                            event_type="KAFKA_CONSUMED",
                                            worker_id=self.worker_id,
                                            kafka_partition=partition,
                                            kafka_offset=record.offset,
                                            severity="INFO",
                                            message="Kafka message consumed and queued for processing.",
                                            metadata={"topic": topic, "partition": partition, "offset": record.offset},
                                        )
                                    succeeded = await self.processor.process_event(payload, worker_id=self.worker_id)
                                    if not succeeded:
                                        raise RuntimeError("processing did not persist successfully")
                                    self.processed_jobs += 1
                                    self.processing_rate = max(0.0, self.processed_jobs / max(1.0, time.monotonic() - started_at))
                                except Exception as err:
                                    self.failed_jobs += 1
                                    logger.error("Error processing record offset %s: %s", record.offset, err)
                                    if repository.pool and payload and (payload.get("document_id") or payload.get("job_id")):
                                        await repository.append_event(
                                            document_id=payload.get("document_id") or payload.get("job_id"),
                                            agency_id=payload.get("agency_id"),
                                            sequence_number=payload.get("sequence_number"),
                                            event_type="PROCESSING_FAILED",
                                            worker_id=self.worker_id,
                                            kafka_partition=partition,
                                            kafka_offset=record.offset,
                                            severity="ERROR",
                                            message=str(err),
                                        )
                                finally:
                                    elapsed = time.monotonic() - started_at
                                    WORKER_PROCESSING_DURATION_SECONDS.labels(worker_id=self.worker_id).observe(elapsed)
                                    self.active_jobs = max(0, self.active_jobs - 1)
                                    self.current_state = "IDLE"
                                    self.current_document_id = None

                        tasks.append(handle_record(msg))

                if tasks:
                    await asyncio.gather(*tasks)

                try:
                    end_offsets = await self.consumer.end_offsets(list(self.consumer.assignment()))
                    for tp in self.consumer.assignment():
                        end_offset = end_offsets.get(tp, 0)
                        current_position = self.consumer.position(tp)
                        lag = max(0, end_offset - current_position)
                        WORKER_CONSUMER_LAG_GAUGE.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
                except Exception as lag_err:
                    logger.debug("Consumer lag calculation skipped: %s", lag_err)

                await self.consumer.commit()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Consumer loop encountered error: %s", e)
                await asyncio.sleep(1.0)

    async def stop(self):
        self.running = False
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped.")
        if repository.pool:
            await repository.heartbeat_worker(
                self.worker_id,
                status="DRAINING",
                current_state="IDLE",
                current_document_id=None,
                cpu_usage_pct=0.0,
                memory_usage_pct=0.0,
                processing_rate=0.0,
                failed_jobs=self.failed_jobs,
                retry_count=self.retry_count,
            )
            await repository.append_event(
                event_type="WORKER_STOPPED",
                worker_id=self.worker_id,
                severity="INFO",
                message="Worker shutdown initiated.",
                metadata={"hostname": socket.gethostname(), "process_id": os.getpid()},
            )
        await repository.stop()
