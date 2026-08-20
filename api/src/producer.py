"""
AegisIngest - Event Producer & Partition Partitioner
Implements deterministic sharding contract: Partition = Hash(agency_id) % P.
High-throughput batch buffering configuration.
"""

import json
import logging
from typing import Dict, Any, Optional
import aiokafka
from api.src.config import settings

logger = logging.getLogger("aegis.producer")

try:
    import mmh3

    def hash_key_to_partition(key: str, num_partitions: int = settings.KAFKA_NUM_PARTITIONS) -> int:
        """MurmurHash3 deterministic 32-bit partition hashing."""
        return abs(mmh3.hash(key)) % num_partitions
except ImportError:
    import zlib

    def hash_key_to_partition(key: str, num_partitions: int = settings.KAFKA_NUM_PARTITIONS) -> int:
        """CRC32 fallback deterministic partition hashing."""
        return (zlib.crc32(key.encode("utf-8")) & 0xFFFFFFFF) % num_partitions


class EventProducer:
    def __init__(self):
        self.producer: Optional[aiokafka.AIOKafkaProducer] = None
        self._connected = False

    async def start(self):
        try:
            self.producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                acks="all",
                linger_ms=5,
                request_timeout_ms=5000,
            )
            await self.producer.start()
            self._connected = True
            logger.info("Kafka producer successfully connected to %s", settings.KAFKA_BOOTSTRAP_SERVERS)
        except Exception as e:
            logger.warning("Kafka producer failed to connect to %s: %s (operating in offline/mock mode)", settings.KAFKA_BOOTSTRAP_SERVERS, e)
            self._connected = False

    async def stop(self):
        if self.producer and self._connected:
            await self.producer.stop()
            self._connected = False

    async def send_audit_event(
        self,
        agency_id: str,
        event_payload: Dict[str, Any],
        topic: str = settings.KAFKA_TOPIC_INGEST,
    ) -> Dict[str, Any]:
        """
        Publishes audit event with explicit deterministic partition mapping using high-speed batch buffer.
        """
        partition = hash_key_to_partition(agency_id, settings.KAFKA_NUM_PARTITIONS)
        event_payload["sharding"] = {
            "routing_key": agency_id,
            "partition": partition,
            "total_partitions": settings.KAFKA_NUM_PARTITIONS,
        }

        serialized = json.dumps(event_payload).encode("utf-8")
        key_bytes = agency_id.encode("utf-8")

        if self._connected and self.producer:
            metadata = await self.producer.send(
                topic=topic,
                value=serialized,
                key=key_bytes,
                partition=partition,
            )
            return {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
                "status": "BUFFERED",
            }
        else:
            return {
                "topic": topic,
                "partition": partition,
                "offset": -1,
                "status": "MOCK_PRODUCED",
            }


producer = EventProducer()
