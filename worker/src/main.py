"""
AegisIngest - Worker Entrypoint
Initializes Prometheus server and runs the batch consumer lifecycle.
"""

import asyncio
import logging
import signal
import sys

from worker.src.config import worker_settings
from worker.src.consumer import WorkerBatchConsumer
from worker.src.metrics import start_worker_metrics_server

logging.basicConfig(
    level=worker_settings.LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("aegis.worker.main")


async def main():
    logger.info("Starting AegisIngest Processing Worker node...")
    
    # Start Prometheus metrics server
    try:
        start_worker_metrics_server(worker_settings.PROMETHEUS_METRICS_PORT)
        logger.info("Worker Prometheus metrics server listening on port %d", worker_settings.PROMETHEUS_METRICS_PORT)
    except Exception as e:
        logger.warning("Could not bind Prometheus metrics server on port %d: %s", worker_settings.PROMETHEUS_METRICS_PORT, e)

    consumer = WorkerBatchConsumer()

    # Register OS signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))
        except (NotImplementedError, AttributeError):
            # Windows signal handling fallback
            pass

    try:
        await consumer.start()
    except Exception as e:
        logger.error("Fatal worker error: %s", e, exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Worker node terminated gracefully.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user.")
        sys.exit(0)
