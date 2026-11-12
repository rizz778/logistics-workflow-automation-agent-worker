import time
from core.config import settings
from core.logging import logger
from core.metrics import QUEUE_PUSH_DURATION
from core.telemetry import inject_trace_context
from db.redis import get_redis
from interfaces.queue import QueueProducerInterface
from schemas.queue_schemas import ShipmentTaskEvent

class RedisQueueProducer(QueueProducerInterface):
    """Redis Streams producer implementation for dispatching shipment tasks to Agent Workers."""

    def __init__(self):
        self.queue_name = settings.redis_queue_name
        self.dlq_name = f"{settings.redis_queue_name}_dlq"

    async def publish_shipment_task(self, task: ShipmentTaskEvent) -> str:
        """Publishes a task to the Redis Stream queue.
        Injects OpenTelemetry traceparent so the worker continues the distributed trace.
        """
        redis = get_redis()
        if not redis:
            raise RuntimeError("Redis connection is unavailable. Cannot publish task to queue.")

        start_time = time.perf_counter()

        # Inject OpenTelemetry distributed trace context into the task
        carrier = {}
        inject_trace_context(carrier)
        if "traceparent" in carrier and not task.traceparent:
            task.traceparent = carrier["traceparent"]

        payload_json = task.model_dump_json()

        try:
            # XADD trade_doc_tasks * payload <json>
            msg_id = await redis.xadd(
                self.queue_name,
                {"payload": payload_json}
            )
            duration = time.perf_counter() - start_time
            QUEUE_PUSH_DURATION.observe(duration)

            logger.info(
                f"Published task {task.shipment_id} (Type: {task.event_type}, Retries: {task.retry_count}) "
                f"to stream '{self.queue_name}'. Message ID: {msg_id}"
            )
            return msg_id
        except Exception as e:
            logger.error(f"Failed to publish task {task.shipment_id} to Redis stream: {e}")
            raise

    async def publish_dlq_task(self, task: ShipmentTaskEvent, failure_reason: str) -> str:
        """Pushes a permanently failed task to the Dead Letter Queue (DLQ) after 3 retries."""
        redis = get_redis()
        if not redis:
            raise RuntimeError("Redis is unavailable for DLQ publishing.")

        payload_json = task.model_dump_json()

        try:
            msg_id = await redis.xadd(
                self.dlq_name,
                {
                    "payload": payload_json,
                    "failure_reason": failure_reason
                }
            )
            logger.warning(
                f"🚨 ROUTED TO DLQ: Shipment {task.shipment_id} reached max retries ({task.retry_count}). "
                f"Reason: {failure_reason}. DLQ Message ID: {msg_id}"
            )
            return msg_id
        except Exception as e:
            logger.error(f"Failed to push task {task.shipment_id} to DLQ: {e}")
            raise

    async def get_queue_depth(self) -> int:
        """Returns the count of messages currently in the Redis Stream queue."""
        redis = get_redis()
        if not redis:
            return 0
        try:
            return await redis.xlen(self.queue_name)
        except Exception as e:
            logger.warning(f"Error checking queue depth: {e}")
            return 0

queue_service: QueueProducerInterface = RedisQueueProducer()
