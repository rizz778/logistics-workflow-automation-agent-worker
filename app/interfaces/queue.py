from abc import ABC, abstractmethod
from schemas.queue_schemas import ShipmentTaskEvent

class QueueProducerInterface(ABC):
    """Abstract interface defining message queue producer operations."""

    @abstractmethod
    async def publish_shipment_task(self, task: ShipmentTaskEvent) -> str:
        """Pushes a shipment job to the active processing queue.
        Returns the unique queue message ID.
        """
        pass

    @abstractmethod
    async def publish_dlq_task(self, task: ShipmentTaskEvent, failure_reason: str) -> str:
        """Pushes a permanently failed shipment task to the Dead Letter Queue (DLQ).
        Returns the DLQ message ID.
        """
        pass

    @abstractmethod
    async def get_queue_depth(self) -> int:
        """Returns the number of pending tasks currently in the queue."""
        pass

