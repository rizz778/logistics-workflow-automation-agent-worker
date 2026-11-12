from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

class PipelineRunRepository(ABC):
    """Abstract interface defining all database operations for pipeline runs."""

    @abstractmethod
    async def create_queued_run(
        self,
        filenames: List[str],
        source: str = "manual_upload",
        shipment_id: Optional[str] = None,
        email_sender: Optional[str] = None,
        email_metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Inserts an initial shipment record with 'queued' status and returns its generated ID."""
        pass

    @abstractmethod
    async def create_cached_run(
        self,
        cached_data: Dict[str, Any],
        filenames: List[str],
        source: str = "manual_upload",
        shipment_id: Optional[str] = None,
        email_sender: Optional[str] = None,
        email_metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Clones a cached pipeline result into a new run record with zero worker latency."""
        pass

    @abstractmethod
    async def get_run_by_id(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a single shipment run by its database ID."""
        pass

    @abstractmethod
    async def get_all_runs(
        self,
        limit: int = 50,
        offset: int = 0,
        status: Optional[str] = None,
        source: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retrieves paginated runs with total count for the Run Registry."""
        pass

    @abstractmethod
    async def get_incoming_runs(self) -> List[Dict[str, Any]]:
        """Retrieves all unprocessed inbox runs."""
        pass

    @abstractmethod
    async def get_processed_runs(self) -> List[Dict[str, Any]]:
        """Retrieves all processed/resolved inbox runs."""
        pass

    @abstractmethod
    async def update_run_status(
        self,
        run_id: int,
        status: str,
        edited_data: Optional[Dict[str, Any]] = None,
        amendment_draft: Optional[str] = None
    ) -> bool:
        """Updates the status and human operator edits for a run."""
        pass

    @abstractmethod
    async def get_analytics(self) -> Dict[str, Any]:
        """Calculates aggregated metrics for dashboard status cards."""
        pass

