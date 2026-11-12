from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

class PipelineRunRepository(ABC):
    """Abstract interface defining all database operations for pipeline runs and multi-file tracking."""

    @abstractmethod
    async def create_shipment_with_files(
        self,
        shipment_id: str,
        files: List[Dict[str, Any]],
        source: str = "manual_upload",
        email_sender: Optional[str] = None,
        email_metadata: Optional[Dict[str, Any]] = None,
        initial_status: str = "pending_upload"
    ) -> int:
        """Atomically inserts a shipment run and its expected files into shipment_files."""
        pass

    @abstractmethod
    async def mark_file_uploaded_and_check_completion(
        self,
        shipment_id: str,
        s3_key: str
    ) -> bool:
        """Atomically marks an S3 file as uploaded and transitions shipment to 'processing' if all files are ready.
        Returns True if this exact call completed the shipment.
        """
        pass

    @abstractmethod
    async def get_shipment_files(self, shipment_id: str) -> List[Dict[str, Any]]:
        """Retrieves all tracked files for a shipment."""
        pass

    @abstractmethod
    async def get_run_by_shipment_id(self, shipment_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a shipment run by its unique shipment_id string."""
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
