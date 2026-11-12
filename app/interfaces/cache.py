from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class CacheServiceInterface(ABC):
    """Abstract interface defining document and shipment caching operations."""

    @abstractmethod
    async def get_shipment_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieves a cached shipment result dictionary by composite cache key."""
        pass

    @abstractmethod
    async def set_shipment_cache(
        self,
        cache_key: str,
        data: Dict[str, Any],
        ttl_days: Optional[int] = None
    ) -> None:
        """Stores a verified shipment compliance result in cache."""
        pass

    @abstractmethod
    async def get_document_cache(self, doc_hash: str) -> Optional[Dict[str, Any]]:
        """Retrieves cached extraction data for an individual document by its file hash."""
        pass

    @abstractmethod
    async def set_document_cache(
        self,
        doc_hash: str,
        extracted_data: Dict[str, Any],
        ttl_days: Optional[int] = None
    ) -> None:
        """Stores extracted fields for an individual document in cache."""
        pass

