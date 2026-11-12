import json
from typing import Optional, Dict, Any
from core.config import settings
from core.logging import logger
from core.metrics import CACHE_HITS_TOTAL, CACHE_MISSES_TOTAL
from db.redis import get_redis
from interfaces.cache import CacheServiceInterface

class RedisCacheService(CacheServiceInterface):
    """Redis implementation of the CacheServiceInterface."""

    def __init__(self):
        self.default_ttl = settings.cache_ttl_days * 86400  # Convert days to seconds

    async def get_shipment_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieves cached shipment compliance outcome from Redis."""
        redis = get_redis()
        if not redis:
            CACHE_MISSES_TOTAL.inc()
            return None

        try:
            val = await redis.get(cache_key)
            if val:
                CACHE_HITS_TOTAL.inc()
                logger.info(f"Redis Cache HIT for key: {cache_key}")
                return json.loads(val)
            
            CACHE_MISSES_TOTAL.inc()
            return None
        except Exception as e:
            logger.warning(f"Error reading from Redis cache: {e}")
            CACHE_MISSES_TOTAL.inc()
            return None

    async def set_shipment_cache(
        self,
        cache_key: str,
        data: Dict[str, Any],
        ttl_days: Optional[int] = None
    ) -> None:
        """Stores a shipment compliance outcome in Redis."""
        redis = get_redis()
        if not redis:
            return

        ttl = (ttl_days * 86400) if ttl_days else self.default_ttl
        try:
            serialized = json.dumps(data)
            await redis.set(cache_key, serialized, ex=ttl)
            logger.info(f"Stored shipment cache in Redis for key: {cache_key} (TTL: {ttl}s)")
        except Exception as e:
            logger.warning(f"Error writing shipment cache to Redis: {e}")

    async def get_document_cache(self, doc_hash: str) -> Optional[Dict[str, Any]]:
        """Retrieves cached extraction fields for an individual document."""
        redis = get_redis()
        if not redis:
            return None

        key = f"cache:doc:{doc_hash.strip().lower()}"
        try:
            val = await redis.get(key)
            if val:
                logger.info(f"Redis Document Cache HIT for doc hash: {doc_hash}")
                return json.loads(val)
            return None
        except Exception as e:
            logger.warning(f"Error reading doc cache from Redis: {e}")
            return None

    async def set_document_cache(
        self,
        doc_hash: str,
        extracted_data: Dict[str, Any],
        ttl_days: Optional[int] = None
    ) -> None:
        """Stores extraction fields for an individual document."""
        redis = get_redis()
        if not redis:
            return

        key = f"cache:doc:{doc_hash.strip().lower()}"
        ttl = (ttl_days * 86400) if ttl_days else self.default_ttl
        try:
            serialized = json.dumps(extracted_data)
            await redis.set(key, serialized, ex=ttl)
            logger.info(f"Stored doc cache in Redis for doc hash: {doc_hash}")
        except Exception as e:
            logger.warning(f"Error writing doc cache to Redis: {e}")

cache_service: CacheServiceInterface = RedisCacheService()

