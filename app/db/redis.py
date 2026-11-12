import redis.asyncio as aioredis
from core.config import settings
from core.logging import logger

_redis_client: aioredis.Redis | None = None

async def init_redis_pool() -> None:
    """Initializes the Redis connection pool."""
    global _redis_client
    if _redis_client is not None:
        return

    logger.info("Initializing Redis connection pool...")
    try:
        _redis_client = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20
        )
        # Test connection
        await _redis_client.ping()
        logger.info("Redis connection pool initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis at {settings.redis_url}: {e}. Running with cache disabled.")
        _redis_client = None

async def close_redis_pool() -> None:
    """Gracefully closes the Redis connection pool."""
    global _redis_client
    if _redis_client is not None:
        logger.info("Closing Redis connection pool...")
        await _redis_client.close()
        _redis_client = None
        logger.info("Redis connection pool closed.")

def get_redis() -> aioredis.Redis | None:
    """Returns the active Redis client or None if disconnected."""
    global _redis_client
    return _redis_client

async def check_redis_health() -> bool:
    """Pings Redis to verify connectivity for /healthz."""
    global _redis_client
    if _redis_client is None:
        return False
    try:
        return await _redis_client.ping()
    except Exception as e:
        logger.warning(f"Redis health check failed: {e}")
        return False

