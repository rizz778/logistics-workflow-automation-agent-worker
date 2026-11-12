import time
from typing import Tuple, Callable
from fastapi import Request, HTTPException, status, Response
from core.config import settings
from core.logging import logger
from db.redis import get_redis
from interfaces.rate_limiter import RateLimiterInterface

# Atomic Redis Lua script for distributed sliding window rate limiting
SLIDING_WINDOW_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local clear_before = now - window

-- 1. Remove timestamps older than rolling window
redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)

-- 2. Count active requests within window
local current_count = redis.call('ZCARD', key)

if current_count < limit then
    -- Record this request
    redis.call('ZADD', key, now, now .. ':' .. redis.call('INCR', key .. ':seq'))
    redis.call('EXPIRE', key, window)
    return {1, limit - current_count - 1, 0}
else
    -- Compute retry-after based on oldest entry
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_after = 1
    if oldest and oldest[2] then
        retry_after = math.ceil(window - (now - tonumber(oldest[2])))
        if retry_after < 1 then retry_after = 1 end
    end
    return {0, 0, retry_after}
end
"""

class RedisRateLimiter(RateLimiterInterface):
    """Distributed sliding-window rate limiter using Redis."""

    def __init__(self):
        self._script = None

    async def is_allowed(
        self,
        key: str,
        max_requests: int,
        window_seconds: int = 60
    ) -> Tuple[bool, int, int]:
        """Atomically checks if the request is within rate limits via Redis Lua script.
        Returns: (is_allowed, remaining_requests, retry_after_seconds)
        """
        if not settings.rate_limit_enabled:
            return True, max_requests, 0

        redis = get_redis()
        if not redis:
            # Fail-open if Redis is down so users are not blocked
            logger.warning("Redis is disconnected. Rate limiter failing open.")
            return True, max_requests, 0

        now = time.time()
        redis_key = f"ratelimit:{key}"

        try:
            # Register or execute Lua script
            result = await redis.eval(
                SLIDING_WINDOW_LUA,
                1,
                redis_key,
                str(now),
                str(window_seconds),
                str(max_requests)
            )

            is_allowed = bool(result[0] == 1)
            remaining = int(result[1])
            retry_after = int(result[2])

            return is_allowed, remaining, retry_after
        except Exception as e:
            logger.error(f"Error evaluating rate limit in Redis: {e}")
            return True, max_requests, 0

rate_limiter: RateLimiterInterface = RedisRateLimiter()

def rate_limit_dependency(key_prefix: str, max_requests: int, window_seconds: int = 60) -> Callable:
    """FastAPI dependency factory enforcing rate limits on specific endpoints."""

    async def _rate_limit(request: Request, response: Response):
        if not settings.rate_limit_enabled:
            return

        # Extract client IP (respecting reverse proxies like Nginx / Cloudflare)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip()
        elif request.client:
            client_ip = request.client.host
        else:
            client_ip = "unknown_client"

        key = f"{key_prefix}:{client_ip}"
        allowed, remaining, retry_after = await rate_limiter.is_allowed(
            key=key,
            max_requests=max_requests,
            window_seconds=window_seconds
        )

        # Inject standard rate limit headers
        response.headers["X-RateLimit-Limit"] = str(max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)

        if not allowed:
            response.headers["Retry-After"] = str(retry_after)
            logger.warning(f"Rate limit exceeded for {key}. Limit: {max_requests}/{window_seconds}s. Retry after: {retry_after}s")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Maximum {max_requests} requests per {window_seconds} seconds. Please retry in {retry_after} seconds.",
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(max_requests),
                    "X-RateLimit-Remaining": "0"
                }
            )

    return _rate_limit

