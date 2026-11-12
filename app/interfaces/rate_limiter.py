from abc import ABC, abstractmethod
from typing import Tuple

class RateLimiterInterface(ABC):
    """Abstract interface defining distributed sliding-window rate limiting."""

    @abstractmethod
    async def is_allowed(
        self,
        key: str,
        max_requests: int,
        window_seconds: int = 60
    ) -> Tuple[bool, int, int]:
        """Checks if a request key is within its allowed rate limit.
        Returns a tuple:
            (is_allowed: bool, remaining_requests: int, retry_after_seconds: int)
        """
        pass

