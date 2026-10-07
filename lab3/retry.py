import asyncio
import random
from enum import Enum
from typing import Awaitable, Callable, Optional, TypeVar


T = TypeVar("T")


class BackoffStrategy(Enum):
    CONSTANT = "constant"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponentialJitter"


class Retry:
    def __init__(
        self,
        max_attempts: int,
        delay_ms: int,
        strategy: BackoffStrategy,
        retry_on: Optional[list[type[Exception]]] = None,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        if delay_ms < 0:
            raise ValueError("delay_ms cannot be negative")

        self.max_attempts = max_attempts
        self.delay_ms = delay_ms
        self.strategy = strategy
        self.retry_on = retry_on

    async def execute(
        self,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return await fn()

            except Exception as error:
                if not self._is_retryable(error):
                    raise

                if attempt >= self.max_attempts:
                    raise

                delay = self._get_delay(attempt)

                if delay > 0:
                    await asyncio.sleep(delay / 1000)

        raise RuntimeError("Retry failed")

    def _is_retryable(self, error: Exception) -> bool:
        if self.retry_on is None:
            return True

        return any(
            isinstance(error, exception_type)
            for exception_type in self.retry_on
        )

    def _get_delay(self, attempt: int) -> float:
        if self.strategy == BackoffStrategy.CONSTANT:
            return self.delay_ms

        if self.strategy == BackoffStrategy.EXPONENTIAL:
            return self.delay_ms * (2 ** (attempt - 1))

        if self.strategy == BackoffStrategy.EXPONENTIAL_JITTER:
            exponential_delay = (
                self.delay_ms * (2 ** (attempt - 1))
            )

            return random.uniform(0, exponential_delay)

        raise ValueError(
            f"Unknown strategy: {self.strategy}"
        )