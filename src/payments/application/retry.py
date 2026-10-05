"""Retry budget: ``max_attempts`` total (initial + retries), exponential delay between them.

With defaults 3 / 1s / x2: attempt 1 now, attempt 2 after 1s, attempt 3 after 2s.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: timedelta = timedelta(seconds=1)
    multiplier: float = 2.0
    max_delay: timedelta = timedelta(seconds=60)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay <= timedelta(0) or self.max_delay < self.base_delay:
            raise ValueError("delays must satisfy 0 < base_delay <= max_delay")

    def is_exhausted(self, attempts_made: int) -> bool:
        return attempts_made >= self.max_attempts

    def delay_before(self, next_attempt: int, *, retry_after: timedelta | None = None) -> timedelta:
        """Delay before ``next_attempt`` (1-based). ``Retry-After`` is honoured within the cap."""
        if next_attempt < 2:
            raise ValueError("delay applies to attempts >= 2")
        backoff = self.base_delay * (self.multiplier ** (next_attempt - 2))
        if retry_after is not None and retry_after > backoff:
            backoff = retry_after
        return min(backoff, self.max_delay)
