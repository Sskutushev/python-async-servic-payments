"""Simulated payment gateway: 2-5 s latency, ~90 % success.

The outcome is a deterministic function of ``(seed, payment_id)`` so that a
redelivered message for the same payment cannot flip a decline into a success.
Sleep and the delay source are injectable: tests never wait for real time.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import random
from collections.abc import Awaitable, Callable

from payments.domain.payment import GatewayOutcome, Payment

SleepFn = Callable[[float], Awaitable[None]]


class SimulatedGateway:
    def __init__(
        self,
        *,
        seed: str,
        success_rate: float = 0.9,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        if not 0 <= success_rate <= 1:
            raise ValueError("success_rate must be within [0, 1]")
        if min_delay < 0 or max_delay < min_delay:
            raise ValueError("delays must satisfy 0 <= min_delay <= max_delay")
        self._seed = seed.encode()
        self._success_rate = success_rate
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._sleep = sleep

    def _roll(self, payment: Payment) -> float:
        digest = hmac.new(self._seed, payment.id.bytes, hashlib.sha256).digest()
        return int.from_bytes(digest[:8], "big") / 2**64  # uniform in [0, 1)

    async def charge(self, payment: Payment) -> GatewayOutcome:
        delay = random.Random(payment.id.int).uniform(self._min_delay, self._max_delay)
        await self._sleep(delay)
        reference = f"sim-{hashlib.sha256(payment.id.bytes).hexdigest()[:16]}"
        if self._roll(payment) < self._success_rate:
            return GatewayOutcome(succeeded=True, reference=reference)
        return GatewayOutcome(succeeded=False, reference=reference, failure_code="declined")
