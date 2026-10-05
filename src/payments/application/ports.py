"""The interfaces the use cases need from the outside world.

Each one has a real implementation (PostgreSQL, RabbitMQ, HTTP) and an in-memory one
for tests. That is the only reason these are interfaces and not concrete classes.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from types import TracebackType
from typing import Protocol, Self

from payments.domain.events import OutboxEvent
from payments.domain.payment import GatewayOutcome, Payment


class Clock(Protocol):
    def now(self) -> datetime: ...


# ------------------------------------------------------------------ gateway
class GatewayTransportError(Exception):
    """We could not get an answer from the gateway, so we do not know if the charge happened."""

    def __init__(self, code: str = "gateway_unavailable") -> None:
        super().__init__(code)
        self.code = code


class PaymentGateway(Protocol):
    """The external payment provider.

    Rules for any real implementation (the simulator satisfies them trivially):

    * ``payment.id`` must be sent to the provider as its idempotency key, so a repeated
      ``charge`` for the same payment can never create a second charge on their side;
    * "no usable answer" (timeout, 5xx, connection lost) must become
      ``GatewayTransportError``, never a decline: the money may have moved;
    * before the retry budget is reused for a real provider, add a status lookup by
      ``payment.id`` and call it instead of charging again. Without it, the service can
      only promise "no double charge" for providers that honour the idempotency key.
    """

    async def charge(self, payment: Payment) -> GatewayOutcome:
        """Charge the payment. Returns paid/declined, or raises ``GatewayTransportError``."""
        ...


# ------------------------------------------------------------------ webhook
class WebhookDeliveryError(Exception):
    def __init__(self, code: str, *, retryable: bool, retry_after: timedelta | None = None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after = retry_after


class WebhookSender(Protocol):
    async def deliver(self, url: str, event_id: uuid.UUID, body: dict[str, object]) -> None:
        """Send the webhook. Raises ``WebhookDeliveryError`` with ``retryable`` set accordingly."""
        ...


# -------------------------------------------------------------- persistence
class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> bool:
        """Insert the payment. Returns False (no exception) if the idempotency key is taken."""
        ...

    async def get(self, payment_id: uuid.UUID) -> Payment | None: ...

    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        """Read and lock the row until the transaction ends, so nobody else can change it."""
        ...

    async def save(self, payment: Payment) -> None: ...

    async def find_stalled(self, *, older_than: datetime, limit: int) -> list[Payment]:
        """Payments that look stuck: work is overdue and no outbox event is waiting for them."""
        ...


class OutboxRepository(Protocol):
    async def add(self, event: OutboxEvent) -> None: ...

    async def claim_due(
        self, *, now: datetime, lease: timedelta, token: uuid.UUID, limit: int
    ) -> list[OutboxEvent]:
        """Reserve events that are due and not yet published, for one relay only."""
        ...

    async def mark_published(self, event_id: uuid.UUID, *, token: uuid.UUID, now: datetime) -> bool:
        """Mark as published, but only if we still hold the lease. Returns False otherwise."""
        ...

    async def record_publish_failure(
        self, event_id: uuid.UUID, *, token: uuid.UUID, error: str, retry_at: datetime
    ) -> None: ...


class UnitOfWork(Protocol):
    @property
    def payments(self) -> PaymentRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]


# ---------------------------------------------------------------- publisher
class PublishError(Exception):
    """RabbitMQ did not confirm the message (no queue for it, connection lost, timeout)."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None:
        """Publish and wait for the broker's confirmation. Raises ``PublishError`` otherwise."""
        ...
