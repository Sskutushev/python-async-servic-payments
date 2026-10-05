"""Ports: the small interfaces the use cases depend on.

Each has at least two implementations (PostgreSQL/RabbitMQ/HTTP in production,
in-memory fakes in tests), which is the only reason a ``Protocol`` exists here.
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
    """The gateway could not be reached or gave no usable answer. Outcome is unknown."""

    def __init__(self, code: str = "gateway_unavailable") -> None:
        super().__init__(code)
        self.code = code


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> GatewayOutcome:
        """Return a business outcome or raise :class:`GatewayTransportError`."""
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
        """Deliver ``body`` (frozen, canonical JSON) or raise :class:`WebhookDeliveryError`."""
        ...


# -------------------------------------------------------------- persistence
class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> bool:
        """Insert; return False when ``idempotency_key`` already exists (no exception)."""
        ...

    async def get(self, payment_id: uuid.UUID) -> Payment | None: ...

    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        """Row-locked read for a read-modify-write inside the current transaction."""
        ...

    async def save(self, payment: Payment) -> None: ...

    async def find_stalled(self, *, older_than: datetime, limit: int) -> list[Payment]:
        """Payments whose work is overdue and which have no unpublished outbox event."""
        ...


class OutboxRepository(Protocol):
    async def add(self, event: OutboxEvent) -> None: ...

    async def claim_due(
        self, *, now: datetime, lease: timedelta, token: uuid.UUID, limit: int
    ) -> list[OutboxEvent]:
        """Lease due, unpublished events (``FOR UPDATE SKIP LOCKED`` in PostgreSQL)."""
        ...

    async def mark_published(self, event_id: uuid.UUID, *, token: uuid.UUID, now: datetime) -> bool:
        """Compare-and-set on the lease token; False when the lease was lost."""
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
    """The broker did not confirm the message (unroutable, connection lost, timeout)."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None:
        """Publish with confirms + mandatory routing; raise :class:`PublishError` otherwise."""
        ...
