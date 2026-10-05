"""In-memory versions of the interfaces, for fast tests without a database or broker.

They behave like transactions: changes are kept aside until ``commit()`` and dropped
otherwise, so crash scenarios can be simulated precisely.
"""

from __future__ import annotations

import copy
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import TracebackType
from typing import Self

from payments.application.ports import (
    GatewayTransportError,
    PublishError,
    WebhookDeliveryError,
)
from payments.domain.events import OutboxEvent
from payments.domain.money import Currency, Money
from payments.domain.payment import GatewayOutcome, NotificationStatus, Payment, PaymentStatus

T0 = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self, start: datetime = T0) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta


def make_payment(
    *,
    payment_id: uuid.UUID | None = None,
    amount: str = "100.00",
    currency: Currency = Currency.USD,
    idempotency_key: str | None = None,
    webhook_url: str = "https://merchant.example/hooks",
    created_at: datetime = T0,
) -> Payment:
    pid = payment_id or uuid.uuid4()
    return Payment(
        id=pid,
        money=Money(Decimal(amount), currency),
        description="order",
        metadata={"order_id": "42"},
        idempotency_key=idempotency_key or f"key-{pid}",
        request_fingerprint="f" * 64,
        webhook_url=webhook_url,
        created_at=created_at,
    )


# ---------------------------------------------------------------- storage


@dataclass
class InMemoryStore:
    payments: dict[uuid.UUID, Payment] = field(default_factory=dict)
    outbox: dict[uuid.UUID, OutboxEvent] = field(default_factory=dict)
    commits: int = 0
    # Crash simulation: called right before a commit is applied; raise to "kill" the process.
    before_commit: Callable[[int], None] | None = None

    def unpublished(self) -> list[OutboxEvent]:
        return sorted(
            (e for e in self.outbox.values() if e.published_at is None),
            key=lambda e: (e.available_at, e.created_at),
        )


class InMemoryPaymentRepository:
    def __init__(self, store: InMemoryStore, staged: dict[uuid.UUID, Payment]) -> None:
        self._store = store
        self._staged = staged

    def _visible(self) -> dict[uuid.UUID, Payment]:
        return {**self._store.payments, **self._staged}

    async def add(self, payment: Payment) -> bool:
        if any(p.idempotency_key == payment.idempotency_key for p in self._visible().values()):
            return False
        self._staged[payment.id] = copy.deepcopy(payment)
        return True

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        found = self._visible().get(payment_id)
        return copy.deepcopy(found) if found else None

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        for p in self._visible().values():
            if p.idempotency_key == key:
                return copy.deepcopy(p)
        return None

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.get(payment_id)

    async def save(self, payment: Payment) -> None:
        self._staged[payment.id] = copy.deepcopy(payment)

    async def find_stalled(self, *, older_than: datetime, limit: int) -> list[Payment]:
        pending_ids = {
            e.aggregate_id for e in self._store.outbox.values() if e.published_at is None
        }
        result = []
        for p in sorted(self._visible().values(), key=lambda p: p.created_at):
            if p.id in pending_ids:
                continue
            lease_overdue = (
                p.processing_lease_until is None or p.processing_lease_until < older_than
            )
            if (
                p.status is PaymentStatus.PENDING
                and not p.is_processing_halted
                and p.created_at < older_than
                and lease_overdue
            ) or (
                p.notification_status is NotificationStatus.PENDING
                and p.notification_next_attempt_at is not None
                and p.notification_next_attempt_at < older_than
            ):
                result.append(copy.deepcopy(p))
        return result[:limit]


class InMemoryOutboxRepository:
    def __init__(self, store: InMemoryStore, staged: dict[uuid.UUID, OutboxEvent]) -> None:
        self._store = store
        self._staged = staged

    def _visible(self) -> dict[uuid.UUID, OutboxEvent]:
        return {**self._store.outbox, **self._staged}

    async def add(self, event: OutboxEvent) -> None:
        if any(e.dedup_key == event.dedup_key for e in self._visible().values()):
            raise RuntimeError(f"duplicate dedup_key {event.dedup_key}")
        self._staged[event.id] = copy.deepcopy(event)

    async def claim_due(
        self, *, now: datetime, lease: timedelta, token: uuid.UUID, limit: int
    ) -> list[OutboxEvent]:
        due = [
            e
            for e in self._visible().values()
            if e.published_at is None
            and e.available_at <= now
            and (e.lease_until is None or e.lease_until < now)
        ]
        due.sort(key=lambda e: (e.available_at, e.created_at))
        claimed = []
        for original in due[:limit]:
            leased = copy.deepcopy(original)
            leased.lease_token, leased.lease_until = token, now + lease
            self._staged[leased.id] = leased
            claimed.append(copy.deepcopy(leased))
        return claimed

    async def mark_published(self, event_id: uuid.UUID, *, token: uuid.UUID, now: datetime) -> bool:
        e = self._visible().get(event_id)
        if e is None or e.lease_token != token or e.published_at is not None:
            return False
        e = copy.deepcopy(e)
        e.published_at, e.lease_token, e.lease_until = now, None, None
        self._staged[e.id] = e
        return True

    async def record_publish_failure(
        self, event_id: uuid.UUID, *, token: uuid.UUID, error: str, retry_at: datetime
    ) -> None:
        e = self._visible().get(event_id)
        if e is None or e.lease_token != token:
            return
        e = copy.deepcopy(e)
        e.publication_attempts += 1
        e.last_error, e.available_at, e.lease_token, e.lease_until = error, retry_at, None, None
        self._staged[e.id] = e


class InMemoryUnitOfWork:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store
        self._staged_payments: dict[uuid.UUID, Payment] = {}
        self._staged_outbox: dict[uuid.UUID, OutboxEvent] = {}
        self.payments = InMemoryPaymentRepository(store, self._staged_payments)
        self.outbox = InMemoryOutboxRepository(store, self._staged_outbox)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._staged_payments.clear()
        self._staged_outbox.clear()

    async def commit(self) -> None:
        if self._store.before_commit is not None:
            self._store.before_commit(self._store.commits)
        self._store.payments.update(self._staged_payments)
        self._store.outbox.update(self._staged_outbox)
        self._staged_payments.clear()
        self._staged_outbox.clear()
        self._store.commits += 1


# ---------------------------------------------------------------- adapters


class FakeGateway:
    """Returns the answers it was given, in order, and remembers every charge it was asked for."""

    def __init__(self, *outcomes: GatewayOutcome | GatewayTransportError) -> None:
        self._script = list(outcomes)
        self.charges: list[uuid.UUID] = []

    async def charge(self, payment: Payment) -> GatewayOutcome:
        self.charges.append(payment.id)
        if not self._script:
            return GatewayOutcome(succeeded=True, reference="ref")
        result = self._script.pop(0)
        if isinstance(result, GatewayTransportError):
            raise result
        return result


class FakeWebhookSender:
    def __init__(self, *failures: WebhookDeliveryError | None) -> None:
        self._script = list(failures)
        self.deliveries: list[tuple[str, uuid.UUID, dict[str, object]]] = []

    async def deliver(self, url: str, event_id: uuid.UUID, body: dict[str, object]) -> None:
        self.deliveries.append((url, event_id, body))
        if self._script:
            failure = self._script.pop(0)
            if failure is not None:
                raise failure


class FakePublisher:
    def __init__(self, *failures: PublishError | None) -> None:
        self._script = list(failures)
        self.published: list[OutboxEvent] = []

    async def publish(self, event: OutboxEvent) -> None:
        if self._script:
            failure = self._script.pop(0)
            if failure is not None:
                raise failure
        self.published.append(copy.deepcopy(event))


SUCCESS = GatewayOutcome(succeeded=True, reference="ref-ok")
DECLINED = GatewayOutcome(succeeded=False, reference="ref-no", failure_code="declined")
TRANSIENT = WebhookDeliveryError("http_500", retryable=True)
PERMANENT = WebhookDeliveryError("http_400", retryable=False)
