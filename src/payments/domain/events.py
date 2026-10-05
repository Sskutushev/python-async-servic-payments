"""Outbox events and the broker topology names they target.

Names live in the domain so that the outbox record is self-describing: the relay
publishes whatever ``exchange``/``routing_key`` the event carries and knows nothing
about payment semantics.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from payments.domain.payment import Payment

EVENT_SCHEMA_VERSION = 1

PAYMENTS_EXCHANGE = "payments.events"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_NEW_QUEUE = "payments.new"

DEAD_LETTER_EXCHANGE = "payments.dead"
DEAD_LETTER_ROUTING_KEY = "payments.failed"
DEAD_LETTER_QUEUE = "payments.dlq"


class Phase(StrEnum):
    PROCESS = "process"  # obtain the gateway result
    NOTIFY = "notify"  # deliver the webhook


class EventType(StrEnum):
    PAYMENT_CREATED = "payment.created"
    PAYMENT_RETRY = "payment.retry"
    PAYMENT_DEAD_LETTER = "payment.dead_letter"


@dataclass(slots=True)
class OutboxEvent:
    id: uuid.UUID
    event_type: EventType
    aggregate_id: uuid.UUID
    exchange: str
    routing_key: str
    payload: dict[str, Any]
    dedup_key: str
    created_at: datetime
    available_at: datetime
    schema_version: int = EVENT_SCHEMA_VERSION
    published_at: datetime | None = None
    lease_token: uuid.UUID | None = None
    lease_until: datetime | None = None
    publication_attempts: int = 0
    last_error: str | None = None
    headers: dict[str, str] = field(default_factory=dict, compare=False)


def _envelope(
    event_id: uuid.UUID, event_type: EventType, payment: Payment, now: datetime, **extra: Any
) -> dict[str, Any]:
    return {
        "event_id": str(event_id),
        "event_type": event_type.value,
        "schema_version": EVENT_SCHEMA_VERSION,
        "payment_id": str(payment.id),
        "occurred_at": now.isoformat(),
        **extra,
    }


def payment_created_event(payment: Payment, *, now: datetime) -> OutboxEvent:
    event_id = uuid.uuid4()
    return OutboxEvent(
        id=event_id,
        event_type=EventType.PAYMENT_CREATED,
        aggregate_id=payment.id,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        payload=_envelope(event_id, EventType.PAYMENT_CREATED, payment, now, phase=Phase.PROCESS),
        dedup_key=f"payment:{payment.id}:created",
        created_at=now,
        available_at=now,
    )


def payment_retry_event(
    payment: Payment, *, phase: Phase, attempt: int, available_at: datetime, now: datetime
) -> OutboxEvent:
    """Durable retry: the message re-enters ``payments.new`` once ``available_at`` passes."""
    event_id = uuid.uuid4()
    return OutboxEvent(
        id=event_id,
        event_type=EventType.PAYMENT_RETRY,
        aggregate_id=payment.id,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        payload=_envelope(
            event_id, EventType.PAYMENT_RETRY, payment, now, phase=phase, attempt=attempt
        ),
        dedup_key=f"payment:{payment.id}:{phase}:attempt:{attempt}",
        created_at=now,
        available_at=available_at,
    )


def payment_recovery_event(
    payment: Payment, *, phase: Phase, now: datetime, reason: str
) -> OutboxEvent:
    """Emitted by the recovery scan for work that lost its in-flight message."""
    event_id = uuid.uuid4()
    return OutboxEvent(
        id=event_id,
        event_type=EventType.PAYMENT_RETRY,
        aggregate_id=payment.id,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        payload=_envelope(
            event_id, EventType.PAYMENT_RETRY, payment, now, phase=phase, reason=reason
        ),
        dedup_key=f"payment:{payment.id}:{phase}:recover:{event_id}",
        created_at=now,
        available_at=now,
    )


def payment_dead_letter_event(
    payment: Payment,
    *,
    phase: Phase,
    attempts: int,
    failure_code: str,
    original_event_id: uuid.UUID | None,
    now: datetime,
) -> OutboxEvent:
    """Technical failure envelope for operators. Never contains secrets or the webhook URL."""
    event_id = uuid.uuid4()
    return OutboxEvent(
        id=event_id,
        event_type=EventType.PAYMENT_DEAD_LETTER,
        aggregate_id=payment.id,
        exchange=DEAD_LETTER_EXCHANGE,
        routing_key=DEAD_LETTER_ROUTING_KEY,
        payload=_envelope(
            event_id,
            EventType.PAYMENT_DEAD_LETTER,
            payment,
            now,
            phase=phase,
            counted_attempts=attempts,
            failure_code=failure_code,
            payment_status=payment.status.value,
            original_event_id=str(original_event_id) if original_event_id else None,
        ),
        dedup_key=f"payment:{payment.id}:{phase}:dead:{event_id}",
        created_at=now,
        available_at=now,
    )
