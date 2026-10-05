"""Payment aggregate: status machine, processing lease and notification lifecycle.

Two independent state machines live on one row on purpose (the task asks for
``payments`` + ``outbox`` only):

* ``status``: pending -> succeeded | failed (terminal, never reverts).
* ``notification_status``: not_ready -> pending -> delivered | exhausted.

A webhook failure can never change ``status``: that is the core guarantee that
"a payment is not re-processed because the notification did not get through".
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

from payments.domain.errors import InvalidTransition
from payments.domain.money import Money

WEBHOOK_SCHEMA_VERSION = 1


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class NotificationStatus(StrEnum):
    NOT_READY = "not_ready"  # gateway result not yet known
    PENDING = "pending"  # result known, webhook not yet accepted by the receiver
    DELIVERED = "delivered"
    EXHAUSTED = "exhausted"  # retry budget spent, sent to DLQ for operators


@dataclass(frozen=True, slots=True)
class GatewayOutcome:
    """Business result of the external gateway. Transport problems are exceptions."""

    succeeded: bool
    reference: str
    failure_code: str | None = None


@dataclass(slots=True)
class Payment:
    id: uuid.UUID
    money: Money
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    request_fingerprint: str
    webhook_url: str
    created_at: datetime
    status: PaymentStatus = PaymentStatus.PENDING
    processed_at: datetime | None = None
    gateway_reference: str | None = None
    failure_code: str | None = None
    gateway_attempts: int = 0
    processing_lease_token: uuid.UUID | None = None
    processing_lease_until: datetime | None = None
    notification_status: NotificationStatus = NotificationStatus.NOT_READY
    notification_event_id: uuid.UUID | None = None
    notification_body: dict[str, Any] | None = None
    notification_attempts: int = 0
    notification_next_attempt_at: datetime | None = None
    notification_delivered_at: datetime | None = None
    notification_last_error: str | None = None

    # ------------------------------------------------------------------ status
    @property
    def is_final(self) -> bool:
        return self.status is not PaymentStatus.PENDING

    # --------------------------------------------------------- processing lease
    def processing_lease_active(self, now: datetime) -> bool:
        return self.processing_lease_until is not None and self.processing_lease_until > now

    def claim_processing(self, token: uuid.UUID, now: datetime, lease: timedelta) -> bool:
        """Take the processing lease. Returns False when another worker holds it."""
        if self.is_final:
            raise InvalidTransition("payment already has a final result")
        if self.processing_lease_active(now) and self.processing_lease_token != token:
            return False
        self.processing_lease_token = token
        self.processing_lease_until = now + lease
        return True

    def release_processing(self, token: uuid.UUID) -> None:
        if self.processing_lease_token == token:
            self.processing_lease_token = None
            self.processing_lease_until = None

    def record_gateway_attempt(self) -> None:
        self.gateway_attempts += 1

    # -------------------------------------------------------------- result
    def record_gateway_result(
        self, outcome: GatewayOutcome, *, now: datetime, event_id: uuid.UUID
    ) -> None:
        """Persist the final result and freeze the webhook event in one step."""
        if self.is_final:
            raise InvalidTransition("gateway result already recorded")
        self.status = PaymentStatus.SUCCEEDED if outcome.succeeded else PaymentStatus.FAILED
        self.processed_at = now
        self.gateway_reference = outcome.reference
        self.failure_code = None if outcome.succeeded else outcome.failure_code
        self.processing_lease_token = None
        self.processing_lease_until = None
        self.notification_status = NotificationStatus.PENDING
        self.notification_event_id = event_id
        self.notification_next_attempt_at = now
        self.notification_body = {
            "event_id": str(event_id),
            "event_type": f"payment.{self.status}",
            "schema_version": WEBHOOK_SCHEMA_VERSION,
            "payment_id": str(self.id),
            "amount": str(self.money.amount),
            "currency": self.money.currency.value,
            "status": self.status.value,
            "failure_code": self.failure_code,
            "occurred_at": now.isoformat(),
        }

    # ---------------------------------------------------------- notification
    def notification_due(self, now: datetime) -> bool:
        return (
            self.notification_status is NotificationStatus.PENDING
            and self.notification_next_attempt_at is not None
            and self.notification_next_attempt_at <= now
        )

    def begin_notification_attempt(self, *, now: datetime, lease: timedelta) -> int:
        """Count an attempt *before* the network call so a crash mid-flight is still counted.

        ``next_attempt_at`` is pushed forward by ``lease`` so a crashed attempt is
        picked up by the recovery scan rather than retried immediately by a duplicate.
        """
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_attempts += 1
        self.notification_next_attempt_at = now + lease
        return self.notification_attempts

    def mark_notification_delivered(self, now: datetime) -> None:
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_status = NotificationStatus.DELIVERED
        self.notification_delivered_at = now
        self.notification_next_attempt_at = None
        self.notification_last_error = None

    def schedule_notification_retry(self, *, error: str, next_attempt_at: datetime) -> None:
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_last_error = error
        self.notification_next_attempt_at = next_attempt_at

    def exhaust_notification(self, *, error: str) -> None:
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_status = NotificationStatus.EXHAUSTED
        self.notification_last_error = error
        self.notification_next_attempt_at = None

    def reopen_notification(self, now: datetime) -> None:
        """Operator replay: give an exhausted notification a fresh retry budget."""
        if self.notification_status is not NotificationStatus.EXHAUSTED:
            raise InvalidTransition("only exhausted notifications can be replayed")
        self.notification_status = NotificationStatus.PENDING
        self.notification_attempts = 0
        self.notification_next_attempt_at = now
        self.notification_last_error = None
