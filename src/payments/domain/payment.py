"""The Payment itself: its status, the processing lease and the webhook lifecycle.

One row tracks two separate things on purpose (the task allows only two tables):

* ``status``: pending -> succeeded or failed. Final, never changes back.
* ``notification_status``: not_ready -> pending -> delivered or exhausted.

A failed webhook can never touch ``status``. That is what guarantees a payment is not
charged again just because the notification did not get through.
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
    NOT_READY = "not_ready"  # no gateway result yet, nothing to send
    PENDING = "pending"  # result known, receiver has not accepted the webhook yet
    DELIVERED = "delivered"
    EXHAUSTED = "exhausted"  # all attempts used, handed to operators via the DLQ


@dataclass(frozen=True, slots=True)
class GatewayOutcome:
    """What the gateway answered: paid or declined. "Could not reach it" is an exception instead."""

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
    processing_halted_at: datetime | None = None  # set when an operator must look at it
    processing_halt_reason: str | None = None
    notification_status: NotificationStatus = NotificationStatus.NOT_READY
    notification_event_id: uuid.UUID | None = None
    notification_body: dict[str, Any] | None = None
    notification_attempts: int = 0
    notification_lease_token: uuid.UUID | None = None  # which worker owns the current attempt
    notification_next_attempt_at: datetime | None = None
    notification_delivered_at: datetime | None = None
    notification_last_error: str | None = None

    # ------------------------------------------------------------------ status
    @property
    def is_final(self) -> bool:
        return self.status is not PaymentStatus.PENDING

    @property
    def is_processing_halted(self) -> bool:
        """True when the gateway could not be reached too many times; only replay resumes it."""
        return self.processing_halted_at is not None

    # --------------------------------------------------------- processing lease
    def processing_lease_active(self, now: datetime) -> bool:
        return self.processing_lease_until is not None and self.processing_lease_until > now

    def owns_processing(self, token: uuid.UUID) -> bool:
        return self.processing_lease_token == token

    def claim_processing(self, token: uuid.UUID, now: datetime, lease: timedelta) -> bool:
        """Reserve the payment for this worker. Returns False if another worker holds it."""
        if self.is_final:
            raise InvalidTransition("payment already has a final result")
        if self.is_processing_halted:
            raise InvalidTransition("processing is halted until an operator replays it")
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

    def halt_processing(self, *, reason: str, now: datetime) -> None:
        """Stop trying the gateway. The payment stays pending; nothing touches it until replay."""
        if self.is_final:
            raise InvalidTransition("payment already has a final result")
        self.processing_lease_token = None
        self.processing_lease_until = None
        self.processing_halted_at = now
        self.processing_halt_reason = reason

    def resume_processing(self) -> None:
        """Used by the replay command: lift the halt and give the gateway three new attempts."""
        if not self.is_processing_halted:
            raise InvalidTransition("processing is not halted")
        self.processing_halted_at = None
        self.processing_halt_reason = None
        self.gateway_attempts = 0

    # -------------------------------------------------------------- result
    def record_gateway_result(
        self, outcome: GatewayOutcome, *, now: datetime, event_id: uuid.UUID
    ) -> None:
        """Store the final result and build the webhook body at the same time.

        The body is built once and saved, so every delivery attempt sends exactly the
        same bytes and the same event id.
        """
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

    def owns_notification_attempt(self, token: uuid.UUID) -> bool:
        return self.notification_lease_token == token

    def begin_notification_attempt(
        self, *, token: uuid.UUID, now: datetime, lease: timedelta
    ) -> int:
        """Count the attempt before the HTTP call, so a crash during the call still counts.

        ``token`` marks which worker owns this attempt: only that worker may record how
        it went. ``next_attempt_at`` is moved forward by ``lease``: if we crash mid-call,
        the recovery scan picks the payment up later instead of a duplicate message
        retrying it right away.
        """
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_attempts += 1
        self.notification_lease_token = token
        self.notification_next_attempt_at = now + lease
        return self.notification_attempts

    def mark_notification_delivered(self, now: datetime) -> None:
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_status = NotificationStatus.DELIVERED
        self.notification_delivered_at = now
        self.notification_lease_token = None
        self.notification_next_attempt_at = None
        self.notification_last_error = None

    def schedule_notification_retry(self, *, error: str, next_attempt_at: datetime) -> None:
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_lease_token = None
        self.notification_last_error = error
        self.notification_next_attempt_at = next_attempt_at

    def exhaust_notification(self, *, error: str) -> None:
        if self.notification_status is not NotificationStatus.PENDING:
            raise InvalidTransition("notification is not pending")
        self.notification_status = NotificationStatus.EXHAUSTED
        self.notification_lease_token = None
        self.notification_last_error = error
        self.notification_next_attempt_at = None

    def reopen_notification(self, now: datetime) -> None:
        """Used by the replay command: give an exhausted notification three new attempts."""
        if self.notification_status is not NotificationStatus.EXHAUSTED:
            raise InvalidTransition("only exhausted notifications can be replayed")
        self.notification_status = NotificationStatus.PENDING
        self.notification_attempts = 0
        self.notification_lease_token = None
        self.notification_next_attempt_at = now
        self.notification_last_error = None
