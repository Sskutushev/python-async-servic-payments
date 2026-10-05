"""The single consumer use case, split into explicit checkpointed stages.

Each stage commits its own short transaction *before* any slow I/O (gateway,
webhook) and re-reads the locked row *after* it. Any message, duplicate or
redelivered, therefore converges on the persisted state:

    load ─► [pending?] claim lease ─► gateway ─► persist result + frozen webhook
         ─► [notification due?] count attempt ─► webhook ─► delivered | retry | DLQ

* A payment with a stored result never hits the gateway again.
* A webhook failure never changes ``status``; it only schedules another attempt.
* Retries and dead letters are outbox rows written atomically with the state
  change, so the message can be ACKed as soon as the transaction commits.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable
from datetime import timedelta
from enum import StrEnum

from payments.application.ports import (
    Clock,
    GatewayTransportError,
    PaymentGateway,
    UnitOfWorkFactory,
    WebhookDeliveryError,
    WebhookSender,
)
from payments.application.retry import RetryPolicy
from payments.domain.events import Phase, payment_dead_letter_event, payment_retry_event
from payments.domain.payment import GatewayOutcome, NotificationStatus, Payment

log = logging.getLogger(__name__)


class ProcessOutcome(StrEnum):
    """What the consumer should tell the broker. ``POISON`` is the only reject."""

    COMPLETED = "completed"  # result stored and webhook delivered
    RESULT_STORED = "result_stored"  # result stored; webhook not due / already handled
    RETRY_SCHEDULED = "retry_scheduled"  # transient failure, durable retry enqueued
    DEAD_LETTERED = "dead_lettered"  # retry budget spent, DLQ event enqueued
    SKIPPED_LEASED = "skipped_leased"  # another worker is processing right now
    SKIPPED_STALE = "skipped_stale"  # our lease expired; another worker took over
    NOOP = "noop"  # nothing left to do (duplicate of finished work)
    POISON = "poison"  # unknown payment id: reject to the broker DLQ


class ProcessPayment:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
        clock: Clock,
        retry_policy: RetryPolicy,
        processing_lease: timedelta,
        notification_lease: timedelta | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._gateway = gateway
        self._webhooks = webhooks
        self._clock = clock
        self._retry = retry_policy
        self._processing_lease = processing_lease
        self._notification_lease = notification_lease or processing_lease

    async def __call__(
        self, payment_id: uuid.UUID, *, event_id: uuid.UUID | None = None
    ) -> ProcessOutcome:
        outcome = await self._process_stage(payment_id, event_id)
        if outcome is not None:
            return outcome
        return await self._notify_stage(payment_id, event_id)

    # ------------------------------------------------------------- stage 1
    async def _process_stage(
        self, payment_id: uuid.UUID, event_id: uuid.UUID | None
    ) -> ProcessOutcome | None:
        """Obtain and persist the gateway result. ``None`` means "continue to notify"."""
        token = uuid.uuid4()
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_for_update(payment_id)
            if payment is None:
                log.warning("payment not found", extra={"payment_id": str(payment_id)})
                return ProcessOutcome.POISON
            if payment.is_final:
                return None
            if not payment.claim_processing(token, self._clock.now(), self._processing_lease):
                return ProcessOutcome.SKIPPED_LEASED
            payment.record_gateway_attempt()
            attempt = payment.gateway_attempts
            await uow.payments.save(payment)
            await uow.commit()

        try:
            result = await self._gateway.charge(payment)
        except GatewayTransportError as exc:
            return await self._gateway_unavailable(payment_id, token, attempt, exc, event_id)
        return await self._store_result(payment_id, token, result)

    async def _store_result(
        self, payment_id: uuid.UUID, token: uuid.UUID, result: GatewayOutcome
    ) -> ProcessOutcome | None:
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if payment.is_final:
                return None  # someone else stored a result while we were waiting
            if payment.processing_lease_token != token:
                # Fencing: our lease expired and was taken over; do not overwrite.
                log.warning("stale processing lease", extra={"payment_id": str(payment_id)})
                return ProcessOutcome.SKIPPED_STALE
            payment.record_gateway_result(result, now=self._clock.now(), event_id=uuid.uuid4())
            await uow.payments.save(payment)
            await uow.commit()
            log.info(
                "gateway result stored",
                extra={"payment_id": str(payment_id), "status": payment.status.value},
            )
        return None

    async def _gateway_unavailable(
        self,
        payment_id: uuid.UUID,
        token: uuid.UUID,
        attempt: int,
        exc: GatewayTransportError,
        event_id: uuid.UUID | None,
    ) -> ProcessOutcome:
        """Transport failure: the outcome is unknown, the payment stays ``pending``."""
        now = self._clock.now()
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            payment.release_processing(token)
            if self._retry.is_exhausted(attempt):
                outcome = ProcessOutcome.DEAD_LETTERED
                await uow.outbox.add(
                    payment_dead_letter_event(
                        payment,
                        phase=Phase.PROCESS,
                        attempts=attempt,
                        failure_code=exc.code,
                        original_event_id=event_id,
                        now=now,
                    )
                )
            else:
                outcome = ProcessOutcome.RETRY_SCHEDULED
                delay = self._retry.delay_before(attempt + 1)
                await uow.outbox.add(
                    payment_retry_event(
                        payment,
                        phase=Phase.PROCESS,
                        attempt=attempt + 1,
                        available_at=now + delay,
                        now=now,
                    )
                )
            await uow.payments.save(payment)
            await uow.commit()
        log.warning(
            "gateway unavailable",
            extra={"payment_id": str(payment_id), "attempt": attempt, "outcome": outcome.value},
        )
        return outcome

    # ------------------------------------------------------------- stage 2
    async def _notify_stage(
        self, payment_id: uuid.UUID, event_id: uuid.UUID | None
    ) -> ProcessOutcome:
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if payment.notification_status is not NotificationStatus.PENDING:
                return ProcessOutcome.NOOP
            if not payment.notification_due(self._clock.now()):
                return ProcessOutcome.RESULT_STORED  # a scheduled retry will arrive later
            attempt = payment.begin_notification_attempt(
                now=self._clock.now(), lease=self._notification_lease
            )
            url, body, webhook_event_id = (
                payment.webhook_url,
                payment.notification_body,
                payment.notification_event_id,
            )
            await uow.payments.save(payment)
            await uow.commit()

        assert body is not None  # frozen together with notification_status=PENDING
        assert webhook_event_id is not None
        try:
            await self._webhooks.deliver(url, webhook_event_id, body)
        except WebhookDeliveryError as exc:
            return await self._notification_failed(payment_id, attempt, exc, event_id)
        return await self._notification_delivered(payment_id)

    async def _notification_delivered(self, payment_id: uuid.UUID) -> ProcessOutcome:
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if payment.notification_status is NotificationStatus.PENDING:
                payment.mark_notification_delivered(self._clock.now())
                await uow.payments.save(payment)
                await uow.commit()
        log.info("webhook delivered", extra={"payment_id": str(payment_id)})
        return ProcessOutcome.COMPLETED

    async def _notification_failed(
        self,
        payment_id: uuid.UUID,
        attempt: int,
        exc: WebhookDeliveryError,
        event_id: uuid.UUID | None,
    ) -> ProcessOutcome:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if payment.notification_status is not NotificationStatus.PENDING:
                return ProcessOutcome.NOOP
            if exc.retryable and not self._retry.is_exhausted(attempt):
                outcome = ProcessOutcome.RETRY_SCHEDULED
                next_attempt = attempt + 1
                next_at = now + self._retry.delay_before(next_attempt, retry_after=exc.retry_after)
                payment.schedule_notification_retry(error=exc.code, next_attempt_at=next_at)
                await uow.outbox.add(
                    payment_retry_event(
                        payment,
                        phase=Phase.NOTIFY,
                        attempt=next_attempt,
                        available_at=next_at,
                        now=now,
                    )
                )
            else:
                outcome = ProcessOutcome.DEAD_LETTERED
                payment.exhaust_notification(error=exc.code)
                await uow.outbox.add(
                    payment_dead_letter_event(
                        payment,
                        phase=Phase.NOTIFY,
                        attempts=attempt,
                        failure_code=exc.code,
                        original_event_id=event_id,
                        now=now,
                    )
                )
            await uow.payments.save(payment)
            await uow.commit()
        log.warning(
            "webhook delivery failed",
            extra={
                "payment_id": str(payment_id),
                "attempt": attempt,
                "error_code": exc.code,
                "outcome": outcome.value,
            },
        )
        return outcome

    @staticmethod
    async def _locked(coro: Awaitable[Payment | None]) -> Payment:
        payment = await coro
        if payment is None:  # pragma: no cover - rows are never deleted
            raise RuntimeError("payment disappeared mid-processing")
        return payment
