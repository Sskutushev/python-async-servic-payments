"""What the consumer does with one message. This is the heart of the service.

The work is split into small steps. Each step saves its progress in a short database
transaction before doing anything slow (calling the gateway or the webhook), and reads
the row again afterwards. So whatever message arrives, even a duplicate, it simply
continues from the saved state:

    load ─► still pending? reserve it ─► gateway ─► save result + webhook body
         ─► webhook due? count the attempt ─► send webhook ─► delivered | retry | DLQ

* A payment that already has a result is never sent to the gateway again.
* A failed webhook never changes the payment status; it only schedules another try.
* Retries and dead letters are rows in the outbox, written in the same transaction as
  the state change. That is why the message can be acknowledged right after the commit.
* Every slow call is owned by a lease token. A worker whose lease was taken over by
  another worker cannot write anything afterwards.
* The attempt budget is checked before every network call, never only after it.
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
    UnitOfWork,
    UnitOfWorkFactory,
    WebhookDeliveryError,
    WebhookSender,
)
from payments.application.retry import RetryPolicy
from payments.domain.events import Phase, payment_dead_letter_event, payment_retry_event
from payments.domain.payment import GatewayOutcome, NotificationStatus, Payment

log = logging.getLogger(__name__)

# Reason codes that end up in the dead-letter message and in ``GET /payments/{id}``.
# "unavailable": every attempt failed to reach the gateway, no money moved as far as we know.
# "outcome unknown": the last attempt was started but its answer was never recorded; with a
# real provider an operator must check the provider's status before replaying.
GATEWAY_UNAVAILABLE = "gateway_unavailable_after_budget"
GATEWAY_OUTCOME_UNKNOWN = "gateway_outcome_unknown_after_budget"
DELIVERY_OUTCOME_UNKNOWN = "delivery_outcome_unknown_after_budget"


class ProcessOutcome(StrEnum):
    """How the message ended. Everything except ``POISON`` means "acknowledge it"."""

    COMPLETED = "completed"  # result saved and webhook delivered
    RESULT_STORED = "result_stored"  # result saved; webhook is not due yet
    RETRY_SCHEDULED = "retry_scheduled"  # temporary failure, a retry is scheduled
    DEAD_LETTERED = "dead_lettered"  # all attempts used, sent to the dead-letter queue
    SKIPPED_LEASED = "skipped_leased"  # another worker is handling this payment right now
    SKIPPED_STALE = "skipped_stale"  # we were too slow; another worker took over
    NOOP = "noop"  # nothing to do: finished, halted, or waiting for an operator
    POISON = "poison"  # unknown payment id: reject so the broker dead-letters it


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
    async def _process_stage(  # noqa: PLR0911 - every return is one documented checkpoint
        self, payment_id: uuid.UUID, event_id: uuid.UUID | None
    ) -> ProcessOutcome | None:
        """Get the gateway result and save it. Returns ``None`` when the webhook step should run."""
        token = uuid.uuid4()
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_for_update(payment_id)
            if payment is None:
                log.warning("payment not found", extra={"payment_id": str(payment_id)})
                return ProcessOutcome.POISON
            if payment.is_final:
                return None
            if payment.is_processing_halted:
                # The gateway budget is used up. Only an operator replay may continue.
                return ProcessOutcome.NOOP
            now = self._clock.now()
            if payment.processing_lease_active(now):
                # Another worker is talking to the gateway right now (maybe on the last
                # attempt). A duplicate message must not interfere with it in any way.
                return ProcessOutcome.SKIPPED_LEASED
            if self._retry.is_exhausted(payment.gateway_attempts):
                # Budget spent, nobody holds the lease, no result: the worker that made the
                # last attempt died before recording the answer. The outcome is unknown; do
                # not call the gateway a fourth time, hand the payment to an operator.
                return await self._halt_processing(
                    uow, payment, payment.gateway_attempts, event_id, GATEWAY_OUTCOME_UNKNOWN
                )
            if not payment.claim_processing(token, now, self._processing_lease):
                return ProcessOutcome.SKIPPED_LEASED  # pragma: no cover - checked above
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
                return None  # another worker saved a result while we were waiting
            if not payment.owns_processing(token):
                # Our reservation expired and another worker took the payment. Its result
                # will be saved by that worker; ours must not overwrite anything.
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
        """We could not reach the gateway. The payment stays pending: unknown is not a decline."""
        now = self._clock.now()
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if payment.is_final or not payment.owns_processing(token):
                # Another worker owns the payment now; it decides about retries.
                log.warning("stale processing lease", extra={"payment_id": str(payment_id)})
                return ProcessOutcome.SKIPPED_STALE
            if self._retry.is_exhausted(attempt):
                return await self._halt_processing(
                    uow, payment, attempt, event_id, GATEWAY_UNAVAILABLE, failure_code=exc.code
                )
            payment.release_processing(token)
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
            "gateway unavailable, retry scheduled",
            extra={"payment_id": str(payment_id), "attempt": attempt, "error_code": exc.code},
        )
        return ProcessOutcome.RETRY_SCHEDULED

    async def _halt_processing(
        self,
        uow: UnitOfWork,
        payment: Payment,
        attempts: int,
        event_id: uuid.UUID | None,
        reason: str,
        *,
        failure_code: str | None = None,
    ) -> ProcessOutcome:
        """Stop calling the gateway and write the dead-letter event in the same transaction.

        ``reason`` is the category stored on the payment (unavailable vs outcome unknown);
        ``failure_code`` is the last concrete error, for the dead-letter message.
        """
        now = self._clock.now()
        payment.halt_processing(reason=reason, now=now)
        await uow.outbox.add(
            payment_dead_letter_event(
                payment,
                phase=Phase.PROCESS,
                attempts=attempts,
                failure_code=failure_code or reason,
                original_event_id=event_id,
                now=now,
            )
        )
        await uow.payments.save(payment)
        await uow.commit()
        log.error(
            "gateway budget exhausted, payment halted",
            extra={
                "payment_id": str(payment.id),
                "attempt": attempts,
                "reason": reason,
                "error_code": failure_code or reason,
            },
        )
        return ProcessOutcome.DEAD_LETTERED

    # ------------------------------------------------------------- stage 2
    async def _notify_stage(
        self, payment_id: uuid.UUID, event_id: uuid.UUID | None
    ) -> ProcessOutcome:
        token = uuid.uuid4()
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if payment.notification_status is not NotificationStatus.PENDING:
                return ProcessOutcome.NOOP
            if not payment.notification_due(self._clock.now()):
                return ProcessOutcome.RESULT_STORED  # the scheduled retry message will come later
            if self._retry.is_exhausted(payment.notification_attempts):
                # The last attempt was counted but its outcome was never recorded (a worker
                # died mid-call). We do not know whether the receiver got it, and we are not
                # allowed a fourth call: hand it to an operator with the same event id.
                return await self._exhaust(uow, payment, DELIVERY_OUTCOME_UNKNOWN, event_id)
            attempt = payment.begin_notification_attempt(
                token=token, now=self._clock.now(), lease=self._notification_lease
            )
            url, body, webhook_event_id = (
                payment.webhook_url,
                payment.notification_body,
                payment.notification_event_id,
            )
            await uow.payments.save(payment)
            await uow.commit()

        assert body is not None  # both are set at the moment the status becomes PENDING
        assert webhook_event_id is not None
        try:
            await self._webhooks.deliver(url, webhook_event_id, body)
        except WebhookDeliveryError as exc:
            return await self._notification_failed(payment_id, token, attempt, exc, event_id)
        return await self._notification_delivered(payment_id, token)

    async def _notification_delivered(
        self, payment_id: uuid.UUID, token: uuid.UUID
    ) -> ProcessOutcome:
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if not payment.owns_notification_attempt(token):
                # Our attempt took so long that another worker started a new one. The
                # receiver accepted ours, but the other worker owns the record now; its
                # outcome wins. If it ended in "exhausted", an operator replay re-sends the
                # same event id and the receiver de-duplicates it.
                log.warning("late webhook success ignored", extra={"payment_id": str(payment_id)})
                return ProcessOutcome.SKIPPED_STALE
            payment.mark_notification_delivered(self._clock.now())
            await uow.payments.save(payment)
            await uow.commit()
        log.info("webhook delivered", extra={"payment_id": str(payment_id)})
        return ProcessOutcome.COMPLETED

    async def _notification_failed(
        self,
        payment_id: uuid.UUID,
        token: uuid.UUID,
        attempt: int,
        exc: WebhookDeliveryError,
        event_id: uuid.UUID | None,
    ) -> ProcessOutcome:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            payment = await self._locked(uow.payments.get_for_update(payment_id))
            if not payment.owns_notification_attempt(token):
                log.warning("late webhook failure ignored", extra={"payment_id": str(payment_id)})
                return ProcessOutcome.SKIPPED_STALE
            if not exc.retryable or self._retry.is_exhausted(attempt):
                return await self._exhaust(uow, payment, exc.code, event_id)
            next_attempt = attempt + 1
            next_at = now + self._retry.delay_before(next_attempt, retry_after=exc.retry_after)
            payment.schedule_notification_retry(error=exc.code, next_attempt_at=next_at)
            await uow.outbox.add(
                payment_retry_event(
                    payment, phase=Phase.NOTIFY, attempt=next_attempt, available_at=next_at, now=now
                )
            )
            await uow.payments.save(payment)
            await uow.commit()
        log.warning(
            "webhook delivery failed, retry scheduled",
            extra={"payment_id": str(payment_id), "attempt": attempt, "error_code": exc.code},
        )
        return ProcessOutcome.RETRY_SCHEDULED

    async def _exhaust(
        self, uow: UnitOfWork, payment: Payment, error_code: str, event_id: uuid.UUID | None
    ) -> ProcessOutcome:
        """Give up on the webhook and write the dead-letter event in the same transaction."""
        now = self._clock.now()
        payment.exhaust_notification(error=error_code)
        await uow.outbox.add(
            payment_dead_letter_event(
                payment,
                phase=Phase.NOTIFY,
                attempts=payment.notification_attempts,
                failure_code=error_code,
                original_event_id=event_id,
                now=now,
            )
        )
        await uow.payments.save(payment)
        await uow.commit()
        log.error(
            "webhook attempts exhausted",
            extra={
                "payment_id": str(payment.id),
                "attempt": payment.notification_attempts,
                "error_code": error_code,
            },
        )
        return ProcessOutcome.DEAD_LETTERED

    @staticmethod
    async def _locked(coro: Awaitable[Payment | None]) -> Payment:
        payment = await coro
        if payment is None:  # pragma: no cover - rows are never deleted
            raise RuntimeError("payment disappeared mid-processing")
        return payment
