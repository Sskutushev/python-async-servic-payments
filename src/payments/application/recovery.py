"""Finds payments that got stuck and puts them back into the queue.

Two situations that neither the outbox nor RabbitMQ can notice on their own:

* a worker died while it had the payment reserved (still pending, reservation expired);
* a worker died after counting a webhook attempt but before saving how it went.

Only payments with no pending outbox event are touched, so a retry that is already
scheduled is never duplicated. The scan writes to the outbox like everything else; it
is not a second consumer.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from payments.application.ports import Clock, UnitOfWorkFactory
from payments.domain.events import Phase, payment_recovery_event
from payments.domain.payment import NotificationStatus, PaymentStatus

log = logging.getLogger(__name__)


async def recover_stalled_payments(
    uow_factory: UnitOfWorkFactory, clock: Clock, *, grace: timedelta, limit: int = 100
) -> int:
    now = clock.now()
    recovered = 0
    async with uow_factory() as uow:
        for payment in await uow.payments.find_stalled(older_than=now - grace, limit=limit):
            if payment.status is PaymentStatus.PENDING:
                phase, reason = Phase.PROCESS, "processing_lease_expired"
            elif payment.notification_status is NotificationStatus.PENDING:
                phase, reason = Phase.NOTIFY, "notification_attempt_lost"
            else:  # pragma: no cover - the query only returns these two cases
                continue
            await uow.outbox.add(
                payment_recovery_event(payment, phase=phase, now=now, reason=reason)
            )
            recovered += 1
            log.warning(
                "stalled payment re-enqueued",
                extra={"payment_id": str(payment.id), "phase": phase.value, "reason": reason},
            )
        await uow.commit()
    return recovered
