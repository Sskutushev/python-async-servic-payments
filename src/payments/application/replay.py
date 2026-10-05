"""Lets an operator retry a payment that ended up in the dead-letter queue.

Only the step that failed is repeated, and only on an explicit command:

* a webhook that used all its attempts gets three new ones (same event id, so the
  receiver can de-duplicate);
* a payment whose gateway processing was halted gets three new gateway attempts.

A payment that already has a result is never charged again.
"""

from __future__ import annotations

import uuid
from enum import StrEnum

from payments.application.ports import Clock, UnitOfWorkFactory
from payments.domain.errors import InvalidTransition, PaymentNotFound
from payments.domain.events import Phase, payment_recovery_event
from payments.domain.payment import NotificationStatus


class ReplayAction(StrEnum):
    NOTIFICATION_REOPENED = "notification_reopened"
    PROCESSING_RESUMED = "processing_resumed"


async def replay_payment(
    uow_factory: UnitOfWorkFactory, clock: Clock, payment_id: uuid.UUID
) -> ReplayAction:
    now = clock.now()
    async with uow_factory() as uow:
        payment = await uow.payments.get_for_update(payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        if payment.notification_status is NotificationStatus.EXHAUSTED:
            payment.reopen_notification(now)
            phase, action = Phase.NOTIFY, ReplayAction.NOTIFICATION_REOPENED
        elif payment.is_processing_halted:
            payment.resume_processing()
            phase, action = Phase.PROCESS, ReplayAction.PROCESSING_RESUMED
        else:
            raise InvalidTransition("payment is not waiting for an operator")
        await uow.payments.save(payment)
        await uow.outbox.add(payment_recovery_event(payment, phase=phase, now=now, reason="replay"))
        await uow.commit()
    return action
