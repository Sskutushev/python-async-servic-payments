"""Operator replay of a dead-lettered payment.

Only the failed phase is replayed: an exhausted webhook gets a fresh budget, a
pending payment whose gateway was unreachable gets re-enqueued. A payment with
a stored result is never charged again.
"""

from __future__ import annotations

import uuid
from enum import StrEnum

from payments.application.ports import Clock, UnitOfWorkFactory
from payments.domain.errors import InvalidTransition, PaymentNotFound
from payments.domain.events import Phase, payment_recovery_event
from payments.domain.payment import NotificationStatus, PaymentStatus


class ReplayAction(StrEnum):
    NOTIFICATION_REOPENED = "notification_reopened"
    PROCESSING_REQUEUED = "processing_requeued"


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
        elif payment.status is PaymentStatus.PENDING and not payment.processing_lease_active(now):
            payment.gateway_attempts = 0
            phase, action = Phase.PROCESS, ReplayAction.PROCESSING_REQUEUED
        else:
            raise InvalidTransition("payment is not in a replayable state")
        await uow.payments.save(payment)
        await uow.outbox.add(payment_recovery_event(payment, phase=phase, now=now, reason="replay"))
        await uow.commit()
    return action
