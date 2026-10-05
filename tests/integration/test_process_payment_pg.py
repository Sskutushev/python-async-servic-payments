"""The consumer use case on real transactions and row locks."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from payments.application.process_payment import ProcessOutcome, ProcessPayment
from payments.application.recovery import recover_stalled_payments
from payments.application.retry import RetryPolicy
from payments.domain.payment import NotificationStatus, PaymentStatus
from payments.infrastructure.models import OutboxRow
from tests.fakes import SUCCESS, TRANSIENT, FakeGateway, FakeWebhookSender, make_payment

LEASE = timedelta(seconds=60)


async def seed(uow_factory, **kw):  # type: ignore[no-untyped-def]
    p = make_payment(**kw)
    async with uow_factory() as uow:
        assert await uow.payments.add(p)
        await uow.commit()
    return p


def build(uow_factory, clock, gateway=None, webhooks=None):  # type: ignore[no-untyped-def]
    gateway = gateway or FakeGateway(SUCCESS)
    webhooks = webhooks or FakeWebhookSender()
    return (
        ProcessPayment(
            uow_factory=uow_factory,
            gateway=gateway,
            webhooks=webhooks,
            clock=clock,
            retry_policy=RetryPolicy(),
            processing_lease=LEASE,
        ),
        gateway,
        webhooks,
    )


async def test_concurrent_duplicates_charge_once(uow_factory, clock) -> None:
    payment = await seed(uow_factory)

    class BlockingGateway(FakeGateway):
        async def charge(self, p):  # type: ignore[no-untyped-def]
            await asyncio.sleep(0.2)  # long enough for the duplicates to arrive
            return await super().charge(p)

    process, gateway, webhooks = build(uow_factory, clock, gateway=BlockingGateway(SUCCESS))
    outcomes = await asyncio.gather(*(process(payment.id) for _ in range(5)))
    assert sorted(o.value for o in outcomes) == sorted(
        [ProcessOutcome.COMPLETED.value] + [ProcessOutcome.SKIPPED_LEASED.value] * 4
    )
    assert gateway.charges == [payment.id]
    assert len(webhooks.deliveries) == 1


async def test_webhook_retry_then_dlq_keeps_result_on_postgres(
    uow_factory, session_factory, clock
) -> None:
    payment = await seed(uow_factory)
    process, gateway, _ = build(
        uow_factory, clock, webhooks=FakeWebhookSender(TRANSIENT, TRANSIENT, TRANSIENT)
    )
    assert await process(payment.id) is ProcessOutcome.RETRY_SCHEDULED
    clock.advance(timedelta(seconds=1))
    assert await process(payment.id) is ProcessOutcome.RETRY_SCHEDULED
    clock.advance(timedelta(seconds=2))
    assert await process(payment.id) is ProcessOutcome.DEAD_LETTERED

    async with uow_factory() as uow:
        saved = await uow.payments.get(payment.id)
    assert saved is not None
    assert saved.status is PaymentStatus.SUCCEEDED
    assert saved.notification_status is NotificationStatus.EXHAUSTED
    assert saved.notification_attempts == 3
    assert gateway.charges == [payment.id]

    async with session_factory() as s:
        rows = (await s.execute(select(OutboxRow).order_by(OutboxRow.created_at))).scalars().all()
    assert [r.event_type for r in rows] == ["payment.retry", "payment.retry", "payment.dead_letter"]
    assert rows[-1].exchange == "payments.dead"
    assert all(r.published_at is None for r in rows)


async def test_recovery_scan_finds_expired_lease_on_postgres(uow_factory, clock) -> None:
    old = clock.now() - timedelta(minutes=10)
    payment = await seed(uow_factory, created_at=old)
    async with uow_factory() as uow:
        locked = await uow.payments.get_for_update(payment.id)
        assert locked is not None
        locked.claim_processing(uuid.uuid4(), old, timedelta(seconds=60))
        await uow.payments.save(locked)
        await uow.commit()
    assert await recover_stalled_payments(uow_factory, clock, grace=timedelta(minutes=2)) == 1
    assert await recover_stalled_payments(uow_factory, clock, grace=timedelta(minutes=2)) == 0


async def test_check_constraints_reject_impossible_states(uow_factory) -> None:
    payment = await seed(uow_factory)
    async with uow_factory() as uow:
        p = await uow.payments.get_for_update(payment.id)
        assert p is not None
        p.status = PaymentStatus.SUCCEEDED  # without processed_at / notification state
        with pytest.raises(IntegrityError, match='violates check constraint "ck_payments_'):
            await uow.payments.save(p)  # the UPDATE itself is rejected
