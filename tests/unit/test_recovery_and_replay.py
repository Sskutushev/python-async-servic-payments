import uuid
from datetime import timedelta

import pytest

from payments.application.recovery import recover_stalled_payments
from payments.application.replay import ReplayAction, replay_payment
from payments.domain.errors import InvalidTransition, PaymentNotFound
from payments.domain.events import EventType, Phase, payment_created_event
from payments.domain.payment import NotificationStatus, PaymentStatus
from tests.fakes import SUCCESS, FakeClock, InMemoryStore, make_payment

GRACE = timedelta(minutes=2)


async def test_recovery_requeues_expired_processing_lease(
    uow_factory, store: InMemoryStore, clock: FakeClock
) -> None:
    p = make_payment(created_at=clock.now() - timedelta(minutes=10))
    p.claim_processing(uuid.uuid4(), clock.now() - timedelta(minutes=5), timedelta(seconds=60))
    store.payments[p.id] = p
    assert await recover_stalled_payments(uow_factory, clock, grace=GRACE) == 1
    [event] = store.unpublished()
    assert event.event_type is EventType.PAYMENT_RETRY
    assert event.payload["phase"] == Phase.PROCESS
    assert event.payload["reason"] == "processing_lease_expired"


async def test_recovery_requeues_lost_notification_attempt(uow_factory, store, clock) -> None:
    p = make_payment(created_at=clock.now() - timedelta(minutes=10))
    p.record_gateway_result(SUCCESS, now=clock.now() - timedelta(minutes=10), event_id=uuid.uuid4())
    p.begin_notification_attempt(
        now=clock.now() - timedelta(minutes=10), lease=timedelta(seconds=15)
    )
    store.payments[p.id] = p
    assert await recover_stalled_payments(uow_factory, clock, grace=GRACE) == 1
    [event] = store.unpublished()
    assert event.payload["phase"] == Phase.NOTIFY


async def test_recovery_ignores_fresh_work_and_scheduled_retries(uow_factory, store, clock) -> None:
    fresh = make_payment(created_at=clock.now())  # within grace
    store.payments[fresh.id] = fresh
    scheduled = make_payment(created_at=clock.now() - timedelta(minutes=10))
    store.payments[scheduled.id] = scheduled
    store.outbox[uuid.uuid4()] = payment_created_event(
        scheduled, now=clock.now()
    )  # unpublished event exists
    done = make_payment(created_at=clock.now() - timedelta(minutes=10))
    done.record_gateway_result(SUCCESS, now=clock.now(), event_id=uuid.uuid4())
    done.begin_notification_attempt(now=clock.now(), lease=timedelta(1))
    done.mark_notification_delivered(clock.now())
    store.payments[done.id] = done
    assert await recover_stalled_payments(uow_factory, clock, grace=GRACE) == 0


async def test_recovery_is_idempotent_across_runs(uow_factory, store, clock) -> None:
    p = make_payment(created_at=clock.now() - timedelta(minutes=10))
    store.payments[p.id] = p
    assert await recover_stalled_payments(uow_factory, clock, grace=GRACE) == 1
    assert await recover_stalled_payments(uow_factory, clock, grace=GRACE) == 0  # event pending


async def test_replay_reopens_exhausted_notification(uow_factory, store, clock) -> None:
    p = make_payment()
    p.record_gateway_result(SUCCESS, now=clock.now(), event_id=uuid.uuid4())
    p.begin_notification_attempt(now=clock.now(), lease=timedelta(1))
    p.exhaust_notification(error="http_500")
    store.payments[p.id] = p
    assert await replay_payment(uow_factory, clock, p.id) is ReplayAction.NOTIFICATION_REOPENED
    saved = store.payments[p.id]
    assert saved.notification_status is NotificationStatus.PENDING
    assert saved.notification_attempts == 0
    assert saved.notification_event_id == p.notification_event_id  # same event id for the receiver
    assert saved.status is PaymentStatus.SUCCEEDED
    [event] = store.unpublished()
    assert event.payload["phase"] == Phase.NOTIFY


async def test_replay_requeues_pending_payment_after_gateway_outage(
    uow_factory, store, clock
) -> None:
    p = make_payment()
    p.gateway_attempts = 3
    store.payments[p.id] = p
    assert await replay_payment(uow_factory, clock, p.id) is ReplayAction.PROCESSING_REQUEUED
    assert store.payments[p.id].gateway_attempts == 0


async def test_replay_refuses_healthy_or_unknown_payments(uow_factory, store, clock) -> None:
    p = make_payment()
    p.record_gateway_result(SUCCESS, now=clock.now(), event_id=uuid.uuid4())
    p.begin_notification_attempt(now=clock.now(), lease=timedelta(1))
    p.mark_notification_delivered(clock.now())
    store.payments[p.id] = p
    with pytest.raises(InvalidTransition):
        await replay_payment(uow_factory, clock, p.id)
    with pytest.raises(PaymentNotFound):
        await replay_payment(uow_factory, clock, uuid.uuid4())
