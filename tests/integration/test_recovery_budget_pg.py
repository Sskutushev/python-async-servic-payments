"""The five review scenarios, on real PostgreSQL transactions and row locks.

1. Gateway budget after DLQ: recovery passes and duplicate messages never trigger a 4th call.
2. Lost lease + transport error: the stale worker writes nothing.
3. Crash on the 3rd webhook attempt: recovery does not send a 4th request.
4. Concurrent webhook attempts: a late outcome from an old attempt cannot change the new state.
5. Operator replay after exhaustion: only the explicit command opens a new budget.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import timedelta

from payments.application.ports import GatewayTransportError, WebhookDeliveryError
from payments.application.process_payment import (
    DELIVERY_OUTCOME_UNKNOWN,
    ProcessOutcome,
    ProcessPayment,
)
from payments.application.recovery import recover_stalled_payments
from payments.application.replay import ReplayAction, replay_payment
from payments.application.retry import RetryPolicy
from payments.domain.payment import NotificationStatus, PaymentStatus
from tests.fakes import SUCCESS, TRANSIENT, FakeClock, FakeGateway, FakeWebhookSender, make_payment

LEASE = timedelta(seconds=60)
GRACE = timedelta(minutes=2)


async def seed(uow_factory, **kw):  # type: ignore[no-untyped-def]
    p = make_payment(**kw)
    async with uow_factory() as uow:
        assert await uow.payments.add(p)
        await uow.commit()
    return p


async def load(uow_factory, payment_id):  # type: ignore[no-untyped-def]
    async with uow_factory() as uow:
        p = await uow.payments.get(payment_id)
    assert p is not None
    return p


async def unpublished(uow_factory, clock):  # type: ignore[no-untyped-def]
    async with uow_factory() as uow:  # not committed: the claim rolls back, this is a read
        return await uow.outbox.claim_due(
            now=clock.now() + timedelta(days=365), lease=timedelta(0), token=uuid.uuid4(), limit=100
        )


async def publish_all(uow_factory, clock) -> int:  # type: ignore[no-untyped-def]
    """Pretend the relay published every pending event."""
    token = uuid.uuid4()
    async with uow_factory() as uow:
        events = await uow.outbox.claim_due(
            now=clock.now() + timedelta(days=365), lease=LEASE, token=token, limit=100
        )
        for e in events:
            await uow.outbox.mark_published(e.id, token=token, now=clock.now())
        await uow.commit()
    return len(events)


def build(uow_factory, clock, gateway=None, webhooks=None, notification_lease=LEASE):  # type: ignore[no-untyped-def]
    gateway = gateway or FakeGateway(SUCCESS)
    webhooks = webhooks or FakeWebhookSender()
    process = ProcessPayment(
        uow_factory=uow_factory,
        gateway=gateway,
        webhooks=webhooks,
        clock=clock,
        retry_policy=RetryPolicy(),
        processing_lease=LEASE,
        notification_lease=notification_lease,
    )
    return process, gateway, webhooks


# 1 ---------------------------------------------------------------------------------------
async def test_gateway_budget_survives_recovery_and_duplicates(
    uow_factory, clock: FakeClock
) -> None:
    payment = await seed(uow_factory, created_at=clock.now() - timedelta(hours=1))
    gateway = FakeGateway(*[GatewayTransportError("down")] * 3, SUCCESS)
    process, _, _ = build(uow_factory, clock, gateway=gateway)

    for _ in range(2):
        assert await process(payment.id) is ProcessOutcome.RETRY_SCHEDULED
        await publish_all(uow_factory, clock)
        clock.advance(timedelta(seconds=5))
    assert await process(payment.id) is ProcessOutcome.DEAD_LETTERED
    assert await publish_all(uow_factory, clock) == 1  # the DLQ event

    for _ in range(5):
        clock.advance(timedelta(hours=1))
        assert await recover_stalled_payments(uow_factory, clock, grace=GRACE) == 0
        assert await process(payment.id) is ProcessOutcome.NOOP  # duplicate broker message
    assert len(gateway.charges) == 3
    assert await unpublished(uow_factory, clock) == []

    saved = await load(uow_factory, payment.id)
    assert saved.status is PaymentStatus.PENDING
    assert saved.is_processing_halted

    # 5: explicit replay opens exactly one new budget
    assert await replay_payment(uow_factory, clock, payment.id) is ReplayAction.PROCESSING_RESUMED
    await publish_all(uow_factory, clock)
    assert await process(payment.id) is ProcessOutcome.COMPLETED
    assert len(gateway.charges) == 4
    assert (await load(uow_factory, payment.id)).status is PaymentStatus.SUCCEEDED


# 1b (review blocker 1) ------------------------------------------------------------------
async def test_duplicate_during_third_gateway_call_does_not_halt(
    uow_factory, clock: FakeClock
) -> None:
    """A duplicate delivered while attempt 3 is in flight must skip, not halt the payment."""
    payment = await seed(uow_factory)
    async with uow_factory() as uow:
        p = await uow.payments.get_for_update(payment.id)
        assert p is not None
        p.gateway_attempts = 2  # two earlier attempts failed to reach the gateway
        await uow.payments.save(p)
        await uow.commit()

    in_flight = asyncio.Event()
    release = asyncio.Event()

    class SlowGateway:
        calls = 0

        async def charge(self, p):  # type: ignore[no-untyped-def]
            SlowGateway.calls += 1
            in_flight.set()
            await release.wait()
            return SUCCESS

    process_a, _, _ = build(uow_factory, clock, gateway=SlowGateway())
    task_a = asyncio.create_task(process_a(payment.id))
    await in_flight.wait()

    process_b, gateway_b, _ = build(uow_factory, clock)  # the duplicate
    assert await process_b(payment.id) is ProcessOutcome.SKIPPED_LEASED
    assert gateway_b.charges == []
    mid = await load(uow_factory, payment.id)
    assert mid.processing_lease_token is not None  # A still owns the payment
    assert not mid.is_processing_halted
    assert await unpublished(uow_factory, clock) == []  # no DLQ event

    release.set()
    assert await task_a is ProcessOutcome.COMPLETED
    saved = await load(uow_factory, payment.id)
    assert saved.status is PaymentStatus.SUCCEEDED
    assert saved.gateway_attempts == 3
    assert SlowGateway.calls == 1


async def test_lost_third_gateway_attempt_halts_without_fourth_call(
    uow_factory, clock: FakeClock
) -> None:
    """The worker died during attempt 3: after its lease expires, halt as "outcome unknown"."""
    payment = await seed(uow_factory)
    async with uow_factory() as uow:
        p = await uow.payments.get_for_update(payment.id)
        assert p is not None
        p.gateway_attempts = 3
        p.claim_processing(uuid.uuid4(), clock.now(), LEASE)  # the dead worker's reservation
        await uow.payments.save(p)
        await uow.commit()

    process, gateway, _ = build(uow_factory, clock)
    assert await process(payment.id) is ProcessOutcome.SKIPPED_LEASED
    clock.advance(LEASE + timedelta(seconds=1))
    assert await process(payment.id) is ProcessOutcome.DEAD_LETTERED
    assert gateway.charges == []
    saved = await load(uow_factory, payment.id)
    assert saved.status is PaymentStatus.PENDING
    assert saved.processing_halt_reason == "gateway_outcome_unknown_after_budget"
    [dead] = await unpublished(uow_factory, clock)
    assert dead.payload["failure_code"] == "gateway_outcome_unknown_after_budget"


# 2 ---------------------------------------------------------------------------------------
async def test_stale_worker_transport_error_writes_nothing(uow_factory, clock: FakeClock) -> None:
    payment = await seed(uow_factory)
    takeover_token = uuid.uuid4()

    class SlowFailingGateway:
        async def charge(self, p):  # type: ignore[no-untyped-def]
            clock.advance(LEASE * 2)
            async with uow_factory() as uow:  # worker B takes over while A waits
                current = await uow.payments.get_for_update(p.id)
                assert current is not None
                assert current.claim_processing(takeover_token, clock.now(), LEASE)
                await uow.payments.save(current)
                await uow.commit()
            raise GatewayTransportError("timeout")

    process, _, _ = build(uow_factory, clock, gateway=SlowFailingGateway())
    assert await process(payment.id) is ProcessOutcome.SKIPPED_STALE
    saved = await load(uow_factory, payment.id)
    assert saved.processing_lease_token == takeover_token
    assert not saved.is_processing_halted
    assert await unpublished(uow_factory, clock) == []


# 3 ---------------------------------------------------------------------------------------
async def test_crash_on_third_webhook_attempt_never_sends_a_fourth(
    uow_factory, clock: FakeClock
) -> None:
    payment = await seed(uow_factory)
    calls = 0

    class CrashingThirdDelivery:
        async def deliver(self, url, event_id, body):  # type: ignore[no-untyped-def]
            nonlocal calls
            calls += 1
            if calls < 3:
                raise TRANSIENT
            raise RuntimeError("process killed mid-request")  # outcome unknown

    process, _, _ = build(uow_factory, clock, webhooks=CrashingThirdDelivery())
    for _ in range(2):
        assert await process(payment.id) is ProcessOutcome.RETRY_SCHEDULED
        await publish_all(uow_factory, clock)
        clock.advance(timedelta(seconds=5))
    with contextlib.suppress(RuntimeError):
        await process(payment.id)
    assert calls == 3
    assert (await load(uow_factory, payment.id)).notification_attempts == 3

    # Recovery finds the lost attempt; the next worker must not call the receiver again.
    clock.advance(LEASE + GRACE + timedelta(seconds=1))
    assert await recover_stalled_payments(uow_factory, clock, grace=GRACE) == 1
    await publish_all(uow_factory, clock)
    process2, _, webhooks2 = build(uow_factory, clock)
    assert await process2(payment.id) is ProcessOutcome.DEAD_LETTERED
    assert webhooks2.deliveries == []
    saved = await load(uow_factory, payment.id)
    assert saved.notification_status is NotificationStatus.EXHAUSTED
    assert saved.notification_last_error == DELIVERY_OUTCOME_UNKNOWN
    assert saved.status is PaymentStatus.SUCCEEDED
    assert calls == 3


# 4 ---------------------------------------------------------------------------------------
async def test_late_outcome_of_an_old_webhook_attempt_is_ignored(
    uow_factory, clock: FakeClock
) -> None:
    payment = await seed(uow_factory)
    short_lease = timedelta(seconds=5)
    started = asyncio.Event()
    release = asyncio.Event()

    class HangingThenSucceeding:
        async def deliver(self, url, event_id, body):  # type: ignore[no-untyped-def]
            started.set()
            await release.wait()  # the HTTP call outlives its lease

    process_a, _, _ = build(
        uow_factory, clock, webhooks=HangingThenSucceeding(), notification_lease=short_lease
    )
    task_a = asyncio.create_task(process_a(payment.id))
    await started.wait()

    # Worker B sees the lease expired, starts attempt 2, fails permanently -> exhausted.
    clock.advance(short_lease * 2)
    process_b, _, _ = build(
        uow_factory,
        clock,
        gateway=FakeGateway(SUCCESS),
        webhooks=FakeWebhookSender(WebhookDeliveryError("http_400", retryable=False)),
    )
    assert await process_b(payment.id) is ProcessOutcome.DEAD_LETTERED
    exhausted = await load(uow_factory, payment.id)
    assert exhausted.notification_status is NotificationStatus.EXHAUSTED

    release.set()  # A's old attempt finally "succeeds"
    assert await task_a is ProcessOutcome.SKIPPED_STALE
    saved = await load(uow_factory, payment.id)
    assert saved.notification_status is NotificationStatus.EXHAUSTED  # B's record stands
    assert saved.notification_attempts == 2
