"""State-boundary matrix: for every slow call, what a duplicate, a crash or a replay does.

Each case states the expected end state *and* the exact number of external calls, so the
table in the README is backed by assertions rather than prose.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from payments.application.ports import GatewayTransportError
from payments.application.process_payment import ProcessOutcome, ProcessPayment
from payments.application.recovery import recover_stalled_payments
from payments.application.replay import ReplayAction, replay_payment
from payments.application.retry import RetryPolicy
from payments.domain.payment import NotificationStatus, PaymentStatus
from tests.fakes import (
    DECLINED,
    SUCCESS,
    TRANSIENT,
    FakeClock,
    FakeGateway,
    FakeWebhookSender,
    InMemoryStore,
    make_payment,
)

LEASE = timedelta(seconds=60)


def build(uow_factory, clock, *, gateway=None, webhooks=None):  # type: ignore[no-untyped-def]
    gateway = gateway or FakeGateway(SUCCESS)
    webhooks = webhooks or FakeWebhookSender()
    use_case = ProcessPayment(
        uow_factory=uow_factory,
        gateway=gateway,
        webhooks=webhooks,
        clock=clock,
        retry_policy=RetryPolicy(),
        processing_lease=LEASE,
    )
    return use_case, gateway, webhooks


def publish_all(store: InMemoryStore, clock: FakeClock) -> None:
    for e in store.unpublished():
        e.published_at = clock.now()
        clock.advance(timedelta(seconds=5))


@pytest.fixture
def seeded(store: InMemoryStore):  # type: ignore[no-untyped-def]
    payment = make_payment()
    store.payments[payment.id] = payment
    return payment


# ---------------------------------------------------- duplicate during gateway attempt n


@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_duplicate_during_gateway_attempt_is_skipped(
    uow_factory, store, clock, seeded, attempt: int
) -> None:
    """Expected: duplicate → SKIPPED_LEASED; exactly ``attempt`` gateway calls; final result saved."""
    failures = [GatewayTransportError("down")] * (attempt - 1)
    duplicate_outcomes: list[ProcessOutcome] = []

    class Gateway(FakeGateway):
        async def charge(self, payment):  # type: ignore[no-untyped-def]
            if len(self.charges) == attempt - 1:  # this is the call under test
                duplicate_outcomes.append(await process_dup(payment.id))
            return await super().charge(payment)

    gateway = Gateway(*failures, SUCCESS)
    process, _, webhooks = build(uow_factory, clock, gateway=gateway)
    process_dup, dup_gateway, dup_webhooks = build(uow_factory, clock)

    outcome = None
    while outcome is not ProcessOutcome.COMPLETED:
        outcome = await process(seeded.id)
        publish_all(store, clock)

    assert duplicate_outcomes == [ProcessOutcome.SKIPPED_LEASED]
    assert len(gateway.charges) == attempt
    assert dup_gateway.charges == []
    assert dup_webhooks.deliveries == []
    assert len(webhooks.deliveries) == 1
    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.SUCCEEDED
    assert saved.gateway_attempts == attempt
    assert saved.notification_status is NotificationStatus.DELIVERED


# ---------------------------------------------------- duplicate during webhook attempt n


@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_duplicate_during_webhook_attempt_is_skipped(
    uow_factory, store, clock, seeded, attempt: int
) -> None:
    """Expected: duplicate → RESULT_STORED (attempt in flight, not due); ``attempt`` deliveries."""
    duplicate_outcomes: list[ProcessOutcome] = []

    class Webhooks(FakeWebhookSender):
        async def deliver(self, url, event_id, body):  # type: ignore[no-untyped-def]
            if len(self.deliveries) == attempt - 1:
                duplicate_outcomes.append(await process_dup(payment_id))
            await super().deliver(url, event_id, body)

    payment_id = seeded.id
    webhooks = Webhooks(*([TRANSIENT] * (attempt - 1)), None)
    process, gateway, _ = build(uow_factory, clock, webhooks=webhooks)
    process_dup, dup_gateway, dup_webhooks = build(uow_factory, clock)

    outcome = None
    while outcome is not ProcessOutcome.COMPLETED:
        outcome = await process(payment_id)
        publish_all(store, clock)

    assert duplicate_outcomes == [ProcessOutcome.RESULT_STORED]
    assert gateway.charges == [payment_id]
    assert dup_gateway.charges == []
    assert dup_webhooks.deliveries == []
    assert len(webhooks.deliveries) == attempt
    saved = store.payments[payment_id]
    assert saved.notification_attempts == attempt
    assert saved.notification_status is NotificationStatus.DELIVERED


# ------------------------------------------------------------- crash around the result


async def test_crash_before_result_is_committed_repeats_the_call_with_the_same_answer(
    uow_factory, store, clock, seeded
) -> None:
    """Expected: 2 gateway calls (same deterministic answer), 1 stored result, 1 webhook."""
    gateway = FakeGateway(DECLINED, DECLINED)
    process, _, webhooks = build(uow_factory, clock, gateway=gateway)

    def crash_on_result_commit(commit_no: int) -> None:
        if commit_no == 1:  # 0: lease, 1: result
            raise RuntimeError("process killed")

    store.before_commit = crash_on_result_commit
    with pytest.raises(RuntimeError):
        await process(seeded.id)
    assert store.payments[seeded.id].status is PaymentStatus.PENDING
    store.before_commit = None

    clock.advance(LEASE + timedelta(seconds=1))  # the lost reservation expires
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    assert len(gateway.charges) == 2
    assert store.payments[seeded.id].status is PaymentStatus.FAILED
    assert store.payments[seeded.id].gateway_attempts == 2
    assert len(webhooks.deliveries) == 1


async def test_crash_after_result_is_committed_never_calls_the_gateway_again(
    uow_factory, store, clock, seeded
) -> None:
    """Expected: 1 gateway call, webhook delivered on the redelivery."""
    process, gateway, webhooks = build(uow_factory, clock)

    def crash_after_result(commit_no: int) -> None:
        if commit_no == 2:  # notification attempt commit
            raise RuntimeError("process killed")

    store.before_commit = crash_after_result
    with pytest.raises(RuntimeError):
        await process(seeded.id)
    store.before_commit = None
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    assert gateway.charges == [seeded.id]
    assert len(webhooks.deliveries) == 1


# ----------------------------------------------------------- outbox re-publish + replay


async def test_republished_outbox_event_changes_nothing(uow_factory, store, clock, seeded) -> None:
    """Expected: the relay re-publishing an event after a crash is a plain NOOP for the consumer."""
    process, gateway, webhooks = build(uow_factory, clock)
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    for _ in range(3):  # the same event id delivered again and again
        assert await process(seeded.id) is ProcessOutcome.NOOP
    assert gateway.charges == [seeded.id]
    assert len(webhooks.deliveries) == 1


async def test_operator_replay_after_dlq_opens_exactly_one_new_budget(
    uow_factory, store, clock, seeded
) -> None:
    """Expected: 3 failed calls → halted; replay → 1 more call; a second replay is refused."""
    gateway = FakeGateway(*[GatewayTransportError("down")] * 3, SUCCESS)
    process, _, _ = build(uow_factory, clock, gateway=gateway)
    outcome = None
    while outcome is not ProcessOutcome.DEAD_LETTERED:
        outcome = await process(seeded.id)
        publish_all(store, clock)
    assert len(gateway.charges) == 3

    assert await replay_payment(uow_factory, clock, seeded.id) is ReplayAction.PROCESSING_RESUMED
    publish_all(store, clock)
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    assert len(gateway.charges) == 4
    assert store.payments[seeded.id].status is PaymentStatus.SUCCEEDED
    assert await recover_stalled_payments(uow_factory, clock, grace=timedelta(0)) == 0
    from payments.domain.errors import InvalidTransition

    with pytest.raises(InvalidTransition):
        await replay_payment(uow_factory, clock, seeded.id)


async def test_lost_last_attempt_is_halted_as_unknown_once_the_lease_expires(
    uow_factory, store, clock, seeded
) -> None:
    """Expected: 3 calls, then no 4th; reason says "unknown", not "unavailable"."""
    seeded.gateway_attempts = 3
    seeded.claim_processing(uuid.uuid4(), clock.now(), LEASE)  # the dead worker's reservation
    store.payments[seeded.id] = seeded
    process, gateway, _ = build(uow_factory, clock)
    assert await process(seeded.id) is ProcessOutcome.SKIPPED_LEASED  # still reserved
    clock.advance(LEASE + timedelta(seconds=1))
    assert await process(seeded.id) is ProcessOutcome.DEAD_LETTERED
    assert gateway.charges == []
    assert (
        store.payments[seeded.id].processing_halt_reason == "gateway_outcome_unknown_after_budget"
    )
