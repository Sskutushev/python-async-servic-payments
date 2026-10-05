"""Behavioural tests of the consumer use case against in-memory ports.

Every test states the guarantee it protects in its name.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from payments.application.ports import GatewayTransportError, WebhookDeliveryError
from payments.application.process_payment import ProcessOutcome, ProcessPayment
from payments.application.retry import RetryPolicy
from payments.domain.events import EventType, Phase
from payments.domain.payment import NotificationStatus, PaymentStatus
from tests.fakes import (
    DECLINED,
    PERMANENT,
    SUCCESS,
    TRANSIENT,
    FakeClock,
    FakeGateway,
    FakeWebhookSender,
    InMemoryStore,
    make_payment,
)

LEASE = timedelta(seconds=60)


def build(
    uow_factory,  # type: ignore[no-untyped-def]
    clock: FakeClock,
    *,
    gateway: FakeGateway | None = None,
    webhooks: FakeWebhookSender | None = None,
    retry: RetryPolicy | None = None,
) -> tuple[ProcessPayment, FakeGateway, FakeWebhookSender]:
    gateway = gateway or FakeGateway(SUCCESS)
    webhooks = webhooks or FakeWebhookSender()
    use_case = ProcessPayment(
        uow_factory=uow_factory,
        gateway=gateway,
        webhooks=webhooks,
        clock=clock,
        retry_policy=retry or RetryPolicy(),
        processing_lease=LEASE,
    )
    return use_case, gateway, webhooks


@pytest.fixture
def seeded(store: InMemoryStore):  # type: ignore[no-untyped-def]
    payment = make_payment()
    store.payments[payment.id] = payment
    return payment


# ----------------------------------------------------------------- happy paths


async def test_success_path_stores_result_and_delivers_signed_event(
    uow_factory, store, clock, seeded
) -> None:
    process, gateway, webhooks = build(uow_factory, clock)
    outcome = await process(seeded.id)

    assert outcome is ProcessOutcome.COMPLETED
    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.SUCCEEDED
    assert saved.processed_at == clock.now()
    assert saved.notification_status is NotificationStatus.DELIVERED
    assert saved.notification_attempts == 1
    assert gateway.charges == [seeded.id]
    [(url, event_id, body)] = webhooks.deliveries
    assert url == seeded.webhook_url
    assert event_id == saved.notification_event_id
    assert body["status"] == "succeeded"
    assert not store.unpublished()


async def test_decline_is_a_business_result_not_a_failure(
    uow_factory, store, clock, seeded
) -> None:
    process, _, webhooks = build(uow_factory, clock, gateway=FakeGateway(DECLINED))
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.FAILED
    assert saved.failure_code == "declined"
    assert webhooks.deliveries[0][2]["event_type"] == "payment.failed"
    assert not store.unpublished()  # no retry, no DLQ


# -------------------------------------------------------- idempotent redelivery


async def test_duplicate_message_after_completion_is_a_noop(
    uow_factory, store, clock, seeded
) -> None:
    process, gateway, webhooks = build(uow_factory, clock)
    await process(seeded.id)
    assert await process(seeded.id) is ProcessOutcome.NOOP
    assert gateway.charges == [seeded.id]
    assert len(webhooks.deliveries) == 1


async def test_webhook_failure_never_reprocesses_the_payment(
    uow_factory, store, clock, seeded
) -> None:
    """The headline scenario: result stored, webhook 500, retry... gateway charged exactly once."""
    process, gateway, webhooks = build(
        uow_factory, clock, webhooks=FakeWebhookSender(TRANSIENT, TRANSIENT, None)
    )
    assert await process(seeded.id) is ProcessOutcome.RETRY_SCHEDULED
    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.SUCCEEDED
    assert saved.notification_attempts == 1
    assert saved.notification_last_error == "http_500"
    [retry] = store.unpublished()
    assert retry.event_type is EventType.PAYMENT_RETRY
    assert retry.payload["phase"] == Phase.NOTIFY
    assert retry.payload["attempt"] == 2
    assert retry.available_at == clock.now() + timedelta(seconds=1)

    # Simulate the relay publishing and the retry arriving on time.
    retry.published_at = clock.now()
    clock.advance(timedelta(seconds=1))
    assert await process(seeded.id) is ProcessOutcome.RETRY_SCHEDULED
    [retry2] = store.unpublished()
    assert retry2.payload["attempt"] == 3
    assert retry2.available_at == clock.now() + timedelta(seconds=2)

    retry2.published_at = clock.now()
    clock.advance(timedelta(seconds=2))
    assert await process(seeded.id) is ProcessOutcome.COMPLETED

    assert gateway.charges == [seeded.id]  # charged exactly once across three messages
    assert len(webhooks.deliveries) == 3
    assert len({d[1] for d in webhooks.deliveries}) == 1  # same event_id every time
    assert store.payments[seeded.id].notification_status is NotificationStatus.DELIVERED


async def test_retry_arriving_early_is_acked_without_sending(
    uow_factory, store, clock, seeded
) -> None:
    process, _, webhooks = build(uow_factory, clock, webhooks=FakeWebhookSender(TRANSIENT))
    await process(seeded.id)
    assert await process(seeded.id) is ProcessOutcome.RESULT_STORED  # not due yet
    assert len(webhooks.deliveries) == 1


# ---------------------------------------------------------------- DLQ paths


async def test_third_failed_attempt_exhausts_and_dead_letters(
    uow_factory, store, clock, seeded
) -> None:
    process, _, webhooks = build(
        uow_factory, clock, webhooks=FakeWebhookSender(TRANSIENT, TRANSIENT, TRANSIENT, None)
    )
    for _ in range(2):
        assert await process(seeded.id) is ProcessOutcome.RETRY_SCHEDULED
        [retry] = store.unpublished()
        retry.published_at = clock.now()
        clock.advance(retry.available_at - clock.now())
    assert await process(seeded.id) is ProcessOutcome.DEAD_LETTERED

    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.SUCCEEDED  # never downgraded because of the webhook
    assert saved.notification_status is NotificationStatus.EXHAUSTED
    assert saved.notification_attempts == 3
    [dead] = store.unpublished()
    assert dead.event_type is EventType.PAYMENT_DEAD_LETTER
    assert dead.exchange == "payments.dead"
    assert dead.routing_key == "payments.failed"
    assert dead.payload["counted_attempts"] == 3
    assert dead.payload["failure_code"] == "http_500"
    assert "webhook_url" not in dead.payload
    assert len(webhooks.deliveries) == 3

    # A late duplicate does nothing.
    assert await process(seeded.id) is ProcessOutcome.NOOP
    assert len(webhooks.deliveries) == 3


async def test_permanent_webhook_error_dead_letters_immediately(
    uow_factory, store, clock, seeded
) -> None:
    process, _, _ = build(uow_factory, clock, webhooks=FakeWebhookSender(PERMANENT))
    assert await process(seeded.id) is ProcessOutcome.DEAD_LETTERED
    assert store.payments[seeded.id].notification_attempts == 1
    assert store.payments[seeded.id].notification_status is NotificationStatus.EXHAUSTED


async def test_retry_after_from_receiver_extends_the_delay(
    uow_factory, store, clock, seeded
) -> None:
    slow = WebhookDeliveryError("http_503", retryable=True, retry_after=timedelta(seconds=30))
    process, _, _ = build(uow_factory, clock, webhooks=FakeWebhookSender(slow))
    await process(seeded.id)
    [retry] = store.unpublished()
    assert retry.available_at == clock.now() + timedelta(seconds=30)


# ------------------------------------------------------- gateway unavailable


async def test_gateway_transport_error_keeps_payment_pending_and_retries(
    uow_factory, store, clock, seeded
) -> None:
    process, gateway, _ = build(
        uow_factory, clock, gateway=FakeGateway(GatewayTransportError("timeout"), SUCCESS)
    )
    assert await process(seeded.id) is ProcessOutcome.RETRY_SCHEDULED
    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.PENDING
    assert saved.gateway_attempts == 1
    assert saved.processing_lease_token is None  # lease released for the retry
    [retry] = store.unpublished()
    assert retry.payload["phase"] == Phase.PROCESS
    assert retry.payload["attempt"] == 2

    retry.published_at = clock.now()
    clock.advance(timedelta(seconds=1))
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    assert gateway.charges == [seeded.id, seeded.id]


async def test_gateway_unreachable_three_times_dead_letters_but_stays_pending(
    uow_factory, store, clock, seeded
) -> None:
    errors = [GatewayTransportError("down")] * 3
    process, _, _ = build(uow_factory, clock, gateway=FakeGateway(*errors))
    for _ in range(2):
        assert await process(seeded.id) is ProcessOutcome.RETRY_SCHEDULED
        [retry] = store.unpublished()
        retry.published_at = clock.now()
        clock.advance(timedelta(seconds=5))
    assert await process(seeded.id) is ProcessOutcome.DEAD_LETTERED
    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.PENDING  # unknown outcome is not a business failure
    assert saved.gateway_attempts == 3
    [dead] = store.unpublished()
    assert dead.payload["phase"] == Phase.PROCESS
    assert dead.payload["payment_status"] == "pending"


# ------------------------------------------------------------ leases / races


async def test_concurrent_duplicate_is_skipped_while_lease_is_held(
    uow_factory, store, clock, seeded
) -> None:
    process, gateway, _ = build(uow_factory, clock)
    seeded.claim_processing(uuid.uuid4(), clock.now(), LEASE)
    store.payments[seeded.id] = seeded
    assert await process(seeded.id) is ProcessOutcome.SKIPPED_LEASED
    assert gateway.charges == []


async def test_expired_lease_can_be_taken_over(uow_factory, store, clock, seeded) -> None:
    process, gateway, _ = build(uow_factory, clock)
    seeded.claim_processing(uuid.uuid4(), clock.now() - LEASE * 2, LEASE)
    store.payments[seeded.id] = seeded
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    assert gateway.charges == [seeded.id]


async def test_stale_worker_cannot_overwrite_after_takeover(
    uow_factory, store, clock, seeded
) -> None:
    """Fencing: worker A's lease expires mid-charge and B takes it over; A's late result is dropped."""
    taken_over_by = uuid.uuid4()

    class SlowGateway:
        async def charge(self, payment):  # type: ignore[no-untyped-def]
            # While A waits on the gateway its lease expires and B claims the payment.
            clock.advance(LEASE * 2)
            current = store.payments[payment.id]
            assert current.claim_processing(taken_over_by, clock.now(), LEASE)
            return DECLINED

    process_a, _, webhooks_a = build(uow_factory, clock, gateway=SlowGateway())  # type: ignore[arg-type]
    assert await process_a(seeded.id) is ProcessOutcome.SKIPPED_STALE
    saved = store.payments[seeded.id]
    assert saved.status is PaymentStatus.PENDING  # A's DECLINED was not written
    assert saved.processing_lease_token == taken_over_by
    assert webhooks_a.deliveries == []


async def test_late_result_after_another_worker_finished_is_dropped(
    uow_factory, store, clock, seeded
) -> None:
    class SlowGateway:
        async def charge(self, payment):  # type: ignore[no-untyped-def]
            clock.advance(LEASE * 2)
            await process_b(payment.id)  # B takes over and completes with SUCCESS
            return DECLINED

    process_a, _, webhooks_a = build(uow_factory, clock, gateway=SlowGateway())  # type: ignore[arg-type]
    process_b, _, webhooks_b = build(uow_factory, clock, gateway=FakeGateway(SUCCESS))
    assert await process_a(seeded.id) is ProcessOutcome.NOOP
    assert store.payments[seeded.id].status is PaymentStatus.SUCCEEDED  # B's result stands
    assert webhooks_a.deliveries == []
    assert len(webhooks_b.deliveries) == 1


async def test_unknown_payment_is_poison(uow_factory, clock) -> None:
    process, _, _ = build(uow_factory, clock)
    assert await process(uuid.uuid4()) is ProcessOutcome.POISON


# --------------------------------------------------------------- crash windows


async def test_crash_after_result_before_webhook_is_resumed_by_redelivery(
    uow_factory, store, clock, seeded
) -> None:
    process, gateway, webhooks = build(uow_factory, clock)

    def crash_on_notification_claim(commit_no: int) -> None:
        if commit_no == 2:  # 0: lease, 1: result, 2: notification attempt
            raise RuntimeError("process killed")

    store.before_commit = crash_on_notification_claim
    with pytest.raises(RuntimeError):
        await process(seeded.id)
    assert store.payments[seeded.id].status is PaymentStatus.SUCCEEDED
    assert webhooks.deliveries == []

    store.before_commit = None
    assert await process(seeded.id) is ProcessOutcome.COMPLETED  # broker redelivers (no ack)
    assert gateway.charges == [seeded.id]
    assert len(webhooks.deliveries) == 1


async def test_crash_after_webhook_accepted_before_commit_redelivers_same_event(
    uow_factory, store, clock, seeded
) -> None:
    process, gateway, webhooks = build(uow_factory, clock)

    def crash_on_delivered_commit(commit_no: int) -> None:
        if commit_no == 3:
            raise RuntimeError("process killed")

    store.before_commit = crash_on_delivered_commit
    with pytest.raises(RuntimeError):
        await process(seeded.id)
    assert len(webhooks.deliveries) == 1
    saved = store.payments[seeded.id]
    assert saved.notification_status is NotificationStatus.PENDING
    assert saved.notification_attempts == 1  # counted before the call

    store.before_commit = None
    clock.advance(LEASE)  # the in-flight attempt lease has passed
    assert await process(seeded.id) is ProcessOutcome.COMPLETED
    assert gateway.charges == [seeded.id]
    assert [d[1] for d in webhooks.deliveries] == [
        saved.notification_event_id
    ] * 2  # receiver dedups by id
