"""Publisher confirms, mandatory routing, consumer ack/reject and DLQ on a real broker."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import timedelta

import pytest
from faststream.rabbit import RabbitBroker

from payments.application.ports import PublishError
from payments.application.process_payment import ProcessPayment
from payments.application.relay import OutboxRelay
from payments.application.retry import RetryPolicy
from payments.domain.events import (
    DEAD_LETTER_QUEUE,
    PAYMENTS_NEW_QUEUE,
    Phase,
    payment_created_event,
    payment_dead_letter_event,
)
from payments.domain.payment import NotificationStatus, PaymentStatus
from payments.messaging.consumer import register_consumer
from payments.messaging.publisher import RabbitEventPublisher
from payments.messaging.topology import dead_letter_queue, payments_exchange, payments_new_queue
from tests.fakes import SUCCESS, TRANSIENT, FakeClock, FakeGateway, FakeWebhookSender, make_payment

pytestmark = [pytest.mark.rabbit, pytest.mark.integration]
QUEUES = {PAYMENTS_NEW_QUEUE: payments_new_queue, DEAD_LETTER_QUEUE: dead_letter_queue}


async def _get_message(broker: RabbitBroker, queue: str, *, deadline_s: float = 5.0):  # type: ignore[no-untyped-def]
    q = await broker.declare_queue(QUEUES[queue])
    deadline = asyncio.get_running_loop().time() + deadline_s
    while asyncio.get_running_loop().time() < deadline:
        msg = await q.get(fail=False, timeout=1)
        if msg is not None:
            await msg.ack()
            return msg
        await asyncio.sleep(0.05)
    raise AssertionError(f"no message in {queue} within {deadline_s}s")


async def _wait_until(predicate, *, deadline_s: float = 10.0):  # type: ignore[no-untyped-def]
    deadline = asyncio.get_running_loop().time() + deadline_s
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


async def test_confirmed_publish_lands_in_queue_as_persistent(broker: RabbitBroker) -> None:
    payment = make_payment()
    event = payment_created_event(payment, now=FakeClock().now())
    await RabbitEventPublisher(broker).publish(event)
    msg = await _get_message(broker, PAYMENTS_NEW_QUEUE)
    assert msg.delivery_mode == 2  # persistent
    assert msg.message_id == str(event.id)
    assert json.loads(msg.body)["payment_id"] == str(payment.id)


async def test_unroutable_publish_raises_and_is_not_marked(broker: RabbitBroker) -> None:
    payment = make_payment()
    event = payment_created_event(payment, now=FakeClock().now())
    event.routing_key = "payments.nowhere"
    with pytest.raises(PublishError, match="unroutable"):
        await RabbitEventPublisher(broker).publish(event)


async def test_dead_letter_event_is_routed_to_dlq(broker: RabbitBroker) -> None:
    payment = make_payment()
    event = payment_dead_letter_event(
        payment,
        phase=Phase.NOTIFY,
        attempts=3,
        failure_code="http_500",
        original_event_id=uuid.uuid4(),
        now=FakeClock().now(),
    )
    await RabbitEventPublisher(broker).publish(event)
    msg = await _get_message(broker, DEAD_LETTER_QUEUE)
    body = json.loads(msg.body)
    assert body["event_type"] == "payment.dead_letter"
    assert body["counted_attempts"] == 3


# ------------------------------------------------------------ full consumer
# ``uow_factory`` here is the real PostgreSQL one (see tests/rabbit/conftest.py).


async def test_consumer_processes_message_end_to_end(
    broker: RabbitBroker, uow_factory, clock
) -> None:
    payment = make_payment()
    async with uow_factory() as uow:
        await uow.payments.add(payment)
        await uow.outbox.add(payment_created_event(payment, now=clock.now()))
        await uow.commit()

    gateway, webhooks = FakeGateway(SUCCESS), FakeWebhookSender()
    process = ProcessPayment(
        uow_factory=uow_factory,
        gateway=gateway,
        webhooks=webhooks,
        clock=clock,
        retry_policy=RetryPolicy(),
        processing_lease=timedelta(seconds=60),
    )
    register_consumer(broker, process)
    await broker.start()

    relay = OutboxRelay(
        uow_factory=uow_factory,
        publisher=RabbitEventPublisher(broker),
        clock=clock,
        lease=timedelta(seconds=30),
    )
    assert await relay.run_once() == 1

    async def delivered() -> bool:
        async with uow_factory() as uow:
            p = await uow.payments.get(payment.id)
        return p is not None and p.notification_status is NotificationStatus.DELIVERED

    await _wait_until(delivered)
    assert gateway.charges == [payment.id]
    assert len(webhooks.deliveries) == 1

    # Redelivering the same event is harmless.
    await RabbitEventPublisher(broker).publish(payment_created_event(payment, now=clock.now()))
    await asyncio.sleep(0.5)
    assert gateway.charges == [payment.id]
    assert len(webhooks.deliveries) == 1


async def test_consumer_retry_round_trips_through_outbox_and_queue(
    broker: RabbitBroker, uow_factory, clock
) -> None:
    payment = make_payment()
    async with uow_factory() as uow:
        await uow.payments.add(payment)
        await uow.outbox.add(payment_created_event(payment, now=clock.now()))
        await uow.commit()

    gateway, webhooks = FakeGateway(SUCCESS), FakeWebhookSender(TRANSIENT, None)
    process = ProcessPayment(
        uow_factory=uow_factory,
        gateway=gateway,
        webhooks=webhooks,
        clock=clock,
        retry_policy=RetryPolicy(base_delay=timedelta(milliseconds=10)),
        processing_lease=timedelta(seconds=60),
    )
    register_consumer(broker, process)
    await broker.start()
    relay = OutboxRelay(
        uow_factory=uow_factory,
        publisher=RabbitEventPublisher(broker),
        clock=clock,
        lease=timedelta(seconds=30),
    )
    stop = asyncio.Event()
    relay_task = asyncio.create_task(relay.run_forever(stop))
    try:

        async def delivered() -> bool:
            # The fake clock stands still; advance it so the scheduled retry becomes due.
            clock.advance(timedelta(milliseconds=20))
            async with uow_factory() as uow:
                p = await uow.payments.get(payment.id)
            return p is not None and p.notification_status is NotificationStatus.DELIVERED

        await _wait_until(delivered)
    finally:
        stop.set()
        await relay_task
    assert gateway.charges == [payment.id]
    assert len(webhooks.deliveries) == 2
    async with uow_factory() as uow:
        saved = await uow.payments.get(payment.id)
    assert saved is not None and saved.status is PaymentStatus.SUCCEEDED
    assert saved.notification_attempts == 2


async def test_unknown_payment_is_rejected_to_dlq(broker: RabbitBroker, uow_factory, clock) -> None:
    process = ProcessPayment(
        uow_factory=uow_factory,
        gateway=FakeGateway(),
        webhooks=FakeWebhookSender(),
        clock=clock,
        retry_policy=RetryPolicy(),
        processing_lease=timedelta(seconds=60),
    )
    register_consumer(broker, process)
    await broker.start()
    ghost = make_payment()  # never stored
    await RabbitEventPublisher(broker).publish(payment_created_event(ghost, now=clock.now()))
    msg = await _get_message(broker, DEAD_LETTER_QUEUE)
    assert json.loads(msg.body)["payment_id"] == str(ghost.id)
    assert msg.headers.get("x-death")  # dead-lettered by the broker, not published by us


async def test_malformed_message_is_rejected_to_dlq(
    broker: RabbitBroker, uow_factory, clock
) -> None:
    process = ProcessPayment(
        uow_factory=uow_factory,
        gateway=FakeGateway(),
        webhooks=FakeWebhookSender(),
        clock=clock,
        retry_policy=RetryPolicy(),
        processing_lease=timedelta(seconds=60),
    )
    register_consumer(broker, process)
    await broker.start()
    await broker.publish(
        {"garbage": True}, exchange=payments_exchange, routing_key=PAYMENTS_NEW_QUEUE
    )
    msg = await _get_message(broker, DEAD_LETTER_QUEUE)
    assert json.loads(msg.body) == {"garbage": True}
