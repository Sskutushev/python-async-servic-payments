"""The one and only consumer of ``payments.new``.

It runs ``ProcessPayment`` and then tells RabbitMQ what to do with the message:

* the work is done, or a retry / dead letter is saved in the outbox -> ACK;
* the message is broken or names a payment we do not have -> REJECT (RabbitMQ moves
  it to ``payments.dlq``);
* our own infrastructure failed (database down, ...) -> NACK and requeue after a short
  pause. Our outage must not use up the payment's attempts.

Acknowledgements are sent by hand on purpose: a message is only acknowledged after the
database change that makes it unnecessary has been committed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from payments.application.process_payment import ProcessOutcome, ProcessPayment
from payments.messaging.envelope import PaymentEnvelope
from payments.messaging.topology import payments_exchange, payments_new_queue

log = logging.getLogger(__name__)
INFRA_FAILURE_PAUSE = 1.0

Handler = Callable[[RabbitMessage], Awaitable[None]]


def build_handler(process: ProcessPayment, *, pause: float = INFRA_FAILURE_PAUSE) -> Handler:
    async def handle(message: RabbitMessage) -> None:
        started = time.perf_counter()
        try:
            envelope = PaymentEnvelope.model_validate_json(message.body)
        except ValidationError:
            log.warning("malformed message rejected", extra={"event_id": message.message_id})
            await message.reject(requeue=False)
            return

        extra = {
            "payment_id": str(envelope.payment_id),
            "event_id": str(envelope.event_id),
            "phase": envelope.phase.value,
            "attempt": envelope.attempt,
        }
        try:
            outcome = await process(envelope.payment_id, event_id=envelope.event_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("infrastructure failure while processing", extra=extra)
            await asyncio.sleep(pause)
            await message.nack(requeue=True)
            return

        log.info(
            "message handled",
            extra={
                **extra,
                "outcome": outcome.value,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            },
        )
        if outcome is ProcessOutcome.POISON:
            await message.reject(requeue=False)
        else:
            await message.ack()

    return handle


def register_consumer(broker: RabbitBroker, process: ProcessPayment) -> None:
    broker.subscriber(
        payments_new_queue,
        payments_exchange,
        ack_policy=AckPolicy.MANUAL,
        title="payments.new consumer",
        description="Charges the gateway, stores the result, delivers the webhook.",
    )(build_handler(process))
