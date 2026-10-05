"""The one subscriber. Maps ``ProcessOutcome`` to explicit broker acknowledgements:

* every business outcome (including retry/DLQ, which are durable outbox rows) -> ACK;
* unknown payment / malformed envelope -> REJECT (broker dead-letters it to ``payments.dlq``);
* infrastructure failure (DB down, ...) -> NACK with requeue after a short pause,
  so the attempt budget is not consumed by our own outage.

Acknowledgement is manual on purpose: nothing is acked before the state change
that makes the message redundant has been committed.
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
