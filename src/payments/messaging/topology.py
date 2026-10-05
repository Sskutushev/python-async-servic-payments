"""Queues and exchanges in RabbitMQ.

    payments.events (direct, durable) --payments.new--> payments.new (durable, DLX -> payments.dead)
    payments.dead   (direct, durable) --payments.failed--> payments.dlq (durable)

Messages reach the dead-letter queue in two ways: the consumer rejects a broken or
unknown message (RabbitMQ moves it), or the service publishes a dead-letter event
through the outbox after the last failed attempt (with a readable explanation inside).
"""

from __future__ import annotations

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from payments.domain.events import (
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE,
    DEAD_LETTER_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_NEW_ROUTING_KEY,
)

payments_exchange = RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange(DEAD_LETTER_EXCHANGE, type=ExchangeType.DIRECT, durable=True)

payments_new_queue = RabbitQueue(
    PAYMENTS_NEW_QUEUE,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
    },
)
dead_letter_queue = RabbitQueue(
    DEAD_LETTER_QUEUE, durable=True, routing_key=DEAD_LETTER_ROUTING_KEY
)

EXCHANGES = {PAYMENTS_EXCHANGE: payments_exchange, DEAD_LETTER_EXCHANGE: dead_letter_exchange}


async def declare_topology(broker: RabbitBroker) -> None:
    """Safe to call on every start: declaring something that already exists changes nothing."""
    await broker.declare_exchange(payments_exchange)
    await broker.declare_exchange(dead_letter_exchange)
    new_q = await broker.declare_queue(payments_new_queue)
    await new_q.bind(PAYMENTS_EXCHANGE, routing_key=PAYMENTS_NEW_ROUTING_KEY)
    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(DEAD_LETTER_EXCHANGE, routing_key=DEAD_LETTER_ROUTING_KEY)
