"""Publishes outbox events to RabbitMQ.

We wait for the broker to confirm every message, and ``mandatory=True`` makes the broker
return a message that no queue would receive. Both cases become ``PublishError``, so an
event is never marked as published unless a queue really got it.
"""

from __future__ import annotations

import asyncio
import logging

from aio_pika.exceptions import AMQPError, DeliveryError
from faststream.rabbit import RabbitBroker

from payments.application.ports import PublishError
from payments.domain.events import OutboxEvent
from payments.messaging.topology import EXCHANGES

log = logging.getLogger(__name__)


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, *, timeout: float = 10.0) -> None:
        self._broker = broker
        self._timeout = timeout

    async def publish(self, event: OutboxEvent) -> None:
        exchange = EXCHANGES.get(event.exchange)
        if exchange is None:
            raise PublishError("unknown_exchange")
        try:
            await self._broker.publish(
                event.payload,
                exchange=exchange,
                routing_key=event.routing_key,
                persist=True,
                mandatory=True,
                message_id=str(event.id),
                correlation_id=str(event.aggregate_id),
                message_type=event.event_type.value,
                headers={"schema_version": event.schema_version},
                timeout=self._timeout,
            )
        except DeliveryError as exc:  # no queue for the message, or the broker refused it
            raise PublishError("unroutable_or_nacked") from exc
        except (AMQPError, TimeoutError, ConnectionError, OSError) as exc:
            raise PublishError("broker_unavailable") from exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("unexpected publish failure", extra={"event_id": str(event.id)})
            raise PublishError("publish_failed") from exc
