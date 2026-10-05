"""Consumer process: FastStream app + supervised outbox relay + recovery scan.

The relay runs here (not in the API) so the API never needs a broker
connection: it only writes to PostgreSQL. A dead background task takes the
whole process down so the orchestrator restarts it instead of silently stalling.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker

from payments.application.ports import Clock, UnitOfWorkFactory
from payments.application.recovery import recover_stalled_payments
from payments.application.relay import OutboxRelay
from payments.bootstrap import Container, build_container
from payments.logging_setup import configure_logging
from payments.messaging.consumer import register_consumer
from payments.messaging.publisher import RabbitEventPublisher
from payments.messaging.topology import declare_topology
from payments.settings import Settings, load_settings

log = logging.getLogger(__name__)


async def recovery_loop(
    uow_factory: UnitOfWorkFactory, clock: Clock, settings: Settings, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            await recover_stalled_payments(uow_factory, clock, grace=settings.recovery_grace)
        except Exception:
            log.exception("recovery scan failed")
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=settings.recovery_interval_seconds)


def build_broker(settings: Settings) -> RabbitBroker:
    # ``on_return_raises``: with ``mandatory=True`` an unroutable message must raise,
    # otherwise the relay would mark an event published that no queue received.
    return RabbitBroker(
        settings.rabbitmq_url,
        graceful_timeout=30.0,
        default_channel=Channel(
            prefetch_count=settings.consumer_prefetch,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )


def build_consumer_app(
    settings: Settings, container: Container, broker: RabbitBroker
) -> FastStream:
    register_consumer(broker, container.process_payment())
    app = FastStream(broker)

    stop = asyncio.Event()
    tasks: list[asyncio.Task[None]] = []

    @app.after_startup
    async def _start_background() -> None:
        await declare_topology(broker)
        relay = OutboxRelay(
            uow_factory=container.uow_factory,
            publisher=RabbitEventPublisher(broker),
            clock=container.clock,
            lease=settings.outbox_lease,
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval_seconds,
        )
        tasks.append(asyncio.create_task(relay.run_forever(stop), name="outbox-relay"))
        tasks.append(
            asyncio.create_task(
                recovery_loop(container.uow_factory, container.clock, settings, stop),
                name="recovery-scan",
            )
        )
        for task in tasks:
            task.add_done_callback(_crash_process_on_unexpected_exit)
        log.info("consumer started")

    @app.on_shutdown
    async def _stop_background() -> None:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("consumer stopped")

    return app


async def run_consumer(settings: Settings | None = None) -> None:
    settings = settings or load_settings()
    configure_logging(settings.log_level)
    async with build_container(settings, with_http=True) as container:
        app = build_consumer_app(settings, container, build_broker(settings))
        await app.run()


def _crash_process_on_unexpected_exit(task: asyncio.Task[None]) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.critical("background task died: %s", task.get_name(), exc_info=exc)
        raise SystemExit(1)
