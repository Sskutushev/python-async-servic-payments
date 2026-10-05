"""Real RabbitMQ fixtures. Skipped when TEST_RABBITMQ_URL is unreachable."""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import pytest
from faststream.rabbit import Channel, RabbitBroker

from payments.messaging.topology import dead_letter_queue, declare_topology, payments_new_queue

# The consumer tests need real PostgreSQL too: reuse the integration fixtures here.
from tests.integration.conftest import (  # noqa: F401
    database_url,
    engine,
    session_factory,
    uow_factory,
)

TEST_RABBITMQ_URL = os.environ.get("TEST_RABBITMQ_URL", "amqp://payments:payments@127.0.0.1:5672/")

pytestmark = pytest.mark.rabbit


def _reachable(url: str) -> bool:
    parts = urlsplit(url)
    try:
        with socket.create_connection((parts.hostname or "127.0.0.1", parts.port or 5672), 1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    if not _reachable(TEST_RABBITMQ_URL):
        pytest.skip(f"RabbitMQ not reachable at {TEST_RABBITMQ_URL}")
    return TEST_RABBITMQ_URL


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[RabbitBroker]:
    broker = RabbitBroker(
        rabbitmq_url,
        default_channel=Channel(prefetch_count=4, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=5,
    )
    await broker.connect()
    await declare_topology(broker)
    for spec in (payments_new_queue, dead_letter_queue):
        queue = await broker.declare_queue(spec)
        await queue.purge()
    try:
        yield broker
    finally:
        await broker.stop()
