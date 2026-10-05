"""Composition root shared by the API and the consumer.

Builds the long-lived resources (engine, HTTP client) and wires the adapters
into the use cases. Nothing else in the codebase instantiates adapters.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.application.create_payment import CreatePayment
from payments.application.ports import Clock, UnitOfWorkFactory
from payments.application.process_payment import ProcessPayment
from payments.application.retry import RetryPolicy
from payments.infrastructure.clock import SystemClock
from payments.infrastructure.db import create_engine, create_session_factory
from payments.infrastructure.gateway import SimulatedGateway
from payments.infrastructure.repositories import SqlUnitOfWork
from payments.infrastructure.url_policy import WebhookUrlPolicy
from payments.infrastructure.webhooks import HttpWebhookSender, build_http_client
from payments.settings import Settings


@dataclass(slots=True)
class Container:
    settings: Settings
    engine: AsyncEngine | None  # None only in tests that run on in-memory persistence
    session_factory: async_sessionmaker[AsyncSession] | None
    uow_factory: UnitOfWorkFactory
    clock: Clock
    retry_policy: RetryPolicy
    url_policy: WebhookUrlPolicy
    http_client: httpx.AsyncClient | None = None

    def create_payment(self) -> CreatePayment:
        return CreatePayment(self.uow_factory, self.clock)

    def process_payment(self) -> ProcessPayment:
        assert self.http_client is not None, "process_payment needs the HTTP client"
        s = self.settings
        gateway = SimulatedGateway(
            seed=s.gateway_seed.get_secret_value(),
            success_rate=s.gateway_success_rate,
            min_delay=s.gateway_min_delay_seconds,
            max_delay=s.gateway_max_delay_seconds,
            sleep=asyncio.sleep,
        )
        sender = HttpWebhookSender(
            self.http_client,
            secret=s.webhook_secret.get_secret_value(),
            policy=self.url_policy,
            clock=self.clock,
            max_response_bytes=s.webhook_max_response_bytes,
        )
        return ProcessPayment(
            uow_factory=self.uow_factory,
            gateway=gateway,
            webhooks=sender,
            clock=self.clock,
            retry_policy=self.retry_policy,
            processing_lease=s.processing_lease,
            notification_lease=timedelta(seconds=max(s.webhook_timeout_seconds * 3, 15)),
        )


def build_url_policy(settings: Settings) -> WebhookUrlPolicy:
    return WebhookUrlPolicy(
        allowed_hosts=tuple(settings.webhook_allowed_hosts),
        allow_private_networks=settings.webhook_allow_private_networks,
        allow_insecure_http=settings.webhook_allow_insecure_http,
    )


@asynccontextmanager
async def build_container(
    settings: Settings, *, with_http: bool = False
) -> AsyncIterator[Container]:
    engine = create_engine(settings.database_url, pool_size=settings.database_pool_size)
    session_factory = create_session_factory(engine)
    http_client = build_http_client(settings.webhook_timeout_seconds) if with_http else None
    container = Container(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        uow_factory=lambda: SqlUnitOfWork(session_factory),
        clock=SystemClock(),
        retry_policy=RetryPolicy(
            max_attempts=settings.retry_max_attempts,
            base_delay=settings.retry_base_delay,
            max_delay=settings.retry_max_delay,
        ),
        url_policy=build_url_policy(settings),
        http_client=http_client,
    )
    try:
        yield container
    finally:
        if http_client is not None:
            await http_client.aclose()
        await engine.dispose()
