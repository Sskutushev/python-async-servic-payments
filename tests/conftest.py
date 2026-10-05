from __future__ import annotations

from datetime import timedelta

import pytest

from payments.application.retry import RetryPolicy
from payments.settings import Environment, Settings
from tests.fakes import FakeClock, InMemoryStore, InMemoryUnitOfWork

TEST_API_KEY = "test-api-key-0123456789abcdef"
TEST_WEBHOOK_SECRET = "test-webhook-secret-0123456789"


def make_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "app_env": Environment.TEST,
        "api_key": TEST_API_KEY,
        "webhook_secret": TEST_WEBHOOK_SECRET,
        "database_url": "postgresql+asyncpg://unused:unused@localhost:1/unused",
        "rabbitmq_url": "amqp://unused:unused@localhost:1/",
        "webhook_allow_private_networks": True,
        "webhook_allow_insecure_http": True,
        "gateway_min_delay_seconds": 0,
        "gateway_max_delay_seconds": 0,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore[call-arg]


@pytest.fixture
def settings() -> Settings:
    return make_settings()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture
def uow_factory(store: InMemoryStore):  # type: ignore[no-untyped-def]
    return lambda: InMemoryUnitOfWork(store)


@pytest.fixture
def retry_policy() -> RetryPolicy:
    return RetryPolicy(
        max_attempts=3, base_delay=timedelta(seconds=1), max_delay=timedelta(seconds=60)
    )
