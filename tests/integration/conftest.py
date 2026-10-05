"""Real PostgreSQL fixtures. Skipped (not failed) when TEST_DATABASE_URL is unreachable.

Schema is created by the real Alembic migrations, never by ``create_all``.
"""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.infrastructure.db import create_engine, create_session_factory
from payments.infrastructure.repositories import SqlUnitOfWork

DEFAULT_URL = "postgresql+asyncpg://payments:payments@127.0.0.1:5433/payments_test"
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", DEFAULT_URL)
ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


def _reachable(url: str) -> bool:
    parts = urlsplit(url)
    try:
        with socket.create_connection((parts.hostname or "127.0.0.1", parts.port or 5432), 1):
            return True
    except OSError:
        return False


def alembic_config(url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    os.environ["DATABASE_URL"] = url
    return config


async def _ensure_database(url: str) -> None:
    """Create the test database if it does not exist (connects to the maintenance DB)."""
    import asyncpg

    parts = urlsplit(url)
    dbname = parts.path.lstrip("/")
    conn = await asyncpg.connect(
        host=parts.hostname,
        port=parts.port,
        user=parts.username,
        password=parts.password,
        database="postgres",
    )
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname)
        if not exists:
            await conn.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def database_url() -> str:
    if not _reachable(TEST_DATABASE_URL):
        pytest.skip(f"PostgreSQL not reachable at {TEST_DATABASE_URL}")
    asyncio.run(_ensure_database(TEST_DATABASE_URL))
    config = alembic_config(TEST_DATABASE_URL)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    return TEST_DATABASE_URL


@pytest.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(database_url, pool_size=20)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE outbox, payments"))
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture
def uow_factory(session_factory: async_sessionmaker[AsyncSession]):  # type: ignore[no-untyped-def]
    return lambda: SqlUnitOfWork(session_factory)
