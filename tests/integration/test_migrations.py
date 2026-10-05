"""Migrations are reversible and the ORM metadata matches the migrated schema."""

from __future__ import annotations

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from payments.infrastructure.models import Base
from tests.integration.conftest import alembic_config


def _sync_url(url: str) -> str:
    return url.replace("+asyncpg", "+psycopg") if "+asyncpg" in url else url


def test_downgrade_and_upgrade_roundtrip(database_url: str) -> None:
    config = alembic_config(database_url)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    command.downgrade(config, "base")
    command.upgrade(config, "head")


def test_models_match_migrations(database_url: str) -> None:
    """``alembic revision --autogenerate`` would be empty: no drift between ORM and migrations."""
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    async def diff() -> list[object]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as conn:
                return await conn.run_sync(
                    lambda sync_conn: compare_metadata(
                        MigrationContext.configure(sync_conn, opts={"compare_type": True}),
                        Base.metadata,
                    )
                )
        finally:
            await engine.dispose()

    assert asyncio.run(diff()) == []


def test_expected_indexes_exist(database_url: str) -> None:
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    async def names() -> set[str]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as conn:
                return await conn.run_sync(
                    lambda c: (
                        {i["name"] for i in inspect(c).get_indexes("outbox")}
                        | {i["name"] for i in inspect(c).get_indexes("payments")}
                    )
                )
        finally:
            await engine.dispose()

    assert {
        "ix_outbox_unpublished",
        "ix_outbox_aggregate_unpublished",
        "ix_payments_pending_work",
    } <= asyncio.run(names())


__all__ = ["_sync_url", "create_engine"]
