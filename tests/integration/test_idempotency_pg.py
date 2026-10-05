"""Idempotency under real PostgreSQL concurrency (READ COMMITTED, ON CONFLICT DO NOTHING)."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from payments.application.create_payment import CreatePayment, CreatePaymentCommand
from payments.domain.errors import IdempotencyConflict
from payments.domain.money import Currency
from payments.infrastructure.models import OutboxRow, PaymentRow
from payments.infrastructure.repositories import SqlUnitOfWork
from tests.fakes import FakeClock


def command(key: str = "race", **overrides: object) -> CreatePaymentCommand:
    base: dict[str, object] = {
        "idempotency_key": key,
        "amount": Decimal("100"),
        "currency": Currency.USD,
        "description": "order",
        "metadata": {"a": {"b": [1, 2]}},
        "webhook_url": "https://merchant.example/hooks",
    }
    base.update(overrides)
    return CreatePaymentCommand(**base)  # type: ignore[arg-type]


async def _counts(session_factory) -> tuple[int, int]:  # type: ignore[no-untyped-def]
    async with session_factory() as s:
        payments = (await s.execute(select(func.count()).select_from(PaymentRow))).scalar_one()
        events = (await s.execute(select(func.count()).select_from(OutboxRow))).scalar_one()
    return payments, events


async def test_twenty_concurrent_posts_create_one_payment_and_one_event(
    uow_factory, session_factory
) -> None:
    use_case = CreatePayment(uow_factory, FakeClock())
    results = await asyncio.gather(*(use_case(command()) for _ in range(20)))
    assert sum(r.created for r in results) == 1
    assert len({r.payment.id for r in results}) == 1
    assert await _counts(session_factory) == (1, 1)


async def test_same_key_different_body_one_winner_others_409(uow_factory, session_factory) -> None:
    use_case = CreatePayment(uow_factory, FakeClock())
    outcomes = await asyncio.gather(
        *(use_case(command(description=f"body-{i}")) for i in range(10)), return_exceptions=True
    )
    winners = [o for o in outcomes if not isinstance(o, BaseException)]
    conflicts = [o for o in outcomes if isinstance(o, IdempotencyConflict)]
    assert len(winners) == 1 and winners[0].created
    assert len(conflicts) == 9
    assert await _counts(session_factory) == (1, 1)


async def test_failure_between_payment_and_outbox_rolls_back_both(session_factory) -> None:
    class ExplodingOutbox:
        async def add(self, event):  # type: ignore[no-untyped-def]
            raise RuntimeError("outbox insert failed")

    class FaultyUnitOfWork(SqlUnitOfWork):
        async def __aenter__(self):  # type: ignore[no-untyped-def]
            await super().__aenter__()
            self.outbox = ExplodingOutbox()  # type: ignore[assignment]
            return self

    with pytest.raises(RuntimeError, match="outbox insert failed"):
        await CreatePayment(lambda: FaultyUnitOfWork(session_factory), FakeClock())(command())
    assert await _counts(session_factory) == (0, 0)


async def test_jsonb_and_numeric_roundtrip(uow_factory) -> None:
    use_case = CreatePayment(uow_factory, FakeClock())
    created = await use_case(command(amount=Decimal("0.10"), metadata={"z": 1, "a": {"n": None}}))
    async with uow_factory() as uow:
        loaded = await uow.payments.get(created.payment.id)
    assert loaded is not None
    assert loaded.money.amount == Decimal("0.10")
    assert str(loaded.money.amount) == "0.10"
    assert loaded.metadata == {"z": 1, "a": {"n": None}}
    assert loaded.created_at.tzinfo is not None
