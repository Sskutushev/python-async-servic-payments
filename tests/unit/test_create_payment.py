import asyncio
from decimal import Decimal

import pytest

from payments.application.create_payment import CreatePayment, CreatePaymentCommand
from payments.domain.errors import IdempotencyConflict
from payments.domain.events import EventType
from payments.domain.money import Currency
from tests.fakes import FakeClock, InMemoryStore


def command(**overrides: object) -> CreatePaymentCommand:
    base: dict[str, object] = {
        "idempotency_key": "k1",
        "amount": Decimal("100"),
        "currency": Currency.USD,
        "description": "order",
        "metadata": {"a": 1},
        "webhook_url": "https://merchant.example/hooks",
    }
    base.update(overrides)
    return CreatePaymentCommand(**base)  # type: ignore[arg-type]


async def test_creates_payment_and_outbox_event_atomically(
    uow_factory, store: InMemoryStore, clock: FakeClock
) -> None:
    result = await CreatePayment(uow_factory, clock)(command())
    assert result.created
    payment = store.payments[result.payment.id]
    assert payment.money.amount == Decimal("100.00")
    assert payment.created_at == clock.now()
    [event] = store.outbox.values()
    assert event.event_type is EventType.PAYMENT_CREATED
    assert event.aggregate_id == payment.id
    assert event.routing_key == "payments.new"
    assert event.payload["payment_id"] == str(payment.id)
    assert store.commits == 1


async def test_same_key_same_body_replays_without_new_event(uow_factory, store, clock) -> None:
    use_case = CreatePayment(uow_factory, clock)
    first = await use_case(command())
    second = await use_case(command(metadata={"a": 1}))
    assert not second.created
    assert second.payment.id == first.payment.id
    assert second.payment.created_at == first.payment.created_at
    assert len(store.outbox) == 1


async def test_same_key_different_body_conflicts(uow_factory, clock) -> None:
    use_case = CreatePayment(uow_factory, clock)
    await use_case(command())
    with pytest.raises(IdempotencyConflict):
        await use_case(command(amount=Decimal("100.01")))


async def test_amount_formats_are_equivalent_for_idempotency(uow_factory, clock) -> None:
    use_case = CreatePayment(uow_factory, clock)
    first = await use_case(command(amount=Decimal("100")))
    second = await use_case(command(amount=Decimal("100.00")))
    assert second.payment.id == first.payment.id


async def test_concurrent_requests_with_one_key_create_one_payment(
    uow_factory, store, clock
) -> None:
    use_case = CreatePayment(uow_factory, clock)
    results = await asyncio.gather(*(use_case(command()) for _ in range(20)))
    assert sum(r.created for r in results) == 1
    assert len({r.payment.id for r in results}) == 1
    assert len(store.payments) == 1
    assert len(store.outbox) == 1


async def test_failed_transaction_leaves_nothing_behind(uow_factory, store, clock) -> None:
    def crash(_: int) -> None:
        raise RuntimeError("db down")

    store.before_commit = crash
    with pytest.raises(RuntimeError):
        await CreatePayment(uow_factory, clock)(command())
    assert not store.payments
    assert not store.outbox
