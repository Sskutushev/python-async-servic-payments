"""Outbox claim/lease semantics on real row locks (FOR UPDATE SKIP LOCKED)."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from payments.application.relay import OutboxRelay
from payments.domain.events import payment_created_event
from tests.fakes import FakeClock, FakePublisher, make_payment

LEASE = timedelta(seconds=30)


async def seed(uow_factory, clock: FakeClock, n: int) -> list[uuid.UUID]:  # type: ignore[no-untyped-def]
    ids = []
    async with uow_factory() as uow:
        for _ in range(n):
            p = make_payment()
            assert await uow.payments.add(p)
            e = payment_created_event(p, now=clock.now())
            await uow.outbox.add(e)
            ids.append(e.id)
        await uow.commit()
    return ids


async def test_two_relays_never_claim_the_same_event(uow_factory, clock) -> None:
    await seed(uow_factory, clock, 10)

    async def claim(token: uuid.UUID) -> set[uuid.UUID]:
        async with uow_factory() as uow:
            events = await uow.outbox.claim_due(now=clock.now(), lease=LEASE, token=token, limit=5)
            await asyncio.sleep(0.05)  # hold the locks while the other relay claims
            await uow.commit()
        return {e.id for e in events}

    a, b = await asyncio.gather(claim(uuid.uuid4()), claim(uuid.uuid4()))
    assert len(a) == 5 and len(b) == 5
    assert not (a & b)


async def test_leased_events_are_invisible_until_lease_expires(uow_factory, clock) -> None:
    await seed(uow_factory, clock, 1)
    token = uuid.uuid4()
    async with uow_factory() as uow:
        assert (
            len(await uow.outbox.claim_due(now=clock.now(), lease=LEASE, token=token, limit=10))
            == 1
        )
        await uow.commit()
    async with uow_factory() as uow:
        assert (
            await uow.outbox.claim_due(now=clock.now(), lease=LEASE, token=uuid.uuid4(), limit=10)
            == []
        )
    clock.advance(LEASE + timedelta(seconds=1))
    async with uow_factory() as uow:
        assert (
            len(
                await uow.outbox.claim_due(
                    now=clock.now(), lease=LEASE, token=uuid.uuid4(), limit=10
                )
            )
            == 1
        )


async def test_mark_published_is_fenced_by_lease_token(uow_factory, clock) -> None:
    [event_id] = await seed(uow_factory, clock, 1)
    token = uuid.uuid4()
    async with uow_factory() as uow:
        await uow.outbox.claim_due(now=clock.now(), lease=LEASE, token=token, limit=1)
        await uow.commit()
    async with uow_factory() as uow:
        assert not await uow.outbox.mark_published(event_id, token=uuid.uuid4(), now=clock.now())
        assert await uow.outbox.mark_published(event_id, token=token, now=clock.now())
        assert not await uow.outbox.mark_published(event_id, token=token, now=clock.now())  # once
        await uow.commit()


async def test_publish_failure_releases_lease_and_delays(uow_factory, clock) -> None:
    [event_id] = await seed(uow_factory, clock, 1)
    token = uuid.uuid4()
    async with uow_factory() as uow:
        await uow.outbox.claim_due(now=clock.now(), lease=LEASE, token=token, limit=1)
        await uow.commit()
    retry_at = clock.now() + timedelta(seconds=10)
    async with uow_factory() as uow:
        await uow.outbox.record_publish_failure(
            event_id, token=token, error="boom", retry_at=retry_at
        )
        await uow.commit()
    async with uow_factory() as uow:
        assert (
            await uow.outbox.claim_due(now=clock.now(), lease=LEASE, token=uuid.uuid4(), limit=1)
            == []
        )
        [event] = await uow.outbox.claim_due(now=retry_at, lease=LEASE, token=uuid.uuid4(), limit=1)
    assert event.publication_attempts == 1
    assert event.last_error == "boom"


async def test_dedup_key_is_unique(uow_factory, clock) -> None:
    async with uow_factory() as uow:
        p = make_payment()
        await uow.payments.add(p)
        await uow.outbox.add(payment_created_event(p, now=clock.now()))
        with pytest.raises(IntegrityError):
            await uow.outbox.add(payment_created_event(p, now=clock.now()))


async def test_relay_end_to_end_on_postgres(uow_factory, clock) -> None:
    await seed(uow_factory, clock, 3)
    publisher = FakePublisher()
    relay = OutboxRelay(uow_factory=uow_factory, publisher=publisher, clock=clock, lease=LEASE)
    assert await relay.run_once() == 3
    assert await relay.run_once() == 0
    assert len(publisher.published) == 3
    assert publisher.published[0].payload["event_type"] == "payment.created"
