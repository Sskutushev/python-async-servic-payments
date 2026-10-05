import asyncio
import uuid
from datetime import timedelta

import pytest

from payments.application.ports import PublishError
from payments.application.relay import OutboxRelay
from payments.domain.events import payment_created_event
from tests.fakes import FakeClock, FakePublisher, InMemoryStore, make_payment

LEASE = timedelta(seconds=30)


def seed_events(
    store: InMemoryStore, clock: FakeClock, n: int, *, delay: timedelta = timedelta(0)
) -> None:
    for _ in range(n):
        p = make_payment()
        store.payments[p.id] = p
        e = payment_created_event(p, now=clock.now())
        e.available_at = clock.now() + delay
        store.outbox[e.id] = e


def relay(uow_factory, clock, publisher, **kw):  # type: ignore[no-untyped-def]
    return OutboxRelay(uow_factory=uow_factory, publisher=publisher, clock=clock, lease=LEASE, **kw)


async def test_due_events_are_published_and_marked(uow_factory, store, clock) -> None:
    seed_events(store, clock, 3)
    publisher = FakePublisher()
    assert await relay(uow_factory, clock, publisher).run_once() == 3
    assert len(publisher.published) == 3
    assert not store.unpublished()
    assert all(e.lease_token is None for e in store.outbox.values())


async def test_future_events_wait_for_available_at(uow_factory, store, clock) -> None:
    seed_events(store, clock, 1, delay=timedelta(seconds=5))
    publisher = FakePublisher()
    r = relay(uow_factory, clock, publisher)
    assert await r.run_once() == 0
    clock.advance(timedelta(seconds=5))
    assert await r.run_once() == 1


async def test_unconfirmed_publish_is_not_marked_and_is_retried_later(
    uow_factory, store, clock
) -> None:
    seed_events(store, clock, 1)
    publisher = FakePublisher(PublishError("broker_unavailable"), None)
    r = relay(uow_factory, clock, publisher)
    assert await r.run_once() == 0
    [event] = store.unpublished()
    assert event.publication_attempts == 1
    assert event.last_error == "broker_unavailable"
    assert event.lease_token is None
    assert event.available_at == clock.now() + timedelta(seconds=1)

    assert await r.run_once() == 0  # backoff not elapsed
    clock.advance(timedelta(seconds=1))
    assert await r.run_once() == 1
    assert not store.unpublished()


async def test_publication_retries_are_never_dropped(uow_factory, store, clock) -> None:
    seed_events(store, clock, 1)
    publisher = FakePublisher(*[PublishError("x")] * 10)
    r = relay(uow_factory, clock, publisher)
    for _ in range(10):
        await r.run_once()
        clock.advance(timedelta(minutes=2))
    [event] = store.unpublished()
    assert event.publication_attempts == 10


async def test_leased_event_is_not_claimed_twice(uow_factory, store, clock) -> None:
    seed_events(store, clock, 1)
    [event] = store.outbox.values()
    event.lease_token, event.lease_until = uuid.uuid4(), clock.now() + LEASE
    publisher = FakePublisher()
    assert await relay(uow_factory, clock, publisher).run_once() == 0
    clock.advance(LEASE + timedelta(seconds=1))
    assert await relay(uow_factory, clock, publisher).run_once() == 1


async def test_lost_lease_after_publish_is_not_marked_by_the_loser(
    uow_factory, store, clock
) -> None:
    """Crash/pause between publish and mark: the duplicate is accepted, bookkeeping is consistent."""
    seed_events(store, clock, 1)

    class SlowPublisher(FakePublisher):
        async def publish(self, event):  # type: ignore[no-untyped-def]
            await super().publish(event)
            clock.advance(LEASE + timedelta(seconds=1))  # lease expires mid-publish
            await relay(uow_factory, clock, fast).run_once()  # another relay re-publishes

    fast = FakePublisher()
    slow = SlowPublisher()
    assert await relay(uow_factory, clock, slow).run_once() == 0
    assert len(slow.published) == 1
    assert len(fast.published) == 1  # at-least-once: duplicate with the same event_id
    assert slow.published[0].id == fast.published[0].id
    assert not store.unpublished()


async def test_event_whose_lease_expired_in_the_batch_is_not_published(
    uow_factory, store, clock
) -> None:
    """Review P3: a slow batch must not publish events another relay may already own."""
    seed_events(store, clock, 3)

    class SlowPublisher(FakePublisher):
        async def publish(self, event):  # type: ignore[no-untyped-def]
            await super().publish(event)
            clock.advance(LEASE)  # each publish takes longer than the whole lease

    publisher = SlowPublisher()
    r = relay(uow_factory, clock, publisher, concurrency=1)
    assert await r.run_once() == 1
    assert len(publisher.published) == 1
    assert len(store.unpublished()) == 2  # left for the next round, not published blindly


async def test_run_forever_exits_after_repeated_failures(uow_factory, store, clock) -> None:
    """Review P3: a permanently broken loop must take the process down, not idle forever."""
    from payments.application.relay import BackgroundTaskUnhealthy

    class BrokenUow:
        def __call__(self):  # type: ignore[no-untyped-def]
            raise ConnectionError("database down")

    stop = asyncio.Event()
    r = OutboxRelay(
        uow_factory=BrokenUow(),
        publisher=FakePublisher(),
        clock=clock,
        lease=LEASE,
        poll_interval=0.001,
        max_consecutive_failures=3,
    )
    with pytest.raises(BackgroundTaskUnhealthy):
        await asyncio.wait_for(r.run_forever(stop), 2)


async def test_run_forever_stops_and_survives_errors(uow_factory, store, clock) -> None:
    seed_events(store, clock, 1)

    class Exploding:
        async def publish(self, event):  # type: ignore[no-untyped-def]
            raise RuntimeError("boom")

    stop = asyncio.Event()
    r = relay(uow_factory, clock, Exploding(), poll_interval=0.01)
    task = asyncio.create_task(r.run_forever(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert store.unpublished()  # still there for the next attempt
