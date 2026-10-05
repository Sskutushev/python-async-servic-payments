"""Moves events from the outbox table to RabbitMQ.

Steps: reserve the events that are due, publish them (a few at a time) and wait for the
broker's confirmation, then mark each one as published. If the process dies between
"published" and "marked", the same event is published again later. That duplicate is
harmless because the consumer checks the database state before doing anything
(see ADR-0001).

Two safety rules:

* an event whose reservation has already expired is skipped, not published, because
  another relay may be publishing it right now;
* ``run_forever`` tolerates occasional errors, but after ``max_consecutive_failures``
  errors in a row it raises, which takes the consumer process down so Docker restarts it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from datetime import timedelta

from payments.application.ports import Clock, EventPublisher, PublishError, UnitOfWorkFactory
from payments.application.retry import RetryPolicy
from payments.domain.events import OutboxEvent

log = logging.getLogger(__name__)


class BackgroundTaskUnhealthy(RuntimeError):
    """A background loop failed too many times in a row and gave up."""


class OutboxRelay:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        publisher: EventPublisher,
        clock: Clock,
        lease: timedelta,
        batch_size: int = 100,
        concurrency: int = 10,
        poll_interval: float = 0.5,
        max_consecutive_failures: int = 10,
        backoff: RetryPolicy | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._clock = clock
        self._lease = lease
        self._batch_size = batch_size
        self._concurrency = concurrency
        self._poll_interval = poll_interval
        self._max_consecutive_failures = max_consecutive_failures
        # Publishing is retried forever: an outbox row is never thrown away, only delayed.
        # The policy below only decides how long to wait between tries.
        self._backoff = backoff or RetryPolicy(
            max_attempts=1_000_000, base_delay=timedelta(seconds=1), max_delay=timedelta(minutes=1)
        )

    async def run_once(self) -> int:
        """Publish one batch of due events. Returns how many were marked as published."""
        token = uuid.uuid4()
        async with self._uow_factory() as uow:
            events = await uow.outbox.claim_due(
                now=self._clock.now(), lease=self._lease, token=token, limit=self._batch_size
            )
            await uow.commit()
        if not events:
            return 0

        limit = asyncio.Semaphore(self._concurrency)

        async def publish_one(event: OutboxEvent) -> bool:
            async with limit:
                return await self._publish_and_mark(event, token)

        results = await asyncio.gather(*(publish_one(e) for e in events))
        return sum(results)

    async def _publish_and_mark(self, event: OutboxEvent, token: uuid.UUID) -> bool:
        if event.lease_until is not None and event.lease_until <= self._clock.now():
            # Waited too long in the batch; someone else may hold this event by now.
            log.warning("outbox lease expired before publish", extra={"event_id": str(event.id)})
            return False
        try:
            await self._publisher.publish(event)
        except PublishError as exc:
            await self._record_failure(event.id, token, event.publication_attempts, exc)
            return False
        async with self._uow_factory() as uow:
            marked = await uow.outbox.mark_published(event.id, token=token, now=self._clock.now())
            await uow.commit()
        if not marked:
            # Our reservation expired while we were publishing. Another relay will publish
            # this event again; the consumer handles the duplicate.
            log.warning("lost outbox lease", extra={"event_id": str(event.id)})
        return marked

    async def _record_failure(
        self, event_id: uuid.UUID, token: uuid.UUID, attempts: int, exc: PublishError
    ) -> None:
        delay = self._backoff.delay_before(max(attempts, 1) + 1)
        async with self._uow_factory() as uow:
            await uow.outbox.record_publish_failure(
                event_id, token=token, error=exc.code, retry_at=self._clock.now() + delay
            )
            await uow.commit()
        log.warning("publish failed", extra={"event_id": str(event_id), "error_code": exc.code})

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Keep publishing until ``stop`` is set.

        A single error is logged and the loop continues. Too many errors in a row raise
        ``BackgroundTaskUnhealthy``: the process is then restarted by the orchestrator.
        """
        failures = 0
        while not stop.is_set():
            try:
                published = await self.run_once()
                failures = 0
            except Exception as exc:
                failures += 1
                log.exception("outbox relay iteration failed (%d in a row)", failures)
                if failures >= self._max_consecutive_failures:
                    raise BackgroundTaskUnhealthy("outbox relay keeps failing") from exc
                published = 0
            if published:
                continue  # there was work, check again right away
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
