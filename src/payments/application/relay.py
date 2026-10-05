"""Outbox relay: lease due events, publish with confirms, then mark them published.

The order "publish, then mark" means a crash between the two re-publishes the
same ``event_id`` later. That is the accepted at-least-once window; consumers
are idempotent so the duplicate is harmless (see ADR-0001).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from datetime import timedelta

from payments.application.ports import Clock, EventPublisher, PublishError, UnitOfWorkFactory
from payments.application.retry import RetryPolicy

log = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        publisher: EventPublisher,
        clock: Clock,
        lease: timedelta,
        batch_size: int = 100,
        poll_interval: float = 0.5,
        backoff: RetryPolicy | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._clock = clock
        self._lease = lease
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        # Publication retries are unbounded by design: an outbox row is never dropped,
        # only delayed; the policy here shapes the delay.
        self._backoff = backoff or RetryPolicy(
            max_attempts=1_000_000, base_delay=timedelta(seconds=1), max_delay=timedelta(minutes=1)
        )

    async def run_once(self) -> int:
        """Publish one batch. Returns the number of events marked published."""
        token = uuid.uuid4()
        async with self._uow_factory() as uow:
            events = await uow.outbox.claim_due(
                now=self._clock.now(), lease=self._lease, token=token, limit=self._batch_size
            )
            await uow.commit()
        if not events:
            return 0

        published = 0
        for event in events:
            try:
                await self._publisher.publish(event)
            except PublishError as exc:
                await self._record_failure(event.id, token, event.publication_attempts, exc)
                continue
            async with self._uow_factory() as uow:
                if await uow.outbox.mark_published(event.id, token=token, now=self._clock.now()):
                    published += 1
                else:
                    # Lease expired while publishing; the other holder will re-publish.
                    log.warning("lost outbox lease", extra={"event_id": str(event.id)})
                await uow.commit()
        return published

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
        """Poll until ``stop`` is set. Unexpected errors are logged and retried after a pause."""
        while not stop.is_set():
            try:
                published = await self.run_once()
            except Exception:
                log.exception("outbox relay iteration failed")
                published = 0
            if published:
                continue  # drain quickly while there is work
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
