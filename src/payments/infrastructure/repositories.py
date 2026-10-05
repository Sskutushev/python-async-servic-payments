"""PostgreSQL implementations of the persistence ports + the SQLAlchemy unit of work."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import TracebackType
from typing import Any, Self, cast

from sqlalchemy import CursorResult, and_, exists, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.domain.events import EventType, OutboxEvent
from payments.domain.money import Currency, Money
from payments.domain.payment import NotificationStatus, Payment, PaymentStatus
from payments.infrastructure.models import OutboxRow, PaymentRow

# ------------------------------------------------------------------ mapping


def _payment_from_row(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        money=Money(row.amount, Currency(row.currency)),
        description=row.description,
        metadata=row.metadata_,
        idempotency_key=row.idempotency_key,
        request_fingerprint=row.request_fingerprint,
        webhook_url=row.webhook_url,
        created_at=row.created_at,
        status=PaymentStatus(row.status),
        processed_at=row.processed_at,
        gateway_reference=row.gateway_reference,
        failure_code=row.failure_code,
        gateway_attempts=row.gateway_attempts,
        processing_lease_token=row.processing_lease_token,
        processing_lease_until=row.processing_lease_until,
        notification_status=NotificationStatus(row.notification_status),
        notification_event_id=row.notification_event_id,
        notification_body=row.notification_body,
        notification_attempts=row.notification_attempts,
        notification_next_attempt_at=row.notification_next_attempt_at,
        notification_delivered_at=row.notification_delivered_at,
        notification_last_error=row.notification_last_error,
    )


def _payment_values(payment: Payment) -> dict[str, object]:
    return {
        "id": payment.id,
        "amount": payment.money.amount,
        "currency": payment.money.currency.value,
        "description": payment.description,
        "metadata_": payment.metadata,  # ORM attribute key; the column itself is "metadata"
        "idempotency_key": payment.idempotency_key,
        "request_fingerprint": payment.request_fingerprint,
        "webhook_url": payment.webhook_url,
        "created_at": payment.created_at,
        "status": payment.status.value,
        "processed_at": payment.processed_at,
        "gateway_reference": payment.gateway_reference,
        "failure_code": payment.failure_code,
        "gateway_attempts": payment.gateway_attempts,
        "processing_lease_token": payment.processing_lease_token,
        "processing_lease_until": payment.processing_lease_until,
        "notification_status": payment.notification_status.value,
        "notification_event_id": payment.notification_event_id,
        "notification_body": payment.notification_body,
        "notification_attempts": payment.notification_attempts,
        "notification_next_attempt_at": payment.notification_next_attempt_at,
        "notification_delivered_at": payment.notification_delivered_at,
        "notification_last_error": payment.notification_last_error,
    }


def _event_from_row(row: OutboxRow) -> OutboxEvent:
    return OutboxEvent(
        id=row.id,
        event_type=EventType(row.event_type),
        schema_version=row.schema_version,
        aggregate_id=row.aggregate_id,
        exchange=row.exchange,
        routing_key=row.routing_key,
        payload=row.payload,
        dedup_key=row.dedup_key,
        created_at=row.created_at,
        available_at=row.available_at,
        published_at=row.published_at,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        publication_attempts=row.publication_attempts,
        last_error=row.last_error,
    )


# ------------------------------------------------------------- repositories


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, payment: Payment) -> bool:
        stmt = (
            insert(PaymentRow)
            .values(**_payment_values(payment))
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(PaymentRow.id)
        )
        inserted = (await self._session.execute(stmt)).scalar_one_or_none()
        return inserted is not None

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        row = await self._session.get(PaymentRow, payment_id)
        return _payment_from_row(row) if row else None

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(PaymentRow).where(PaymentRow.idempotency_key == key)
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        return _payment_from_row(row) if row else None

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        stmt = select(PaymentRow).where(PaymentRow.id == payment_id).with_for_update()
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        return _payment_from_row(row) if row else None

    async def save(self, payment: Payment) -> None:
        values = _payment_values(payment)
        values.pop("id")
        values.pop("idempotency_key")
        values.pop("created_at")
        await self._session.execute(
            update(PaymentRow).where(PaymentRow.id == payment.id).values(**values)
        )

    async def find_stalled(self, *, older_than: datetime, limit: int) -> list[Payment]:
        has_unpublished = exists().where(
            and_(OutboxRow.aggregate_id == PaymentRow.id, OutboxRow.published_at.is_(None))
        )
        lease_overdue = or_(
            PaymentRow.processing_lease_until.is_(None),
            PaymentRow.processing_lease_until < older_than,
        )
        stmt = (
            select(PaymentRow)
            .where(
                or_(
                    and_(
                        PaymentRow.status == PaymentStatus.PENDING.value,
                        PaymentRow.created_at < older_than,
                        lease_overdue,
                    ),
                    and_(
                        PaymentRow.notification_status == NotificationStatus.PENDING.value,
                        PaymentRow.notification_next_attempt_at < older_than,
                    ),
                ),
                ~has_unpublished,
            )
            .order_by(PaymentRow.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [_payment_from_row(r) for r in rows]


class SqlOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        self._session.add(
            OutboxRow(
                id=event.id,
                event_type=event.event_type.value,
                schema_version=event.schema_version,
                aggregate_id=event.aggregate_id,
                exchange=event.exchange,
                routing_key=event.routing_key,
                payload=event.payload,
                dedup_key=event.dedup_key,
                created_at=event.created_at,
                available_at=event.available_at,
            )
        )
        await self._session.flush()

    async def claim_due(
        self, *, now: datetime, lease: timedelta, token: uuid.UUID, limit: int
    ) -> list[OutboxEvent]:
        due = (
            select(OutboxRow.id)
            .where(
                OutboxRow.published_at.is_(None),
                OutboxRow.available_at <= now,
                or_(OutboxRow.lease_until.is_(None), OutboxRow.lease_until < now),
            )
            .order_by(OutboxRow.available_at, OutboxRow.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        stmt = (
            update(OutboxRow)
            .where(OutboxRow.id.in_(due.scalar_subquery()))
            .values(lease_token=token, lease_until=now + lease)
            .returning(OutboxRow)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        events = [_event_from_row(r) for r in rows]
        events.sort(key=lambda e: (e.available_at, e.created_at))
        return events

    async def mark_published(self, event_id: uuid.UUID, *, token: uuid.UUID, now: datetime) -> bool:
        stmt = (
            update(OutboxRow)
            .where(
                OutboxRow.id == event_id,
                OutboxRow.lease_token == token,
                OutboxRow.published_at.is_(None),
            )
            .values(published_at=now, lease_token=None, lease_until=None)
        )
        result = cast("CursorResult[Any]", await self._session.execute(stmt))
        return bool(result.rowcount)

    async def record_publish_failure(
        self, event_id: uuid.UUID, *, token: uuid.UUID, error: str, retry_at: datetime
    ) -> None:
        stmt = (
            update(OutboxRow)
            .where(OutboxRow.id == event_id, OutboxRow.lease_token == token)
            .values(
                publication_attempts=OutboxRow.publication_attempts + 1,
                last_error=error[:1000],
                available_at=retry_at,
                lease_token=None,
                lease_until=None,
            )
        )
        await self._session.execute(stmt)


# ------------------------------------------------------------ unit of work


class SqlUnitOfWork:
    """One session, one transaction. Rolls back unless ``commit`` was called."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self.payments: SqlPaymentRepository
        self.outbox: SqlOutboxRepository

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self.payments = SqlPaymentRepository(self._session)
        self.outbox = SqlOutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._session is not None
        try:
            await self._session.rollback()  # no-op after commit; discards partial work otherwise
        finally:
            await self._session.close()
            self._session = None

    async def commit(self) -> None:
        assert self._session is not None
        await self._session.commit()
