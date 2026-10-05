"""SQLAlchemy table definitions. Constraints encode the domain invariants a second time
so that no code path (including manual SQL) can store an impossible state."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payments.domain.money import Currency
from payments.domain.payment import NotificationStatus, PaymentStatus

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def _in_list(values: type[Currency] | type[PaymentStatus] | type[NotificationStatus]) -> str:
    return ", ".join(f"'{v.value}'" for v in values)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class PaymentRow(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String(1000), nullable=False, default="")
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    gateway_reference: Mapped[str | None] = mapped_column(String(128))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    gateway_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processing_lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    processing_lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notification_status: Mapped[str] = mapped_column(String(16), nullable=False)
    notification_event_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), unique=True)
    notification_body: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    notification_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    notification_next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notification_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notification_last_error: Mapped[str | None] = mapped_column(String(128))

    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint(f"currency IN ({_in_list(Currency)})", name="currency_supported"),
        CheckConstraint(f"status IN ({_in_list(PaymentStatus)})", name="status_known"),
        CheckConstraint(
            f"notification_status IN ({_in_list(NotificationStatus)})",
            name="notification_status_known",
        ),
        CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)", name="processed_at_matches_status"
        ),
        CheckConstraint(
            "(status = 'pending') = (notification_status = 'not_ready')",
            name="notification_follows_result",
        ),
        CheckConstraint("gateway_attempts >= 0", name="gateway_attempts_non_negative"),
        CheckConstraint("notification_attempts >= 0", name="notification_attempts_non_negative"),
        # Recovery scan: pending payments and pending notifications are looked up by time.
        Index(
            "ix_payments_pending_work",
            "notification_next_attempt_at",
            "processing_lease_until",
            "created_at",
            postgresql_where="status = 'pending' OR notification_status = 'pending'",
        ),
    )


class OutboxRow(Base):
    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("payments.id", ondelete="RESTRICT"), nullable=False
    )
    exchange: Mapped[str] = mapped_column(String(255), nullable=False)
    routing_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    dedup_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publication_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        # The relay only ever scans unpublished rows ordered by availability.
        Index(
            "ix_outbox_unpublished",
            "available_at",
            "created_at",
            postgresql_where="published_at IS NULL",
        ),
        Index(
            "ix_outbox_aggregate_unpublished",
            "aggregate_id",
            postgresql_where="published_at IS NULL",
        ),
    )
