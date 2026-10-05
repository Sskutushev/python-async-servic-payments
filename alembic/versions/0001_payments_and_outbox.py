"""payments and outbox tables

Revision ID: 0001
Revises:
Create Date: 2026-10-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("gateway_reference", sa.String(128), nullable=True),
        sa.Column("failure_code", sa.String(64), nullable=True),
        sa.Column("gateway_attempts", sa.Integer(), nullable=False),
        sa.Column("processing_lease_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("processing_lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notification_status", sa.String(16), nullable=False),
        sa.Column("notification_event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("notification_body", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("notification_attempts", sa.Integer(), nullable=False),
        sa.Column("notification_next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notification_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notification_last_error", sa.String(128), nullable=True),
        sa.CheckConstraint("amount > 0", name="amount_positive"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="currency_supported"),
        sa.CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="status_known"),
        sa.CheckConstraint(
            "notification_status IN ('not_ready', 'pending', 'delivered', 'exhausted')",
            name="notification_status_known",
        ),
        sa.CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)",
            name="processed_at_matches_status",
        ),
        sa.CheckConstraint(
            "(status = 'pending') = (notification_status = 'not_ready')",
            name="notification_follows_result",
        ),
        sa.CheckConstraint("gateway_attempts >= 0", name="gateway_attempts_non_negative"),
        sa.CheckConstraint("notification_attempts >= 0", name="notification_attempts_non_negative"),
        sa.PrimaryKeyConstraint("id", name="pk_payments"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        sa.UniqueConstraint("notification_event_id", name="uq_payments_notification_event_id"),
    )
    op.create_index(
        "ix_payments_pending_work",
        "payments",
        ["notification_next_attempt_at", "processing_lease_until", "created_at"],
        postgresql_where=sa.text("status = 'pending' OR notification_status = 'pending'"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("exchange", sa.String(255), nullable=False),
        sa.Column("routing_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("dedup_key", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publication_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["aggregate_id"],
            ["payments.id"],
            name="fk_outbox_aggregate_id_payments",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
        sa.UniqueConstraint("dedup_key", name="uq_outbox_dedup_key"),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["available_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index(
        "ix_outbox_aggregate_unpublished",
        "outbox",
        ["aggregate_id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_aggregate_unpublished", table_name="outbox")
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_payments_pending_work", table_name="payments")
    op.drop_table("payments")
