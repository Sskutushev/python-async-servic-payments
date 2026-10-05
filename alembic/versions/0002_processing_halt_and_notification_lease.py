"""processing halt for operators, lease token for webhook attempts

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payments", sa.Column("processing_halted_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("payments", sa.Column("processing_halt_reason", sa.String(64), nullable=True))
    op.add_column(
        "payments",
        sa.Column("notification_lease_token", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_check_constraint(
        "halt_reason_with_timestamp",
        "payments",
        "(processing_halted_at IS NULL) = (processing_halt_reason IS NULL)",
    )
    op.create_check_constraint(
        "halt_only_while_pending", "payments", "status = 'pending' OR processing_halted_at IS NULL"
    )


def downgrade() -> None:
    # Short names: the naming convention in env.py adds the "ck_payments_" prefix itself.
    op.drop_constraint("halt_only_while_pending", "payments", type_="check")
    op.drop_constraint("halt_reason_with_timestamp", "payments", type_="check")
    op.drop_column("payments", "notification_lease_token")
    op.drop_column("payments", "processing_halt_reason")
    op.drop_column("payments", "processing_halted_at")
