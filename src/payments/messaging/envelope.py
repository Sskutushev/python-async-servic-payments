"""What a message in ``payments.new`` looks like.

The consumer only really uses ``payment_id``. Everything about the payment's current
state is read from the database, never trusted from the message.
"""

from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field

from payments.domain.events import EventType, Phase


class PaymentEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    event_id: uuid.UUID
    event_type: EventType
    schema_version: int = Field(ge=1)
    payment_id: uuid.UUID
    phase: Phase = Phase.PROCESS
    attempt: int | None = None
