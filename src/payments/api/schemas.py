"""Shapes of requests and responses.

Amounts must be a decimal string (``"100.00"``) or an integer. JSON floats are refused
because a float cannot represent money exactly (0.1 + 0.2 is not 0.3).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

from payments.domain.errors import InvalidAmount
from payments.domain.money import Currency, parse_amount
from payments.domain.payment import NotificationStatus, Payment, PaymentStatus

IDEMPOTENCY_KEY_PATTERN = r"^[\x21-\x7E]{1,128}$"  # 1..128 printable ASCII, no spaces
MAX_METADATA_BYTES = 16 * 1024


def _validate_amount(value: object) -> Decimal:
    if isinstance(value, float):
        raise ValueError('amount must be a decimal string (e.g. "100.00"), not a JSON float')
    if not isinstance(value, str | int | Decimal):
        raise ValueError("amount must be a decimal string or integer")
    try:
        return parse_amount(value)
    except InvalidAmount as exc:
        raise ValueError(str(exc)) from exc


Amount = Annotated[Decimal, BeforeValidator(_validate_amount)]
IdempotencyKey = Annotated[str, StringConstraints(pattern=IDEMPOTENCY_KEY_PATTERN)]


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    amount: Amount = Field(examples=["100.00"])
    currency: Currency
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str = Field(
        min_length=1, max_length=2048, examples=["https://merchant.example/hooks"]
    )

    @field_validator("metadata")
    @classmethod
    def _bounded_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} bytes")
        return value


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID
    amount: str = Field(examples=["100.00"])
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    failure_code: str | None
    webhook_url: str
    processing_halt_reason: str | None = Field(
        description="Set when the gateway could not be reached and an operator must replay"
    )
    notification_status: NotificationStatus
    notification_attempts: int
    notification_last_error: str | None
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_domain(cls, payment: Payment) -> PaymentResponse:
        return cls(
            payment_id=payment.id,
            amount=str(payment.money.amount),
            currency=payment.money.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            failure_code=payment.failure_code,
            webhook_url=payment.webhook_url,
            processing_halt_reason=payment.processing_halt_reason,
            notification_status=payment.notification_status,
            notification_attempts=payment.notification_attempts,
            notification_last_error=payment.notification_last_error,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: list[dict[str, Any]] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody
