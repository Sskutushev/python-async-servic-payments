"""Create a payment idempotently and enqueue its outbox event in the same transaction.

Concurrency model (READ COMMITTED):

1. ``INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`` — exactly one of N
   concurrent requests wins; the winner also inserts the initial outbox event
   before committing, so "payment exists" implies "event will be published".
2. Losers re-read the committed winner and compare fingerprints: same body ->
   same ``payment_id``; different body -> :class:`IdempotencyConflict`.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from payments.application.ports import Clock, UnitOfWorkFactory
from payments.domain.errors import IdempotencyConflict
from payments.domain.events import payment_created_event
from payments.domain.fingerprint import request_fingerprint
from payments.domain.money import Currency, Money
from payments.domain.payment import Payment


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool  # False => replay of an earlier request with the same key


class CreatePayment:
    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def __call__(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        money = Money(command.amount, command.currency)
        fingerprint = request_fingerprint(
            amount=money.amount,
            currency=money.currency.value,
            description=command.description,
            metadata=command.metadata,
            webhook_url=command.webhook_url,
        )
        now = self._clock.now()
        candidate = Payment(
            id=uuid.uuid4(),
            money=money,
            description=command.description,
            metadata=command.metadata,
            idempotency_key=command.idempotency_key,
            request_fingerprint=fingerprint,
            webhook_url=command.webhook_url,
            created_at=now,
        )

        async with self._uow_factory() as uow:
            inserted = await uow.payments.add(candidate)
            if inserted:
                await uow.outbox.add(payment_created_event(candidate, now=now))
                await uow.commit()
                return CreatePaymentResult(payment=candidate, created=True)

            # A concurrent/earlier request owns this key; it is committed and visible now.
            existing = await uow.payments.get_by_idempotency_key(command.idempotency_key)
            if existing is None:  # pragma: no cover - defensive: winner cannot vanish
                raise IdempotencyConflict("idempotency key owner not found")
            if existing.request_fingerprint != fingerprint:
                raise IdempotencyConflict(
                    "Idempotency-Key was already used with a different request body"
                )
            return CreatePaymentResult(payment=existing, created=False)
