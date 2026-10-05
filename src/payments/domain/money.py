"""Money: an amount plus a currency.

Rules: amounts are ``Decimal`` (never ``float``), greater than zero, with at most two
decimal places (all three supported currencies use two), and small enough for the
``NUMERIC(18, 2)`` column. ``100``, ``100.0`` and ``100.00`` all mean the same amount;
``100.001`` is rejected instead of being rounded.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from payments.domain.errors import InvalidAmount

MAX_AMOUNT = Decimal("9999999999999999.99")  # NUMERIC(18, 2) upper bound
SCALE = Decimal("0.01")


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


def parse_amount(raw: Decimal | int | str) -> Decimal:
    """Check an amount and return it with exactly two decimal places."""
    if isinstance(raw, bool | float):  # bool is an int subclass; floats are never money
        raise InvalidAmount("amount must be a decimal string or integer")
    try:
        value = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except InvalidOperation as exc:
        raise InvalidAmount("amount is not a valid decimal") from exc
    if not value.is_finite():
        raise InvalidAmount("amount must be finite")
    if value <= 0:
        raise InvalidAmount("amount must be positive")
    if value > MAX_AMOUNT:
        raise InvalidAmount("amount exceeds the supported maximum")
    if value.as_tuple().exponent < -2:  # type: ignore[operator]  # finite => int exponent
        raise InvalidAmount("amount must have at most two fractional digits")
    return value.quantize(SCALE)


@dataclass(frozen=True, slots=True)
class Money:
    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        object.__setattr__(self, "amount", parse_amount(self.amount))
        object.__setattr__(self, "currency", Currency(self.currency))

    def __str__(self) -> str:
        return f"{self.amount} {self.currency}"
