from __future__ import annotations


class DomainError(Exception):
    """A business rule was broken. ``code`` is a short, stable name for the API/logs."""

    code = "domain_error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)


class InvalidAmount(DomainError):
    code = "invalid_amount"


class InvalidTransition(DomainError):
    code = "invalid_transition"


class IdempotencyConflict(DomainError):
    """The same Idempotency-Key was sent again, but with a different request body."""

    code = "idempotency_conflict"


class PaymentNotFound(DomainError):
    code = "payment_not_found"
