from __future__ import annotations


class DomainError(Exception):
    """Base class for rule violations. Carries a stable machine-readable ``code``."""

    code = "domain_error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)


class InvalidAmount(DomainError):
    code = "invalid_amount"


class InvalidTransition(DomainError):
    code = "invalid_transition"


class IdempotencyConflict(DomainError):
    """Same ``Idempotency-Key`` reused with a different request body."""

    code = "idempotency_conflict"


class PaymentNotFound(DomainError):
    code = "payment_not_found"
