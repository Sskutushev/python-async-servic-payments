from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy import text

from payments.api.schemas import (
    CreatePaymentRequest,
    ErrorResponse,
    IdempotencyKey,
    PaymentAccepted,
    PaymentResponse,
)
from payments.application.create_payment import CreatePayment, CreatePaymentCommand
from payments.application.ports import UnitOfWorkFactory
from payments.bootstrap import Container
from payments.domain.errors import PaymentNotFound
from payments.infrastructure.url_policy import WebhookUrlPolicy, WebhookUrlRejected

router = APIRouter()
ERROR = {"model": ErrorResponse}


def get_container(request: Request) -> Container:
    container: Container = request.app.state.container
    return container


def get_uow_factory(container: Annotated[Container, Depends(get_container)]) -> UnitOfWorkFactory:
    return container.uow_factory


def get_create_payment(container: Annotated[Container, Depends(get_container)]) -> CreatePayment:
    return container.create_payment()


def get_url_policy(container: Annotated[Container, Depends(get_container)]) -> WebhookUrlPolicy:
    return container.url_policy


@router.post(
    "/api/v1/payments",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={401: ERROR, 409: ERROR, 413: ERROR, 422: ERROR, 503: ERROR},
    summary="Create a payment (idempotent)",
)
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[IdempotencyKey, Header(alias="Idempotency-Key")],
    response: Response,
    use_case: Annotated[CreatePayment, Depends(get_create_payment)],
    url_policy: Annotated[WebhookUrlPolicy, Depends(get_url_policy)],
) -> PaymentAccepted:
    try:
        url_policy.validate(body.webhook_url)
    except WebhookUrlRejected as exc:
        raise HTTPException(422, detail=f"webhook_url rejected: {exc.reason}") from exc

    result = await use_case(
        CreatePaymentCommand(
            idempotency_key=idempotency_key,
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            metadata=body.metadata,
            webhook_url=body.webhook_url,
        )
    )
    payment = result.payment
    response.headers["Location"] = f"/api/v1/payments/{payment.id}"
    response.headers["Idempotency-Replayed"] = "false" if result.created else "true"
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentResponse,
    responses={401: ERROR, 404: ERROR, 503: ERROR},
    summary="Get payment details",
)
async def get_payment(
    payment_id: uuid.UUID,
    uow_factory: Annotated[UnitOfWorkFactory, Depends(get_uow_factory)],
) -> PaymentResponse:
    async with uow_factory() as uow:
        payment = await uow.payments.get(payment_id)
    if payment is None:
        raise PaymentNotFound(str(payment_id))
    return PaymentResponse.from_domain(payment)


@router.get("/health/live", include_in_schema=False)
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready", include_in_schema=False)
async def ready(container: Annotated[Container, Depends(get_container)]) -> dict[str, str]:
    if container.engine is None:
        raise HTTPException(503, detail="database not configured")
    async with container.engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ok"}
