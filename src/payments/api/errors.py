"""Every error looks the same: ``{"error": {"code", "message", "request_id", "details"?}}``."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from payments.domain.errors import IdempotencyConflict, InvalidAmount, PaymentNotFound
from payments.logging_setup import request_id_var

log = logging.getLogger(__name__)
UNPROCESSABLE = 422


def error_response(
    status_code: int, code: str, message: str, details: list[dict[str, Any]] | None = None
) -> JSONResponse:
    body: dict[str, Any] = {"code": code, "message": message, "request_id": request_id_var.get()}
    if details:
        body["details"] = details
    return JSONResponse(status_code=status_code, content={"error": body})


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(IdempotencyConflict)
    async def _conflict(_: Request, exc: IdempotencyConflict) -> JSONResponse:
        return error_response(status.HTTP_409_CONFLICT, exc.code, str(exc))

    @app.exception_handler(PaymentNotFound)
    async def _not_found(_: Request, exc: PaymentNotFound) -> JSONResponse:
        return error_response(status.HTTP_404_NOT_FOUND, exc.code, "payment not found")

    @app.exception_handler(InvalidAmount)
    async def _invalid_amount(_: Request, exc: InvalidAmount) -> JSONResponse:
        return error_response(UNPROCESSABLE, exc.code, str(exc))

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {
                "loc": [str(p) for p in e.get("loc", ())],
                "msg": e.get("msg", ""),
                "type": e.get("type", ""),
            }
            for e in exc.errors()
        ]
        return error_response(
            UNPROCESSABLE, "validation_error", "request validation failed", details
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        codes = {
            401: "unauthorized",
            404: "not_found",
            405: "method_not_allowed",
            413: "payload_too_large",
            422: "validation_error",
        }
        return error_response(
            exc.status_code, codes.get(exc.status_code, "http_error"), str(exc.detail)
        )

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error", exc_info=exc)
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "service_unavailable",
            "temporary failure, retry later",
        )
