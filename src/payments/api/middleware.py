"""Things that apply to every HTTP request: the API key check (docs and health included),
request ids, the request body size limit and the access log."""

from __future__ import annotations

import hmac
import logging
import time
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from payments.api.errors import error_response
from payments.logging_setup import request_id_var

log = logging.getLogger("payments.access")
API_KEY_HEADER = b"x-api-key"


class ApiKeyMiddleware:
    """Written as plain ASGI (not BaseHTTPMiddleware) so streaming and context variables work."""

    def __init__(self, app: ASGIApp, *, api_key: str) -> None:
        self._app = app
        self._api_key = api_key.encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        provided = next((v for k, v in scope["headers"] if k == API_KEY_HEADER), b"")
        if not hmac.compare_digest(provided, self._api_key):
            response = error_response(401, "unauthorized", "missing or invalid X-API-Key")
            await response(scope, receive, send)
            return
        await self._app(scope, receive, send)


class RequestContextMiddleware:
    """Gives each request an ``X-Request-Id``, writes the access log, limits the body size."""

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self._app = app
        self._max_body = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        headers = dict(scope["headers"])
        incoming = headers.get(b"x-request-id", b"").decode("latin-1")
        request_id = (
            incoming if 0 < len(incoming) <= 64 and incoming.isprintable() else str(uuid.uuid4())
        )
        token = request_id_var.set(request_id)
        started = time.perf_counter()
        status_code = 500

        declared = headers.get(b"content-length")
        if declared is not None and declared.isdigit() and int(declared) > self._max_body:
            response = error_response(413, "payload_too_large", "request body too large")
            await response(scope, receive, send)
            request_id_var.reset(token)
            return

        received = 0

        async def bounded_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self._max_body:
                    raise _BodyTooLarge
            return message

        async def tagged_send(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                message["headers"] = [
                    *message.get("headers", []),
                    (b"x-request-id", request_id.encode()),
                ]
            await send(message)

        try:
            await self._app(scope, bounded_receive, tagged_send)
        except _BodyTooLarge:
            response = error_response(413, "payload_too_large", "request body too large")
            await response(scope, receive, tagged_send)
        finally:
            log.info(
                "request",
                extra={
                    "method": scope["method"],
                    "path": scope["path"],
                    "status_code": status_code,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                },
            )
            request_id_var.reset(token)


class _BodyTooLarge(Exception):
    pass
