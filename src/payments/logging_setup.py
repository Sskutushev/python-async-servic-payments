"""Structured JSON logging with request/payment correlation fields.

Only whitelisted ``extra`` keys are emitted so a careless log call cannot leak
request bodies, webhook URLs with query strings, or secrets.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from datetime import UTC, datetime

request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id", default=None
)

CONTEXT_FIELDS = (
    "request_id",
    "payment_id",
    "event_id",
    "phase",
    "attempt",
    "status",
    "outcome",
    "error_code",
    "duration_ms",
    "reason",
    "path",
    "method",
    "status_code",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = request_id_var.get()
        if request_id:
            payload["request_id"] = request_id
        for key in CONTEXT_FIELDS:
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("uvicorn.access", "aiormq", "aio_pika"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
