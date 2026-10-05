"""Signing webhooks so the receiver can check they really came from us.

Header: ``X-Webhook-Signature: v1=<hex>`` where ``hex = HMAC-SHA256(secret, "<timestamp>.<body>")``.
"""

from __future__ import annotations

import hashlib
import hmac
import json

SIGNATURE_VERSION = "v1"


def canonical_body(body: dict[str, object]) -> bytes:
    """Same data, same bytes (sorted keys, no spaces): every retry signs exactly the same thing."""
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"{SIGNATURE_VERSION}={mac.hexdigest()}"


def verify(secret: str, timestamp: str, body: bytes, signature: str) -> bool:
    return hmac.compare_digest(sign(secret, timestamp, body), signature)
