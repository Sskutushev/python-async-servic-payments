"""Webhook signing: ``X-Webhook-Signature: v1=<hex HMAC-SHA256(secret, "<ts>.<body>")>``."""

from __future__ import annotations

import hashlib
import hmac
import json

SIGNATURE_VERSION = "v1"


def canonical_body(body: dict[str, object]) -> bytes:
    """Stable byte representation so every retry signs exactly the same payload."""
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"{SIGNATURE_VERSION}={mac.hexdigest()}"


def verify(secret: str, timestamp: str, body: bytes, signature: str) -> bool:
    return hmac.compare_digest(sign(secret, timestamp, body), signature)
