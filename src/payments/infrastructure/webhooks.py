"""Sends webhooks over HTTP.

How the answer is interpreted:

* 2xx: delivered.
* 408, 429, 5xx, timeouts, connection errors: worth retrying.
* anything else (other 4xx, redirects, URLs our policy rejects): give up, no retry.
* A ``Retry-After`` header (in seconds) is respected, up to the configured maximum.

Against DNS rebinding: the policy resolves the hostname and approves one public address,
and the request is sent to that address. The original hostname stays in the ``Host``
header and in the TLS handshake (SNI + certificate check), so the receiver sees a normal
request. In dev mode (private networks allowed) hostnames are used as they are.
"""

from __future__ import annotations

import ipaddress
import uuid
from datetime import timedelta
from urllib.parse import urlsplit, urlunsplit

import httpx

from payments.application.ports import Clock, WebhookDeliveryError
from payments.infrastructure.signing import canonical_body, sign
from payments.infrastructure.url_policy import IpAddress, WebhookUrlPolicy, WebhookUrlRejected

RETRYABLE_STATUSES = frozenset({408, 429})
MAX_RETRY_AFTER = timedelta(hours=1)


def build_http_client(timeout_seconds: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds, connect=min(timeout_seconds, 3.0)),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
        follow_redirects=False,
        trust_env=False,  # ignore proxy settings from the environment
        headers={"User-Agent": "payments-webhook/1.0"},
    )


def pin_to_address(url: str, address: IpAddress) -> tuple[str, dict[str, str]]:
    """Rewrite ``url`` so it connects to ``address``; return it with the extra request options.

    The returned headers carry the original ``Host``; the ``sni_hostname`` extension makes
    httpx present and verify the certificate for the original hostname.
    """
    parts = urlsplit(url)
    host = parts.hostname or ""
    literal = f"[{address}]" if isinstance(address, ipaddress.IPv6Address) else str(address)
    netloc = f"{literal}:{parts.port}" if parts.port else literal
    pinned = urlunsplit((parts.scheme, netloc, parts.path, parts.query, ""))
    return pinned, {"Host": host, "sni_hostname": host}


class HttpWebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        secret: str,
        policy: WebhookUrlPolicy,
        clock: Clock,
        max_response_bytes: int = 4096,
    ) -> None:
        self._client = client
        self._secret = secret
        self._policy = policy
        self._clock = clock
        self._max_response_bytes = max_response_bytes

    async def deliver(self, url: str, event_id: uuid.UUID, body: dict[str, object]) -> None:
        raw = canonical_body(body)
        target, headers, extensions = await self._prepare(url, event_id, raw)
        status, retry_after = await self._send(target, raw, headers, extensions)
        if 200 <= status < 300:
            return
        retryable = status >= 500 or status in RETRYABLE_STATUSES
        raise WebhookDeliveryError(
            f"http_{status}", retryable=retryable, retry_after=retry_after if retryable else None
        )

    async def _prepare(
        self, url: str, event_id: uuid.UUID, raw: bytes
    ) -> tuple[str, dict[str, str], dict[str, object]]:
        """Check the URL, sign the body, and pin the request to the approved address."""
        try:
            self._policy.validate(url)
            address = await self._policy.check_resolved(url)
        except WebhookUrlRejected as exc:
            raise WebhookDeliveryError(f"url_rejected:{exc.reason}", retryable=False) from exc

        timestamp = str(int(self._clock.now().timestamp()))
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": str(event_id),
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": sign(self._secret, timestamp, raw),
        }
        extensions: dict[str, object] = {}
        if address is None:
            return url, headers, extensions
        target, pinned = pin_to_address(url, address)
        headers["Host"] = pinned["Host"]
        if target.startswith("https://"):
            extensions["sni_hostname"] = pinned["sni_hostname"]
        return target, headers, extensions

    async def _send(
        self, target: str, raw: bytes, headers: dict[str, str], extensions: dict[str, object]
    ) -> tuple[int, timedelta | None]:
        """POST and return (status, Retry-After). Network problems become retryable errors."""
        try:
            async with self._client.stream(
                "POST", target, content=raw, headers=headers, extensions=extensions
            ) as resp:
                # Read a little of the response so the connection can be reused,
                # but never more than the cap: a huge response must not hurt us.
                received = 0
                async for chunk in resp.aiter_bytes():
                    received += len(chunk)
                    if received > self._max_response_bytes:
                        break
                return resp.status_code, _parse_retry_after(resp.headers.get("Retry-After"))
        except httpx.TimeoutException as exc:
            raise WebhookDeliveryError("timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError("connection_error", retryable=True) from exc


def _parse_retry_after(value: str | None) -> timedelta | None:
    if value is None or not value.isdigit():
        return None
    return min(timedelta(seconds=int(value)), MAX_RETRY_AFTER)
