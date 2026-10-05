"""HTTP webhook sender.

Delivery semantics (documented in README):

* 2xx -> delivered.
* 408, 429, 5xx, timeouts and connection errors -> retryable.
* any other status, redirects (not followed) and policy rejections -> permanent.
* ``Retry-After`` (seconds) on 429/503 is honoured up to the retry policy cap.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import httpx

from payments.application.ports import Clock, WebhookDeliveryError
from payments.infrastructure.signing import canonical_body, sign
from payments.infrastructure.url_policy import WebhookUrlPolicy, WebhookUrlRejected

RETRYABLE_STATUSES = frozenset({408, 429})
MAX_RETRY_AFTER = timedelta(hours=1)


def build_http_client(timeout_seconds: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds, connect=min(timeout_seconds, 3.0)),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
        follow_redirects=False,
        trust_env=False,  # never pick up proxies from the environment by accident
        headers={"User-Agent": "payments-webhook/1.0"},
    )


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
        try:
            self._policy.validate(url)
            await self._policy.check_resolved(url)
        except WebhookUrlRejected as exc:
            raise WebhookDeliveryError(f"url_rejected:{exc.reason}", retryable=False) from exc

        raw = canonical_body(body)
        timestamp = str(int(self._clock.now().timestamp()))
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": str(event_id),
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": sign(self._secret, timestamp, raw),
        }
        try:
            async with self._client.stream("POST", url, content=raw, headers=headers) as resp:
                status = resp.status_code
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                # Drain a bounded amount of the body so the connection can be reused;
                # anything beyond the cap is simply not read.
                received = 0
                async for chunk in resp.aiter_bytes():
                    received += len(chunk)
                    if received > self._max_response_bytes:
                        break
        except httpx.TimeoutException as exc:
            raise WebhookDeliveryError("timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError("connection_error", retryable=True) from exc

        if 200 <= status < 300:
            return
        retryable = status >= 500 or status in RETRYABLE_STATUSES
        raise WebhookDeliveryError(
            f"http_{status}", retryable=retryable, retry_after=retry_after if retryable else None
        )


def _parse_retry_after(value: str | None) -> timedelta | None:
    if value is None or not value.isdigit():
        return None
    return min(timedelta(seconds=int(value)), MAX_RETRY_AFTER)
