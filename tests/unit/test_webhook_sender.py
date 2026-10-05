import json
import uuid
from datetime import timedelta

import httpx
import pytest

from payments.application.ports import WebhookDeliveryError
from payments.infrastructure.signing import verify
from payments.infrastructure.url_policy import WebhookUrlPolicy
from payments.infrastructure.webhooks import HttpWebhookSender
from tests.fakes import FakeClock

SECRET = "s3cret-s3cret-s3cret"
BODY: dict[str, object] = {"event_id": "e", "amount": "1.00", "status": "succeeded"}
DEV = WebhookUrlPolicy(allow_private_networks=True, allow_insecure_http=True)


def sender(handler, policy=DEV):  # type: ignore[no-untyped-def]
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return HttpWebhookSender(client, secret=SECRET, policy=policy, clock=FakeClock())


async def test_sends_signed_canonical_body() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    event_id = uuid.uuid4()
    await sender(handler).deliver("http://receiver.local/hooks", event_id, BODY)
    [req] = seen
    assert req.headers["X-Webhook-Id"] == str(event_id)
    assert req.headers["Content-Type"] == "application/json"
    assert json.loads(req.content) == BODY
    assert req.content == b'{"amount":"1.00","event_id":"e","status":"succeeded"}'
    assert verify(
        SECRET, req.headers["X-Webhook-Timestamp"], req.content, req.headers["X-Webhook-Signature"]
    )


@pytest.mark.parametrize(
    ("status", "retryable"),
    [
        (500, True),
        (503, True),
        (502, True),
        (429, True),
        (408, True),
        (400, False),
        (404, False),
        (301, False),
    ],
)
async def test_status_classification(status: int, retryable: bool) -> None:
    with pytest.raises(WebhookDeliveryError) as exc:
        await sender(lambda _: httpx.Response(status)).deliver(
            "http://r.local/", uuid.uuid4(), BODY
        )
    assert exc.value.code == f"http_{status}"
    assert exc.value.retryable is retryable


async def test_retry_after_header_is_parsed_and_capped() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"Retry-After": "7"})

    with pytest.raises(WebhookDeliveryError) as exc:
        await sender(handler).deliver("http://r.local/", uuid.uuid4(), BODY)
    assert exc.value.retry_after == timedelta(seconds=7)

    def huge(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "999999"})

    with pytest.raises(WebhookDeliveryError) as exc:
        await sender(huge).deliver("http://r.local/", uuid.uuid4(), BODY)
    assert exc.value.retry_after == timedelta(hours=1)


async def test_network_errors_are_retryable() -> None:
    def timeout(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    with pytest.raises(WebhookDeliveryError) as exc:
        await sender(timeout).deliver("http://r.local/", uuid.uuid4(), BODY)
    assert exc.value.code == "timeout" and exc.value.retryable

    def refused(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(WebhookDeliveryError) as exc:
        await sender(refused).deliver("http://r.local/", uuid.uuid4(), BODY)
    assert exc.value.code == "connection_error" and exc.value.retryable


async def test_policy_rejection_is_permanent_and_never_hits_the_network() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(204)

    with pytest.raises(WebhookDeliveryError) as exc:
        await sender(handler, WebhookUrlPolicy()).deliver(
            "https://127.0.0.1/hooks", uuid.uuid4(), BODY
        )
    assert exc.value.code == "url_rejected:ip_literal_not_allowed"
    assert not exc.value.retryable
    assert calls == 0


async def test_large_response_body_is_not_fully_read() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 1_000_000)

    await sender(handler).deliver("http://r.local/", uuid.uuid4(), BODY)  # no error, no OOM
