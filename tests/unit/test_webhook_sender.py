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


async def test_strict_policy_connects_to_the_checked_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DNS rebinding defence: the request goes to the IP we approved, with the original Host/SNI."""
    import asyncio

    async def fake_getaddrinfo(*_: object, **__: object) -> list[tuple]:  # type: ignore[type-arg]
        return [(0, 0, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", fake_getaddrinfo)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    await sender(handler, WebhookUrlPolicy()).deliver(
        "https://merchant.example/hooks?x=1", uuid.uuid4(), BODY
    )
    [req] = seen
    assert req.url.host == "93.184.216.34"
    assert req.url.path == "/hooks"
    assert req.url.query == b"x=1"
    assert req.headers["Host"] == "merchant.example"
    assert req.extensions["sni_hostname"] == "merchant.example"


def _scripted_dns(monkeypatch: pytest.MonkeyPatch, *answers: object) -> list[int]:
    """Each call to getaddrinfo pops one answer: an exception to raise, a list, or "hang"."""
    import asyncio
    import socket

    script = list(answers)
    calls = [0]

    async def fake_getaddrinfo(*_: object, **__: object) -> list[tuple]:  # type: ignore[type-arg]
        calls[0] += 1
        answer = script.pop(0)
        if answer == "hang":
            await asyncio.sleep(3600)
        if isinstance(answer, BaseException):
            raise answer
        return answer  # type: ignore[return-value]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", fake_getaddrinfo)
    _ = socket  # keep the import explicit for readers
    return calls


PUBLIC = [(0, 0, 0, "", ("93.184.216.34", 0))]


async def test_dns_failure_is_a_retryable_delivery_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import socket

    calls = _scripted_dns(monkeypatch, socket.gaierror("temporary failure"), PUBLIC)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    s = sender(handler, WebhookUrlPolicy())
    with pytest.raises(WebhookDeliveryError) as exc:
        await s.deliver("https://merchant.example/hooks", uuid.uuid4(), BODY)
    assert exc.value.code == "dns_resolution_failed"
    assert exc.value.retryable
    assert seen == []  # nothing was sent
    await s.deliver("https://merchant.example/hooks", uuid.uuid4(), BODY)  # next attempt works
    assert len(seen) == 1
    assert calls[0] == 2  # the name was resolved and checked again


async def test_dns_timeout_is_bounded_and_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    _scripted_dns(monkeypatch, "hang")
    s = sender(lambda _: httpx.Response(204), WebhookUrlPolicy(dns_timeout_seconds=0.05))
    with pytest.raises(WebhookDeliveryError) as exc:
        await s.deliver("https://merchant.example/hooks", uuid.uuid4(), BODY)
    assert exc.value.code == "dns_timeout"
    assert exc.value.retryable


async def test_dns_answer_with_private_address_is_permanent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _scripted_dns(monkeypatch, [(0, 0, 0, "", ("10.0.0.5", 0))])
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(204)

    with pytest.raises(WebhookDeliveryError) as exc:
        await sender(handler, WebhookUrlPolicy()).deliver(
            "https://merchant.example/hooks", uuid.uuid4(), BODY
        )
    assert exc.value.code == "url_rejected:resolves_to_private_address"
    assert not exc.value.retryable
    assert calls == 0


async def test_three_dns_failures_use_the_normal_budget(
    monkeypatch: pytest.MonkeyPatch, uow_factory, store, clock
) -> None:
    """DNS outage → retry, retry, DLQ — exactly like a 500 from the receiver."""
    import socket
    from datetime import timedelta

    from payments.application.process_payment import ProcessOutcome, ProcessPayment
    from payments.application.retry import RetryPolicy
    from payments.domain.payment import NotificationStatus
    from tests.fakes import SUCCESS, FakeGateway, make_payment

    _scripted_dns(monkeypatch, *[socket.gaierror("down")] * 3)
    payment = make_payment(webhook_url="https://merchant.example/hooks")
    store.payments[payment.id] = payment
    process = ProcessPayment(
        uow_factory=uow_factory,
        gateway=FakeGateway(SUCCESS),
        webhooks=sender(lambda _: httpx.Response(204), WebhookUrlPolicy()),
        clock=clock,
        retry_policy=RetryPolicy(),
        processing_lease=timedelta(seconds=60),
    )
    outcomes = []
    for _ in range(3):
        outcomes.append(await process(payment.id))
        for e in store.unpublished():
            e.published_at = clock.now()
        clock.advance(timedelta(seconds=5))
    assert outcomes == [ProcessOutcome.RETRY_SCHEDULED] * 2 + [ProcessOutcome.DEAD_LETTERED]
    saved = store.payments[payment.id]
    assert saved.notification_status is NotificationStatus.EXHAUSTED
    assert saved.notification_attempts == 3
    assert saved.notification_last_error == "dns_resolution_failed"


async def test_dev_policy_keeps_hostnames(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    await sender(handler).deliver("http://webhook-receiver:9000/hooks", uuid.uuid4(), BODY)
    assert seen[0].url.host == "webhook-receiver"
    assert "sni_hostname" not in seen[0].extensions


async def test_large_response_body_is_not_fully_read() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 1_000_000)

    await sender(handler).deliver("http://r.local/", uuid.uuid4(), BODY)  # no error, no OOM
