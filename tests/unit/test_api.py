"""HTTP contract tests: real FastAPI app, in-memory persistence."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from payments.api.app import create_app
from payments.application.retry import RetryPolicy
from payments.bootstrap import Container, build_url_policy
from payments.settings import Settings
from tests.conftest import TEST_API_KEY, make_settings
from tests.fakes import FakeClock, InMemoryStore, InMemoryUnitOfWork

AUTH = {"X-API-Key": TEST_API_KEY}
VALID: dict[str, Any] = {
    "amount": "100.00",
    "currency": "USD",
    "description": "order 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/hooks",
}


@pytest.fixture
def settings() -> Settings:
    # Strict (production-like) webhook policy so SSRF rejections are exercised end to end.
    return make_settings(webhook_allow_private_networks=False, webhook_allow_insecure_http=False)


@pytest.fixture
async def client(
    settings: Settings, store: InMemoryStore, clock: FakeClock
) -> AsyncIterator[httpx.AsyncClient]:
    container = Container(
        settings=settings,
        engine=None,
        session_factory=None,
        uow_factory=lambda: InMemoryUnitOfWork(store),
        clock=clock,
        retry_policy=RetryPolicy(),
        url_policy=build_url_policy(settings),
    )
    app = create_app(settings, container=container)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://api") as c:
            yield c


def post(client: httpx.AsyncClient, body: dict[str, Any], key: str = "key-1", **headers: str):  # type: ignore[no-untyped-def]
    return client.post(
        "/api/v1/payments", json=body, headers={**AUTH, "Idempotency-Key": key, **headers}
    )


# ------------------------------------------------------------------ auth


@pytest.mark.parametrize("path", ["/api/v1/payments", "/docs", "/openapi.json", "/health/live"])
async def test_every_endpoint_requires_api_key(client: httpx.AsyncClient, path: str) -> None:
    r = await client.get(path)
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "unauthorized"
    assert "x-request-id" in r.headers
    r = await client.get(path, headers={"X-API-Key": "wrong-key-wrong-key-wrong"})
    assert r.status_code == 401


async def test_docs_are_available_with_api_key(client: httpx.AsyncClient) -> None:
    assert (await client.get("/openapi.json", headers=AUTH)).status_code == 200
    assert (await client.get("/health/live", headers=AUTH)).json() == {"status": "ok"}


# ---------------------------------------------------------------- create


async def test_create_returns_202_with_location(
    client: httpx.AsyncClient, store: InMemoryStore
) -> None:
    r = await post(client, VALID)
    assert r.status_code == 202
    body = r.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    assert r.headers["Location"] == f"/api/v1/payments/{body['payment_id']}"
    assert r.headers["Idempotency-Replayed"] == "false"
    assert len(store.outbox) == 1


async def test_replay_returns_same_payment(client: httpx.AsyncClient) -> None:
    first = (await post(client, VALID)).json()
    r = await post(client, {**VALID, "amount": "100"})  # equivalent amount
    assert r.status_code == 202
    assert r.json()["payment_id"] == first["payment_id"]
    assert r.headers["Idempotency-Replayed"] == "true"


async def test_conflict_on_different_body(client: httpx.AsyncClient) -> None:
    await post(client, VALID)
    r = await post(client, {**VALID, "description": "other"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"


async def test_concurrent_posts_with_one_key(
    client: httpx.AsyncClient, store: InMemoryStore
) -> None:
    responses = await asyncio.gather(*(post(client, VALID, key="race") for _ in range(20)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert len(store.payments) == 1


@pytest.mark.parametrize(
    ("field", "value", "fragment"),
    [
        ("amount", 100.5, "JSON float"),
        ("amount", "100.001", "two fractional digits"),
        ("amount", "0", "positive"),
        ("amount", "-5", "positive"),
        ("amount", "abc", "decimal"),
        ("currency", "GBP", "currency"),
        ("description", "x" * 1001, "1000"),
        ("metadata", {"k": "v" * 17000}, "16384"),
        ("webhook_url", "ftp://x.example/", "scheme_not_allowed"),
        ("webhook_url", "https://169.254.169.254/", "ip_literal_not_allowed"),
        ("webhook_url", "http://merchant.example/hooks", "scheme_not_allowed"),
        ("webhook_url", "https://localhost/hooks", "host_not_allowed"),
        ("webhook_url", "https://user:pw@merchant.example/hooks", "userinfo_not_allowed"),
        ("extra_field", 1, "extra"),
    ],
)
async def test_validation_errors(
    client: httpx.AsyncClient, field: str, value: Any, fragment: str
) -> None:
    r = await post(client, {**VALID, field: value})
    assert r.status_code == 422, r.text
    assert r.json()["error"]["code"] == "validation_error"
    assert fragment.lower() in r.text.lower()


async def test_integer_amount_is_accepted(client: httpx.AsyncClient) -> None:
    r = await post(client, {**VALID, "amount": 100})
    assert r.status_code == 202
    detail = await client.get(r.headers["Location"], headers=AUTH)
    assert detail.json()["amount"] == "100.00"


@pytest.mark.parametrize("key", ["", "has space", "x" * 129, "tab\there"])
async def test_invalid_idempotency_key(client: httpx.AsyncClient, key: str) -> None:
    r = await post(client, VALID, key=key)
    assert r.status_code == 422


async def test_missing_idempotency_key(client: httpx.AsyncClient) -> None:
    r = await client.post("/api/v1/payments", json=VALID, headers=AUTH)
    assert r.status_code == 422
    assert "idempotency-key" in r.text.lower()


async def test_oversized_body_is_rejected(client: httpx.AsyncClient) -> None:
    r = await post(client, {**VALID, "description": "x" * 40_000})
    assert r.status_code == 413


# ------------------------------------------------------------------- get


async def test_get_returns_details(client: httpx.AsyncClient, clock: FakeClock) -> None:
    created = (await post(client, VALID)).json()
    r = await client.get(f"/api/v1/payments/{created['payment_id']}", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["amount"] == "100.00"
    assert body["currency"] == "USD"
    assert body["status"] == "pending"
    assert body["processed_at"] is None
    assert body["notification_status"] == "not_ready"
    assert body["metadata"] == {"order_id": 42}
    assert body["created_at"].endswith("+00:00") or body["created_at"].endswith("Z")


async def test_get_unknown_or_malformed_id(client: httpx.AsyncClient) -> None:
    r = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=AUTH)
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "payment_not_found"
    assert (await client.get("/api/v1/payments/not-a-uuid", headers=AUTH)).status_code == 422


async def test_request_id_is_propagated(client: httpx.AsyncClient) -> None:
    r = await client.get("/health/live", headers={**AUTH, "X-Request-Id": "abc-123"})
    assert r.headers["x-request-id"] == "abc-123"


async def test_infrastructure_failure_maps_to_503(
    client: httpx.AsyncClient, store: InMemoryStore
) -> None:
    def crash(_: int) -> None:
        raise RuntimeError("db down")

    store.before_commit = crash
    r = await post(client, VALID)
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "service_unavailable"
    assert "db down" not in r.text


async def test_error_envelope_never_echoes_secrets(client: httpx.AsyncClient) -> None:
    r = await client.get("/api/v1/payments/x", headers={"X-API-Key": "nope-nope-nope-nope"})
    assert "nope" not in r.text
    assert TEST_API_KEY not in r.text


async def test_ready_without_database_is_503(client: httpx.AsyncClient) -> None:
    assert (await client.get("/health/ready", headers=AUTH)).status_code == 503
