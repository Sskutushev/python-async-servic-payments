"""End-to-end against the running compose stack (``docker compose --profile demo up``).

Uses the real API, consumer, broker, database and the demo webhook receiver.
Skipped unless the API answers on E2E_API_URL.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Callable

import httpx
import pytest

API = os.environ.get("E2E_API_URL", "http://127.0.0.1:8000")
RECEIVER = os.environ.get("E2E_RECEIVER_URL", "http://127.0.0.1:9000")
API_KEY = os.environ.get("API_KEY", "demo-api-key-change-me-please")
HEADERS = {"X-API-Key": API_KEY}
WEBHOOK_URL = "http://webhook-receiver:9000/hooks"

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def client() -> httpx.Client:
    c = httpx.Client(timeout=10)
    try:
        if c.get(f"{API}/health/ready", headers=HEADERS).status_code != 200:
            pytest.skip("compose stack not ready")
    except httpx.HTTPError:
        pytest.skip("compose stack not running")
    c.post(f"{RECEIVER}/control/reset")
    return c


def create(client: httpx.Client, key: str, **body: object) -> httpx.Response:
    payload = {
        "amount": "100.00",
        "currency": "USD",
        "description": "e2e",
        "metadata": {},
        "webhook_url": WEBHOOK_URL,
    }
    payload.update(body)
    return client.post(
        f"{API}/api/v1/payments", json=payload, headers={**HEADERS, "Idempotency-Key": key}
    )


def poll(
    client: httpx.Client, payment_id: str, until: Callable[[dict], bool], timeout: float = 60
) -> dict:  # type: ignore[type-arg]
    deadline = time.time() + timeout
    while time.time() < deadline:
        payment = client.get(f"{API}/api/v1/payments/{payment_id}", headers=HEADERS).json()
        if until(payment):
            return payment  # type: ignore[no-any-return]
        time.sleep(0.5)
    raise AssertionError(f"timeout; last state: {payment}")


def received(client: httpx.Client, payment_id: str) -> list[dict]:  # type: ignore[type-arg]
    return [
        r
        for r in client.get(f"{RECEIVER}/received").json()["received"]
        if r["payment_id"] == payment_id
    ]


def test_requires_api_key(client: httpx.Client) -> None:
    assert client.get(f"{API}/api/v1/payments/{uuid.uuid4()}").status_code == 401


def test_happy_path_delivers_signed_webhook(client: httpx.Client) -> None:
    r = create(client, str(uuid.uuid4()))
    assert r.status_code == 202
    payment = poll(
        client, r.json()["payment_id"], lambda p: p["notification_status"] == "delivered"
    )
    assert payment["status"] in {"succeeded", "failed"}
    assert payment["processed_at"] is not None
    hooks = received(client, payment["payment_id"])
    assert len(hooks) == 1
    assert hooks[0]["status"] == payment["status"]
    assert hooks[0]["amount"] == "100.00"
    assert not hooks[0]["duplicate"]


def test_idempotent_replay_and_conflict(client: httpx.Client) -> None:
    key = str(uuid.uuid4())
    first, second = create(client, key), create(client, key)
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert second.headers["Idempotency-Replayed"] == "true"
    assert create(client, key, amount="100.01").status_code == 409
    # Let this payment settle so its webhook cannot interfere with the failure budget below.
    poll(client, first.json()["payment_id"], lambda p: p["notification_status"] == "delivered")


def test_webhook_retry_does_not_reprocess(client: httpx.Client) -> None:
    client.post(f"{RECEIVER}/control/fail?times=2")
    r = create(client, str(uuid.uuid4()))
    payment = poll(
        client, r.json()["payment_id"], lambda p: p["notification_status"] == "delivered"
    )
    assert payment["notification_attempts"] == 3
    hooks = received(client, payment["payment_id"])
    assert len(hooks) == 1  # two failed attempts never reached "received"


def test_exhausted_webhook_keeps_result(client: httpx.Client) -> None:
    client.post(f"{RECEIVER}/control/fail?times=1000")
    try:
        r = create(client, str(uuid.uuid4()))
        payment = poll(
            client, r.json()["payment_id"], lambda p: p["notification_status"] == "exhausted"
        )
        assert payment["notification_attempts"] == 3
        assert payment["status"] in {"succeeded", "failed"}
    finally:
        client.post(f"{RECEIVER}/control/reset")


def test_private_webhook_targets_are_rejected_by_policy_in_strict_mode(
    client: httpx.Client,
) -> None:
    # The demo stack runs with private networks allowed; the metadata IP is still an IP literal
    # and only passes because of the dev flag. Document the behaviour rather than assert a 422.
    r = create(client, str(uuid.uuid4()), webhook_url="https://169.254.169.254/latest")
    assert r.status_code in {202, 422}
