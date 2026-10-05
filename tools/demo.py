"""Five-minute demo against a running compose stack (``make up``).

Scenarios (pick with ``--scenario``):
  happy     POST -> 202 -> consumer -> webhook delivered -> GET shows succeeded/failed
  replay    same Idempotency-Key twice (same id), then a different body (409)
  retry     receiver fails twice, third delivery succeeds; payment is charged once
  dlq       receiver fails forever: notification exhausted, DLQ message, result kept
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

import httpx

API = "http://127.0.0.1:8000"
RECEIVER = "http://127.0.0.1:9000"
API_KEY = "demo-api-key-change-me-please"
HEADERS = {"X-API-Key": API_KEY}


def create(client: httpx.Client, key: str, **body: object) -> httpx.Response:
    payload = {
        "amount": "100.00",
        "currency": "USD",
        "description": "demo order",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:9000/hooks",
        **body,
    }
    return client.post(
        f"{API}/api/v1/payments", json=payload, headers={**HEADERS, "Idempotency-Key": key}
    )


def wait_for(
    client: httpx.Client, payment_id: str, *, notification: str, timeout: float = 60
) -> dict:  # type: ignore[type-arg]
    deadline = time.time() + timeout
    while time.time() < deadline:
        payment = client.get(f"{API}/api/v1/payments/{payment_id}", headers=HEADERS).json()
        if payment["notification_status"] == notification:
            return payment  # type: ignore[no-any-return]
        time.sleep(0.5)
    raise SystemExit(f"timeout waiting for notification_status={notification}")


def show(label: str, payload: object) -> None:
    print(f"\n== {label}\n{json.dumps(payload, indent=2, ensure_ascii=False)}")


def happy(client: httpx.Client) -> None:
    r = create(client, str(uuid.uuid4()))
    show("POST /api/v1/payments -> 202", r.json())
    payment = wait_for(client, r.json()["payment_id"], notification="delivered")
    show("GET after processing", payment)
    show("receiver", client.get(f"{RECEIVER}/received").json())


def replay(client: httpx.Client) -> None:
    key = str(uuid.uuid4())
    first, second = create(client, key), create(client, key)
    show("first", first.json())
    show(f"replay (Idempotency-Replayed={second.headers['Idempotency-Replayed']})", second.json())
    conflict = create(client, key, amount="100.01")
    show(f"different body -> {conflict.status_code}", conflict.json())


def retry(client: httpx.Client) -> None:
    client.post(f"{RECEIVER}/control/fail?times=2")
    r = create(client, str(uuid.uuid4()))
    payment = wait_for(client, r.json()["payment_id"], notification="delivered")
    show("delivered after 2 failures (attempts=3, status unchanged)", payment)


def dlq(client: httpx.Client) -> None:
    client.post(f"{RECEIVER}/control/fail?times=1000")
    r = create(client, str(uuid.uuid4()))
    payment = wait_for(client, r.json()["payment_id"], notification="exhausted")
    show("exhausted after 3 attempts; result kept; DLQ message published", payment)
    client.post(f"{RECEIVER}/control/reset")
    print("\nreplay it with:  docker compose run --rm api replay", payment["payment_id"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["happy", "replay", "retry", "dlq"], default="happy")
    args = parser.parse_args()
    with httpx.Client(timeout=10) as client:
        client.post(f"{RECEIVER}/control/reset")
        {"happy": happy, "replay": replay, "retry": retry, "dlq": dlq}[args.scenario](client)
    return 0


if __name__ == "__main__":
    sys.exit(main())
