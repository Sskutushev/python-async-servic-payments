"""Demo webhook receiver (not part of the service).

* verifies ``X-Webhook-Signature`` with the shared secret and a 5-minute timestamp window;
* de-duplicates by ``X-Webhook-Id`` (the same event may legitimately arrive twice);
* can be told to fail: ``POST /control/fail?times=N`` makes the next N deliveries return 500.

Endpoints: ``POST /hooks``, ``GET /received``, ``POST /control/fail``, ``POST /control/reset``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

SECRET = os.environ.get("WEBHOOK_SECRET", "demo-webhook-secret-change-me")
PORT = int(os.environ.get("PORT", "9000"))
MAX_SKEW_SECONDS = 300

state: dict[str, object] = {"fail_remaining": 0, "received": [], "seen": set(), "rejected": 0}


def _verify(timestamp: str, body: bytes, signature: str) -> bool:
    if not timestamp.isdigit() or abs(time.time() - int(timestamp)) > MAX_SKEW_SECONDS:
        return False
    mac = hmac.new(SECRET.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"v1={mac}", signature)


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, payload: object) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        if urlsplit(self.path).path == "/received":
            self._json(200, {"received": state["received"], "rejected": state["rejected"]})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        parts = urlsplit(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

        if parts.path == "/control/fail":
            state["fail_remaining"] = int(parse_qs(parts.query).get("times", ["1"])[0])
            self._json(200, {"fail_remaining": state["fail_remaining"]})
            return
        if parts.path == "/control/reset":
            state.update({"fail_remaining": 0, "received": [], "seen": set(), "rejected": 0})
            self._json(200, {"ok": True})
            return
        if parts.path != "/hooks":
            self._json(404, {"error": "not found"})
            return

        event_id = self.headers.get("X-Webhook-Id", "")
        timestamp = self.headers.get("X-Webhook-Timestamp", "")
        signature = self.headers.get("X-Webhook-Signature", "")
        if not _verify(timestamp, body, signature):
            state["rejected"] = int(state["rejected"]) + 1  # type: ignore[call-overload]
            print(f"[receiver] REJECTED bad signature event_id={event_id}", flush=True)
            self._json(HTTPStatus.UNAUTHORIZED, {"error": "bad signature"})
            return

        remaining = int(state["fail_remaining"])  # type: ignore[call-overload]
        if remaining > 0:
            state["fail_remaining"] = remaining - 1
            print(
                f"[receiver] FAILING on purpose event_id={event_id} ({remaining - 1} left)",
                flush=True,
            )
            self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "simulated failure"})
            return

        seen: set[str] = state["seen"]  # type: ignore[assignment]
        duplicate = event_id in seen
        seen.add(event_id)
        payload = json.loads(body)
        state["received"].append({"event_id": event_id, "duplicate": duplicate, **payload})  # type: ignore[attr-defined]
        tag = "DUPLICATE" if duplicate else "OK"
        print(
            f"[receiver] {tag} {payload.get('event_type')} payment_id={payload.get('payment_id')} "
            f"amount={payload.get('amount')} {payload.get('currency')} event_id={event_id}",
            flush=True,
        )
        self._json(HTTPStatus.NO_CONTENT if not duplicate else HTTPStatus.OK, {"ok": True})

    def log_message(self, *_: object) -> None:  # keep the demo output clean
        return


if __name__ == "__main__":
    print(f"[receiver] listening on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()  # noqa: S104 - demo container
