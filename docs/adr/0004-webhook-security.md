# ADR-0004: Webhook security — signed events and an SSRF policy for merchant URLs

**Status:** accepted · **Date:** 2026-10-05

## Context

`webhook_url` is attacker-controllable input that the service will connect to
from inside the private network; the receiver, in turn, needs proof that a
notification came from us and was not replayed.

## Decision

**Outbound policy** (`WebhookUrlPolicy`), applied at request time and again at
delivery time:

* `https://` only; `http://` only with `WEBHOOK_ALLOW_INSECURE_HTTP` (dev).
* No userinfo, no fragment, ports 80/443 only, no IP literals, no `localhost`.
* Optional host allow-list (`WEBHOOK_ALLOWED_HOSTS`, suffix match).
* At delivery every A/AAAA answer must be public (loopback, RFC 1918, link-local,
  CGNAT, multicast, reserved and IPv4-mapped IPv6 rejected). Redirects are not
  followed; environment proxies are ignored; response bodies are read up to a cap.
* `APP_ENV=prod` refuses to start with either dev flag enabled.

**Signature**: `X-Webhook-Signature: v1=HMAC-SHA256(secret, "<ts>.<body>")`,
`X-Webhook-Timestamp` (per delivery), `X-Webhook-Id` (stable `event_id`). The
body is canonical JSON (sorted keys, no whitespace) and frozen when the gateway
result is stored, so every retry signs identical bytes.

**Inbound**: every HTTP endpoint — including `/docs` and health — requires
`X-API-Key`, compared with `hmac.compare_digest`. Errors never echo the key.

## Consequences

* Known gap: DNS rebinding between resolution and connection is not prevented
  in-process; the allow-list plus egress network rules are the production answer.
* The demo stack targets a private receiver and therefore runs with both dev
  flags on; the compose file documents this explicitly.
* Receivers must verify the signature, bound the timestamp window and
  de-duplicate on `X-Webhook-Id` (the demo receiver does all three).
