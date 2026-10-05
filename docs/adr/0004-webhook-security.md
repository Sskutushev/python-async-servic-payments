# ADR-0004: Webhook security — signed events and a strict policy for merchant URLs

**Status:** accepted · **Date:** 2026-10-05

## The problem

`webhook_url` is input we do not control, and the service connects to it from inside our
network. Without rules it could be used to reach internal services (SSRF). The receiver,
on the other side, needs proof that a notification came from us and was not replayed.

## What we do

**Outgoing URL policy** (`WebhookUrlPolicy`), applied when the payment is created and
again right before sending:

* `https://` only (`http://` only with the dev flag `WEBHOOK_ALLOW_INSECURE_HTTP`);
* no credentials in the URL, no fragment, only ports 80/443, no IP addresses, no `localhost`;
* optional allow-list of hostnames (`WEBHOOK_ALLOWED_HOSTS`, sub-domains included);
* before sending, the hostname is resolved and every answer must be a public address
  (loopback, private, link-local, shared/CGNAT, multicast and IPv4-mapped IPv6 are refused);
* the request is then sent **to the address that was checked**, with the original hostname
  in the `Host` header and in the TLS handshake. A DNS name that changes between the check
  and the connection (DNS rebinding) cannot redirect us;
* no redirects, no proxy settings from the environment, response bodies read up to a cap,
  explicit timeouts, bounded connection pool;
* `APP_ENV=prod` refuses to start with either dev flag or with demo secrets.

**Signature:** `X-Webhook-Signature: v1=HMAC-SHA256(secret, "<timestamp>.<body>")`,
`X-Webhook-Timestamp` (per delivery), `X-Webhook-Id` (stable event id). The body is
canonical JSON (sorted keys, no spaces) saved when the result is stored, so every retry
signs identical bytes.

**Incoming:** every HTTP endpoint — including `/docs` and health — requires `X-API-Key`,
compared in constant time. Errors never echo the key.

## Consequences

* The demo compose stack targets a receiver on the private network and therefore runs with
  both dev flags on. `docker-compose.prod.yml` turns them off and requires real secrets.
* Receivers must verify the signature, bound the timestamp window and de-duplicate on
  `X-Webhook-Id`. The demo receiver does all three.
* Pinning the IP is done in the application. Network egress rules are still a good second
  layer in production; they are outside this repository.
