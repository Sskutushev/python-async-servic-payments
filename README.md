# Asynchronous payment processing service

FastAPI · Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL · RabbitMQ (FastStream) · Alembic · Docker Compose

`POST /api/v1/payments` stores a payment **and** its outbox event in one transaction and
answers `202` at once. A single consumer charges a simulated gateway (2–5 s, ~90 % success),
stores the result and delivers a signed webhook — with three counted attempts, exponential
backoff and a dead-letter queue. Everything is idempotent: the same request, the same
message or the same crash replayed twice converges to the same state.

```
            ┌──────────┐  1 tx: payments + outbox   ┌──────────────┐
 client ───►│   api    │───────────────────────────►│  PostgreSQL  │◄───────────────┐
   ▲        └──────────┘                            └──────┬───────┘                │
   │ 202 / GET                                             │ outbox relay            │ checkpointed
   │                                                       ▼ (confirms, mandatory)   │ stages
   │                                                ┌──────────────┐          ┌──────┴──────┐
   │                                                │   RabbitMQ   │ ───────► │  consumer   │──► gateway (sim)
   │                                                │ payments.new │          │  (1 handler)│──► webhook (HMAC)
 webhook ◄──────────────────────────────────────────│ payments.dlq │ ◄─────── └─────────────┘
```

## Quick start

```bash
cp .env.example .env            # demo secrets; change them for anything shared
docker compose --profile demo up --build -d --wait
```

Services: `postgres`, `rabbitmq` (management UI on http://127.0.0.1:15672, `payments`/`payments`),
`migrate` (one-shot `alembic upgrade head`), `api` (http://127.0.0.1:8000), `consumer`
(handler + outbox relay + recovery scan) and, in the `demo` profile, `webhook-receiver`
(http://127.0.0.1:9000) which verifies signatures and can be told to fail.

### Create and read a payment

```bash
export API_KEY=demo-api-key-change-me-please

curl -s -X POST http://127.0.0.1:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" -H "Idempotency-Key: order-42" -H "Content-Type: application/json" \
  -d '{"amount":"100.00","currency":"USD","description":"order 42",
       "metadata":{"order_id":42},"webhook_url":"http://webhook-receiver:9000/hooks"}'
# 202 {"payment_id":"…","status":"pending","created_at":"…"}   Location: /api/v1/payments/…

curl -s http://127.0.0.1:8000/api/v1/payments/<payment_id> -H "X-API-Key: $API_KEY"
# {"payment_id":"…","amount":"100.00","currency":"USD","status":"succeeded",
#  "failure_code":null,"notification_status":"delivered","notification_attempts":1,
#  "processed_at":"…","created_at":"…",…}

curl -s http://127.0.0.1:9000/received     # what the demo receiver got (signature-verified)
```

OpenAPI: http://127.0.0.1:8000/docs (needs the `X-API-Key` header too — every endpoint does).

### Scripted demos

```bash
uv run python tools/demo.py --scenario happy    # 202 → consumer → signed webhook → GET
uv run python tools/demo.py --scenario replay   # same key → same id; different body → 409
uv run python tools/demo.py --scenario retry    # receiver fails twice → delivered on 3rd, charged once
uv run python tools/demo.py --scenario dlq      # receiver always fails → exhausted + DLQ, result kept
docker compose run --rm api replay <payment_id> # operator replay of the failed phase only
```

Watch it happen: `docker compose --profile demo logs -f consumer webhook-receiver`.

## API contract

| | |
|---|---|
| Auth | `X-API-Key` on **all** routes (docs, health included), constant-time compare → `401` |
| `POST /api/v1/payments` | `Idempotency-Key` header required: 1–128 printable ASCII. Body: `amount`, `currency` (`RUB`/`USD`/`EUR`), `description` (≤1000), `metadata` (JSON object ≤16 KiB), `webhook_url`. Request body ≤32 KiB. |
| `amount` | Decimal **string** (`"100.00"`) or integer. JSON floats are rejected (`422`), as are >2 fractional digits, zero, negatives, NaN/∞ and values above `NUMERIC(18,2)`. `100`, `"100"`, `"100.0"` are the same amount. |
| `202` | `{payment_id, status, created_at}` + `Location` + `Idempotency-Replayed: true|false` |
| Replay | Same key + semantically same body → `202` with the original `payment_id` and current status |
| Conflict | Same key + different body → `409 idempotency_conflict` (fingerprint = SHA-256 of the normalized payload, key order irrelevant) |
| `GET /api/v1/payments/{id}` | Full details incl. `failure_code`, `notification_status`, `notification_attempts`, `processed_at`; `404` otherwise |
| Errors | Always `{"error": {"code", "message", "request_id", "details"?}}`; `413` body too large, `422` validation, `503` infrastructure |
| Correlation | `X-Request-Id` echoed/assigned on every response and present in JSON logs |

## Webhook contract

Body (canonical JSON, frozen when the result is stored, byte-identical on every retry):

```json
{"event_id":"…","event_type":"payment.succeeded","schema_version":1,"payment_id":"…",
 "amount":"100.00","currency":"USD","status":"succeeded","failure_code":null,"occurred_at":"…"}
```

Headers: `X-Webhook-Id` (= `event_id`, stable across retries — **de-duplicate on it**),
`X-Webhook-Timestamp` (unix seconds of this delivery), `X-Webhook-Signature: v1=<hex>` where
`hex = HMAC-SHA256(WEBHOOK_SECRET, "<timestamp>.<raw body>")`. See `tools/webhook_receiver.py`
for a reference verifier (constant-time compare, 5-minute window, dedup).

Classification: `2xx` delivered · `408/429/5xx`/timeout/connection error → retry (`Retry-After`
honoured up to the cap) · any other status or redirect → permanent failure → DLQ.

## Guarantees — and their limits

| Scenario | What happens | Proof |
|---|---|---|
| 20 concurrent `POST` with one key | one row, one outbox event, 20 × `202` with the same id | `tests/integration/test_idempotency_pg.py` |
| RabbitMQ down while `POST`ing | API still answers `202`; relay catches up when the broker returns | outbox + `test_relay.py` |
| Relay crashes between publish and mark | event re-published with the same `event_id`; consumer ignores it | `test_relay.py::test_lost_lease…` |
| Payment `succeeded`, webhook `500` | retry scheduled (1 s, then 2 s); **gateway never called again** | `test_process_payment.py::test_webhook_failure_never_reprocesses_the_payment` |
| Third webhook attempt fails | `notification_status=exhausted`, DLQ message; payment **stays** `succeeded`/`failed` | `…::test_third_failed_attempt_exhausts_and_dead_letters` |
| Consumer crashes after result, before webhook | redelivery resumes at the webhook stage | `…::test_crash_after_result_before_webhook…` |
| Consumer crashes after receiver accepted, before commit | the same `event_id` is delivered again (at-least-once) | `…::test_crash_after_webhook_accepted…` |
| Two workers get the same message | processing lease: one charges, the other skips; stale lease cannot overwrite | `…::test_stale_worker…`, `test_process_payment_pg.py::test_concurrent_duplicates_charge_once` |
| Gateway unreachable 3× | payment stays `pending` (unknown ≠ declined), DLQ for operators | `…::test_gateway_unreachable_three_times…` |
| Unknown payment / malformed message | rejected → broker dead-letters to `payments.dlq` | `tests/rabbit/test_broker.py` |
| `webhook_url` → loopback / RFC 1918 / metadata IP / userinfo / http | `422` at creation, permanent failure at delivery | `test_url_policy.py`, `test_api.py` |

**Delivery semantics are at-least-once with idempotent processing, not exactly-once**
(ADR-0001). The service never charges a payment twice; a webhook can arrive twice with the
same `X-Webhook-Id`. "3 attempts" means three *counted* attempts in total (initial + 2 retries),
counted before the HTTP call, so a crash mid-flight still consumes budget. "Unknown" is a
distinct state from "declined": infrastructure failures never turn a payment into `failed`.

## Retry, DLQ and recovery (ADR-0002, ADR-0003)

* Retries are **outbox rows** (`payment.retry`, `available_at = now + base·2^(n-2)`) written in
  the same transaction as the attempt counter; the relay re-publishes them to `payments.new`.
  Default budget: 3 attempts, delays 1 s and 2 s, cap 60 s (`RETRY_*` settings).
* Exhaustion writes a `payment.dead_letter` outbox row routed to exchange `payments.dead` →
  queue `payments.dlq` with `{payment_id, phase, counted_attempts, failure_code, …}` — never
  the webhook URL or secrets. `payments.new` also has broker-level DLX to the same queue for
  rejected (poison) messages.
* The recovery scan (every `RECOVERY_INTERVAL_SECONDS`) re-enqueues payments whose in-flight
  message was lost: expired processing leases and webhook attempts that never recorded an
  outcome — only when no unpublished outbox row exists, so nothing is scheduled twice.
* `payments replay <payment_id>` reopens an exhausted notification (fresh budget, **same**
  `event_id`) or re-queues a `pending` payment after a gateway outage. It never re-charges a
  payment with a stored result.

## Data model

`payments` (one row, two state machines, enforced by `CHECK`s):

| group | columns |
|---|---|
| business | `id`, `amount NUMERIC(18,2)`, `currency`, `description`, `metadata JSONB`, `status`, `idempotency_key UNIQUE`, `request_fingerprint`, `webhook_url`, `created_at`, `processed_at`, `gateway_reference`, `failure_code` |
| processing | `gateway_attempts`, `processing_lease_token`, `processing_lease_until` |
| notification | `notification_status (not_ready→pending→delivered\|exhausted)`, `notification_event_id UNIQUE`, `notification_body` (frozen), `notification_attempts`, `notification_next_attempt_at`, `notification_delivered_at`, `notification_last_error` |

`outbox`: `id` (= event id), `event_type`, `schema_version`, `aggregate_id → payments`, `exchange`,
`routing_key`, `payload JSONB`, `dedup_key UNIQUE`, `created_at`, `available_at`, `published_at`,
`lease_token`, `lease_until`, `publication_attempts`, `last_error`. Partial indexes cover the only
hot queries (unpublished by `available_at`; unpublished by aggregate; pending work by time).

Migrations: `alembic/versions/0001_payments_and_outbox.py`; a test asserts the ORM metadata and
the migrated schema have no drift and that downgrade/upgrade round-trips.

## Broker topology

```
exchange payments.events (direct, durable) ─payments.new─► queue payments.new (durable, DLX → payments.dead)
exchange payments.dead   (direct, durable) ─payments.failed─► queue payments.dlq (durable)
```

Publishing: persistent delivery mode, publisher confirms, `mandatory=true` with returns raised —
an unroutable message is never marked published. Consuming: manual acks, prefetch
`CONSUMER_PREFETCH` (8), ACK only after commit, NACK+requeue on infrastructure errors, REJECT
(→ DLQ) for poison.

## Security (ADR-0004)

* Static API key on every route; `hmac.compare_digest`; never echoed in errors or logs.
* Webhook URL policy: `https` only (http in dev), no userinfo/fragment, ports 80/443, no IP
  literals, no `localhost`, optional host allow-list; at delivery all resolved addresses must be
  public (IPv4-mapped IPv6, CGNAT, link-local, multicast covered). No redirects, no env proxies,
  bounded response read, explicit timeouts, bounded connection pool.
* `APP_ENV=prod` refuses to start with `WEBHOOK_ALLOW_PRIVATE_NETWORKS` / `…INSECURE_HTTP`.
* Secrets are `SecretStr`; JSON logs emit a fixed allow-list of fields (no bodies, no URLs with
  query strings, no keys).
* Request body cap, metadata cap, description cap; `extra="forbid"` on input models.
* Non-root container, pinned `uv.lock`, `bandit` + `pip-audit` in CI.
* Known limit: DNS rebinding between resolution and connect is not prevented in-process.

## Configuration

All settings are environment variables validated at startup (`src/payments/settings.py`);
see `.env.example`. Notables: `API_KEY`, `WEBHOOK_SECRET` (≥16 chars), `DATABASE_URL`,
`RABBITMQ_URL`, `WEBHOOK_ALLOWED_HOSTS`, `GATEWAY_*` (delay range, success rate, seed),
`RETRY_*`, `PROCESSING_LEASE_SECONDS`, `OUTBOX_*`, `RECOVERY_*`, `CONSUMER_PREFETCH`.

The simulated gateway's outcome is a deterministic function of `(seed, payment_id)`: a
redelivered message cannot turn a decline into a success.

## Project layout

```
src/payments/
  domain/         money, payment aggregate (state machines, lease), outbox events, fingerprint — no I/O
  application/    use cases: create_payment, process_payment (stages), relay, recovery, replay, retry policy, ports
  infrastructure/ PostgreSQL repositories + unit of work, simulated gateway, HTTP webhook sender, URL policy, signing
  api/            FastAPI app, routes, schemas, middleware (auth, request id, body cap), error envelope
  messaging/      FastStream topology, publisher (confirms), the single consumer, consumer process
  bootstrap.py    composition root · cli.py: api | consumer | migrate | replay
alembic/          async env + migration 0001
tests/            unit (fakes, Hypothesis) · integration (real PostgreSQL) · rabbit (real RabbitMQ) · e2e (compose)
tools/            demo webhook receiver, demo script
docs/adr/         four decisions: delivery guarantees, outbox retries, consumer stages, webhook security
```

Dependency rule: `domain` imports nothing from the outer layers; `application` depends only on
`ports` (Protocols with real and in-memory implementations); HTTP and AMQP translate errors at
the edge (`409`/`422`/`503`, ACK/NACK/REJECT).

## Development

```bash
uv sync --all-groups
make check             # ruff format/lint, mypy --strict, bandit, pip-audit
make test-unit         # 180 unit + property tests, no services, ~5 s
POSTGRES_HOST_PORT=5433 docker compose up -d postgres rabbitmq
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@127.0.0.1:5433/payments_test make test-integration
make cov               # branch coverage (92 % on the last run)
make up && make test-e2e
```

Integration suites skip themselves when the service is unreachable, so `pytest` is always safe
to run. The `tests/rabbit` suite starts its own consumer on `payments.new`, so it must not share
a broker with a running compose `consumer` (`docker compose stop consumer` first, or point
`TEST_RABBITMQ_URL` at a separate vhost). CI (`.github/workflows/ci.yml`) runs static gates,
unit, PostgreSQL+RabbitMQ and the compose e2e suite on isolated services.

## Operations

* **Health**: `GET /health/live`, `GET /health/ready` (DB round-trip) — both need `X-API-Key`.
* **Logs**: JSON lines with `request_id`, `payment_id`, `event_id`, `phase`, `attempt`,
  `outcome`, `error_code`, `duration_ms`.
* **Signals worth alerting on**: messages in `payments.dlq`, oldest unpublished outbox age,
  `pending` payments older than the lease, relay/recovery task death (the process exits with 1).
* **Shutdown**: SIGTERM stops consuming, waits up to 30 s for in-flight handlers, unacked
  messages are redelivered; expired leases are recovered by the scan.

## What this is not

A fiat payment *simulation* with the integration seams a real gateway needs (`PaymentGateway`
port, idempotent stages, unknown-outcome handling). It is not a ledger, not multi-tenant
(one static key ⇒ global idempotency scope; the next step would be
`UNIQUE(merchant_id, idempotency_key)` and tenant-scoped reads), and it does not promise
exactly-once webhooks. Single-node Compose is not HA: volumes are mandatory, replication is not
provided.
