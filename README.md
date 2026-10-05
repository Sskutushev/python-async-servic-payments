# Asynchronous payment processing service

FastAPI · Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL · RabbitMQ (FastStream) · Alembic · Docker Compose

A client sends a payment. The API saves it and answers `202` right away. A background consumer
then charges a (simulated) payment gateway, saves the result and sends a signed webhook to the
client.

The exact promises, in one place:

* one `Idempotency-Key` → one stored payment, however many times the request is repeated;
* every event is delivered **at least once**; the consumer reads the saved state before each
  step, so a duplicate message or a crash replayed later ends in the same state;
* the simulated gateway is called at most three counted times per payment, and its answer
  for a given payment id is always the same;
* a webhook **may arrive more than once** with the same `event_id`; it is never sent more
  than three counted times without an operator's replay.

There is no "exactly once" anywhere in this list on purpose — see ADR-0001.

```
            ┌──────────┐  one transaction: payment + event  ┌──────────────┐
 client ───►│   api    │───────────────────────────────────►│  PostgreSQL  │◄──────────────┐
   ▲        └──────────┘                                    └──────┬───────┘               │
   │ 202 / GET                                                     │ outbox relay           │ saves progress
   │                                                               ▼ (waits for confirm)    │ after every step
   │                                                        ┌──────────────┐        ┌───────┴───────┐
   │                                                        │   RabbitMQ   │ ─────► │   consumer    │──► gateway (simulated)
   │                                                        │ payments.new │        │  one handler  │──► webhook (signed)
 webhook ◄──────────────────────────────────────────────────│ payments.dlq │ ◄───── └───────────────┘
```

## Run it

```bash
cp .env.example .env            # demo secrets; change them for anything shared
docker compose --profile demo up --build -d --wait
```

What starts: `postgres`, `rabbitmq` (management UI: http://127.0.0.1:15672, `payments` /
`payments`), `migrate` (runs the migrations once), `api` (http://127.0.0.1:8000), `consumer`
(the handler plus the outbox relay and the recovery scan) and, in the `demo` profile,
`webhook-receiver` (http://127.0.0.1:9000) — a small server that checks signatures and can be
told to fail on purpose.

### Create a payment and read it back

```bash
export API_KEY=demo-api-key-change-me-please

curl -s -X POST http://127.0.0.1:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" -H "Idempotency-Key: order-42" -H "Content-Type: application/json" \
  -d '{"amount":"100.00","currency":"USD","description":"order 42",
       "metadata":{"order_id":42},"webhook_url":"http://webhook-receiver:9000/hooks"}'
# 202 {"payment_id":"…","status":"pending","created_at":"…"}    Location: /api/v1/payments/…

curl -s http://127.0.0.1:8000/api/v1/payments/<payment_id> -H "X-API-Key: $API_KEY"
# {"payment_id":"…","amount":"100.00","currency":"USD","status":"succeeded","failure_code":null,
#  "notification_status":"delivered","notification_attempts":1,"processed_at":"…",…}

curl -s http://127.0.0.1:9000/received      # what the demo receiver got (signature verified)
```

API docs: http://127.0.0.1:8000/docs — the `X-API-Key` header is needed there too, like
everywhere else.

### Watch the interesting cases

```bash
uv run python tools/demo.py --scenario happy    # 202 → consumer → signed webhook → GET
uv run python tools/demo.py --scenario replay   # same key → same id; different body → 409
uv run python tools/demo.py --scenario retry    # receiver fails twice → delivered on the 3rd try, charged once
uv run python tools/demo.py --scenario dlq      # receiver always fails → exhausted + DLQ, result is kept
docker compose run --rm api replay <payment_id> # operator: retry only the step that failed
docker compose --profile demo logs -f consumer webhook-receiver
```

## The API

| | |
|---|---|
| Authentication | `X-API-Key` on **every** route, docs and health included. Wrong or missing key → `401`. |
| `POST /api/v1/payments` | Header `Idempotency-Key` (required, 1–128 printable ASCII characters). Body: `amount`, `currency` (`RUB`, `USD`, `EUR`), `description` (≤ 1000 chars), `metadata` (JSON object ≤ 16 KiB), `webhook_url`. Whole body ≤ 32 KiB. |
| `amount` | A decimal **string** (`"100.00"`) or an integer. JSON floats are refused (`422`) — floats cannot hold money exactly. Also refused: more than 2 decimals, zero, negatives, NaN, values above `NUMERIC(18,2)`. `100`, `"100"` and `"100.0"` are the same amount. |
| Response `202` | `{payment_id, status, created_at}` plus headers `Location` and `Idempotency-Replayed: true|false`. |
| Same key again | Same body → `202` with the original `payment_id` and the current status. Different body → `409 idempotency_conflict`. "Same body" is decided by a hash of the cleaned-up values, so key order and `100` vs `100.00` do not matter. |
| `GET /api/v1/payments/{id}` | All details, including `failure_code`, `notification_status`, `notification_attempts`, `notification_last_error`, `processing_halt_reason`, `processed_at`. Unknown id → `404`. |
| Errors | Always `{"error": {"code", "message", "request_id", "details"?}}`. `413` body too large, `422` validation, `503` the database is unavailable. |
| Tracing | `X-Request-Id` is accepted or generated, returned on every response and written into every log line. |

## The webhook

Body — canonical JSON, built once when the result is saved, identical bytes on every attempt:

```json
{"event_id":"…","event_type":"payment.succeeded","schema_version":1,"payment_id":"…",
 "amount":"100.00","currency":"USD","status":"succeeded","failure_code":null,"occurred_at":"…"}
```

Headers: `X-Webhook-Id` (= `event_id`, the same on every attempt — **de-duplicate on it**),
`X-Webhook-Timestamp` (unix seconds of this attempt), `X-Webhook-Signature: v1=<hex>` where
`hex = HMAC-SHA256(WEBHOOK_SECRET, "<timestamp>.<raw body>")`. `tools/webhook_receiver.py`
shows how to verify it (constant-time compare, 5-minute window, de-duplication by id).

How answers are read: `2xx` delivered · `408`, `429`, `5xx`, timeout, connection error → retry
(`Retry-After` is respected up to a cap) · anything else, including redirects → give up.

## What is guaranteed, and what is not

| Situation | What happens | Where it is proven |
|---|---|---|
| 20 identical `POST`s at the same time | one payment, one event, every request gets `202` with the same id | `tests/integration/test_idempotency_pg.py` |
| RabbitMQ is down while clients `POST` | the API still answers `202`; the relay catches up when the broker is back | outbox design, `tests/unit/test_relay.py` |
| Relay crashes between "published" and "marked" | the event is published again with the same id; the consumer ignores the duplicate | `test_relay.py::test_lost_lease…` |
| Payment succeeded, webhook answers `500` | retry after 1 s, then 2 s; **the gateway is not called again** | `test_process_payment.py::test_webhook_failure_never_reprocesses_the_payment` |
| Third webhook attempt fails too | `notification_status = exhausted`, a message in `payments.dlq`; the payment **stays** `succeeded` / `failed` | `…::test_third_failed_attempt_exhausts_and_dead_letters` |
| Worker dies after saving the result, before the webhook | the redelivered message continues at the webhook step | `…::test_crash_after_result_before_webhook…` |
| Worker dies during the 1st or 2nd webhook attempt | the attempt is counted; the recovery scan continues with the next one | `…::test_crash_during_early_webhook_attempt…` |
| Worker dies during the **3rd** webhook attempt | no 4th call: the payment goes to an operator as `delivery_outcome_unknown_after_budget` | `…::test_crash_during_third_webhook_attempt…`, `tests/integration/test_recovery_budget_pg.py` |
| Two workers get the same message | one charges, the other skips; a worker whose reservation expired cannot write anything — not a result, not a retry | `…::test_stale_worker…`, `test_recovery_budget_pg.py` |
| Two webhook attempts overlap | the late outcome of the old attempt is ignored; the newer record wins | `…::test_late_outcome_of_a_stale_webhook_attempt…`, `test_recovery_budget_pg.py` |
| Gateway unreachable three times | the payment stays `pending` (unknown ≠ declined), is **halted** with reason `gateway_unavailable_after_budget` and goes to the DLQ; duplicates and the recovery scan never trigger a 4th call | `…::test_halted_payment_is_never_charged_again_until_replay`, `test_recovery_budget_pg.py` |
| Duplicate message arrives **while the 3rd gateway call is in flight** | the duplicate sees the active reservation and skips; the running call finishes and its result is saved | `test_recovery_budget_pg.py::test_duplicate_during_third_gateway_call_does_not_halt`, `test_state_boundaries.py` |
| Worker dies **during** the 3rd gateway call | once the reservation expires: no 4th call, halted with reason `gateway_outcome_unknown_after_budget` — the answer is unknown, not a confirmed decline | `test_recovery_budget_pg.py::test_lost_third_gateway_attempt_halts_without_fourth_call` |
| DNS fails or hangs while sending a webhook | counts as a temporary delivery error: the normal three attempts apply; a hanging resolver is cut off after `dns_timeout_seconds` | `test_webhook_sender.py::test_dns_*` |
| Two merchant hostnames share one IP | each webhook opens its own TLS connection; a connection verified for one hostname is never reused for another | `tests/unit/test_webhook_tls.py` (real TLS server) |
| Operator runs `replay` | only then a new set of three attempts opens, for the failed step only, with the same webhook event id | `tests/unit/test_recovery_and_replay.py` |
| Broken message or unknown payment id | rejected; RabbitMQ moves it to `payments.dlq` | `tests/rabbit/test_broker.py` |
| `webhook_url` points at localhost, a private network, the cloud metadata IP, has credentials, uses `http` | `422` at creation; refused again at send time | `test_url_policy.py`, `test_api.py` |
| DNS answer changes between check and connection | we connect to the address we checked, with the original hostname for TLS and `Host` | `test_webhook_sender.py::test_strict_policy_connects_to_the_checked_address` |

In short: **at least once, and every step is safe to repeat** (ADR-0001). The simulated gateway
is never charged twice for one payment. A webhook can arrive twice with the same id, but it is
never sent more than three counted times without an operator's replay. "Three attempts" means
three in total, counted *before* each call, so a crash during a call still counts. "Unknown" is a
separate state from "declined": an outage never turns a payment into `failed`.

### Not for real money without a provider adapter

The gateway here is a simulator, as the task asks. Its answer depends only on the payment id,
so "call it again" is always safe. A real provider is different: after a network failure the
money may have moved even though we never saw the answer. Before this service is pointed at a
real provider, the adapter must send `payment.id` as the provider's idempotency key and must
offer a status lookup, and an operator must consult that lookup before replaying a payment
halted as `gateway_outcome_unknown_after_budget`. The `PaymentGateway` interface documents
both requirements; nothing in this repository claims to have met them.

## Retries, dead letters, recovery (ADR-0002, ADR-0003)

* A retry is a row in the outbox, written in the same transaction as the attempt counter.
  The relay publishes it back to `payments.new` when it is due. Default: 3 attempts, delays
  1 s and 2 s, cap 60 s (`RETRY_*`).
* After the last attempt the same transaction writes a `payment.dead_letter` row that is
  routed to `payments.dead` → `payments.dlq`. It contains `payment_id`, `phase`,
  `counted_attempts`, `failure_code` — never the webhook URL or any secret.
* The budget is checked **before** every call. If a worker died right after counting the
  last attempt, the next worker does not call again; it hands the payment to an operator.
* A halted payment or an exhausted webhook is left alone by everything automatic. The
  recovery scan skips it, duplicate messages do nothing. `payments replay <id>` is the only
  way to continue, and it retries just the failed step.
* The recovery scan (every `RECOVERY_INTERVAL_SECONDS`) finds payments whose message was
  lost — an expired processing reservation, or a webhook attempt that never recorded its
  outcome — and only when no outbox event is already waiting for them.

## Data model

One table for payments, one for the outbox, as the task asks. Two migrations
(`alembic/versions/`); a test checks that the ORM and the migrated schema match and that
downgrade/upgrade round-trips.

`payments`:

| group | columns |
|---|---|
| business | `id`, `amount NUMERIC(18,2)`, `currency`, `description`, `metadata JSONB`, `status`, `idempotency_key UNIQUE`, `request_fingerprint`, `webhook_url`, `created_at`, `processed_at`, `gateway_reference`, `failure_code` |
| processing | `gateway_attempts`, `processing_lease_token`, `processing_lease_until`, `processing_halted_at`, `processing_halt_reason` |
| notification | `notification_status` (`not_ready → pending → delivered | exhausted`), `notification_event_id UNIQUE`, `notification_body` (frozen), `notification_attempts`, `notification_lease_token`, `notification_next_attempt_at`, `notification_delivered_at`, `notification_last_error` |

`CHECK` constraints make impossible states impossible: a final status needs `processed_at`,
a pending one cannot have a webhook, a halt needs a reason and only applies while pending.

`outbox`: `id` (= event id), `event_type`, `schema_version`, `aggregate_id → payments`,
`exchange`, `routing_key`, `payload JSONB`, `dedup_key UNIQUE`, `created_at`, `available_at`,
`published_at`, `lease_token`, `lease_until`, `publication_attempts`, `last_error`.

## RabbitMQ

```
exchange payments.events (direct, durable) ─payments.new─►    queue payments.new (durable, dead-letters to payments.dead)
exchange payments.dead   (direct, durable) ─payments.failed─► queue payments.dlq (durable)
```

Publishing: persistent messages, publisher confirms, `mandatory` with returns raised — a
message no queue accepts is never marked as published. Consuming: manual acknowledgements,
prefetch `CONSUMER_PREFETCH` (8), ACK after commit, NACK + requeue if our own infrastructure
fails, REJECT (→ DLQ) for broken messages.

## Security (ADR-0004)

* Static API key on every route, constant-time compare, never echoed in errors or logs.
* Webhook URL policy: `https` only (`http` in dev), no credentials, ports 80/443, no IP
  addresses, no `localhost`, optional allow-list; before sending, every resolved address
  must be public, and the request goes to that checked address (DNS rebinding is closed).
  No redirects, no environment proxies, bounded response read, explicit timeouts.
* `APP_ENV=prod` refuses to start with the dev flags or with demo-looking secrets.
* Secrets are `SecretStr`; logs contain only an allow-listed set of fields.
* Body, metadata and description size caps; unknown JSON fields are rejected.
* Non-root container; pinned `uv.lock`; `bandit`, `pip-audit`, `gitleaks`, CodeQL, Trivy and
  hadolint in CI.

## Configuration

Everything comes from environment variables and is validated at start-up
(`src/payments/settings.py`, `.env.example`). The important ones: `API_KEY`, `WEBHOOK_SECRET`
(≥ 16 chars), `DATABASE_URL`, `RABBITMQ_URL`, `WEBHOOK_ALLOWED_HOSTS`, `GATEWAY_*` (delay range,
success rate, seed), `RETRY_*`, `PROCESSING_LEASE_SECONDS`, `OUTBOX_*`, `RECOVERY_*`,
`CONSUMER_PREFETCH`, `BACKGROUND_MAX_CONSECUTIVE_FAILURES`.

The simulated gateway answers deterministically for a given `(seed, payment_id)`, so a repeated
message cannot turn a decline into a success.

### Production-style run

`docker-compose.yml` is a **demo**: default credentials, ports on `127.0.0.1`, dev flags on
(the demo receiver lives on the private network). For a production-style run:

```bash
API_KEY=… WEBHOOK_SECRET=… GATEWAY_SEED=… POSTGRES_PASSWORD=… RABBITMQ_PASSWORD=… \
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

The overlay sets `APP_ENV=prod`, requires every secret, publishes no database or broker ports
and has no demo receiver. The service itself refuses to start with a demo value in any secret.

## Layout

```
src/payments/
  domain/         money, the Payment (two state machines, leases, halt), outbox events, fingerprint — no I/O
  application/    use cases: create_payment, process_payment (steps), relay, recovery, replay, retry policy, ports
  infrastructure/ PostgreSQL repositories + unit of work, simulated gateway, HTTP webhook sender, URL policy, signing
  api/            FastAPI app, routes, schemas, middleware (auth, request id, body cap), error format
  messaging/      RabbitMQ topology, publisher with confirms, the one consumer, the consumer process
  bootstrap.py    wiring · cli.py: api | consumer | migrate | replay
alembic/          async env + migrations 0001, 0002
tests/            unit (fakes, Hypothesis) · integration (real PostgreSQL) · rabbit (real RabbitMQ) · e2e (compose)
tools/            demo webhook receiver, demo script, repository hygiene check
docs/adr/         four decisions: delivery guarantees, retries, consumer steps, webhook security
```

Rule of thumb: `domain` imports nothing from the outer layers; `application` depends only on
the interfaces in `ports.py`; HTTP and AMQP translate results at the edge (`409`/`422`/`503`,
ACK/NACK/REJECT).

## Development and quality gates

```bash
uv sync --all-groups
uv run pre-commit install                      # ruff, gitleaks, file checks before every commit
make check                                     # ruff format/lint, mypy --strict, bandit, pip-audit, radon
make test-unit                                 # ~200 unit + property tests, no services, a few seconds
POSTGRES_HOST_PORT=5433 docker compose up -d postgres rabbitmq
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@127.0.0.1:5433/payments_test make test-integration
make up && make test-e2e
```

The integration suites skip themselves when the service is unreachable, so `pytest` is always
safe to run. `tests/rabbit` starts its own consumer on `payments.new`, so stop the compose
`consumer` first (or point `TEST_RABBITMQ_URL` at another vhost).

CI on GitHub runs four independent gates on every push and pull request:

| workflow | what it checks |
|---|---|
| `code` | formatting, lint, `mypy --strict`, cyclomatic complexity (no function worse than grade B) |
| `logic` | unit + property tests with ≥ 85 % coverage, real PostgreSQL + RabbitMQ, docker compose end-to-end |
| `security` | `pip-audit` (also weekly), `bandit`, `gitleaks` over the whole history, CodeQL, hadolint, Trivy for config and the built image |
| `files` | lockfile in sync, forbidden files, YAML/TOML/JSON validity, line endings, single migration head, `docker compose config`, all pre-commit hooks |

Dependabot opens weekly update PRs for Python packages, GitHub Actions and base images.

## Operations

* Health: `GET /health/live`, `GET /health/ready` (database round-trip). Both need the API key.
* Logs: one JSON line per event with `request_id`, `payment_id`, `event_id`, `phase`,
  `attempt`, `outcome`, `error_code`, `duration_ms`.
* Worth an alert: anything in `payments.dlq`, old unpublished outbox rows, `pending` payments
  older than the lease, the consumer process restarting (a background loop gave up).
* Shutdown: SIGTERM stops consuming, waits up to 30 s for in-flight handlers; unacknowledged
  messages are redelivered and expired reservations are picked up by the recovery scan.

## What this is not

A simulation of a card payment flow with the seams a real gateway needs. It is not a ledger,
not multi-tenant (one static key means one global idempotency scope), and it does not promise
exactly-once webhooks. Single-node compose is not highly available: volumes are mandatory,
replication is not provided.
