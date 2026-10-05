# ADR-0001: At-least-once delivery with idempotent processing (no exactly-once promise)

**Status:** accepted · **Date:** 2026-10-05

## Context

The service spans three systems that cannot share a transaction: PostgreSQL,
RabbitMQ and the merchant's webhook endpoint. RabbitMQ confirms *publication*
and *consumption* with separate mechanisms; a crash between any two steps can
replay the step that follows.

## Decision

1. **Outbox**: the payment row and its `payments.new` event are written in one
   PostgreSQL transaction. The API never talks to the broker.
2. **Relay**: leases due outbox rows (`FOR UPDATE SKIP LOCKED`), publishes with
   publisher confirms + `mandatory`, then marks the row published. A crash
   between publish and mark re-publishes the same `event_id`.
3. **Consumer**: every stage re-reads the row under a lock and checks the
   persisted state, so any duplicate converges to the same result. ACK is sent
   only after the state change that makes the message redundant is committed.
4. **Webhook**: carries a stable `event_id` (`X-Webhook-Id`); receivers must
   de-duplicate on it.

## Consequences

* Guarantee: every accepted payment is processed and its webhook is attempted,
  as long as PostgreSQL data survives. No payment is charged twice by this
  service; a webhook may be delivered more than once.
* Not guaranteed: exactly-once webhook delivery, a hard cap of exactly three
  *physical* HTTP calls under every crash (counted attempts are capped at three).
* A real gateway integration must add a provider idempotency key and a status
  lookup: the crash window between an external charge and `record_gateway_result`
  cannot be closed by the outbox alone.
