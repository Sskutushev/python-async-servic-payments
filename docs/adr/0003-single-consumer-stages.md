# ADR-0003: One consumer, explicit checkpointed stages, two state machines on one row

**Status:** accepted · **Date:** 2026-10-05

## Context

The task asks for *one* consumer that charges the gateway, updates the
database and sends the webhook. Doing all of that inside one transaction is
impossible (two external calls, 2–5 s each) and doing it without checkpoints
makes a webhook failure re-charge the payment.

## Decision

`ProcessPayment` is one use case with two stages, each a short transaction
around a slow external call:

```
load (FOR UPDATE) ─ pending? ─ claim lease ─ commit ─ gateway ─ store result + freeze webhook ─ commit
                 ─ notification pending & due? ─ count attempt ─ commit ─ webhook ─ delivered | retry | DLQ ─ commit
```

The payment row carries two independent machines:

* `status`: `pending → succeeded | failed`, terminal, protected by `CHECK`s.
* `notification_status`: `not_ready → pending → delivered | exhausted`.

A processing **lease** (`processing_lease_token/until`) prevents two workers
from charging concurrently; a stale worker whose lease was taken over cannot
write its late result (fencing). A webhook attempt is **counted before** the
HTTP call so a crash mid-flight still consumes budget; the recovery scan
re-enqueues work whose in-flight message was lost.

The outbox relay and the recovery scan run inside the consumer process as
supervised tasks: they are publishers, not a second consumer.

## Consequences

* Any redelivery after a stored result skips the gateway — the headline
  guarantee, covered by unit, PostgreSQL and RabbitMQ tests.
* `pending` after an infrastructure failure means "unknown", not "declined";
  such payments are dead-lettered for operators but never flipped to `failed`.
* Extra columns on `payments` instead of extra tables, matching the brief.
