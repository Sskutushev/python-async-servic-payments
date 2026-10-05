# ADR-0003: One consumer, small saved steps, two state machines on one row

**Status:** accepted · **Date:** 2026-10-05

## The problem

The task asks for one consumer that charges the gateway, updates the database and sends
the webhook. One big transaction is impossible (two external calls of 2–5 s each), and
doing it without saving progress means a failed webhook would charge the payment again.

## What we do

`ProcessPayment` is one use case with two steps. Each step is a short transaction around
one slow external call:

```
load (FOR UPDATE) ─ pending? ─ reserve ─ commit ─ gateway ─ save result + webhook body ─ commit
                  ─ webhook due? ─ count attempt ─ commit ─ send ─ delivered | retry | DLQ ─ commit
```

The payment row carries two independent state machines:

* `status`: `pending → succeeded | failed`, final, protected by `CHECK` constraints.
* `notification_status`: `not_ready → pending → delivered | exhausted`.

Both slow calls are owned by a **lease token**:

* `processing_lease_token`: one worker charges at a time. A worker whose lease expired and
  was taken over cannot write anything afterwards — not a result, not a retry, not a dead
  letter.
* `notification_lease_token`: the same rule for webhook attempts. A late outcome from an
  old attempt is ignored; the record of the worker that currently owns the attempt wins.

When the gateway budget is spent the payment is **halted** (`processing_halted_at`,
`processing_halt_reason`). It stays `pending` — unknown is not declined — and waits for an
operator. A webhook whose last attempt has an unknown outcome is marked `exhausted` with
reason `delivery_outcome_unknown_after_budget`; replay re-sends the same event id.

The outbox relay and the recovery scan run inside the consumer process as supervised
background loops. They publish through the outbox; they are not a second consumer. If one of
them keeps failing, the process exits so the orchestrator restarts it.

## Consequences

* A redelivery after a stored result never reaches the gateway. Covered by unit,
  PostgreSQL and RabbitMQ tests.
* Extra columns on `payments` instead of extra tables, as the brief allows two tables only.
* A webhook that really was accepted during a crashed third attempt is not re-sent
  automatically. That is deliberate: "at most three calls" is kept, and the receiver
  de-duplicates the replay by event id.
