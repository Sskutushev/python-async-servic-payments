# ADR-0002: Durable retries through the outbox instead of broker TTL queues

**Status:** accepted · **Date:** 2026-10-05

## Context

"3 attempts with exponential backoff" can be built with per-delay TTL queues
and dead-letter routing, with `nack(requeue=True)` loops, or with FastStream's
in-process retry. All three keep the retry schedule *outside* the database.

## Decision

A retry is an outbox row: `payment_retry_event(phase, attempt, available_at)`
written in the **same transaction** as the attempt counter and the error. The
relay publishes it back to `payments.new` when `available_at` passes. After the
third counted attempt the transaction writes a `payment_dead_letter_event`
routed to `payments.dead` → `payments.dlq` instead.

Schedule: attempt 1 immediately, attempt 2 after 1 s, attempt 3 after 2 s
(`base · 2^(n-2)`, capped by `RETRY_MAX_DELAY_SECONDS`; `Retry-After` is honoured
within the cap). Three means three in total, not three retries.

The same budget applies to gateway *transport* failures; a gateway **decline**
is a business result (`failed` + webhook), never a retry.

## Consequences

* Attempt count, next due time and payment state can never disagree: one commit.
* Broker topology stays minimal (one work queue, one DLQ); no delay queues to tune.
* Retry latency granularity equals the relay poll interval (0.5 s default).
* Broker dead-lettering is still configured on `payments.new` for messages the
  consumer *rejects* (malformed envelope, unknown payment). Both paths end in
  `payments.dlq`; the outbox path carries a diagnostic envelope, the broker path
  carries the original message with `x-death` headers.
