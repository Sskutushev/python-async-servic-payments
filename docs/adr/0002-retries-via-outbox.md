# ADR-0002: Retries live in the database, not in the broker

**Status:** accepted · **Date:** 2026-10-05

## The problem

"Three attempts with exponential backoff" can be built with RabbitMQ delay queues, with
`nack` + requeue loops, or with the framework's in-process retry. In all three the retry
schedule lives outside the database, so the attempt counter and the payment state can
disagree after a crash.

## What we do

A retry is a row in the outbox (`payment.retry` with `available_at` in the future). It is
written in the **same transaction** as the attempt counter and the error. The relay
publishes it back to `payments.new` when its time comes. After the last attempt the
transaction writes a `payment.dead_letter` row instead, routed to `payments.dlq`.

Schedule: attempt 1 now, attempt 2 after 1 s, attempt 3 after 2 s
(`1 s × 2^(n−2)`, capped by `RETRY_MAX_DELAY_SECONDS`; a `Retry-After` header is honoured
up to the cap). Three means three in total, not three retries.

The budget is checked **before** every network call, not only after a failure:

* a gateway attempt is counted when the worker reserves the payment;
* a webhook attempt is counted before the HTTP request;
* if the counter is already at the limit when a worker picks the payment up (the previous
  worker died mid-call), no further call is made. The payment is handed to an operator with
  the reason `..._after_budget`.

The same budget applies to "could not reach the gateway". A gateway **decline** is a normal
result (`failed` + webhook), never a retry.

## Consequences

* Attempt count, next due time and payment state can never disagree: one commit.
* Broker topology stays small: one work queue, one dead-letter queue.
* Retry latency granularity is the relay poll interval (0.5 s by default).
* Broker-level dead-lettering is still configured on `payments.new` for messages the
  consumer rejects (broken message, unknown payment). Both paths end in `payments.dlq`.
* Once a payment is handed to an operator, nothing automatic touches it: not duplicate
  messages, not the recovery scan. Only `payments replay <id>` opens a new budget.
