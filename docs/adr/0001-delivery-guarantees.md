# ADR-0001: Deliver at least once, make every step safe to repeat

**Status:** accepted · **Date:** 2026-10-05

## The problem

Three systems take part in one payment: PostgreSQL, RabbitMQ and the merchant's webhook
endpoint. They cannot share a transaction. Whatever we do, a crash between two steps means
the next step may run twice. "Exactly once" is not something we can promise honestly.

## What we do

1. **Outbox.** The API writes the payment and its `payments.new` event in one database
   transaction. The API never talks to RabbitMQ.
2. **Relay.** A background loop reserves due events (`FOR UPDATE SKIP LOCKED`), publishes
   them, waits for the broker's confirmation (`mandatory` + publisher confirms) and only
   then marks them published. A crash in between publishes the same event again.
3. **Consumer.** Every step reads the payment row under a lock and looks at what is already
   saved. A repeated message simply continues from the saved state. The message is
   acknowledged only after the database change that makes it unnecessary is committed.
4. **Webhook.** The event id (`X-Webhook-Id`) never changes between attempts. Receivers
   de-duplicate on it.

## What this gives you

* Every accepted payment is processed and its webhook is attempted, as long as the
  PostgreSQL data survives.
* The *simulated* gateway is never charged twice for one payment: the result is stored before
  the webhook is sent, and a stale worker cannot overwrite it. For a real provider the same
  holds only if the provider honours `payment.id` as its idempotency key (see the
  `PaymentGateway` interface).
* A webhook may arrive more than once with the same id. It is never sent more than three
  counted times without an operator's explicit replay.

## What this does not give you

* Exactly-once webhook delivery.
* Protection against a crash between a *real* external charge and saving its result. That
  needs a status lookup at the provider; the interface documents it, the simulator does
  not need it.
