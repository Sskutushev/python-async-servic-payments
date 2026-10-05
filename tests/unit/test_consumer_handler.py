"""Ack/nack/reject mapping of the single subscriber, with a fake broker message."""

from __future__ import annotations

import json
import uuid
from typing import Any

from payments.application.process_payment import ProcessOutcome
from payments.domain.events import EventType
from payments.messaging.consumer import build_handler


class FakeMessage:
    def __init__(self, body: Any) -> None:
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.message_id = "m1"
        self.actions: list[str] = []

    async def ack(self) -> None:
        self.actions.append("ack")

    async def nack(self, requeue: bool = True) -> None:
        self.actions.append(f"nack(requeue={requeue})")

    async def reject(self, requeue: bool = False) -> None:
        self.actions.append(f"reject(requeue={requeue})")


class ScriptedProcess:
    def __init__(self, outcome: ProcessOutcome | Exception) -> None:
        self._outcome = outcome
        self.calls: list[tuple[uuid.UUID, uuid.UUID | None]] = []

    async def __call__(
        self, payment_id: uuid.UUID, *, event_id: uuid.UUID | None = None
    ) -> ProcessOutcome:
        self.calls.append((payment_id, event_id))
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def envelope(**overrides: object) -> dict[str, object]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": EventType.PAYMENT_CREATED.value,
        "schema_version": 1,
        "payment_id": str(uuid.uuid4()),
        "phase": "process",
        **overrides,
    }


async def test_business_outcomes_are_acked() -> None:
    for outcome in ProcessOutcome:
        if outcome is ProcessOutcome.POISON:
            continue
        process = ScriptedProcess(outcome)
        message = FakeMessage(envelope())
        await build_handler(process, pause=0)(message)  # type: ignore[arg-type]
        assert message.actions == ["ack"], outcome


async def test_poison_is_rejected_without_requeue() -> None:
    message = FakeMessage(envelope())
    await build_handler(ScriptedProcess(ProcessOutcome.POISON), pause=0)(message)  # type: ignore[arg-type]
    assert message.actions == ["reject(requeue=False)"]


async def test_malformed_envelope_is_rejected_without_calling_the_use_case() -> None:
    process = ScriptedProcess(ProcessOutcome.COMPLETED)
    for body in (b"not json", {"payment_id": "nope"}, envelope(payment_id="x")):
        message = FakeMessage(body)
        await build_handler(process, pause=0)(message)  # type: ignore[arg-type]
        assert message.actions == ["reject(requeue=False)"]
    assert process.calls == []


async def test_infrastructure_error_is_nacked_with_requeue() -> None:
    message = FakeMessage(envelope())
    await build_handler(ScriptedProcess(ConnectionError("db down")), pause=0)(message)  # type: ignore[arg-type]
    assert message.actions == ["nack(requeue=True)"]


async def test_envelope_ids_are_passed_through_and_extra_fields_ignored() -> None:
    process = ScriptedProcess(ProcessOutcome.COMPLETED)
    data = envelope(attempt=2, unknown="ignored")
    await build_handler(process, pause=0)(FakeMessage(data))  # type: ignore[arg-type]
    [(payment_id, event_id)] = process.calls
    assert str(payment_id) == data["payment_id"]
    assert str(event_id) == data["event_id"]
