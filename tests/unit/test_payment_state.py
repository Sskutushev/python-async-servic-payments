import uuid
from datetime import timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from payments.domain.errors import InvalidTransition
from payments.domain.payment import NotificationStatus, PaymentStatus
from tests.fakes import DECLINED, SUCCESS, T0, make_payment

LEASE = timedelta(seconds=60)


def test_new_payment_is_pending_and_not_ready_for_notification() -> None:
    p = make_payment()
    assert p.status is PaymentStatus.PENDING
    assert p.notification_status is NotificationStatus.NOT_READY
    assert not p.is_final


def test_processing_lease_is_exclusive_until_expiry() -> None:
    p = make_payment()
    a, b = uuid.uuid4(), uuid.uuid4()
    assert p.claim_processing(a, T0, LEASE)
    assert not p.claim_processing(b, T0 + timedelta(seconds=30), LEASE)
    assert p.claim_processing(a, T0 + timedelta(seconds=30), LEASE)  # owner renews: until T0+90s
    assert not p.claim_processing(b, T0 + timedelta(seconds=89), LEASE)
    assert p.claim_processing(b, T0 + timedelta(seconds=91), LEASE)  # expired


def test_release_only_by_owner() -> None:
    p = make_payment()
    a, b = uuid.uuid4(), uuid.uuid4()
    p.claim_processing(a, T0, LEASE)
    p.release_processing(b)
    assert p.processing_lease_token == a
    p.release_processing(a)
    assert p.processing_lease_token is None


@pytest.mark.parametrize(("outcome", "status"), [(SUCCESS, "succeeded"), (DECLINED, "failed")])
def test_gateway_result_freezes_webhook_event(outcome, status) -> None:  # type: ignore[no-untyped-def]
    p = make_payment()
    event_id = uuid.uuid4()
    p.record_gateway_result(outcome, now=T0, event_id=event_id)
    assert p.status.value == status
    assert p.processed_at == T0
    assert p.notification_status is NotificationStatus.PENDING
    assert p.notification_due(T0)
    assert p.processing_lease_token is None
    assert p.notification_body == {
        "event_id": str(event_id),
        "event_type": f"payment.{status}",
        "schema_version": 1,
        "payment_id": str(p.id),
        "amount": "100.00",
        "currency": "USD",
        "status": status,
        "failure_code": None if outcome.succeeded else "declined",
        "occurred_at": T0.isoformat(),
    }


def test_result_cannot_be_recorded_twice() -> None:
    p = make_payment()
    p.record_gateway_result(SUCCESS, now=T0, event_id=uuid.uuid4())
    with pytest.raises(InvalidTransition):
        p.record_gateway_result(DECLINED, now=T0, event_id=uuid.uuid4())
    assert p.status is PaymentStatus.SUCCEEDED


def test_final_payment_cannot_be_claimed() -> None:
    p = make_payment()
    p.record_gateway_result(SUCCESS, now=T0, event_id=uuid.uuid4())
    with pytest.raises(InvalidTransition):
        p.claim_processing(uuid.uuid4(), T0, LEASE)


def test_notification_lifecycle_never_touches_status() -> None:
    p = make_payment()
    p.record_gateway_result(SUCCESS, now=T0, event_id=uuid.uuid4())
    assert p.begin_notification_attempt(token=uuid.uuid4(), now=T0, lease=LEASE) == 1
    assert not p.notification_due(T0)  # pushed forward while in flight
    p.schedule_notification_retry(error="http_500", next_attempt_at=T0 + timedelta(seconds=1))
    assert p.notification_due(T0 + timedelta(seconds=1))
    assert p.begin_notification_attempt(token=uuid.uuid4(), now=T0, lease=LEASE) == 2
    p.exhaust_notification(error="http_500")
    assert p.notification_status is NotificationStatus.EXHAUSTED
    assert p.status is PaymentStatus.SUCCEEDED

    p.reopen_notification(T0)
    assert p.notification_status is NotificationStatus.PENDING
    assert p.notification_attempts == 0
    p.begin_notification_attempt(token=uuid.uuid4(), now=T0, lease=LEASE)
    p.mark_notification_delivered(T0)
    assert p.notification_status is NotificationStatus.DELIVERED
    assert p.notification_delivered_at == T0


def test_notification_transitions_require_pending() -> None:
    p = make_payment()
    with pytest.raises(InvalidTransition):
        p.begin_notification_attempt(token=uuid.uuid4(), now=T0, lease=LEASE)
    with pytest.raises(InvalidTransition):
        p.mark_notification_delivered(T0)
    with pytest.raises(InvalidTransition):
        p.reopen_notification(T0)


@given(st.lists(st.sampled_from(["retry", "delivered", "exhaust", "result"]), max_size=20))
def test_terminal_status_is_sticky_under_any_notification_sequence(ops: list[str]) -> None:
    p = make_payment()
    p.record_gateway_result(DECLINED, now=T0, event_id=uuid.uuid4())
    for op in ops:
        try:
            if op == "retry":
                p.begin_notification_attempt(token=uuid.uuid4(), now=T0, lease=LEASE)
                p.schedule_notification_retry(error="x", next_attempt_at=T0)
            elif op == "delivered":
                p.mark_notification_delivered(T0)
            elif op == "exhaust":
                p.exhaust_notification(error="x")
            else:
                p.record_gateway_result(SUCCESS, now=T0, event_id=uuid.uuid4())
        except InvalidTransition:
            pass
        assert p.status is PaymentStatus.FAILED
        assert p.notification_attempts >= 0
