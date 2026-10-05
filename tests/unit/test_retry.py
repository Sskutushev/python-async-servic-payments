from datetime import timedelta

import pytest

from payments.application.retry import RetryPolicy


def test_default_budget_is_three_attempts_with_1s_then_2s() -> None:
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.delay_before(2) == timedelta(seconds=1)
    assert policy.delay_before(3) == timedelta(seconds=2)
    assert not policy.is_exhausted(2)
    assert policy.is_exhausted(3)


def test_delay_is_capped() -> None:
    policy = RetryPolicy(
        max_attempts=10, base_delay=timedelta(seconds=1), max_delay=timedelta(seconds=5)
    )
    assert policy.delay_before(10) == timedelta(seconds=5)


def test_retry_after_is_honoured_but_clamped() -> None:
    policy = RetryPolicy(max_delay=timedelta(seconds=10))
    assert policy.delay_before(2, retry_after=timedelta(seconds=4)) == timedelta(seconds=4)
    assert policy.delay_before(2, retry_after=timedelta(hours=1)) == timedelta(seconds=10)
    assert policy.delay_before(3, retry_after=timedelta(seconds=1)) == timedelta(seconds=2)


def test_invalid_policies_are_rejected() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError, match="delays"):
        RetryPolicy(base_delay=timedelta(seconds=5), max_delay=timedelta(seconds=1))
    with pytest.raises(ValueError, match="attempts >= 2"):
        RetryPolicy().delay_before(1)
