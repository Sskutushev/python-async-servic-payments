import uuid

import pytest

from payments.infrastructure.gateway import SimulatedGateway
from tests.fakes import make_payment


class RecordingSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


async def test_outcome_is_deterministic_per_payment() -> None:
    sleep = RecordingSleep()
    gateway = SimulatedGateway(seed="seed", sleep=sleep)
    payment = make_payment()
    first = await gateway.charge(payment)
    second = await gateway.charge(payment)
    assert first == second
    assert all(2.0 <= s <= 5.0 for s in sleep.calls)


async def test_success_rate_is_approximately_honoured() -> None:
    gateway = SimulatedGateway(seed="seed", success_rate=0.9, sleep=RecordingSleep())
    results = [await gateway.charge(make_payment(payment_id=uuid.UUID(int=i))) for i in range(1000)]
    successes = sum(r.succeeded for r in results)
    assert 850 <= successes <= 950
    assert all(r.failure_code == "declined" for r in results if not r.succeeded)
    assert all(r.reference.startswith("sim-") for r in results)


async def test_zero_and_full_success_rates() -> None:
    always = SimulatedGateway(
        seed="s", success_rate=1.0, min_delay=0, max_delay=0, sleep=RecordingSleep()
    )
    never = SimulatedGateway(
        seed="s", success_rate=0.0, min_delay=0, max_delay=0, sleep=RecordingSleep()
    )
    p = make_payment()
    assert (await always.charge(p)).succeeded
    assert not (await never.charge(p)).succeeded


def test_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="success_rate"):
        SimulatedGateway(seed="s", success_rate=1.5)
    with pytest.raises(ValueError, match="delays"):
        SimulatedGateway(seed="s", min_delay=5, max_delay=2)
