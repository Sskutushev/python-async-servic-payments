from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from payments.domain.fingerprint import request_fingerprint

BASE = {
    "amount": Decimal("100.00"),
    "currency": "USD",
    "description": "order 42",
    "metadata": {"a": 1, "b": {"c": [1, 2]}},
    "webhook_url": "https://merchant.example/hooks",
}


def test_metadata_key_order_does_not_matter() -> None:
    reordered = {**BASE, "metadata": {"b": {"c": [1, 2]}, "a": 1}}
    assert request_fingerprint(**BASE) == request_fingerprint(**reordered)


def test_amount_representation_does_not_matter() -> None:
    assert request_fingerprint(**{**BASE, "amount": Decimal("100")}) == request_fingerprint(**BASE)


def test_each_business_field_changes_the_fingerprint() -> None:
    base = request_fingerprint(**BASE)
    assert request_fingerprint(**{**BASE, "amount": Decimal("100.01")}) != base
    assert request_fingerprint(**{**BASE, "currency": "EUR"}) != base
    assert request_fingerprint(**{**BASE, "description": "order 43"}) != base
    assert request_fingerprint(**{**BASE, "metadata": {"a": 2}}) != base
    assert request_fingerprint(**{**BASE, "webhook_url": "https://merchant.example/other"}) != base


def test_list_order_in_metadata_is_significant() -> None:
    assert request_fingerprint(**{**BASE, "metadata": {"c": [2, 1]}}) != request_fingerprint(
        **{**BASE, "metadata": {"c": [1, 2]}}
    )


json_scalars = st.none() | st.booleans() | st.integers() | st.text(max_size=20)
json_values = st.recursive(
    json_scalars,
    lambda children: (
        st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=8), children, max_size=4)
    ),
    max_leaves=12,
)


@given(st.dictionaries(st.text(max_size=8), json_values, max_size=6))
def test_fingerprint_is_stable_under_key_reordering(metadata: dict[str, object]) -> None:
    reversed_keys = dict(reversed(list(metadata.items())))
    assert request_fingerprint(**{**BASE, "metadata": metadata}) == request_fingerprint(
        **{**BASE, "metadata": reversed_keys}
    )
