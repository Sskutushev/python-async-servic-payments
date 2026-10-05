from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from payments.domain.errors import InvalidAmount
from payments.domain.money import MAX_AMOUNT, Currency, Money, parse_amount


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("100", "100.00"),
        ("100.0", "100.00"),
        ("100.00", "100.00"),
        (100, "100.00"),
        ("0.01", "0.01"),
        (Decimal("1E+2"), "100.00"),
        (str(MAX_AMOUNT), str(MAX_AMOUNT)),
    ],
)
def test_equivalent_representations_normalize_to_two_decimals(raw: object, expected: str) -> None:
    assert str(parse_amount(raw)) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "raw",
    ["0", "-1", "100.001", "abc", "NaN", "Infinity", "-Infinity", "", 1.5, True, "1e999"],
)
def test_invalid_amounts_are_rejected(raw: object) -> None:
    with pytest.raises(InvalidAmount):
        parse_amount(raw)  # type: ignore[arg-type]


def test_amount_above_column_bound_is_rejected() -> None:
    with pytest.raises(InvalidAmount):
        parse_amount(MAX_AMOUNT + Decimal("0.01"))


def test_money_value_object_normalizes_and_validates_currency() -> None:
    money = Money(Decimal("5"), Currency.EUR)
    assert money.amount == Decimal("5.00")
    assert str(money) == "5.00 EUR"
    with pytest.raises(ValueError, match="GBP"):
        Money(Decimal("5"), "GBP")  # type: ignore[arg-type]


@given(st.decimals(min_value=Decimal("0.01"), max_value=MAX_AMOUNT, places=2))
def test_two_decimal_amounts_roundtrip_without_loss(value: Decimal) -> None:
    assert parse_amount(str(value)) == value
    assert parse_amount(parse_amount(value)) == parse_amount(value)
