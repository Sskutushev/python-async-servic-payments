import pytest
from pydantic import ValidationError

from payments.settings import Environment, Settings
from tests.conftest import make_settings


def test_prod_rejects_dev_only_switches() -> None:
    with pytest.raises(ValidationError, match="webhook_allow_private_networks"):
        make_settings(app_env=Environment.PROD, webhook_allow_insecure_http=False)
    make_settings(
        app_env=Environment.PROD,
        webhook_allow_private_networks=False,
        webhook_allow_insecure_http=False,
    )


def test_short_secrets_are_rejected() -> None:
    with pytest.raises(ValidationError, match="api_key"):
        make_settings(api_key="short")


def test_secrets_do_not_leak_in_repr() -> None:
    s = make_settings()
    assert "test-api-key" not in repr(s)
    assert "test-webhook-secret" not in repr(s)


def test_allowed_hosts_accepts_comma_separated_string() -> None:
    s = make_settings(webhook_allowed_hosts="Merchant.Example, other.example ,")
    assert s.webhook_allowed_hosts == ["merchant.example", "other.example"]


def test_allowed_hosts_from_environment_is_not_parsed_as_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_KEY", "test-api-key-0123456789abcdef")
    monkeypatch.setenv("WEBHOOK_SECRET", "test-webhook-secret-0123456789")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h:1/d")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://u:p@h:1/")
    monkeypatch.setenv("WEBHOOK_ALLOWED_HOSTS", "")
    assert Settings(_env_file=None).webhook_allowed_hosts == []  # type: ignore[call-arg]
    monkeypatch.setenv("WEBHOOK_ALLOWED_HOSTS", "a.example,b.example")
    assert Settings(_env_file=None).webhook_allowed_hosts == ["a.example", "b.example"]  # type: ignore[call-arg]


def test_gateway_delay_bounds_are_validated() -> None:
    with pytest.raises(ValidationError, match="gateway_max_delay_seconds"):
        make_settings(gateway_min_delay_seconds=5, gateway_max_delay_seconds=2)
