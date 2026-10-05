import ipaddress

import pytest

from payments.infrastructure.url_policy import (
    WebhookUrlPolicy,
    WebhookUrlRejected,
    is_public_address,
)

STRICT = WebhookUrlPolicy()
ALLOWLIST = WebhookUrlPolicy(allowed_hosts=("merchant.example",))
DEV = WebhookUrlPolicy(allow_private_networks=True, allow_insecure_http=True)


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("http://merchant.example/hooks", "scheme_not_allowed"),
        ("ftp://merchant.example/hooks", "scheme_not_allowed"),
        ("https://user:pw@merchant.example/hooks", "userinfo_not_allowed"),
        ("https://merchant.example/hooks#frag", "fragment_not_allowed"),
        ("https:///hooks", "host_required"),
        ("https://merchant.example:8443/hooks", "port_not_allowed"),
        ("https://merchant.example:abc/hooks", "invalid_port"),
        ("https://127.0.0.1/hooks", "ip_literal_not_allowed"),
        ("https://[::1]/hooks", "ip_literal_not_allowed"),
        ("https://169.254.169.254/latest/meta-data", "ip_literal_not_allowed"),
        ("https://[::ffff:10.0.0.1]/hooks", "ip_literal_not_allowed"),
        ("https://localhost/hooks", "host_not_allowed"),
        ("https://" + "a" * 2050, "url_too_long"),
    ],
)
def test_strict_policy_rejects_unsafe_urls(url: str, reason: str) -> None:
    with pytest.raises(WebhookUrlRejected) as exc:
        STRICT.validate(url)
    assert exc.value.reason == reason


def test_strict_policy_accepts_public_https() -> None:
    assert (
        STRICT.validate("https://merchant.example/hooks?x=1")
        == "https://merchant.example/hooks?x=1"
    )
    assert STRICT.validate("https://merchant.example:443/hooks")


@pytest.mark.parametrize(
    ("url", "ok"),
    [
        ("https://merchant.example/h", True),
        ("https://api.merchant.example/h", True),
        ("https://MERCHANT.example./h", True),
        ("https://merchant.example.evil.com/h", False),
        ("https://evilmerchant.example/h", False),
        ("https://other.example/h", False),
    ],
)
def test_allowlist_matches_host_or_subdomain_only(url: str, ok: bool) -> None:
    if ok:
        ALLOWLIST.validate(url)
    else:
        with pytest.raises(WebhookUrlRejected, match="host_not_allowed"):
            ALLOWLIST.validate(url)


def test_dev_policy_allows_private_http_targets() -> None:
    DEV.validate("http://webhook-receiver:9000/hooks")
    DEV.validate("http://127.0.0.1:9000/hooks")


@pytest.mark.parametrize(
    "addr",
    [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.5.5",
        "192.168.1.1",
        "169.254.169.254",
        "0.0.0.0",
        "::1",
        "fe80::1",
        "fc00::1",
        "::ffff:192.168.0.1",
        "224.0.0.1",
        "100.64.0.1",
    ],
)
def test_non_public_addresses(addr: str) -> None:
    assert not is_public_address(ipaddress.ip_address(addr))


@pytest.mark.parametrize("addr", ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"])
def test_public_addresses(addr: str) -> None:
    assert is_public_address(ipaddress.ip_address(addr))


async def test_resolution_check_rejects_private_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    async def fake_getaddrinfo(*_: object, **__: object) -> list[tuple]:  # type: ignore[type-arg]
        return [(0, 0, 0, "", ("93.184.216.34", 0)), (0, 0, 0, "", ("10.0.0.5", 0))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", fake_getaddrinfo)
    with pytest.raises(WebhookUrlRejected, match="resolves_to_private_address"):
        await STRICT.check_resolved("https://merchant.example/hooks")


async def test_resolution_check_skipped_when_private_allowed() -> None:
    await DEV.check_resolved("http://definitely-not-resolvable.invalid/")
