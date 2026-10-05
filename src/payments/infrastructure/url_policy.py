"""Rules for which webhook URLs we are willing to call.

A merchant can put any URL into ``webhook_url``, and our service would then connect to it
from inside our network. These rules stop that from being used to reach internal
services (SSRF). They run twice:

* ``validate`` when the payment is created: cheap checks on the URL text itself;
* ``check_resolved`` right before sending: the hostname must resolve to public addresses.

To close the gap between "checked" and "connected" (DNS rebinding), ``check_resolved``
returns the address it approved and the sender connects to that exact address, keeping
the original hostname for TLS and the ``Host`` header.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Sequence
from dataclasses import dataclass, field
from urllib.parse import SplitResult, urlsplit

MAX_URL_LENGTH = 2048
STANDARD_PORTS = {80, 443}

IpAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


class WebhookUrlRejected(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def is_public_address(addr: IpAddress) -> bool:
    """True only for addresses on the public internet."""
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped  # ::ffff:10.0.0.1 is really 10.0.0.1
    # ``is_global`` already excludes private, loopback, link-local, reserved and
    # shared (100.64/10) ranges; multicast is the one case it lets through.
    return addr.is_global and not addr.is_multicast


def parse_ip_literal(host: str) -> IpAddress | None:
    try:
        return ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return None


@dataclass(frozen=True, slots=True)
class WebhookUrlPolicy:
    allowed_hosts: Sequence[str] = field(default_factory=tuple)
    allow_private_networks: bool = False  # dev only
    allow_insecure_http: bool = False  # dev only

    def validate(self, url: str) -> str:
        """Return the URL if it is acceptable, otherwise raise ``WebhookUrlRejected``."""
        if len(url) > MAX_URL_LENGTH:
            raise WebhookUrlRejected("url_too_long")
        parts = urlsplit(url)
        self._check_scheme(parts)
        self._check_no_credentials_or_fragment(parts)
        host = self._check_host_and_port(parts)
        self._check_host_allowed(host)
        return url

    async def check_resolved(self, url: str) -> IpAddress | None:
        """Resolve the host and refuse it if any answer is a private/internal address.

        Returns the address the sender must connect to (the first public answer), so the
        connection goes to exactly what was checked. Returns ``None`` in dev mode, where
        hostnames are used as they are.
        """
        if self.allow_private_networks:
            return None
        host = urlsplit(url).hostname or ""
        if parse_ip_literal(host) is not None:
            raise WebhookUrlRejected("ip_literal_not_allowed")
        addresses = await self._resolve(host)
        for addr in addresses:
            if not is_public_address(addr):
                raise WebhookUrlRejected("resolves_to_private_address")
        return addresses[0]

    # --- individual rules ---------------------------------------------------------------

    def _check_scheme(self, parts: SplitResult) -> None:
        allowed = {"https", "http"} if self.allow_insecure_http else {"https"}
        if parts.scheme not in allowed:
            raise WebhookUrlRejected("scheme_not_allowed")

    @staticmethod
    def _check_no_credentials_or_fragment(parts: SplitResult) -> None:
        if parts.username or parts.password:
            raise WebhookUrlRejected("userinfo_not_allowed")
        if parts.fragment:
            raise WebhookUrlRejected("fragment_not_allowed")

    def _check_host_and_port(self, parts: SplitResult) -> str:
        try:
            host, port = parts.hostname, parts.port
        except ValueError as exc:
            raise WebhookUrlRejected("invalid_port") from exc
        if not host:
            raise WebhookUrlRejected("host_required")
        if port is not None and port not in STANDARD_PORTS and not self.allow_private_networks:
            raise WebhookUrlRejected("port_not_allowed")
        return host

    def _check_host_allowed(self, host: str) -> None:
        if self.allow_private_networks:
            return  # dev mode: anything goes, including IPs and localhost
        if parse_ip_literal(host) is not None:
            raise WebhookUrlRejected("ip_literal_not_allowed")
        if host == "localhost":
            raise WebhookUrlRejected("host_not_allowed")
        if self.allowed_hosts and not self._in_allow_list(host):
            raise WebhookUrlRejected("host_not_allowed")

    def _in_allow_list(self, host: str) -> bool:
        host = host.lower().rstrip(".")
        return any(host == h or host.endswith("." + h) for h in self.allowed_hosts)

    @staticmethod
    async def _resolve(host: str) -> list[IpAddress]:
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise WebhookUrlRejected("dns_resolution_failed") from exc
        if not infos:
            raise WebhookUrlRejected("dns_resolution_failed")
        return [ipaddress.ip_address(info[4][0]) for info in infos]
