"""Webhook URL policy — the SSRF boundary.

Two checks at two moments:

* :meth:`WebhookUrlPolicy.validate` at request time (cheap, syntactic): scheme,
  no userinfo, host allow-list, IP literals.
* :meth:`WebhookUrlPolicy.check_resolved` at delivery time: every A/AAAA answer
  must be a public address unless private networks are explicitly allowed.

Known limit: DNS rebinding between resolution and connection is not prevented
here; the allow-list plus network egress rules are the production answer.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Sequence
from dataclasses import dataclass, field
from urllib.parse import urlsplit

MAX_URL_LENGTH = 2048


class WebhookUrlRejected(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def is_public_address(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped
    # ``is_global`` also excludes shared address space (100.64/10) which ``is_private`` misses.
    return addr.is_global and not (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
        or (isinstance(addr, ipaddress.IPv6Address) and addr.is_site_local)
    )


@dataclass(frozen=True, slots=True)
class WebhookUrlPolicy:
    allowed_hosts: Sequence[str] = field(default_factory=tuple)
    allow_private_networks: bool = False
    allow_insecure_http: bool = False

    def validate(self, url: str) -> str:
        """Return the URL unchanged if acceptable; raise :class:`WebhookUrlRejected`."""
        if len(url) > MAX_URL_LENGTH:
            raise WebhookUrlRejected("url_too_long")
        parts = urlsplit(url)
        allowed_schemes = {"https", "http"} if self.allow_insecure_http else {"https"}
        if parts.scheme not in allowed_schemes:
            raise WebhookUrlRejected("scheme_not_allowed")
        if parts.username or parts.password:
            raise WebhookUrlRejected("userinfo_not_allowed")
        if parts.fragment:
            raise WebhookUrlRejected("fragment_not_allowed")
        try:
            host = parts.hostname
            port = parts.port  # raises ValueError when malformed
        except ValueError as exc:
            raise WebhookUrlRejected("invalid_port") from exc
        if not host:
            raise WebhookUrlRejected("host_required")
        if port is not None and port not in (80, 443) and not self.allow_private_networks:
            raise WebhookUrlRejected("port_not_allowed")

        literal = self._ip_literal(host)
        if literal is not None:
            if not self.allow_private_networks:
                raise WebhookUrlRejected("ip_literal_not_allowed")
            return url
        if host == "localhost" and not self.allow_private_networks:
            raise WebhookUrlRejected("host_not_allowed")
        if self.allowed_hosts and not self._host_allowed(host):
            raise WebhookUrlRejected("host_not_allowed")
        return url

    async def check_resolved(self, url: str) -> None:
        """Resolve the host and reject if any answer points into a non-public network."""
        if self.allow_private_networks:
            return
        host = urlsplit(url).hostname or ""
        if self._ip_literal(host) is not None:
            raise WebhookUrlRejected("ip_literal_not_allowed")
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise WebhookUrlRejected("dns_resolution_failed") from exc
        if not infos:
            raise WebhookUrlRejected("dns_resolution_failed")
        for info in infos:
            addr = ipaddress.ip_address(info[4][0])
            if not is_public_address(addr):
                raise WebhookUrlRejected("resolves_to_private_address")

    def _host_allowed(self, host: str) -> bool:
        host = host.lower().rstrip(".")
        return any(host == h or host.endswith("." + h) for h in self.allowed_hosts)

    @staticmethod
    def _ip_literal(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
        try:
            return ipaddress.ip_address(host.strip("[]"))
        except ValueError:
            return None
