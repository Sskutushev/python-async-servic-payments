"""Real TLS behaviour of the webhook client (review blocker 2).

A local HTTPS server with a certificate for two names records the SNI of every connection.
Both names are "resolved" to the server's address, so for the connection pool they are the
same IP origin. The webhook client must still open a fresh, separately verified TLS
connection for each hostname. A plain keep-alive client is shown to reuse the first
connection, which proves the test would catch the bug.
"""

from __future__ import annotations

import asyncio
import ssl
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import httpx
import pytest
import trustme

from payments.infrastructure.url_policy import WebhookUrlPolicy
from payments.infrastructure.webhooks import HttpWebhookSender, build_http_client
from tests.fakes import FakeClock

HOST_A, HOST_B = "a.merchant.test", "b.merchant.test"


@dataclass
class TlsServer:
    port: int
    connections: list[str | None] = field(default_factory=list)  # SNI per TCP connection


async def _serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Minimal HTTP/1.1: answer every request on the connection with 204 and keep it open."""
    try:
        while True:
            head = await reader.readuntil(b"\r\n\r\n")
            length = 0
            for line in head.decode().split("\r\n"):
                if line.lower().startswith("content-length:"):
                    length = int(line.split(":", 1)[1])
            if length:
                await reader.readexactly(length)
            writer.write(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


@pytest.fixture
async def tls_server() -> AsyncIterator[tuple[TlsServer, ssl.SSLContext]]:
    ca = trustme.CA()
    server_cert = ca.issue_cert(HOST_A, HOST_B)
    server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    server_cert.configure_cert(server_ctx)
    state = TlsServer(port=0)

    def on_sni(sock: ssl.SSLObject, name: str | None, _: ssl.SSLContext) -> None:
        state.connections.append(name)

    server_ctx.sni_callback = on_sni
    server = await asyncio.start_server(_serve, "127.0.0.1", 0, ssl=server_ctx)
    state.port = server.sockets[0].getsockname()[1]
    client_ctx = ssl.create_default_context()
    ca.configure_trust(client_ctx)
    try:
        yield state, client_ctx
    finally:
        server.close()
        await server.wait_closed()


def _resolve_to_server(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_getaddrinfo(*_: object, **__: object) -> list[tuple]:  # type: ignore[type-arg]
        return [(0, 0, 0, "", ("127.0.0.1", 0))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", fake_getaddrinfo)


class LoopbackIsPublic(WebhookUrlPolicy):
    """Strict policy, except that the test server's loopback address counts as public."""

    __slots__ = ()

    async def check_resolved(self, url):  # type: ignore[no-untyped-def]
        addresses = await self._resolve(url.split("/")[2].split(":")[0])
        return addresses[0]


async def test_each_hostname_gets_its_own_verified_tls_connection(
    tls_server: tuple[TlsServer, ssl.SSLContext], monkeypatch: pytest.MonkeyPatch
) -> None:
    server, client_ctx = tls_server
    _resolve_to_server(monkeypatch)
    client = build_http_client(5.0, verify=client_ctx)
    sender = HttpWebhookSender(
        client,
        secret="s" * 16,
        policy=LoopbackIsPublic(allow_private_networks=True),
        clock=FakeClock(),
    )
    try:
        await sender.deliver(f"https://{HOST_A}:{server.port}/hooks", uuid.uuid4(), {"n": 1})
        await sender.deliver(f"https://{HOST_B}:{server.port}/hooks", uuid.uuid4(), {"n": 2})
        await sender.deliver(f"https://{HOST_A}:{server.port}/hooks", uuid.uuid4(), {"n": 3})
    finally:
        await client.aclose()
    # Three requests, three TLS handshakes, each presenting its own hostname.
    assert server.connections == [HOST_A, HOST_B, HOST_A]


async def test_certificate_is_checked_for_the_pinned_hostname(
    tls_server: tuple[TlsServer, ssl.SSLContext], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hostname the certificate does not cover must fail the handshake, IP pinning or not."""
    server, client_ctx = tls_server
    _resolve_to_server(monkeypatch)
    client = build_http_client(5.0, verify=client_ctx)
    sender = HttpWebhookSender(
        client,
        secret="s" * 16,
        policy=LoopbackIsPublic(allow_private_networks=True),
        clock=FakeClock(),
    )
    try:
        from payments.application.ports import WebhookDeliveryError

        with pytest.raises(WebhookDeliveryError) as exc:
            await sender.deliver(
                f"https://evil.merchant.test:{server.port}/hooks", uuid.uuid4(), {"n": 1}
            )
        assert exc.value.code == "connection_error"
    finally:
        await client.aclose()


async def test_a_keep_alive_client_would_reuse_the_connection(
    tls_server: tuple[TlsServer, ssl.SSLContext],
) -> None:
    """Control case: proves the scenario above is real and the test can see it."""
    server, client_ctx = tls_server
    client = httpx.AsyncClient(verify=client_ctx, limits=httpx.Limits(max_keepalive_connections=10))
    try:
        for host in (HOST_A, HOST_B):
            r = await client.post(
                f"https://127.0.0.1:{server.port}/hooks",
                headers={"Host": host},
                extensions={"sni_hostname": host},
                content=b"{}",
            )
            assert r.status_code == 204
    finally:
        await client.aclose()
    assert server.connections == [HOST_A]  # second request rode on the first TLS connection
