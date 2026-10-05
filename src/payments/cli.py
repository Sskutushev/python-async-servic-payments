"""Command line: ``payments api``, ``payments consumer``, ``payments migrate``,
``payments replay <payment_id>``."""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from pathlib import Path

from payments.settings import load_settings


def _run_api(host: str, port: int) -> None:
    import uvicorn

    uvicorn.run("payments.api.app:app_factory", factory=True, host=host, port=port, log_config=None)


def _run_consumer() -> None:
    from payments.messaging.app import run_consumer

    asyncio.run(run_consumer())


def _run_migrations(revision: str) -> None:
    from alembic import command
    from alembic.config import Config

    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, revision)


def _run_replay(payment_id: str) -> int:
    from payments.application.replay import replay_payment
    from payments.bootstrap import build_container
    from payments.domain.errors import DomainError

    async def _go() -> int:
        async with build_container(load_settings()) as container:
            try:
                action = await replay_payment(
                    container.uow_factory, container.clock, uuid.UUID(payment_id)
                )
            except DomainError as exc:
                print(f"replay refused: {exc.code}: {exc}", file=sys.stderr)
                return 1
            print(f"{payment_id}: {action.value}")
            return 0

    return asyncio.run(_go())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="payments")
    sub = parser.add_subparsers(dest="command", required=True)

    api = sub.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default="0.0.0.0")  # noqa: S104 - container-internal bind
    api.add_argument("--port", type=int, default=8000)

    sub.add_parser("consumer", help="run the payments.new consumer + outbox relay")

    migrate = sub.add_parser("migrate", help="apply database migrations")
    migrate.add_argument("revision", nargs="?", default="head")

    replay = sub.add_parser("replay", help="replay a dead-lettered payment's failed phase")
    replay.add_argument("payment_id")

    args = parser.parse_args(argv)
    if args.command == "api":
        _run_api(args.host, args.port)
    elif args.command == "consumer":
        _run_consumer()
    elif args.command == "migrate":
        _run_migrations(args.revision)
    elif args.command == "replay":
        return _run_replay(args.payment_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
