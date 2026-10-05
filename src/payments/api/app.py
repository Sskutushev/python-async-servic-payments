from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payments.api.errors import install_error_handlers
from payments.api.middleware import ApiKeyMiddleware, RequestContextMiddleware
from payments.api.routes import router
from payments.bootstrap import Container, build_container
from payments.logging_setup import configure_logging
from payments.settings import Settings, load_settings

API_DESCRIPTION = """
Asynchronous payment processing. `POST /api/v1/payments` saves the payment and returns
**202** right away. A background consumer then charges the (simulated) gateway and sends
a signed webhook with the result.

Every endpoint needs the `X-API-Key` header. `POST` also needs `Idempotency-Key`.
"""


def create_app(settings: Settings | None = None, *, container: Container | None = None) -> FastAPI:
    settings = settings or load_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if container is not None:  # tests pass in a ready-made container
            app.state.container = container
            yield
            return
        async with build_container(settings) as built:
            app.state.container = built
            yield

    app = FastAPI(
        title="Payments Service",
        version="1.0.0",
        description=API_DESCRIPTION,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )
    app.include_router(router)
    install_error_handlers(app)
    # Order of execution: request context -> API key check -> routes.
    app.add_middleware(ApiKeyMiddleware, api_key=settings.api_key.get_secret_value())
    app.add_middleware(RequestContextMiddleware, max_body_bytes=settings.max_request_body_bytes)
    return app


def app_factory() -> FastAPI:  # uvicorn --factory payments.api.app:app_factory
    return create_app()
