# syntax=docker/dockerfile:1.7
# One image for api, consumer, migrations and the CLI. Non-root, reproducible via uv.lock.

FROM python:3.14-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.14-slim-bookworm AS runtime
# Numeric uid/gid so the user resolves on any host and in Kubernetes runAsNonRoot checks.
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --home /app --shell /usr/sbin/nologin app \
    && apt-get update \
    # pick up Debian security fixes published after the base image was built
    && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder --chown=10001:10001 /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["payments"]
CMD ["api"]
