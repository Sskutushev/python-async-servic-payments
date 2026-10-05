"""Typed runtime configuration.

Every value is validated once at process start. Secrets are ``SecretStr`` so they
never leak through ``repr``/logging. Production-unsafe switches are rejected when
``APP_ENV=prod``.
"""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Environment(StrEnum):
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", frozen=True
    )

    app_env: Environment = Environment.DEV
    log_level: str = "INFO"

    # --- auth ------------------------------------------------------------------
    api_key: SecretStr = Field(min_length=16, description="Static key expected in X-API-Key")

    # --- storage / broker -------------------------------------------------------
    database_url: str = Field(description="postgresql+asyncpg://user:pass@host:5432/db")
    database_pool_size: int = Field(default=10, ge=1, le=100)
    rabbitmq_url: str = Field(description="amqp://user:pass@host:5672/")

    # --- request limits ---------------------------------------------------------
    max_request_body_bytes: int = Field(default=32 * 1024, ge=1024)
    max_metadata_bytes: int = Field(default=16 * 1024, ge=256)

    # --- webhook delivery -------------------------------------------------------
    webhook_secret: SecretStr = Field(min_length=16, description="HMAC key for X-Webhook-Signature")
    # NoDecode: the env value is a plain comma-separated string, not JSON.
    webhook_allowed_hosts: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Allow-list of webhook hostnames (suffix match). Empty = any public host.",
    )
    webhook_allow_private_networks: bool = Field(
        default=False, description="DEV ONLY: permit loopback/RFC1918 webhook targets"
    )
    webhook_allow_insecure_http: bool = Field(
        default=False, description="DEV ONLY: permit http:// webhook targets"
    )
    webhook_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    webhook_max_response_bytes: int = Field(default=4096, ge=0)

    # --- retry policy (shared by webhook delivery and gateway transport errors) --
    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=1.0, gt=0)
    retry_max_delay_seconds: float = Field(default=60.0, gt=0)

    # --- simulated gateway ------------------------------------------------------
    gateway_min_delay_seconds: float = Field(default=2.0, ge=0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    gateway_seed: SecretStr = Field(
        default=SecretStr("simulated-gateway-seed"),
        description="Makes the simulated outcome a deterministic function of payment_id",
    )
    processing_lease_seconds: float = Field(default=60.0, gt=0)

    # --- outbox relay / consumer -----------------------------------------------
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_lease_seconds: float = Field(default=30.0, gt=0)
    recovery_interval_seconds: float = Field(default=30.0, gt=0)
    recovery_grace_seconds: float = Field(default=120.0, gt=0)
    consumer_prefetch: int = Field(default=8, ge=1, le=1000)

    @field_validator("webhook_allowed_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, value: object) -> object:
        if isinstance(value, str):
            return [h.strip().lower() for h in value.split(",") if h.strip()]
        return value

    @model_validator(mode="after")
    def _reject_unsafe_production(self) -> Self:
        if self.gateway_max_delay_seconds < self.gateway_min_delay_seconds:
            msg = "gateway_max_delay_seconds must be >= gateway_min_delay_seconds"
            raise ValueError(msg)
        if self.app_env is Environment.PROD:
            unsafe = [
                name
                for name in ("webhook_allow_private_networks", "webhook_allow_insecure_http")
                if getattr(self, name)
            ]
            if unsafe:
                msg = f"{', '.join(unsafe)} must be disabled when APP_ENV=prod"
                raise ValueError(msg)
        return self

    @property
    def retry_base_delay(self) -> timedelta:
        return timedelta(seconds=self.retry_base_delay_seconds)

    @property
    def retry_max_delay(self) -> timedelta:
        return timedelta(seconds=self.retry_max_delay_seconds)

    @property
    def processing_lease(self) -> timedelta:
        return timedelta(seconds=self.processing_lease_seconds)

    @property
    def outbox_lease(self) -> timedelta:
        return timedelta(seconds=self.outbox_lease_seconds)

    @property
    def recovery_grace(self) -> timedelta:
        return timedelta(seconds=self.recovery_grace_seconds)


def load_settings() -> Settings:
    return Settings()
