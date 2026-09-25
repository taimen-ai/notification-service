"""Service settings, read from ``NS_*`` environment variables."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NS_", extra="ignore")

    database_url: str = "postgresql+psycopg://notify:notify@localhost:5432/notify"

    # Token validation (platform-auth-sdk). The service accepts exactly its own
    # audience; the JWKS URL defaults to the IAM well-known document.
    iam_url: str = ""
    iam_issuer: str = ""
    iam_jwks_url: str = ""
    audience: str = "notification-service"

    # The service's own identity towards the Control Plane (client credentials
    # exchanged at IAM for an audience-bound token).
    control_plane_url: str = ""
    service_client_id: str = ""
    service_client_secret: SecretStr = SecretStr("")

    # Delivery worker.
    worker_enabled: bool = True
    worker_poll_seconds: float = 1.0
    worker_batch_size: int = 50
    worker_lease_seconds: float = 120.0
    delivery_max_attempts: int = 8
    delivery_backoff_seconds: float = 5.0
    delivery_backoff_max_seconds: float = 3600.0

    # Email channel: ``smtp`` sends, ``log`` only writes the message to the log
    # (staging), ``disabled`` removes the channel.
    email_mode: Literal["smtp", "log", "disabled"] = "log"
    email_from: str = "notifications@localhost"
    smtp_host: str = "localhost"
    smtp_port: int = 587
    smtp_starttls: bool = True
    smtp_username: str = ""
    smtp_password: SecretStr = SecretStr("")
    smtp_timeout_seconds: float = 10.0

    # Web inbox stream: how often an idle stream re-checks the database (a
    # fallback for deliveries made by another process) and sends a keep-alive.
    inbox_poll_seconds: float = 5.0
    inbox_keepalive_seconds: float = 15.0

    @property
    def jwks_url(self) -> str:
        if self.iam_jwks_url:
            return self.iam_jwks_url
        return f"{self.iam_url.rstrip('/')}/.well-known/jwks.json" if self.iam_url else ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
