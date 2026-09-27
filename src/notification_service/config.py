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

    # Consumer of Control Plane events, on the filter of the applied notification
    # rules (ADR-0005); the rules are re-read every poll. ``latest`` on the very
    # first start: a new installation does not notify about the journal's history.
    events_enabled: bool = True
    events_start: Literal["earliest", "latest"] = "latest"
    events_workspace_id: str = ""
    events_poll_seconds: float = 30.0

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

    # Telegram channel (Bot API). Without a bot token the channel is not
    # configured; without the webhook secret the webhook refuses every call.
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_webhook_secret: SecretStr = SecretStr("")
    telegram_api_url: str = "https://api.telegram.org"
    # The bot's @username without "@": deep links in group binding codes.
    telegram_bot_username: str = ""
    telegram_timeout_seconds: float = 10.0
    # How long a group binding code issued to an administrator is valid.
    channel_group_code_ttl_seconds: int = 600

    # The service's identity towards IAM as a channel adapter: confirming links
    # and exchanging channel assertions (``iam:channel-links`` in audience ``iam``).
    iam_channel_audience: str = "iam"
    iam_channel_scope: str = "iam:channel-links"
    # Launcher of personal harnesses (TAI-ADR-0051 §7): free text of a linked
    # person and presses of harness confirmations go into their conversation.
    # Empty — the channel stays notifications-only. The service account needs
    # audience ``human-harness`` with scope ``harness:inbound``.
    harness_launcher_url: str = ""
    harness_audience: str = "human-harness"
    harness_scope: str = "harness:inbound"

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
