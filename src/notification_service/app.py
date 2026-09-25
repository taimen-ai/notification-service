"""Application factory: API and delivery worker in one process (plan: staging budget)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from control_plane_client import ControlPlaneClient
from fastapi import FastAPI
from platform_auth import (
    JwksCache,
    ServiceCredentials,
    ServiceTokenProvider,
    TokenVerifier,
    VerifierConfig,
)
from sqlalchemy.ext.asyncio import AsyncEngine

from notification_service import errors
from notification_service.api import router
from notification_service.channels import Channel, ChannelRegistry
from notification_service.channels.email import EmailChannel
from notification_service.channels.web import InboxBroker, WebChannel
from notification_service.config import Settings, get_settings
from notification_service.db import create_engine, session_factory
from notification_service.directory import (
    ControlPlaneDirectory,
    Directory,
    ServiceCredential,
    UnconfiguredDirectory,
)
from notification_service.sending import NotificationSender
from notification_service.worker import DeliveryWorker

logger = logging.getLogger("notification_service")

CONTROL_PLANE_AUDIENCE = "control-plane"


@dataclass
class Overrides:
    """Seams for tests: anything left ``None`` is built from settings."""

    engine: AsyncEngine | None = None
    verifier: TokenVerifier | None = None
    directory: Directory | None = None
    extra_channels: list[Channel] = field(default_factory=list)


def build_verifier(settings: Settings) -> TokenVerifier | None:
    if not settings.jwks_url or not settings.iam_issuer:
        return None
    return TokenVerifier(
        JwksCache(settings.jwks_url),
        VerifierConfig(issuer=settings.iam_issuer, audience=settings.audience),
    )


def build_directory(settings: Settings) -> tuple[Directory, ControlPlaneClient | None]:
    secret = settings.service_client_secret.get_secret_value()
    if not (
        settings.control_plane_url and settings.iam_url and settings.service_client_id and secret
    ):
        return UnconfiguredDirectory(), None
    tokens = ServiceTokenProvider(
        settings.iam_url,
        ServiceCredentials(settings.service_client_id, secret, CONTROL_PLANE_AUDIENCE),
    )
    client = ControlPlaneClient(
        settings.control_plane_url,
        ServiceCredential(tokens),
        user_agent="notification-service/0.1",
    )
    return ControlPlaneDirectory(client, iam_issuer=settings.iam_issuer), client


def create_app(settings: Settings | None = None, overrides: Overrides | None = None) -> FastAPI:
    settings = settings or get_settings()
    overrides = overrides or Overrides()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = overrides.engine or create_engine(settings.database_url)
        sessions = session_factory(engine)
        broker = InboxBroker()
        channels: list[Channel] = [WebChannel(sessions, broker)]
        if settings.email_mode != "disabled":
            channels.append(EmailChannel(settings))
        registry = ChannelRegistry([*channels, *overrides.extra_channels])

        cp_client = None
        directory = overrides.directory
        if directory is None:
            directory, cp_client = build_directory(settings)
        worker = DeliveryWorker(sessions, registry, settings)

        app.state.settings = settings
        app.state.sessions = sessions
        app.state.broker = broker
        app.state.channels = registry
        app.state.worker = worker
        app.state.verifier = overrides.verifier or build_verifier(settings)
        app.state.sender = NotificationSender(
            sessions, directory, registry, on_accepted=worker.wake
        )

        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop)) if settings.worker_enabled else None
        try:
            yield
        finally:
            stop.set()
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if cp_client is not None:
                await cp_client.aclose()
            if overrides.engine is None:
                await engine.dispose()

    app = FastAPI(
        title="notification-service",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        openapi_url="/openapi.json",
    )
    errors.install(app)
    app.include_router(router)

    # authz: public — liveness probe, reveals nothing.
    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
