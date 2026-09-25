"""Application factory: API, delivery worker and event consumer in one process (staging)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from control_plane_client import ControlPlaneClient
from control_plane_client.events import EventConsumer
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
from notification_service.events import (
    ControlPlaneCore,
    CoreEventHandler,
    ServiceIdentity,
    build_consumer,
)
from notification_service.sending import NotificationSender
from notification_service.worker import DeliveryWorker

logger = logging.getLogger("notification_service")

CONTROL_PLANE_AUDIENCE = "control-plane"
CONSUMER_STOP_SECONDS = 10.0


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


@dataclass
class ControlPlaneConnection:
    client: ControlPlaneClient
    tokens: ServiceTokenProvider


def connect_control_plane(settings: Settings) -> ControlPlaneConnection | None:
    secret = settings.service_client_secret.get_secret_value()
    if not (
        settings.control_plane_url and settings.iam_url and settings.service_client_id and secret
    ):
        return None
    tokens = ServiceTokenProvider(
        settings.iam_url,
        ServiceCredentials(settings.service_client_id, secret, CONTROL_PLANE_AUDIENCE),
    )
    client = ControlPlaneClient(
        settings.control_plane_url,
        ServiceCredential(tokens),
        user_agent="notification-service/0.1",
    )
    return ControlPlaneConnection(client, tokens)


def build_event_consumer(
    settings: Settings,
    connection: ControlPlaneConnection,
    engine: AsyncEngine,
    sender: NotificationSender,
) -> EventConsumer | None:
    """The consumer of core events; it needs the service's identity, hence IAM."""
    if not (settings.events_enabled and settings.jwks_url and settings.iam_issuer):
        return None
    # The service's own token is addressed to the Control Plane: verified with
    # that audience, it names the sender of the notifications built from events.
    own_tokens = TokenVerifier(
        JwksCache(settings.jwks_url),
        VerifierConfig(issuer=settings.iam_issuer, audience=CONTROL_PLANE_AUDIENCE),
    )
    handler = CoreEventHandler(
        sender,
        ControlPlaneCore(connection.client),
        ServiceIdentity(connection.tokens, own_tokens),
        task_url_template=settings.task_url_template,
    )
    return build_consumer(settings, connection.client, engine, handler)


async def run_event_consumer(consumer: EventConsumer) -> None:
    try:
        await consumer.run()
    except asyncio.CancelledError:
        raise
    except Exception:
        # The core refuses the subscription itself (no events.read, a
        # credential it rejects): rereading changes nothing until an operator
        # fixes the grant. The API and delivery keep working.
        logger.exception("event consumer stopped: the Control Plane refused the subscription")


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

        connection = connect_control_plane(settings)
        directory = overrides.directory
        if directory is None:
            directory = (
                ControlPlaneDirectory(connection.client, iam_issuer=settings.iam_issuer)
                if connection is not None
                else UnconfiguredDirectory()
            )
        worker = DeliveryWorker(sessions, registry, settings)

        app.state.settings = settings
        app.state.sessions = sessions
        app.state.broker = broker
        app.state.channels = registry
        app.state.worker = worker
        app.state.verifier = overrides.verifier or build_verifier(settings)
        sender = NotificationSender(sessions, directory, registry, on_accepted=worker.wake)
        app.state.sender = sender
        consumer = (
            build_event_consumer(settings, connection, engine, sender)
            if connection is not None
            else None
        )
        app.state.event_consumer = consumer

        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop)) if settings.worker_enabled else None
        events = asyncio.create_task(run_event_consumer(consumer)) if consumer else None
        try:
            yield
        finally:
            stop.set()
            if consumer is not None and events is not None:
                # The event in hand is finished; a read hanging on the core is
                # cancelled — its event is simply read again after the restart.
                consumer.stop()
                with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                    await asyncio.wait_for(events, CONSUMER_STOP_SECONDS)
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if connection is not None:
                await connection.client.aclose()
                await connection.tokens.aclose()
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
