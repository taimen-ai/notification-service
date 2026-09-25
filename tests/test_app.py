"""Wiring and schema: migrations match the models, fail-closed defaults."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import httpx
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy.ext.asyncio import AsyncEngine

from notification_service.app import Overrides, create_app
from notification_service.models import Base

from conftest import alembic, auth, notification, settings_for


async def test_migrations_match_the_models(engine: AsyncEngine) -> None:
    def diff(connection: Any) -> list[Any]:
        return compare_metadata(MigrationContext.configure(connection), Base.metadata)

    async with engine.connect() as conn:
        assert await conn.run_sync(diff) == []


def test_migration_chain_downgrades_and_upgrades(engine: AsyncEngine) -> None:
    alembic("downgrade", "base")
    alembic("upgrade", "head")


async def test_unconfigured_stand_fails_closed(
    engine: AsyncEngine, sessions: Any, token: Callable[..., str]
) -> None:
    # No IAM and no Control Plane configured: nothing is accepted, nothing is open.
    app = create_app(settings_for(), Overrides(engine=engine))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://ns") as client:
            health = await client.get("/healthz")
            inbox = await client.get("/api/v1/me/notifications", headers=auth(token()))
            sent = await client.post(
                "/api/v1/notifications",
                json=notification({"kind": "principal", "id": str(uuid.uuid4())}),
                headers={**auth(token()), "Idempotency-Key": "k"},
            )

    assert health.status_code == 200
    assert inbox.status_code == 503
    assert inbox.json()["error"]["code"] == "verification_unavailable"
    assert sent.status_code == 503


async def test_unconfigured_directory_answers_503(
    engine: AsyncEngine, sessions: Any, verifier: Any, token: Callable[..., str]
) -> None:
    app = create_app(settings_for(), Overrides(engine=engine, verifier=verifier))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://ns") as client:
            sent = await client.post(
                "/api/v1/notifications",
                json=notification({"kind": "principal", "id": str(uuid.uuid4())}),
                headers={**auth(token()), "Idempotency-Key": "k"},
            )
    assert sent.status_code == 503
    assert sent.json()["error"]["code"] == "dependency_unavailable"


def test_openapi_describes_the_contract() -> None:
    schema = create_app(settings_for()).openapi()
    paths = set(schema["paths"])
    assert {
        "/api/v1/notifications",
        "/api/v1/notifications/{notification_id}",
        "/api/v1/me/notifications",
        "/api/v1/me/notifications/stream",
        "/api/v1/me/notifications/{item_id}:read",
        "/api/v1/me/notifications:read-all",
        "/api/v1/me/notification-preferences",
        "/api/v1/mandatory-rules",
        "/api/v1/mandatory-rules/{rule_id}",
    } <= paths
