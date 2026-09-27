from __future__ import annotations

import os
import re
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from platform_auth import StaticKeySet, TokenVerifier, VerifierConfig
from platform_auth.testing import SigningKey
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from notification_service.app import Overrides, create_app
from notification_service.auth import SCOPE_READ, SCOPE_SEND
from notification_service.channels import Channel
from notification_service.config import Settings
from notification_service.db import create_engine, session_factory
from notification_service.directory import Addressee, DirectoryUnavailable, UnknownRecipient
from notification_service.models import Base

ROOT = Path(__file__).resolve().parents[1]
ISSUER = "https://iam.test"
AUDIENCE = "notification-service"


def database_url() -> str:
    url = os.environ.get("NS_TEST_DATABASE_URL")
    if not url:
        pytest.exit("NS_TEST_DATABASE_URL is not set: the tests need a PostgreSQL database", 2)
    return url


def alembic(*args: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env={**os.environ, "NS_DATABASE_URL": database_url()},
        check=True,
        capture_output=True,
    )


@pytest.fixture(scope="session")
def migrated() -> None:
    # A clean schema built by the migration chain, not by metadata.create_all:
    # the tests then also prove the migrations produce a working schema.
    alembic("downgrade", "base")
    alembic("upgrade", "head")


@pytest.fixture(scope="session")
async def engine(migrated: None) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(database_url())
    yield engine
    await engine.dispose()


@pytest.fixture
async def sessions(engine: AsyncEngine) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    tables = ", ".join(table.name for table in Base.metadata.sorted_tables)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {tables} CASCADE"))
    yield session_factory(engine)


@pytest.fixture(scope="session")
def signing_key() -> SigningKey:
    return SigningKey.generate("ns-test")


@pytest.fixture(scope="session")
def verifier(signing_key: SigningKey) -> TokenVerifier:
    return TokenVerifier(
        StaticKeySet(signing_key.public_pem, key_id=signing_key.key_id),
        VerifierConfig(issuer=ISSUER, audience=AUDIENCE),
    )


@dataclass
class Person:
    """A test participant: a Control Plane principal bound to an IAM identity."""

    principal_id: uuid.UUID
    iam_principal_id: uuid.UUID
    tenant_id: uuid.UUID


@dataclass
class FakeDirectory:
    """The Control Plane directory as the service sees it (contract: ``Directory``)."""

    people: dict[uuid.UUID, Addressee] = field(default_factory=dict)
    roles: dict[tuple[uuid.UUID, uuid.UUID], list[uuid.UUID]] = field(default_factory=dict)
    unavailable: bool = False

    async def principal(self, tenant_id: uuid.UUID, principal_id: uuid.UUID) -> Addressee:
        if self.unavailable:
            raise DirectoryUnavailable("down")
        if principal_id not in self.people:
            raise UnknownRecipient(f"principal {principal_id}")
        return self.people[principal_id]

    async def role_holders(
        self, tenant_id: uuid.UUID, role_id: uuid.UUID, workspace_id: uuid.UUID
    ) -> list[Addressee]:
        if self.unavailable:
            raise DirectoryUnavailable("down")
        if (role_id, workspace_id) not in self.roles:
            raise UnknownRecipient(f"role {role_id}")
        return [self.people[p] for p in self.roles[(role_id, workspace_id)]]


@pytest.fixture
def tenant_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def directory() -> FakeDirectory:
    return FakeDirectory()


@pytest.fixture
def make_person(directory: FakeDirectory, tenant_id: uuid.UUID) -> Callable[..., Person]:
    def make(*, bound: bool = True) -> Person:
        person = Person(uuid.uuid4(), uuid.uuid4(), tenant_id)
        directory.people[person.principal_id] = Addressee(
            person.principal_id, person.iam_principal_id if bound else None
        )
        return person

    return make


@pytest.fixture
def token(signing_key: SigningKey, tenant_id: uuid.UUID) -> Callable[..., str]:
    def issue(
        subject: uuid.UUID | None = None,
        *,
        scopes: tuple[str, ...] = (SCOPE_SEND, SCOPE_READ),
        tenant: uuid.UUID | None = None,
        audience: str = AUDIENCE,
        principal_type: str = "service_account",
        ttl_seconds: int = 300,
    ) -> str:
        return signing_key.issue(
            ttl_seconds=ttl_seconds,
            issuer=ISSUER,
            audience=audience,
            tenant_id=tenant or tenant_id,
            subject=subject or uuid.uuid4(),
            scopes=list(scopes),
            principal_type=principal_type,
        )

    return issue


def auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def settings_for(**changes: Any) -> Settings:
    base: dict[str, Any] = {
        "worker_enabled": False,
        "email_mode": "log",
        "delivery_backoff_seconds": 0.0,
        "inbox_poll_seconds": 0.2,
        "inbox_keepalive_seconds": 30.0,
    }
    base.update(changes)
    return Settings(database_url=database_url(), **base)


@dataclass
class Harness:
    client: httpx.AsyncClient
    app: Any
    sessions: async_sessionmaker[AsyncSession]

    @property
    def worker(self) -> Any:
        return self.app.state.worker

    async def drain(self, rounds: int = 10) -> None:
        """Run the worker until nothing is due."""
        for _ in range(rounds):
            if not await self.worker.run_once():
                return


@pytest.fixture
def harness_factory(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    verifier: TokenVerifier,
    directory: FakeDirectory,
) -> Callable[..., Any]:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def build(
        *,
        settings: Settings | None = None,
        channels: list[Channel] | None = None,
        **overrides: Any,
    ) -> AsyncIterator[Harness]:
        app = create_app(
            settings or settings_for(),
            Overrides(
                engine=engine,
                verifier=verifier,
                directory=directory,
                extra_channels=channels or [],
                **overrides,
            ),
        )
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://ns") as client:
                yield Harness(client, app, app.state.sessions)

    return build


@pytest.fixture
async def harness(harness_factory: Callable[..., Any]) -> AsyncIterator[Harness]:
    async with harness_factory() as built:
        yield built


def notification(recipient: dict[str, Any], **fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "recipient": recipient,
        "type": "work.review_requested",
        "title": "Review requested",
        "body": "A change waits for your review.",
        "links": [{"label": "Open", "url": "https://example.test/items/1"}],
    }
    body.update(fields)
    return body


def to_principal(person: Person) -> dict[str, str]:
    return {"kind": "principal", "id": str(person.principal_id)}


@pytest.fixture
def unique_key() -> Iterator[Callable[[], str]]:
    yield lambda: uuid.uuid4().hex


ADR_0005 = ROOT / "docs" / "adr" / "0005-notification-rules-as-data.md"


def adr_rules(task_url_base: str = "https://console.test/tasks") -> list[dict[str, Any]]:
    """The rules of the former built-in behaviour: the YAML blocks of ADR-0005 §8.

    ``${TASK_URL_BASE}`` is what the package installer substitutes before applying.
    """
    text = ADR_0005.read_text(encoding="utf-8").replace("${TASK_URL_BASE}", task_url_base)
    return [yaml.safe_load(block) for block in re.findall(r"```yaml\n(.*?)```", text, re.DOTALL)]
