"""Versions of notification rules in the service's database (ADR-0005 §5).

Versions are immutable. Applying a spec whose hash equals the active version's
creates nothing, so a package applied again is a no-op; another spec becomes
the next version and supersedes the active one in the same transaction.
Changes of one key are serialized by a transaction-scoped advisory lock: a key
that has no row yet has nothing to lock otherwise.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service.db import utcnow
from notification_service.models import NotificationRule
from notification_service.rules import enabled, spec_hash

ACTIVE = "active"
SUPERSEDED = "superseded"
RETIRED = "retired"


async def _lock(session: AsyncSession, tenant_id: uuid.UUID, key: str) -> None:
    digest = hashlib.sha256(f"notification-rule:{tenant_id}:{key}".encode()).digest()
    await session.execute(
        select(func.pg_advisory_xact_lock(int.from_bytes(digest[:8], "big", signed=True)))
    )


async def latest(session: AsyncSession, tenant_id: uuid.UUID, key: str) -> NotificationRule | None:
    """The newest version of a key: the active one, or the retired one."""
    return await session.scalar(
        select(NotificationRule)
        .where(NotificationRule.tenant_id == tenant_id, NotificationRule.key == key)
        .order_by(NotificationRule.version.desc())
        .limit(1)
    )


async def apply(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    key: str,
    spec: Mapping[str, Any],
    created_by: uuid.UUID,
) -> tuple[NotificationRule, bool]:
    """Make ``spec`` the active version of ``key``; ``True`` when a version was created."""
    await _lock(session, tenant_id, key)
    digest = spec_hash(spec)
    current = await latest(session, tenant_id, key)
    if current is not None and current.state == ACTIVE:
        if current.spec_hash == digest:
            return current, False
        current.state = SUPERSEDED
        # Before the insert: one active version per key is a unique index.
        await session.flush()
    created = NotificationRule(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        key=key,
        version=current.version + 1 if current is not None else 1,
        spec=dict(spec),
        spec_hash=digest,
        state=ACTIVE,
        created_by=created_by,
        created_at=utcnow(),
    )
    session.add(created)
    await session.flush()
    return created, True


async def retire(session: AsyncSession, tenant_id: uuid.UUID, key: str) -> NotificationRule | None:
    """Take the key out of use; a repeat returns the retired version, ``None`` — no such key."""
    await _lock(session, tenant_id, key)
    current = await latest(session, tenant_id, key)
    if current is not None and current.state == ACTIVE:
        current.state = RETIRED
        await session.flush()
    return current


async def listing(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    key: str | None = None,
    include_retired: bool = False,
    after: str | None = None,
    limit: int = 100,
) -> list[NotificationRule]:
    """The newest version of every key, active ones (and retired ones), by key."""
    newest = (
        select(NotificationRule.key, func.max(NotificationRule.version).label("version"))
        .where(NotificationRule.tenant_id == tenant_id)
        .group_by(NotificationRule.key)
    )
    if key is not None:
        newest = newest.where(NotificationRule.key == key)
    if after is not None:
        newest = newest.where(NotificationRule.key > after)
    heads = newest.subquery()
    states = (ACTIVE, RETIRED) if include_retired else (ACTIVE,)
    rows = await session.scalars(
        select(NotificationRule)
        .join(
            heads,
            (NotificationRule.key == heads.c.key) & (NotificationRule.version == heads.c.version),
        )
        .where(NotificationRule.tenant_id == tenant_id, NotificationRule.state.in_(states))
        .order_by(NotificationRule.key)
        .limit(limit)
    )
    return list(rows)


@dataclass(frozen=True)
class ActiveRule:
    key: str
    version: int
    spec: Mapping[str, Any]


async def executable(
    sessions: async_sessionmaker[AsyncSession], tenant_id: uuid.UUID
) -> list[ActiveRule]:
    """The rules the consumer executes: active versions with ``status: enabled``, by key."""
    async with sessions() as session:
        rows = await session.scalars(
            select(NotificationRule)
            .where(NotificationRule.tenant_id == tenant_id, NotificationRule.state == ACTIVE)
            .order_by(NotificationRule.key)
        )
        return [ActiveRule(row.key, row.version, row.spec) for row in rows if enabled(row.spec)]
