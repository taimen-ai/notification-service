"""The web inbox: listing, read marks and the SSE stream with ``Last-Event-ID``.

Every inbox item has a per-recipient ``seq`` (see ``channels.web``). The stream
sends it as the SSE ``id``, so a reconnecting browser presents the last one it
saw and receives everything after it — nothing missed while offline.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Select, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service.channels.web import InboxBroker, InboxKey
from notification_service.db import utcnow
from notification_service.models import InboxItem, Notification
from notification_service.schemas import InboxItemOut

STREAM_BATCH = 100


def _query(tenant_id: uuid.UUID, principal_id: uuid.UUID) -> Select[tuple[InboxItem, Notification]]:
    return (
        select(InboxItem, Notification)
        .join(Notification, Notification.id == InboxItem.notification_id)
        .where(InboxItem.tenant_id == tenant_id, InboxItem.iam_principal_id == principal_id)
    )


def to_out(item: InboxItem, notification: Notification) -> InboxItemOut:
    return InboxItemOut(
        id=item.id,
        seq=item.seq,
        notification_id=notification.id,
        type=notification.type,
        title=notification.title,
        body=notification.body,
        links=list(notification.links),
        actions=list(notification.actions),
        actions_closed_at=notification.actions_closed_at,
        actions_outcome=notification.actions_outcome,
        sender_id=notification.sender_id,
        created_at=notification.created_at,
        read_at=item.read_at,
    )


@dataclass(frozen=True)
class InboxPage:
    items: list[InboxItemOut]
    unread_count: int
    next_cursor: str | None


async def list_inbox(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    principal_id: uuid.UUID,
    *,
    unread_only: bool,
    limit: int,
    before_seq: int | None,
) -> InboxPage:
    """Newest first; the cursor is the ``seq`` to continue below."""
    query = _query(tenant_id, principal_id)
    if unread_only:
        query = query.where(InboxItem.read_at.is_(None))
    if before_seq is not None:
        query = query.where(InboxItem.seq < before_seq)
    rows = (await session.execute(query.order_by(InboxItem.seq.desc()).limit(limit + 1))).all()
    items = [to_out(item, notification) for item, notification in rows[:limit]]
    unread = await session.scalar(
        select(func.count())
        .select_from(InboxItem)
        .where(
            InboxItem.tenant_id == tenant_id,
            InboxItem.iam_principal_id == principal_id,
            InboxItem.read_at.is_(None),
        )
    )
    next_cursor = str(items[-1].seq) if len(rows) > limit else None
    return InboxPage(items, int(unread or 0), next_cursor)


async def items_after(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID, after_seq: int, limit: int
) -> list[InboxItemOut]:
    rows = (
        await session.execute(
            _query(tenant_id, principal_id)
            .where(InboxItem.seq > after_seq)
            .order_by(InboxItem.seq)
            .limit(limit)
        )
    ).all()
    return [to_out(item, notification) for item, notification in rows]


async def last_seq(session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID) -> int:
    value = await session.scalar(
        select(func.coalesce(func.max(InboxItem.seq), 0)).where(
            InboxItem.tenant_id == tenant_id, InboxItem.iam_principal_id == principal_id
        )
    )
    return int(value or 0)


async def mark_read(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID, item_id: uuid.UUID
) -> InboxItemOut | None:
    row = (
        await session.execute(_query(tenant_id, principal_id).where(InboxItem.id == item_id))
    ).first()
    if row is None:
        return None
    item, notification = row
    if item.read_at is None:
        item.read_at = utcnow()
    return to_out(item, notification)


async def mark_all_read(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID
) -> int:
    result = await session.execute(
        update(InboxItem)
        .where(
            InboxItem.tenant_id == tenant_id,
            InboxItem.iam_principal_id == principal_id,
            InboxItem.read_at.is_(None),
        )
        .values(read_at=utcnow())
    )
    return int(result.rowcount or 0)  # type: ignore[attr-defined]


def sse_event(item: InboxItemOut) -> str:
    data = json.dumps(item.model_dump(mode="json", by_alias=True), separators=(",", ":"))
    return f"id: {item.seq}\nevent: notification\ndata: {data}\n\n"


async def stream(
    sessions: async_sessionmaker[AsyncSession],
    broker: InboxBroker,
    key: InboxKey,
    *,
    after_seq: int,
    poll_seconds: float,
    keepalive_seconds: float,
    until: datetime | None = None,
) -> AsyncIterator[str]:
    """Catch up after ``after_seq``, then follow the inbox.

    The stream ends when the token that opened it expires (``until``): the
    client reconnects with a fresh token and ``Last-Event-ID``, losing nothing.
    """
    tenant_id, principal_id = key
    wakeup = broker.subscribe(key)
    try:
        yield "retry: 3000\n\n"
        last_write = time.monotonic()
        while until is None or utcnow() < until:
            # Cleared before reading: a delivery landing after the read sets it
            # again, so no wakeup is lost between the read and the wait.
            wakeup.clear()
            async with sessions() as session:
                items = await items_after(session, tenant_id, principal_id, after_seq, STREAM_BATCH)
            for item in items:
                yield sse_event(item)
                after_seq = item.seq
            if items:
                last_write = time.monotonic()
                if len(items) == STREAM_BATCH:
                    continue
            try:
                await asyncio.wait_for(wakeup.wait(), timeout=poll_seconds)
            except TimeoutError:
                if time.monotonic() - last_write >= keepalive_seconds:
                    yield ": keep-alive\n\n"
                    last_write = time.monotonic()
    finally:
        broker.unsubscribe(key, wakeup)
