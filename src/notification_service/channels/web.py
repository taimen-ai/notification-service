"""The ``web`` channel: the recipient's inbox, streamed over SSE."""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict

from sqlalchemy import func, insert, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service.channels.base import (
    OutboundMessage,
    PermanentDeliveryError,
    SendResult,
)
from notification_service.db import transaction
from notification_service.models import InboxItem

InboxKey = tuple[uuid.UUID, uuid.UUID]


class InboxBroker:
    """Wakes this process's open streams when their inbox grows.

    Only a hint: a stream re-reads the database after waking and also on a
    timer, so deliveries made by another process are still picked up.
    """

    def __init__(self) -> None:
        self._waiters: dict[InboxKey, set[asyncio.Event]] = defaultdict(set)

    def subscribe(self, key: InboxKey) -> asyncio.Event:
        event = asyncio.Event()
        self._waiters[key].add(event)
        return event

    def unsubscribe(self, key: InboxKey, event: asyncio.Event) -> None:
        waiters = self._waiters.get(key)
        if waiters is None:
            return
        waiters.discard(event)
        if not waiters:
            del self._waiters[key]

    def publish(self, key: InboxKey) -> None:
        for event in self._waiters.get(key, ()):
            event.set()


async def append_to_inbox(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID, notification_id: uuid.UUID
) -> int:
    """Put a notification into an inbox; returns its ``seq``.

    A per-recipient transaction lock serializes appends, so a later ``seq`` is
    never committed before an earlier one and a reader resuming after ``seq``
    cannot skip a row. Appending the same notification twice is a no-op.
    """
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"inbox:{tenant_id}:{principal_id}"},
    )
    existing = await session.scalar(
        select(InboxItem.seq).where(
            InboxItem.tenant_id == tenant_id,
            InboxItem.iam_principal_id == principal_id,
            InboxItem.notification_id == notification_id,
        )
    )
    if existing is not None:
        return existing
    last = await session.scalar(
        select(func.coalesce(func.max(InboxItem.seq), 0)).where(
            InboxItem.tenant_id == tenant_id, InboxItem.iam_principal_id == principal_id
        )
    )
    seq = int(last or 0) + 1
    await session.execute(
        insert(InboxItem).values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            iam_principal_id=principal_id,
            seq=seq,
            notification_id=notification_id,
        )
    )
    return seq


class WebChannel:
    name = "web"
    push = False
    needs_address = False

    def __init__(self, sessions: async_sessionmaker[AsyncSession], broker: InboxBroker) -> None:
        self._sessions = sessions
        self._broker = broker

    async def send(self, message: OutboundMessage) -> SendResult:
        if message.iam_principal_id is None:
            raise PermanentDeliveryError("recipient_has_no_identity")
        async with transaction(self._sessions) as session:
            seq = await append_to_inbox(
                session, message.tenant_id, message.iam_principal_id, message.notification_id
            )
        self._broker.publish((message.tenant_id, message.iam_principal_id))
        return SendResult(external_id=str(seq))
