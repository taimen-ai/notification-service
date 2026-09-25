"""The delivery worker: sends due deliveries and journals every attempt.

A delivery is claimed with ``FOR UPDATE SKIP LOCKED`` and a lease, sent outside
any transaction, and its outcome is written back fenced by the attempt number:
if the lease ran out and another worker took the delivery over, the late
outcome of the first one is dropped instead of overwriting the second.

Transient failures are retried with exponential backoff up to the attempt
limit; permanent ones fail at once. Each delivery is independent — one dead
channel or address never holds back the others. Deliveries of one recipient
over one channel go out one after another in the order they were created, so
an inbox or a chat shows them in sending order; different recipients are
served concurrently.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Row, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service.channels import (
    ChannelRegistry,
    DeliveryError,
    OutboundMessage,
    PermanentDeliveryError,
    RecipientUnreachable,
    SendResult,
    TransientDeliveryError,
)
from notification_service.config import Settings
from notification_service.db import transaction, utcnow
from notification_service.models import ChannelAddress, ChannelGroup, Delivery, Notification

logger = logging.getLogger("notification_service.worker")

CLAIM = text(
    """
    UPDATE deliveries
       SET status = 'sending', attempts = attempts + 1, locked_until = :lease, updated_at = :now
     WHERE id IN (
           SELECT id FROM deliveries
            WHERE (status = 'pending' AND next_attempt_at <= :now)
               OR (status = 'sending' AND locked_until < :now)
            ORDER BY next_attempt_at, created_at
            LIMIT :limit
              FOR UPDATE SKIP LOCKED)
    RETURNING id, attempts, channel, recipient_kind, recipient_id, next_attempt_at, created_at
    """
)


def backoff(attempts: int, *, base: float, cap: float) -> timedelta:
    return timedelta(seconds=min(base * 2 ** max(attempts - 1, 0), cap))


class DeliveryWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        channels: ChannelRegistry,
        settings: Settings,
        *,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._sessions = sessions
        self._channels = channels
        self._settings = settings
        self._clock = clock
        self._wakeup = asyncio.Event()

    def wake(self) -> None:
        self._wakeup.set()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self._wakeup.clear()
            try:
                handled = await self.run_once()
            except Exception:
                logger.exception("delivery pass failed")
                handled = 0
            if handled:
                continue
            waiters = {
                asyncio.ensure_future(stop.wait()),
                asyncio.ensure_future(self._wakeup.wait()),
            }
            _, pending = await asyncio.wait(
                waiters,
                timeout=self._settings.worker_poll_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for waiter in pending:
                waiter.cancel()

    async def run_once(self) -> int:
        """Claim one batch of due deliveries and send them; returns how many."""
        now = self._clock()
        async with transaction(self._sessions) as session:
            claimed = (
                await session.execute(
                    CLAIM,
                    {
                        "now": now,
                        "lease": now + timedelta(seconds=self._settings.worker_lease_seconds),
                        "limit": self._settings.worker_batch_size,
                    },
                )
            ).all()
        queues: dict[tuple[str, str, uuid.UUID], list[Row[Any]]] = defaultdict(list)
        for row in sorted(claimed, key=lambda r: (r.next_attempt_at, r.created_at)):
            queues[(row.channel, row.recipient_kind, row.recipient_id)].append(row)
        outcomes = await asyncio.gather(
            *(self._deliver_in_order(queue) for queue in queues.values()), return_exceptions=True
        )
        for outcome in outcomes:
            # Not a channel failure (those are journaled) but e.g. a lost database
            # connection: the lease runs out and the delivery is claimed again.
            if isinstance(outcome, Exception):
                logger.error("delivery queue aborted", exc_info=outcome)
        return len(claimed)

    async def _deliver_in_order(self, queue: Sequence[Row[Any]]) -> None:
        for row in queue:
            await self._deliver(row.id, row.attempts)

    async def _deliver(self, delivery_id: uuid.UUID, attempts: int) -> None:
        async with self._sessions() as session:
            delivery = await session.get(Delivery, delivery_id)
            assert delivery is not None
            notification = await session.get(Notification, delivery.notification_id)
            assert notification is not None
            address = await self._address(session, delivery)

        channel = self._channels.get(delivery.channel)
        try:
            if channel is None:
                raise PermanentDeliveryError("channel_not_configured")
            if address is None:
                raise PermanentDeliveryError("recipient_unreachable")
            message = OutboundMessage(
                delivery_id=delivery.id,
                notification_id=notification.id,
                tenant_id=notification.tenant_id,
                channel=delivery.channel,
                recipient_kind=delivery.recipient_kind,
                recipient_id=delivery.recipient_id,
                iam_principal_id=delivery.iam_principal_id,
                address=address,
                type=notification.type,
                title=notification.title,
                body=notification.body,
                links=list(notification.links),
                actions=list(notification.actions),
                created_at=notification.created_at,
            )
            # Never outlive the lease: past it the delivery may be someone else's.
            result = await asyncio.wait_for(
                channel.send(message), timeout=self._settings.worker_lease_seconds / 2
            )
        except DeliveryError as exc:
            await self._failed(delivery, attempts, exc, address)
        except TimeoutError:
            await self._failed(delivery, attempts, TransientDeliveryError("send_timeout"), address)
        except Exception as exc:
            logger.exception("channel %s crashed on delivery %s", delivery.channel, delivery.id)
            await self._failed(
                delivery,
                attempts,
                TransientDeliveryError(f"channel_error: {type(exc).__name__}"),
                address,
            )
        else:
            await self._delivered(delivery, attempts, result)

    async def _address(self, session: AsyncSession, delivery: Delivery) -> str | None:
        """The address to send to now; ``None`` when the recipient cannot be reached."""
        if delivery.recipient_kind == "group":
            group = await session.get(ChannelGroup, delivery.recipient_id)
            if group is None or group.disabled_at is not None:
                return None
            return group.external_chat_id
        channel = self._channels.get(delivery.channel)
        if channel is None or not channel.needs_address:
            return ""
        if delivery.iam_principal_id is None:
            return None
        row = await session.get(
            ChannelAddress, (delivery.tenant_id, delivery.iam_principal_id, delivery.channel)
        )
        if row is None or row.disabled_at is not None:
            return None
        return row.address

    def _fence(self, delivery: Delivery, attempts: int):  # type: ignore[no-untyped-def]
        return update(Delivery).where(
            Delivery.id == delivery.id,
            Delivery.status == "sending",
            Delivery.attempts == attempts,
        )

    async def _delivered(self, delivery: Delivery, attempts: int, result: SendResult) -> None:
        now = self._clock()
        async with transaction(self._sessions) as session:
            await session.execute(
                self._fence(delivery, attempts).values(
                    status="delivered",
                    delivered_at=now,
                    external_id=result.external_id,
                    last_error=None,
                    locked_until=None,
                    updated_at=now,
                )
            )

    async def _failed(
        self, delivery: Delivery, attempts: int, error: DeliveryError, address: str | None
    ) -> None:
        now = self._clock()
        limit = self._settings.delivery_max_attempts
        if not error.permanent and attempts < limit:
            values: dict[str, object] = {
                "status": "pending",
                "next_attempt_at": now
                + backoff(
                    attempts,
                    base=self._settings.delivery_backoff_seconds,
                    cap=self._settings.delivery_backoff_max_seconds,
                ),
                "last_error": error.reason,
            }
        else:
            reason = error.reason if error.permanent else f"retries_exhausted: {error.reason}"
            values = {"status": "failed", "last_error": reason[:500]}
        async with transaction(self._sessions) as session:
            fenced = await session.execute(
                self._fence(delivery, attempts).values(locked_until=None, updated_at=now, **values)
            )
            if fenced.rowcount and isinstance(error, RecipientUnreachable) and address:  # type: ignore[attr-defined]
                await self._disable_address(session, delivery, address, error.reason, now)
        logger.info(
            "delivery %s over %s: %s (%s)",
            delivery.id,
            delivery.channel,
            values["status"],
            error.reason,
        )

    async def _disable_address(
        self, session: AsyncSession, delivery: Delivery, address: str, reason: str, now: datetime
    ) -> None:
        if delivery.recipient_kind == "group":
            await session.execute(
                update(ChannelGroup)
                .where(ChannelGroup.id == delivery.recipient_id, ChannelGroup.disabled_at.is_(None))
                .values(disabled_at=now, disabled_reason=reason[:200])
            )
            return
        # Only the address that bounced: one set meanwhile stays enabled.
        await session.execute(
            update(ChannelAddress)
            .where(
                ChannelAddress.tenant_id == delivery.tenant_id,
                ChannelAddress.principal_id == delivery.iam_principal_id,
                ChannelAddress.channel == delivery.channel,
                ChannelAddress.address == address,
                ChannelAddress.disabled_at.is_(None),
            )
            .values(disabled_at=now, disabled_reason=reason[:200])
        )
