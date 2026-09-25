"""Accepting a notification: deduplicate, resolve recipients, plan deliveries.

Delivery itself is the worker's job; here a notification becomes rows in the
delivery journal, one per recipient and selected channel, in the same
transaction as the notification. What could not even be attempted (no
identity, no address for a mandatory channel, channel not configured) is
journaled as ``failed`` right away, so the sender sees it.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any

from platform_auth import TrustedAuthContext
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service.channels import ChannelRegistry
from notification_service.db import transaction, utcnow
from notification_service.directory import (
    Addressee,
    Directory,
    DirectoryUnavailable,
    UnknownRecipient,
)
from notification_service.errors import Conflict, Unavailable, Unprocessable
from notification_service.models import (
    ChannelAddress,
    ChannelGroup,
    Delivery,
    MandatoryRule,
    Notification,
    Preference,
    QuietHours,
)
from notification_service.routing import (
    PreferenceRule,
    QuietWindow,
    select_channels,
)
from notification_service.schemas import NotificationCreate

WEB = "web"


@dataclass(frozen=True)
class Accepted:
    notification: Notification
    deliveries: list[Delivery]
    created: bool


@dataclass(frozen=True)
class Targets:
    people: list[Addressee]
    groups: list[ChannelGroup]


def request_hash(payload: NotificationCreate) -> str:
    canonical = json.dumps(
        payload.model_dump(mode="json", by_alias=True), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def minute_to_time(minute: int) -> time:
    return time(minute // 60, minute % 60)


class NotificationSender:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        directory: Directory,
        channels: ChannelRegistry,
        *,
        on_accepted: Callable[[], None] | None = None,
    ) -> None:
        self._sessions = sessions
        self._directory = directory
        self._channels = channels
        self._on_accepted = on_accepted

    async def accept(
        self, ctx: TrustedAuthContext, payload: NotificationCreate, dedup_key: str
    ) -> Accepted:
        digest = request_hash(payload)
        async with transaction(self._sessions) as session:
            replay = await self._replay(session, ctx, dedup_key, digest)
        if replay is not None:
            return replay

        # The directory is asked outside any transaction: it is a network call.
        targets = await self._resolve(ctx.tenant_id, payload)
        try:
            async with transaction(self._sessions) as session:
                accepted = await self._store(session, ctx, payload, dedup_key, digest, targets)
        except IntegrityError:
            # A concurrent request with the same key won the insert.
            async with transaction(self._sessions) as session:
                replay = await self._replay(session, ctx, dedup_key, digest)
            if replay is None:  # pragma: no cover - the constraint only fires on a duplicate
                raise
            return replay
        if self._on_accepted is not None:
            self._on_accepted()
        return accepted

    async def close_actions(
        self,
        tenant_id: uuid.UUID,
        sender_id: uuid.UUID,
        dedup_key: str,
        outcome: dict[str, Any],
    ) -> Notification | None:
        """Mark the actions of the sender's notification ``dedup_key`` as no longer valid.

        The first closure wins: a repeated or late one (a decision event read
        again) leaves the recorded outcome as it is. ``None`` — no such
        notification (it was never sent, e.g. before the consumer started).
        """
        async with transaction(self._sessions) as session:
            notification = await session.scalar(
                select(Notification)
                .where(
                    Notification.tenant_id == tenant_id,
                    Notification.sender_id == sender_id,
                    Notification.dedup_key == dedup_key,
                )
                .with_for_update()
            )
            if notification is None:
                return None
            if notification.actions_closed_at is None:
                notification.actions_closed_at = utcnow()
                notification.actions_outcome = outcome
            return notification

    async def _replay(
        self, session: AsyncSession, ctx: TrustedAuthContext, dedup_key: str, digest: str
    ) -> Accepted | None:
        existing = await session.scalar(
            select(Notification).where(
                Notification.tenant_id == ctx.tenant_id,
                Notification.sender_id == ctx.principal_id,
                Notification.dedup_key == dedup_key,
            )
        )
        if existing is None:
            return None
        if existing.request_hash != digest:
            raise Conflict(
                "Idempotency-Key was already used with a different notification",
                code="idempotency_conflict",
            )
        deliveries = await load_deliveries(session, existing.id)
        return Accepted(existing, deliveries, created=False)

    async def _resolve(self, tenant_id: uuid.UUID, payload: NotificationCreate) -> Targets:
        recipient = payload.recipient
        try:
            if recipient.kind == "principal":
                return Targets([await self._directory.principal(tenant_id, recipient.id)], [])
            if recipient.kind == "role":
                assert recipient.workspace_id is not None  # guaranteed by the schema
                people = await self._directory.role_holders(
                    tenant_id, recipient.id, recipient.workspace_id
                )
                groups = await self._groups(
                    ChannelGroup.workspace_id == recipient.workspace_id,
                    ChannelGroup.role_id == recipient.id,
                    tenant_id=tenant_id,
                )
                return Targets(people, groups)
            groups = await self._groups(ChannelGroup.id == recipient.id, tenant_id=tenant_id)
            if not groups:
                raise UnknownRecipient(f"group {recipient.id}")
            return Targets([], groups)
        except UnknownRecipient as exc:
            raise Unprocessable(str(exc), code="unknown_recipient") from exc
        except DirectoryUnavailable as exc:
            raise Unavailable("Control Plane directory is unavailable") from exc

    async def _groups(self, *criteria: object, tenant_id: uuid.UUID) -> list[ChannelGroup]:
        async with self._sessions() as session:
            rows = await session.scalars(
                select(ChannelGroup)
                .where(
                    ChannelGroup.tenant_id == tenant_id,
                    ChannelGroup.disabled_at.is_(None),
                    *criteria,  # type: ignore[arg-type]
                )
                .order_by(ChannelGroup.created_at)
            )
            return list(rows)

    async def _store(
        self,
        session: AsyncSession,
        ctx: TrustedAuthContext,
        payload: NotificationCreate,
        dedup_key: str,
        digest: str,
        targets: Targets,
    ) -> Accepted:
        now = utcnow()
        notification = Notification(
            id=uuid.uuid4(),
            tenant_id=ctx.tenant_id,
            sender_id=ctx.principal_id,
            sender_type=ctx.principal_type or "unknown",
            dedup_key=dedup_key,
            request_hash=digest,
            recipient_kind=payload.recipient.kind,
            recipient_id=payload.recipient.id,
            workspace_id=payload.recipient.workspace_id,
            type=payload.type,
            title=payload.title,
            body=payload.body,
            links=[link.model_dump(mode="json", by_alias=True) for link in payload.links],
            actions=[action.model_dump(mode="json", by_alias=True) for action in payload.actions],
            created_at=now,
        )
        session.add(notification)
        await session.flush()

        deliveries: list[Delivery] = []
        people = list({person.principal_id: person for person in targets.people}.values())
        routing = await RoutingData.load(
            session,
            ctx.tenant_id,
            [p.iam_principal_id for p in people if p.iam_principal_id is not None],
        )
        for person in people:
            deliveries += self._plan_person(notification, person, routing, now)
        for group in targets.groups:
            deliveries.append(self._plan_group(notification, group, now))
        session.add_all(deliveries)
        await session.flush()
        return Accepted(notification, deliveries, created=True)

    def _plan_person(
        self, notification: Notification, person: Addressee, routing: RoutingData, now: datetime
    ) -> list[Delivery]:
        def delivery(channel: str, **fields: object) -> Delivery:
            return Delivery(
                id=uuid.uuid4(),
                tenant_id=notification.tenant_id,
                notification_id=notification.id,
                channel=channel,
                recipient_kind="principal",
                recipient_id=person.principal_id,
                iam_principal_id=person.iam_principal_id,
                attempts=0,
                created_at=now,
                updated_at=now,
                **fields,
            )

        if person.iam_principal_id is None:
            return [
                delivery(
                    WEB,
                    mandatory=False,
                    status="failed",
                    next_attempt_at=now,
                    last_error="recipient_has_no_identity",
                )
            ]
        iam_id = person.iam_principal_id
        decisions = select_channels(
            notification.type,
            channels=self._channels.names(),
            reachable={
                channel.name: (not channel.needs_address)
                or (iam_id, channel.name) in routing.addresses
                for channel in self._channels
            },
            preferences=routing.preferences.get(iam_id, []),
            mandatory_patterns=routing.mandatory,
            push_channels=[channel.name for channel in self._channels if channel.push],
            quiet=routing.quiet.get(iam_id),
            now=now,
        )
        return [
            delivery(
                decision.channel,
                mandatory=decision.mandatory,
                status="pending" if decision.reachable else "failed",
                next_attempt_at=decision.not_before or now,
                last_error=None if decision.reachable else "recipient_unreachable",
            )
            for decision in decisions
        ]

    def _plan_group(
        self, notification: Notification, group: ChannelGroup, now: datetime
    ) -> Delivery:
        configured = self._channels.get(group.channel) is not None
        return Delivery(
            id=uuid.uuid4(),
            tenant_id=notification.tenant_id,
            notification_id=notification.id,
            channel=group.channel,
            recipient_kind="group",
            recipient_id=group.id,
            iam_principal_id=None,
            mandatory=False,
            status="pending" if configured else "failed",
            attempts=0,
            next_attempt_at=now,
            last_error=None if configured else "channel_not_configured",
            created_at=now,
            updated_at=now,
        )


@dataclass
class RoutingData:
    """Everything channel selection needs for a batch of recipients, in four queries."""

    preferences: dict[uuid.UUID, list[PreferenceRule]]
    mandatory: dict[str, list[str]]
    quiet: dict[uuid.UUID, QuietWindow]
    addresses: set[tuple[uuid.UUID, str]]

    @classmethod
    async def load(
        cls, session: AsyncSession, tenant_id: uuid.UUID, principals: list[uuid.UUID]
    ) -> RoutingData:
        preferences: dict[uuid.UUID, list[PreferenceRule]] = defaultdict(list)
        mandatory: dict[str, list[str]] = defaultdict(list)
        quiet: dict[uuid.UUID, QuietWindow] = {}
        addresses: set[tuple[uuid.UUID, str]] = set()
        for rule in await session.scalars(
            select(MandatoryRule).where(MandatoryRule.tenant_id == tenant_id)
        ):
            mandatory[rule.channel].append(rule.type_pattern)
        if principals:
            for pref in await session.scalars(
                select(Preference).where(
                    Preference.tenant_id == tenant_id, Preference.principal_id.in_(principals)
                )
            ):
                preferences[pref.principal_id].append(
                    PreferenceRule(pref.type_pattern, pref.channel, pref.enabled)
                )
            for window in await session.scalars(
                select(QuietHours).where(
                    QuietHours.tenant_id == tenant_id, QuietHours.principal_id.in_(principals)
                )
            ):
                quiet[window.principal_id] = QuietWindow(
                    minute_to_time(window.start_minute),
                    minute_to_time(window.end_minute),
                    window.timezone,
                )
            for address in await session.scalars(
                select(ChannelAddress).where(
                    ChannelAddress.tenant_id == tenant_id,
                    ChannelAddress.principal_id.in_(principals),
                    ChannelAddress.disabled_at.is_(None),
                )
            ):
                addresses.add((address.principal_id, address.channel))
        return cls(preferences, mandatory, quiet, addresses)


async def load_deliveries(session: AsyncSession, notification_id: uuid.UUID) -> list[Delivery]:
    rows = await session.scalars(
        select(Delivery)
        .where(Delivery.notification_id == notification_id)
        .order_by(Delivery.created_at, Delivery.channel, Delivery.id)
    )
    return list(rows)
