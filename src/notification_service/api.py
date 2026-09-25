"""HTTP routes of ``/api/v1``."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Header, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service import inbox
from notification_service.auth import SCOPE_ADMIN, Admin, Reader, Sender, SenderOrAdmin
from notification_service.channels import ChannelRegistry
from notification_service.channels.web import InboxBroker
from notification_service.config import Settings
from notification_service.db import transaction, utcnow
from notification_service.errors import NotFound, Unprocessable
from notification_service.models import (
    ChannelAddress,
    ChannelGroup,
    ChannelGroupIntent,
    Delivery,
    MandatoryRule,
    Notification,
    Preference,
    QuietHours,
)
from notification_service.schemas import (
    ChannelAddressOut,
    ChannelGroupCreate,
    ChannelGroupIntentOut,
    ChannelGroupOut,
    DeliveryOut,
    InboxItemOut,
    InboxPageOut,
    MandatoryRuleIn,
    MandatoryRuleOut,
    NotificationCreate,
    PreferenceOut,
    PreferencesOut,
    PreferencesPatch,
    QuietHoursOut,
    ReadAllOut,
    Recipient,
    SentNotificationOut,
)
from notification_service.sending import NotificationSender, load_deliveries
from notification_service.telegram_bot import hash_code, new_group_code

router = APIRouter(prefix="/api/v1")
EMAIL = "email"


def _sessions(request: Request) -> async_sessionmaker[AsyncSession]:
    return request.app.state.sessions  # type: ignore[no-any-return]


def _channels(request: Request) -> ChannelRegistry:
    return request.app.state.channels  # type: ignore[no-any-return]


def _dump(model: object) -> dict[str, object]:
    return model.model_dump(mode="json", by_alias=True)  # type: ignore[attr-defined,no-any-return]


def sent_out(notification: Notification, deliveries: list[Delivery]) -> SentNotificationOut:
    return SentNotificationOut(
        id=notification.id,
        type=notification.type,
        title=notification.title,
        body=notification.body,
        links=list(notification.links),
        actions=list(notification.actions),
        actions_closed_at=notification.actions_closed_at,
        actions_outcome=notification.actions_outcome,
        recipient=Recipient(
            kind=notification.recipient_kind,  # type: ignore[arg-type]
            id=notification.recipient_id,
            workspace_id=notification.workspace_id,
        ),
        sender_id=notification.sender_id,
        created_at=notification.created_at,
        deliveries=[DeliveryOut.model_validate(d) for d in deliveries],
    )


def _require_channel(request: Request, channel: str) -> None:
    if _channels(request).get(channel) is None:
        raise Unprocessable(
            f"channel '{channel}' is not configured",
            code="unknown_channel",
            details={"channels": _channels(request).names()},
        )


# --- Sending -------------------------------------------------------------------


@router.post(
    "/notifications",
    response_model=SentNotificationOut,
    status_code=201,
    summary="Send a notification; a repeated Idempotency-Key returns the first one",
)
async def send_notification(
    payload: NotificationCreate,
    request: Request,
    ctx: Sender,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
) -> JSONResponse:
    sender: NotificationSender = request.app.state.sender
    accepted = await sender.accept(ctx, payload, idempotency_key)
    body = _dump(sent_out(accepted.notification, accepted.deliveries))
    return JSONResponse(body, status_code=201 if accepted.created else 200)


@router.get(
    "/notifications/{notification_id}",
    response_model=SentNotificationOut,
    summary="A sent notification with its delivery journal (its sender or an admin)",
)
async def get_notification(
    notification_id: uuid.UUID, request: Request, ctx: SenderOrAdmin
) -> SentNotificationOut:
    async with _sessions(request)() as session:
        notification = await session.get(Notification, notification_id)
        visible = notification is not None and notification.tenant_id == ctx.tenant_id
        if visible and notification.sender_id != ctx.principal_id:  # type: ignore[union-attr]
            visible = ctx.has_scope(SCOPE_ADMIN)
        if not visible:
            raise NotFound("notification not found")
        assert notification is not None
        return sent_out(notification, await load_deliveries(session, notification.id))


# --- Inbox ---------------------------------------------------------------------


@router.get("/me/notifications", response_model=InboxPageOut, summary="My inbox, newest first")
async def my_notifications(
    request: Request,
    ctx: Reader,
    unread_only: Annotated[bool, Query(alias="unreadOnly")] = False,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[str | None, Query()] = None,
) -> InboxPageOut:
    before = None
    if cursor is not None:
        if not cursor.isdigit():
            raise Unprocessable("cursor is invalid", code="invalid_cursor")
        before = int(cursor)
    async with _sessions(request)() as session:
        page = await inbox.list_inbox(
            session,
            ctx.tenant_id,
            ctx.principal_id,
            unread_only=unread_only,
            limit=limit,
            before_seq=before,
        )
    return InboxPageOut(
        items=page.items, unread_count=page.unread_count, next_cursor=page.next_cursor
    )


@router.get(
    "/me/notifications/stream",
    summary="Server-sent events of my inbox; resumes after Last-Event-ID",
    response_class=StreamingResponse,
)
async def my_notification_stream(
    request: Request,
    ctx: Reader,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    last_event_id_query: Annotated[str | None, Query(alias="lastEventId")] = None,
) -> StreamingResponse:
    # A browser's EventSource sends the header on reconnect by itself; the query
    # parameter lets a client resume a stream it opens anew.
    raw = last_event_id if last_event_id is not None else last_event_id_query
    after: int | None = None
    if raw is not None and raw.strip():
        if not raw.strip().isdigit():
            raise Unprocessable("Last-Event-ID is invalid", code="invalid_last_event_id")
        after = int(raw.strip())
    if after is None:
        # A fresh stream starts at the current end of the inbox, fixed here,
        # before the response starts: whatever lands later is streamed.
        async with _sessions(request)() as session:
            after = await inbox.last_seq(session, ctx.tenant_id, ctx.principal_id)
    settings: Settings = request.app.state.settings
    broker: InboxBroker = request.app.state.broker
    events = inbox.stream(
        _sessions(request),
        broker,
        (ctx.tenant_id, ctx.principal_id),
        after_seq=after,
        poll_seconds=settings.inbox_poll_seconds,
        keepalive_seconds=settings.inbox_keepalive_seconds,
        until=ctx.expires_at,
    )
    return StreamingResponse(
        events,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post(
    "/me/notifications/{item_id}:read", response_model=InboxItemOut, summary="Mark one as read"
)
async def mark_read(item_id: uuid.UUID, request: Request, ctx: Reader) -> InboxItemOut:
    async with transaction(_sessions(request)) as session:
        item = await inbox.mark_read(session, ctx.tenant_id, ctx.principal_id, item_id)
    if item is None:
        raise NotFound("inbox item not found")
    return item


@router.post("/me/notifications:read-all", response_model=ReadAllOut, summary="Mark all as read")
async def mark_all_read(request: Request, ctx: Reader) -> ReadAllOut:
    async with transaction(_sessions(request)) as session:
        marked = await inbox.mark_all_read(session, ctx.tenant_id, ctx.principal_id)
    return ReadAllOut(marked=marked)


# --- Preferences ---------------------------------------------------------------


async def _preferences_out(
    session: AsyncSession, request: Request, tenant_id: uuid.UUID, principal_id: uuid.UUID
) -> PreferencesOut:
    prefs = await session.scalars(
        select(Preference)
        .where(Preference.tenant_id == tenant_id, Preference.principal_id == principal_id)
        .order_by(Preference.type_pattern, Preference.channel)
    )
    quiet = await session.get(QuietHours, (tenant_id, principal_id))
    addresses = await session.scalars(
        select(ChannelAddress)
        .where(ChannelAddress.tenant_id == tenant_id, ChannelAddress.principal_id == principal_id)
        .order_by(ChannelAddress.channel)
    )
    return PreferencesOut(
        channels=_channels(request).names(),
        preferences=[
            PreferenceOut(type=p.type_pattern, channel=p.channel, enabled=p.enabled) for p in prefs
        ],
        quiet_hours=(
            QuietHoursOut(
                start=f"{quiet.start_minute // 60:02d}:{quiet.start_minute % 60:02d}",
                end=f"{quiet.end_minute // 60:02d}:{quiet.end_minute % 60:02d}",
                timezone=quiet.timezone,
            )
            if quiet is not None
            else None
        ),
        addresses=[ChannelAddressOut.model_validate(a) for a in addresses],
        mandatory=await _rules(session, tenant_id),
    )


async def _rules(session: AsyncSession, tenant_id: uuid.UUID) -> list[MandatoryRuleOut]:
    rules = await session.scalars(
        select(MandatoryRule)
        .where(MandatoryRule.tenant_id == tenant_id)
        .order_by(MandatoryRule.type_pattern, MandatoryRule.channel)
    )
    return [_rule_out(rule) for rule in rules]


def _rule_out(rule: MandatoryRule) -> MandatoryRuleOut:
    return MandatoryRuleOut(
        id=rule.id,
        type=rule.type_pattern,
        channel=rule.channel,
        created_by=rule.created_by,
        created_at=rule.created_at,
    )


@router.get(
    "/me/notification-preferences",
    response_model=PreferencesOut,
    summary="My channel preferences, quiet hours, addresses and the rules I cannot opt out of",
)
async def get_preferences(request: Request, ctx: Reader) -> PreferencesOut:
    async with _sessions(request)() as session:
        return await _preferences_out(session, request, ctx.tenant_id, ctx.principal_id)


@router.patch(
    "/me/notification-preferences",
    response_model=PreferencesOut,
    summary="Change preferences; absent fields stay as they are",
)
async def patch_preferences(
    payload: PreferencesPatch, request: Request, ctx: Reader
) -> PreferencesOut:
    tenant_id, principal_id = ctx.tenant_id, ctx.principal_id
    for pref in payload.preferences:
        _require_channel(request, pref.channel)
    now = utcnow()
    async with transaction(_sessions(request)) as session:
        for pref in payload.preferences:
            match = (
                Preference.tenant_id == tenant_id,
                Preference.principal_id == principal_id,
                Preference.type_pattern == pref.type,
                Preference.channel == pref.channel,
            )
            if pref.enabled is None:
                await session.execute(delete(Preference).where(*match))
                continue
            await session.execute(
                insert(Preference)
                .values(
                    id=uuid.uuid4(),
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    type_pattern=pref.type,
                    channel=pref.channel,
                    enabled=pref.enabled,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=["tenant_id", "principal_id", "type_pattern", "channel"],
                    set_={"enabled": pref.enabled, "updated_at": now},
                )
            )
        if "quiet_hours" in payload.model_fields_set:
            quiet = await session.get(QuietHours, (tenant_id, principal_id))
            if payload.quiet_hours is None:
                if quiet is not None:
                    await session.delete(quiet)
            else:
                window = payload.quiet_hours
                values = {
                    "start_minute": window.start.hour * 60 + window.start.minute,
                    "end_minute": window.end.hour * 60 + window.end.minute,
                    "timezone": window.timezone,
                    "updated_at": now,
                }
                if quiet is None:
                    session.add(
                        QuietHours(tenant_id=tenant_id, principal_id=principal_id, **values)
                    )
                else:
                    for name, value in values.items():
                        setattr(quiet, name, value)
        if "email" in payload.model_fields_set:
            address = await session.get(ChannelAddress, (tenant_id, principal_id, EMAIL))
            if payload.email is None:
                if address is not None:
                    await session.delete(address)
            elif address is None:
                session.add(
                    ChannelAddress(
                        tenant_id=tenant_id,
                        principal_id=principal_id,
                        channel=EMAIL,
                        address=str(payload.email),
                        updated_at=now,
                    )
                )
            else:
                # Setting the address again is how a bounced one is re-enabled.
                address.address = str(payload.email)
                address.disabled_at = None
                address.disabled_reason = None
                address.updated_at = now
        await session.flush()
        return await _preferences_out(session, request, tenant_id, principal_id)


# --- Mandatory rules -----------------------------------------------------------


@router.get(
    "/mandatory-rules",
    response_model=list[MandatoryRuleOut],
    summary="The organization's mandatory delivery rules",
)
async def list_rules(request: Request, ctx: Admin) -> list[MandatoryRuleOut]:
    async with _sessions(request)() as session:
        return await _rules(session, ctx.tenant_id)


@router.post(
    "/mandatory-rules",
    response_model=MandatoryRuleOut,
    status_code=201,
    summary="Make matching notifications mandatory on a channel (a repeat returns the rule)",
)
async def create_rule(payload: MandatoryRuleIn, request: Request, ctx: Admin) -> JSONResponse:
    _require_channel(request, payload.channel)
    async with transaction(_sessions(request)) as session:
        created = await session.scalar(
            insert(MandatoryRule)
            .values(
                id=uuid.uuid4(),
                tenant_id=ctx.tenant_id,
                type_pattern=payload.type,
                channel=payload.channel,
                created_by=ctx.principal_id,
                created_at=utcnow(),
            )
            .on_conflict_do_nothing(index_elements=["tenant_id", "type_pattern", "channel"])
            .returning(MandatoryRule.id)
        )
        rule = await session.scalar(
            select(MandatoryRule).where(
                MandatoryRule.tenant_id == ctx.tenant_id,
                MandatoryRule.type_pattern == payload.type,
                MandatoryRule.channel == payload.channel,
            )
        )
        assert rule is not None
        return JSONResponse(_dump(_rule_out(rule)), status_code=201 if created else 200)


@router.delete("/mandatory-rules/{rule_id}", status_code=204, summary="Drop a mandatory rule")
async def delete_rule(rule_id: uuid.UUID, request: Request, ctx: Admin) -> Response:
    async with transaction(_sessions(request)) as session:
        result = await session.execute(
            delete(MandatoryRule).where(
                MandatoryRule.id == rule_id, MandatoryRule.tenant_id == ctx.tenant_id
            )
        )
    if not result.rowcount:  # type: ignore[attr-defined]
        raise NotFound("mandatory rule not found")
    return Response(status_code=204)


# --- Channel groups ------------------------------------------------------------


@router.post(
    "/workspaces/{workspace_id}/channel-groups",
    response_model=ChannelGroupIntentOut,
    status_code=201,
    summary="Get a one-time code that binds a group chat to the workspace (and role)",
)
async def create_channel_group(
    workspace_id: uuid.UUID, payload: ChannelGroupCreate, request: Request, ctx: Admin
) -> ChannelGroupIntentOut:
    _require_channel(request, payload.channel)
    settings: Settings = request.app.state.settings
    code = new_group_code()
    now = utcnow()
    intent = ChannelGroupIntent(
        id=uuid.uuid4(),
        tenant_id=ctx.tenant_id,
        channel=payload.channel,
        code_hash=hash_code(code),
        workspace_id=workspace_id,
        role_id=payload.role_id,
        created_by=ctx.principal_id,
        created_at=now,
        expires_at=now + timedelta(seconds=settings.channel_group_code_ttl_seconds),
    )
    async with transaction(_sessions(request)) as session:
        session.add(intent)
    bot = settings.telegram_bot_username.lstrip("@")
    return ChannelGroupIntentOut(
        id=intent.id,
        channel=intent.channel,
        workspace_id=workspace_id,
        role_id=intent.role_id,
        code=code,
        # In a group a command reaches a bot in privacy mode only when it is
        # addressed to it.
        command=f"/start@{bot} {code}" if bot else f"/start {code}",
        deep_link=f"https://t.me/{bot}?startgroup={code}" if bot else None,
        expires_at=intent.expires_at,
    )


@router.get(
    "/workspaces/{workspace_id}/channel-groups",
    response_model=list[ChannelGroupOut],
    summary="Group chats bound to the workspace, including disabled ones",
)
async def list_channel_groups(
    workspace_id: uuid.UUID, request: Request, ctx: Admin
) -> list[ChannelGroupOut]:
    async with _sessions(request)() as session:
        rows = await session.scalars(
            select(ChannelGroup)
            .where(
                ChannelGroup.tenant_id == ctx.tenant_id, ChannelGroup.workspace_id == workspace_id
            )
            .order_by(ChannelGroup.created_at)
        )
        return [ChannelGroupOut.model_validate(row) for row in rows]


@router.delete(
    "/workspaces/{workspace_id}/channel-groups/{group_id}",
    status_code=204,
    summary="Unbind a group chat: nothing more is delivered there",
)
async def delete_channel_group(
    workspace_id: uuid.UUID, group_id: uuid.UUID, request: Request, ctx: Admin
) -> Response:
    async with transaction(_sessions(request)) as session:
        group = await session.scalar(
            select(ChannelGroup).where(
                ChannelGroup.id == group_id,
                ChannelGroup.tenant_id == ctx.tenant_id,
                ChannelGroup.workspace_id == workspace_id,
            )
        )
        if group is None:
            raise NotFound("channel group not found")
        if group.disabled_at is None:
            group.disabled_at = utcnow()
            group.disabled_reason = "unlinked_by_admin"
    return Response(status_code=204)
