"""The Telegram bot's webhook: linking, groups, and decisions taken with buttons.

Updates of the Bot API arrive at ``POST /channels/telegram/webhook``; the only
proof that they come from Telegram is the secret the webhook was registered
with (``X-Telegram-Bot-Api-Secret-Token``). What an update can do:

- ``/start <code>`` in the private chat with the bot — the code the person got
  in the web (IAM ``channel-link-intents``) is confirmed at IAM, and the chat
  becomes the person's ``telegram`` address;
- ``/start <code>`` in a group — the code an administrator got from
  ``POST /workspaces/{id}/channel-groups`` binds the group to the workspace
  (and role);
- ``/unlink`` in the private chat — the address is disabled: no messages, and
  its buttons are refused;
- a press of a decision button — the link and the open approval are checked,
  the assertion is exchanged at IAM for the person's one-decision token, and
  the approval is decided in the core with ``Idempotency-Key`` = callback id;
  every message of the notification is then updated with the outcome;
- the bot blocked by the person or removed from a group — the address or the
  group is disabled until it is linked again.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import uuid
from dataclasses import dataclass
from typing import Any

from control_plane_client import (
    AuthenticationError,
    ConflictError,
    ControlPlaneError,
    NotFoundError,
    PermissionDeniedError,
)
from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service.channels.telegram import (
    DECIDE,
    TELEGRAM,
    BotApi,
    TelegramApiError,
    TelegramMessages,
    TelegramUnavailable,
    outcome_line,
    parse_callback_data,
)
from notification_service.config import Settings
from notification_service.db import transaction, utcnow
from notification_service.decisions import (
    Approvals,
    ChannelLinks,
    LinkRefused,
    LinksUnavailable,
    purpose_ref,
)
from notification_service.errors import envelope
from notification_service.models import (
    ChannelAddress,
    ChannelCallback,
    ChannelGroup,
    ChannelGroupIntent,
    Delivery,
    Notification,
)
from notification_service.sending import NotificationSender

logger = logging.getLogger("notification_service.telegram")

SECRET_HEADER = "X-Telegram-Bot-Api-Secret-Token"
PRIVATE = "private"
GROUPS = ("group", "supergroup")
# Why an address or a group stopped: set here, shown in the preferences.
UNLINKED = "unlinked_by_user"
BOT_BLOCKED = "bot_blocked"
BOT_REMOVED = "bot_removed"
LINK_REVOKED = "iam_link_revoked"
RELINKED = "linked_to_another_account"

HELP = (
    "Я присылаю уведомления платформы. Чтобы привязать Telegram, получите код "
    "в веб-интерфейсе и отправьте его сюда: /start <код>. Отвязать — /unlink."
)
LINK_REFUSALS = {
    "invalid_link_code": "Код не подходит: он неверный, уже использован или истёк.",
    "channel_already_linked": "К вашей учётной записи уже привязан другой Telegram.",
    "channel_account_linked": "Этот Telegram уже привязан к другой учётной записи.",
    "channel_provider_disabled": "Вход через Telegram выключен организацией.",
    "principal_not_active": "Учётная запись неактивна.",
    "human_principal_required": "Привязать Telegram может только человек.",
    "rate_limited": "Слишком много попыток. Попробуйте позже.",
}
UNAVAILABLE = "Сервис сейчас недоступен. Попробуйте позже."


def hash_code(code: str) -> str:
    return hashlib.sha256(code.encode()).hexdigest()


def new_group_code() -> str:
    # URL-safe: it also works as the ``startgroup`` parameter of a deep link.
    return secrets.token_urlsafe(18)


def parse_command(text: str) -> tuple[str, str] | None:
    """``/start@bot code`` -> ``("start", "code")``; not a command -> ``None``."""
    if not text.startswith("/"):
        return None
    head, _, rest = text.strip().partition(" ")
    command = head[1:].split("@", 1)[0].lower()
    return (command, rest.strip()) if command else None


@dataclass(frozen=True)
class Verdict:
    """What a press came to: a result code, what the person is told, what closed."""

    result: str
    answer: str
    # ``False`` for outcomes worth trying again (IAM or the core unavailable):
    # a redelivered callback then decides anew instead of replaying the refusal.
    final: bool = True
    alert: bool = True
    outcome: dict[str, Any] | None = None


class TelegramWebhook:
    def __init__(
        self,
        *,
        settings: Settings,
        sessions: async_sessionmaker[AsyncSession],
        bot: BotApi,
        messages: TelegramMessages,
        sender: NotificationSender,
        links: ChannelLinks,
        approvals: Approvals,
    ) -> None:
        self._secret = settings.telegram_webhook_secret.get_secret_value()
        self._sessions = sessions
        self._bot = bot
        self._messages = messages
        self._sender = sender
        self._links = links
        self._approvals = approvals

    def authentic(self, presented: str | None) -> bool:
        if not self._secret or presented is None:
            return False
        return hmac.compare_digest(presented.encode(), self._secret.encode())

    async def handle(self, update: dict[str, Any]) -> None:
        if isinstance(update.get("callback_query"), dict):
            await self._press(update["callback_query"])
        elif isinstance(update.get("my_chat_member"), dict):
            await self._membership(update["my_chat_member"])
        elif isinstance(update.get("message"), dict):
            await self._message(update["message"])
        # Other updates (edits, channel posts, ...) mean nothing to the service.

    # -- messages and commands ---------------------------------------------------

    async def _message(self, message: dict[str, Any]) -> None:
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        if message.get("migrate_to_chat_id") is not None:
            await self._migrated(chat_id, str(message["migrate_to_chat_id"]))
            return
        parsed = parse_command(str(message.get("text") or ""))
        user = message.get("from") or {}
        if parsed is None or not chat_id or user.get("is_bot"):
            return
        command, argument = parsed
        if chat.get("type") == PRIVATE:
            if command == "start" and argument:
                await self._reply(chat_id, await self._link_person(chat_id, argument))
            elif command == "start":
                await self._reply(chat_id, await self._resume(chat_id))
            elif command == "unlink":
                await self._reply(chat_id, await self._unlink(chat_id))
            elif command == "help":
                await self._reply(chat_id, HELP)
        elif chat.get("type") in GROUPS and command == "start" and argument:
            await self._reply(
                chat_id, await self._bind_group(chat_id, str(chat.get("title") or ""), argument)
            )

    async def _link_person(self, chat_id: str, code: str) -> str:
        # In a private chat the chat id is the person's Telegram user id: the
        # account IAM links is the one that sent the code.
        if len(code) > 200:
            return LINK_REFUSALS["invalid_link_code"]
        try:
            linked = await self._links.confirm(TELEGRAM, code, chat_id)
        except LinkRefused as exc:
            logger.info("telegram link refused by IAM: %s", exc.code)
            return LINK_REFUSALS.get(exc.code, "Привязка не удалась.")
        except LinksUnavailable as exc:
            logger.warning("telegram link: IAM unavailable (%s)", exc)
            return UNAVAILABLE
        now = utcnow()
        async with transaction(self._sessions) as session:
            # The account now belongs to this person only: an older address
            # of someone else with the same chat stops.
            await session.execute(
                update(ChannelAddress)
                .where(
                    ChannelAddress.tenant_id == linked.tenant_id,
                    ChannelAddress.channel == TELEGRAM,
                    ChannelAddress.address == chat_id,
                    ChannelAddress.principal_id != linked.iam_principal_id,
                    ChannelAddress.disabled_at.is_(None),
                )
                .values(disabled_at=now, disabled_reason=RELINKED, updated_at=now)
            )
            await session.execute(
                insert(ChannelAddress)
                .values(
                    tenant_id=linked.tenant_id,
                    principal_id=linked.iam_principal_id,
                    channel=TELEGRAM,
                    address=chat_id,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=["tenant_id", "principal_id", "channel"],
                    set_={
                        "address": chat_id,
                        "disabled_at": None,
                        "disabled_reason": None,
                        "updated_at": now,
                    },
                )
            )
        return "Готово: Telegram привязан. Уведомления будут приходить сюда. Отвязать — /unlink."

    async def _resume(self, chat_id: str) -> str:
        """``/start`` without a code: back after unblocking the bot, else help.

        Only an address stopped by a block comes back this way; one the person
        unlinked needs a new link — unlinking is a security move.
        """
        now = utcnow()
        async with transaction(self._sessions) as session:
            resumed = await session.execute(
                update(ChannelAddress)
                .where(
                    ChannelAddress.channel == TELEGRAM,
                    ChannelAddress.address == chat_id,
                    # Stopped by ``my_chat_member`` or by a refused send (403).
                    or_(
                        ChannelAddress.disabled_reason == BOT_BLOCKED,
                        ChannelAddress.disabled_reason.like("telegram_403%"),
                    ),
                )
                .values(disabled_at=None, disabled_reason=None, updated_at=now)
            )
        if resumed.rowcount:  # type: ignore[attr-defined]
            return "С возвращением: уведомления снова будут приходить сюда."
        return HELP

    async def _unlink(self, chat_id: str) -> str:
        now = utcnow()
        async with transaction(self._sessions) as session:
            result = await session.execute(
                update(ChannelAddress)
                .where(
                    ChannelAddress.channel == TELEGRAM,
                    ChannelAddress.address == chat_id,
                    ChannelAddress.disabled_at.is_(None),
                )
                .values(disabled_at=now, disabled_reason=UNLINKED, updated_at=now)
            )
        if not result.rowcount:  # type: ignore[attr-defined]
            return "Этот Telegram не привязан."
        return (
            "Telegram отвязан: уведомления сюда больше не приходят, кнопки не действуют. "
            "Отозвать привязку в учётной записи и привязать заново — в веб-интерфейсе."
        )

    async def _bind_group(self, chat_id: str, title: str, code: str) -> str:
        now = utcnow()
        async with transaction(self._sessions) as session:
            intent = await session.scalar(
                select(ChannelGroupIntent)
                .where(ChannelGroupIntent.code_hash == hash_code(code))
                .with_for_update()
            )
            if (
                intent is None
                or intent.channel != TELEGRAM
                or intent.used_at is not None
                or intent.expires_at <= now
            ):
                return "Код не подходит: он неверный, уже использован или истёк."
            group = await session.scalar(
                select(ChannelGroup).where(
                    ChannelGroup.tenant_id == intent.tenant_id,
                    ChannelGroup.channel == TELEGRAM,
                    ChannelGroup.external_chat_id == chat_id,
                    ChannelGroup.disabled_at.is_(None),
                )
            )
            if group is None:
                group = ChannelGroup(
                    id=uuid.uuid4(),
                    tenant_id=intent.tenant_id,
                    channel=TELEGRAM,
                    external_chat_id=chat_id,
                    created_at=now,
                )
                session.add(group)
            # A chat bound again moves to the new workspace and role.
            group.title = title[:200]
            group.workspace_id = intent.workspace_id
            group.role_id = intent.role_id
            group.linked_by = intent.created_by
            await session.flush()
            intent.used_at = now
            intent.group_id = group.id
        return "Группа привязана: сюда будут приходить уведомления для команды."

    async def _migrated(self, old_chat_id: str, new_chat_id: str) -> None:
        async with transaction(self._sessions) as session:
            await session.execute(
                update(ChannelGroup)
                .where(
                    ChannelGroup.channel == TELEGRAM,
                    ChannelGroup.external_chat_id == old_chat_id,
                    ChannelGroup.disabled_at.is_(None),
                )
                .values(external_chat_id=new_chat_id)
            )

    async def _membership(self, change: dict[str, Any]) -> None:
        """The bot's own membership: blocked in a private chat, removed from a group."""
        chat = change.get("chat") or {}
        status = (change.get("new_chat_member") or {}).get("status")
        if status not in ("kicked", "left"):
            return
        chat_id = str(chat.get("id", ""))
        now = utcnow()
        async with transaction(self._sessions) as session:
            if chat.get("type") == PRIVATE:
                await session.execute(
                    update(ChannelAddress)
                    .where(
                        ChannelAddress.channel == TELEGRAM,
                        ChannelAddress.address == chat_id,
                        ChannelAddress.disabled_at.is_(None),
                    )
                    .values(disabled_at=now, disabled_reason=BOT_BLOCKED, updated_at=now)
                )
            elif chat.get("type") in GROUPS:
                await session.execute(
                    update(ChannelGroup)
                    .where(
                        ChannelGroup.channel == TELEGRAM,
                        ChannelGroup.external_chat_id == chat_id,
                        ChannelGroup.disabled_at.is_(None),
                    )
                    .values(disabled_at=now, disabled_reason=BOT_REMOVED)
                )

    async def _reply(self, chat_id: str, text: str) -> None:
        try:
            await self._bot.call("sendMessage", chat_id=chat_id, text=text)
        except (TelegramApiError, TelegramUnavailable) as exc:
            logger.warning("telegram reply to %s not sent: %s", chat_id, exc)

    # -- button presses ----------------------------------------------------------

    async def _press(self, query: dict[str, Any]) -> None:
        callback_id = str(query.get("id") or "")
        user_id = str((query.get("from") or {}).get("id") or "")
        if not callback_id or not user_id:
            return
        target = await self._target(query)
        if target is None:
            await self._answer(callback_id, "Кнопка больше не действует.", alert=True)
            return
        notification, action = target

        async with transaction(self._sessions) as session:
            recorded = await self._record(session, callback_id, notification, action, user_id)
        if recorded is not None and recorded.result is not None:
            # A redelivered update: the same press, the same answer.
            await self._answer(callback_id, recorded.answer or "", alert=False)
            return

        verdict = await self._decide(callback_id, user_id, notification, action)
        async with transaction(self._sessions) as session:
            await session.execute(
                update(ChannelCallback)
                .where(
                    ChannelCallback.channel == TELEGRAM,
                    ChannelCallback.callback_id == callback_id,
                )
                .values(
                    result=verdict.result if verdict.final else None,
                    answer=verdict.answer[:200],
                    completed_at=utcnow() if verdict.final else None,
                )
            )
        await self._answer(callback_id, verdict.answer, alert=verdict.alert)
        if verdict.outcome is not None:
            # Close the actions here as well as from the core's event: every
            # recipient's message shows the outcome without waiting for it.
            await self._sender.close_actions(
                notification.tenant_id,
                notification.sender_id,
                notification.dedup_key,
                verdict.outcome,
            )

    async def _target(self, query: dict[str, Any]) -> tuple[Notification, dict[str, Any]] | None:
        """The notification and action a press is for — only on a message this bot sent."""
        parsed = parse_callback_data(str(query.get("data") or ""))
        message = query.get("message") or {}
        chat_id = (message.get("chat") or {}).get("id")
        if parsed is None or chat_id is None or message.get("message_id") is None:
            return None
        notification_id, index = parsed
        async with self._sessions() as session:
            notification = await session.get(Notification, notification_id)
            if notification is None or index >= len(notification.actions):
                return None
            sent_here = await session.scalar(
                select(Delivery.id).where(
                    Delivery.notification_id == notification_id,
                    Delivery.channel == TELEGRAM,
                    Delivery.external_id == f"{chat_id}:{message['message_id']}",
                )
            )
        action = notification.actions[index]
        data = action.get("data") or {}
        if (
            sent_here is None
            or data.get("kind") != DECIDE
            or data.get("decision") not in ("approve", "reject")
        ):
            return None
        try:
            uuid.UUID(str(data.get("approvalId")))
        except ValueError:
            return None
        return notification, action

    async def _record(
        self,
        session: AsyncSession,
        callback_id: str,
        notification: Notification,
        action: dict[str, Any],
        user_id: str,
    ) -> ChannelCallback | None:
        """Register the press; an already registered one is returned instead."""
        inserted = await session.scalar(
            insert(ChannelCallback)
            .values(
                channel=TELEGRAM,
                callback_id=callback_id,
                tenant_id=notification.tenant_id,
                notification_id=notification.id,
                action_id=str(action.get("id") or "")[:64],
                external_subject=user_id,
                created_at=utcnow(),
            )
            .on_conflict_do_nothing(index_elements=["channel", "callback_id"])
            .returning(ChannelCallback.callback_id)
        )
        if inserted is not None:
            return None
        return await session.get(ChannelCallback, (TELEGRAM, callback_id))

    async def _person(self, tenant_id: uuid.UUID, user_id: str) -> uuid.UUID | None:
        async with self._sessions() as session:
            return await session.scalar(
                select(ChannelAddress.principal_id).where(
                    ChannelAddress.tenant_id == tenant_id,
                    ChannelAddress.channel == TELEGRAM,
                    ChannelAddress.address == user_id,
                    ChannelAddress.disabled_at.is_(None),
                )
            )

    async def _decide(
        self,
        callback_id: str,
        user_id: str,
        notification: Notification,
        action: dict[str, Any],
    ) -> Verdict:
        data = action["data"]
        approval_id = uuid.UUID(str(data["approvalId"]))
        approve = data["decision"] == "approve"

        # The tenant is the one the message came from: a person linked in
        # several tenants decides where they were asked.
        person = await self._person(notification.tenant_id, user_id)
        if person is None:
            return Verdict(
                "not_linked",
                "Ваш Telegram не привязан к учётной записи — решение не принято. "
                "Привяжите его кодом из веб-интерфейса.",
            )
        if notification.actions_closed_at is not None:
            return await self._already(notification.actions_outcome)
        approval = await self._approvals.get(approval_id)
        if approval is not None and approval.get("status") != "pending":
            return await self._already(self._outcome_of(approval), close=True)

        try:
            token = await self._links.exchange(
                notification.tenant_id, TELEGRAM, user_id, purpose_ref(approval_id)
            )
        except LinkRefused as exc:
            return await self._link_refused(exc, notification.tenant_id, user_id)
        except LinksUnavailable as exc:
            logger.warning("decision %s: IAM unavailable (%s)", approval_id, exc)
            return Verdict("unavailable", UNAVAILABLE, final=False)

        try:
            decided = await self._approvals.decide(
                approval_id, approve=approve, token=token, idempotency_key=callback_id
            )
        except ConflictError as exc:
            if exc.code != "approval_already_decided":
                logger.warning("decision %s: conflict %s", approval_id, exc.code)
                return Verdict("conflict", UNAVAILABLE, final=False)
            current = await self._approvals.get(approval_id)
            outcome = (
                self._outcome_of(current) if current else {"status": exc.details.get("status")}
            )
            return await self._already(outcome, close=True)
        except PermissionDeniedError as exc:
            logger.info("decision %s refused by the core: %s", approval_id, exc.code)
            return Verdict(
                "not_eligible", "У вас нет права принять это решение — решение не записано."
            )
        except NotFoundError:
            return Verdict("not_found", "Решение не найдено — кнопка больше не действует.")
        except (AuthenticationError, ControlPlaneError) as exc:
            logger.warning("decision %s: the core did not decide (%s)", approval_id, exc.code)
            return Verdict("unavailable", UNAVAILABLE, final=False)

        outcome = self._outcome_of(decided, channel=TELEGRAM)
        return Verdict(
            "decided",
            "Решение принято: " + ("одобрено." if approve else "отклонено."),
            alert=False,
            outcome=outcome,
        )

    async def _link_refused(self, exc: LinkRefused, tenant_id: uuid.UUID, user_id: str) -> Verdict:
        logger.info("decision: IAM refused the assertion (%s)", exc.code)
        if exc.code == "channel_account_not_linked":
            # Revoked in the web: the address stops here too.
            now = utcnow()
            async with transaction(self._sessions) as session:
                await session.execute(
                    update(ChannelAddress)
                    .where(
                        ChannelAddress.tenant_id == tenant_id,
                        ChannelAddress.channel == TELEGRAM,
                        ChannelAddress.address == user_id,
                        ChannelAddress.disabled_at.is_(None),
                    )
                    .values(disabled_at=now, disabled_reason=LINK_REVOKED, updated_at=now)
                )
            return Verdict("not_linked", "Привязка Telegram отозвана — решение не принято.")
        if exc.code == "channel_provider_disabled":
            return Verdict("channel_disabled", "Решения из Telegram выключены организацией.")
        if exc.code == "rate_limited":
            return Verdict("rate_limited", LINK_REFUSALS["rate_limited"], final=False)
        return Verdict("refused", "Решение из Telegram не разрешено для вашей учётной записи.")

    async def _already(self, outcome: dict[str, Any] | None, *, close: bool = False) -> Verdict:
        by = (outcome or {}).get("by")
        name = await self._messages.name_of(str(by)) if by else None
        return Verdict(
            "already_decided",
            "Уже решено: " + outcome_line(outcome, name) + ".",
            outcome=outcome if close else None,
        )

    @staticmethod
    def _outcome_of(approval: dict[str, Any], *, channel: str | None = None) -> dict[str, Any]:
        outcome = {
            "status": approval.get("status"),
            "by": approval.get("decisionByPrincipalId"),
            "channel": channel,
            "at": approval.get("decisionAt") or approval.get("updatedAt"),
        }
        return {key: value for key, value in outcome.items() if value is not None}

    async def _answer(self, callback_id: str, text: str, *, alert: bool) -> None:
        try:
            await self._bot.answer_callback_query(callback_id, text, show_alert=alert)
        except (TelegramApiError, TelegramUnavailable) as exc:
            # A late answer (the query expired) changes nothing for the decision.
            logger.info("callback %s not answered: %s", callback_id, exc)


# --- Route -----------------------------------------------------------------------

router = APIRouter()


# authz: public — вебхук Telegram, проверка X-Telegram-Bot-Api-Secret-Token
@router.post("/channels/telegram/webhook", summary="Telegram Bot API webhook", tags=["telegram"])
async def telegram_webhook(
    request: Request,
    secret: str | None = Header(default=None, alias=SECRET_HEADER),
) -> JSONResponse:
    webhook: TelegramWebhook | None = request.app.state.telegram
    if webhook is None:
        return JSONResponse(
            envelope("not_found", "telegram channel is not configured"), status_code=404
        )
    if not webhook.authentic(secret):
        return JSONResponse(envelope("invalid_token", "invalid_token"), status_code=401)
    try:
        update_body = await request.json()
    except ValueError:
        return JSONResponse(envelope("bad_request", "update is not JSON"), status_code=400)
    if not isinstance(update_body, dict):
        return JSONResponse(envelope("bad_request", "update is not an object"), status_code=400)
    try:
        await webhook.handle(update_body)
    except Exception:
        # Answering 200 anyway: Telegram would redeliver the same update over
        # and over; a person whose press failed simply presses again.
        logger.exception("telegram update %s failed", update_body.get("update_id"))
    return JSONResponse({"ok": True})
