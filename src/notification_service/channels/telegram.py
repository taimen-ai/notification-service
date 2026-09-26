"""The ``telegram`` channel: messages from the bot, with decision buttons.

A person is reached in the private chat with the bot (the chat id, stored as
their ``telegram`` address when they link their account), a group in its chat.
Actions of kind ``approval.decide`` become inline buttons; the press comes back
to the webhook (``notification_service.telegram``). When the actions of a
notification close, every message already sent for it is edited to show the
outcome and loses its buttons.

Bot API errors map onto the delivery contract: a blocked bot, a chat that is
gone or a bot removed from a group make the address unreachable (the worker
disables it), rate limits and outages are retried, a migrated group moves to
its new chat id and is retried there.
"""

from __future__ import annotations

import html
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notification_service.channels.base import (
    OutboundMessage,
    PermanentDeliveryError,
    RecipientUnreachable,
    SendResult,
    TransientDeliveryError,
)
from notification_service.db import transaction

# ``data.kind`` of the actions this channel renders as buttons: the decision
# actions of the core's approvals and the confirmations of a person's harness
# (TAI-ADR-0051 §7).
from notification_service.events import DECIDE
from notification_service.models import ChannelGroup, Delivery, Notification

logger = logging.getLogger("notification_service.telegram")

TELEGRAM = "telegram"
HARNESS_APPROVAL = "harness_approval"
BUTTON_KINDS = (DECIDE, HARNESS_APPROVAL)
# Bot API limit on the text of a message (after entity parsing).
MAX_TEXT = 4096
CALLBACK_PREFIX = "a"

OUTCOME_TEXT = {
    "approved": "✅ Одобрено",
    "rejected": "❌ Отклонено",
    "cancelled": "Отменено",
}
CHANNEL_TEXT = {TELEGRAM: "Telegram"}


class TelegramApiError(Exception):
    """The Bot API answered ``ok: false`` (its documented error object)."""

    def __init__(
        self,
        status: int,
        description: str,
        *,
        retry_after: int | None = None,
        migrate_to_chat_id: int | None = None,
    ) -> None:
        super().__init__(f"{status}: {description}")
        self.status = status
        self.description = description
        self.retry_after = retry_after
        self.migrate_to_chat_id = migrate_to_chat_id


class TelegramUnavailable(Exception):
    """The Bot API could not be reached; the request may be repeated."""


class BotApi:
    """A minimal Bot API client: ``POST /bot<token>/<method>`` with a JSON body."""

    def __init__(
        self,
        api_url: str,
        token: str,
        *,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        # The token is part of the URL: it never goes into errors or logs.
        self._http = httpx.AsyncClient(
            base_url=f"{api_url.rstrip('/')}/bot{token}/", timeout=timeout, transport=transport
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def call(self, method: str, **params: Any) -> Any:
        body = {key: value for key, value in params.items() if value is not None}
        try:
            response = await self._http.post(method, json=body)
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TelegramUnavailable(type(exc).__name__) from exc
        if not isinstance(payload, dict):
            raise TelegramUnavailable("unexpected_response")
        if payload.get("ok"):
            return payload.get("result")
        parameters = payload.get("parameters") or {}
        raise TelegramApiError(
            int(payload.get("error_code") or response.status_code),
            str(payload.get("description") or "")[:200],
            retry_after=parameters.get("retry_after"),
            migrate_to_chat_id=parameters.get("migrate_to_chat_id"),
        )

    async def send_message(
        self, chat_id: str, text: str, *, reply_markup: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self.call(  # type: ignore[no-any-return]
            "sendMessage",
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            link_preview_options={"is_disabled": True},
            reply_markup=reply_markup,
        )

    async def edit_message_text(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        *,
        reply_markup: dict[str, Any] | None = None,
    ) -> Any:
        # Without ``reply_markup`` the edited message has no buttons.
        return await self.call(
            "editMessageText",
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode="HTML",
            link_preview_options={"is_disabled": True},
            reply_markup=reply_markup,
        )

    async def answer_callback_query(
        self, callback_query_id: str, text: str, *, show_alert: bool = False
    ) -> Any:
        return await self.call(
            "answerCallbackQuery",
            callback_query_id=callback_query_id,
            text=text[:200],
            show_alert=show_alert,
        )


# --- Rendering -----------------------------------------------------------------


def outcome_line(outcome: dict[str, Any] | None, by_name: str | None = None) -> str:
    """How closed actions read in a message: the outcome, who and where."""
    if not outcome:
        return "Действия больше не доступны"
    status = str(outcome.get("status") or "")
    line = OUTCOME_TEXT.get(status, f"Закрыто: {status}" if status else "Закрыто")
    if by_name:
        line += f" — {by_name}"
    channel = outcome.get("channel")
    if channel:
        line += f", через {CHANNEL_TEXT.get(str(channel), str(channel))}"
    return line


def render_text(
    title: str,
    body: str,
    links: list[dict[str, Any]],
    *,
    closing: str | None = None,
) -> str:
    """The message in Bot API HTML; a long body is cut so links and outcome stay."""
    head = f"<b>{html.escape(title, quote=False)}</b>"
    tail_parts = [
        f'<a href="{html.escape(str(link["url"]), quote=True)}">'
        f"{html.escape(str(link.get('label') or link['url']), quote=False)}</a>"
        for link in links
    ]
    if closing:
        tail_parts.append(f"<i>{html.escape(closing, quote=False)}</i>")
    tail = "\n".join(tail_parts)
    # The limit counts visible characters; markup is not visible, so measuring
    # the escaped text is conservative.
    budget = MAX_TEXT - len(head) - len(tail) - 4
    text = body.strip()
    if len(text) > budget:
        text = text[: max(budget - 1, 0)].rstrip() + "…"
    parts = [head]
    if text:
        parts.append(html.escape(text, quote=False))
    if tail:
        parts.append(tail)
    return "\n\n".join(parts)


def callback_data(notification_id: uuid.UUID, index: int) -> str:
    """What a button carries back (≤ 64 bytes): the notification and the action's index."""
    return f"{CALLBACK_PREFIX}:{notification_id.hex}:{index}"


def parse_callback_data(data: str) -> tuple[uuid.UUID, int] | None:
    prefix, _, rest = data.partition(":")
    raw_id, _, raw_index = rest.partition(":")
    if prefix != CALLBACK_PREFIX or not raw_index.isdigit():
        return None
    try:
        return uuid.UUID(hex=raw_id), int(raw_index)
    except ValueError:
        return None


def keyboard(notification_id: uuid.UUID, actions: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Inline buttons for the actions this channel can execute; others are left out."""
    row = [
        {
            "text": str(action.get("label") or action.get("id")),
            "callback_data": callback_data(notification_id, index),
        }
        for index, action in enumerate(actions)
        if (action.get("data") or {}).get("kind") in BUTTON_KINDS
    ]
    return {"inline_keyboard": [row]} if row else None


def message_ref(result: dict[str, Any]) -> str:
    """``external_id`` of a delivered message: the chat and the message in it."""
    return f"{result['chat']['id']}:{result['message_id']}"


def parse_message_ref(ref: str) -> tuple[str, int] | None:
    chat, _, message = ref.rpartition(":")
    if not chat or not message.isdigit():
        return None
    return chat, int(message)


# --- The channel -----------------------------------------------------------------


class TelegramChannel:
    name = TELEGRAM
    push = True
    needs_address = True

    def __init__(self, sessions: async_sessionmaker[AsyncSession], bot: BotApi) -> None:
        self._sessions = sessions
        self._bot = bot

    async def send(self, message: OutboundMessage) -> SendResult:
        if not message.address:
            raise PermanentDeliveryError("no_address")
        text = render_text(message.title, message.body, message.links)
        try:
            result = await self._bot.send_message(
                message.address,
                text,
                reply_markup=keyboard(message.notification_id, message.actions),
            )
        except TelegramUnavailable as exc:
            raise TransientDeliveryError(f"telegram_unavailable: {exc}") from exc
        except TelegramApiError as exc:
            await self._raise_for(exc, message)
        return SendResult(external_id=message_ref(result))

    async def _raise_for(self, error: TelegramApiError, message: OutboundMessage) -> None:
        reason = f"telegram_{error.status}: {error.description}"
        if error.migrate_to_chat_id is not None and message.recipient_kind == "group":
            # A group became a supergroup: same group, new chat id. The retry
            # reads the address again and goes to the new chat.
            async with transaction(self._sessions) as session:
                await session.execute(
                    update(ChannelGroup)
                    .where(
                        ChannelGroup.id == message.recipient_id,
                        ChannelGroup.external_chat_id == message.address,
                    )
                    .values(external_chat_id=str(error.migrate_to_chat_id))
                )
            raise TransientDeliveryError("telegram_chat_migrated")
        if error.status == 429 or error.status >= 500:
            raise TransientDeliveryError(reason)
        if error.status == 403 or (
            error.status == 400 and "chat not found" in error.description.lower()
        ):
            # Blocked by the person, removed from the group, chat deleted.
            raise RecipientUnreachable(reason)
        if error.status in (401, 404):
            # The bot token is wrong: a configuration fault an operator fixes.
            raise TransientDeliveryError("telegram_unauthorized")
        raise PermanentDeliveryError(reason)


@dataclass(frozen=True)
class SentMessage:
    chat_id: str
    message_id: int


PrincipalNames = Callable[[str], Awaitable[str | None]]


class TelegramMessages:
    """Brings messages already sent for a notification in line with its closed actions."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        bot: BotApi,
        *,
        names: PrincipalNames | None = None,
    ) -> None:
        self._sessions = sessions
        self._bot = bot
        self._names = names

    async def sent(self, notification_id: uuid.UUID) -> list[SentMessage]:
        async with self._sessions() as session:
            refs = await session.scalars(
                select(Delivery.external_id).where(
                    Delivery.notification_id == notification_id,
                    Delivery.channel == TELEGRAM,
                    Delivery.status == "delivered",
                    Delivery.external_id.is_not(None),
                )
            )
            messages = []
            for ref in refs:
                parsed = parse_message_ref(ref or "")
                if parsed is not None:
                    messages.append(SentMessage(*parsed))
            return messages

    async def name_of(self, principal_id: str) -> str | None:
        if self._names is None:
            return None
        try:
            return await self._names(principal_id)
        except Exception:  # the name only decorates the outcome
            logger.warning("principal %s: name not read", principal_id)
            return None

    async def closing_text(self, notification: Notification) -> str:
        outcome = notification.actions_outcome or {}
        by = outcome.get("by")
        return outcome_line(outcome, await self.name_of(str(by)) if by else None)

    async def actions_closed(self, notification: Notification) -> None:
        """Edit every delivered message: the outcome instead of the buttons.

        Best effort: a message that cannot be edited keeps its buttons, and a
        press on them is answered with the outcome all the same.
        """
        closing = await self.closing_text(notification)
        text = render_text(
            notification.title, notification.body, list(notification.links), closing=closing
        )
        for message in await self.sent(notification.id):
            try:
                await self._bot.edit_message_text(message.chat_id, message.message_id, text)
            except TelegramApiError as exc:
                if "not modified" in exc.description.lower():
                    continue
                logger.warning(
                    "notification %s: message %s:%s not updated (%s)",
                    notification.id,
                    message.chat_id,
                    message.message_id,
                    exc,
                )
            except TelegramUnavailable as exc:
                logger.warning(
                    "notification %s: Telegram unavailable while updating (%s)",
                    notification.id,
                    exc,
                )
