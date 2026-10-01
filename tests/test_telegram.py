"""The Telegram channel end to end: linking, groups, messages with buttons, decisions.

The Bot API, IAM and the Control Plane are fakes by their contracts
(``telegram_fakes``); the service runs whole — webhook, worker, sender, database.
"""

from __future__ import annotations

import itertools
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import httpx
import pytest
from control_plane_client import ControlPlaneClient
from platform_auth import StaticKeySet, TokenVerifier, VerifierConfig
from platform_auth.testing import SigningKey
from sqlalchemy import select, update

from notification_service.auth import SCOPE_ADMIN, SCOPE_READ, SCOPE_SEND
from notification_service.channels.telegram import (
    MAX_TEXT,
    callback_data,
    parse_callback_data,
    render_text,
)
from notification_service.db import utcnow
from notification_service.decisions import ControlPlaneApprovals, IamChannelLinks
from notification_service.models import (
    ChannelAddress,
    ChannelCallback,
    ChannelGroup,
    ChannelGroupIntent,
    Delivery,
    Notification,
)
from notification_service.telegram_bot import SECRET_HEADER, parse_command

from conftest import ISSUER, FakeDirectory, Harness, Person, auth, settings_for
from telegram_fakes import (
    BOT_TOKEN,
    FakeApproval,
    FakeBotApi,
    FakeControlPlane,
    FakeIam,
    StaticTokens,
)

SECRET = "hook-secret"
BOT = "example_bot"
_ids = itertools.count(1)


def telegram_settings(**changes: Any) -> Any:
    return settings_for(
        telegram_bot_token=BOT_TOKEN,
        telegram_webhook_secret=SECRET,
        telegram_api_url="https://tg.test",
        telegram_bot_username=BOT,
        **changes,
    )


def message(chat: dict[str, Any], user_id: int, text: str) -> dict[str, Any]:
    return {
        "update_id": next(_ids),
        "message": {
            "message_id": next(_ids),
            "from": {"id": user_id, "is_bot": False, "first_name": "Someone"},
            "chat": chat,
            "date": 0,
            "text": text,
        },
    }


def private(user_id: int, text: str) -> dict[str, Any]:
    return message({"id": user_id, "type": "private", "first_name": "Someone"}, user_id, text)


def in_group(chat_id: int, user_id: int, text: str) -> dict[str, Any]:
    return message({"id": chat_id, "type": "supergroup", "title": "Team"}, user_id, text)


def decide_actions(approval_id: uuid.UUID) -> list[dict[str, Any]]:
    return [
        {
            "id": verb,
            "label": label,
            "data": {"kind": "approval.decide", "approvalId": str(approval_id), "decision": verb},
        }
        for verb, label in (("approve", "Одобрить"), ("reject", "Отклонить"))
    ]


class FakeLauncher:
    """The launcher of personal harnesses: what reached it, and what it answers."""

    def __init__(self) -> None:
        self.received: list[tuple[uuid.UUID, dict[str, Any]]] = []
        self.answer = "accepted"

    async def deliver(self, principal_id: uuid.UUID, body: dict[str, Any]) -> str:
        if self.answer == "accepted":
            self.received.append((principal_id, body))
        return self.answer

    async def aclose(self) -> None:
        return None


@dataclass
class Telegram:
    h: Harness
    bot: FakeBotApi
    iam: FakeIam
    cp: FakeControlPlane
    token: Callable[..., str]
    launcher: FakeLauncher

    async def update(self, body: dict[str, Any], secret: str | None = SECRET) -> httpx.Response:
        headers = {SECRET_HEADER: secret} if secret is not None else {}
        return await self.h.client.post("/channels/telegram/webhook", json=body, headers=headers)

    async def link(self, person: Person, user_id: int) -> None:
        code = uuid.uuid4().hex
        self.iam.codes[code] = person.iam_principal_id
        self.cp.principals[person.iam_principal_id] = person.principal_id
        response = await self.update(private(user_id, f"/start {code}"))
        assert response.status_code == 200
        assert "привязан" in self.bot.of("sendMessage")[-1]["text"]

    async def send(self, recipient: dict[str, Any], approval: FakeApproval) -> uuid.UUID:
        response = await self.h.client.post(
            "/api/v1/notifications",
            json={
                "recipient": recipient,
                "type": "approval.requested",
                "title": "Нужно решение: TASK-1 <Release>",
                "body": "Запрашивает: Someone",
                "links": [{"label": "Открыть", "url": "https://example.test/tasks/1"}],
                "actions": decide_actions(approval.id),
            },
            headers={**auth(self.token()), "Idempotency-Key": uuid.uuid4().hex},
        )
        assert response.status_code == 201, response.text
        await self.h.drain()
        return uuid.UUID(response.json()["id"])

    def sent_to(self, chat: int) -> dict[str, Any]:
        return [m for m in self.bot.messages if m["chat_id"] == str(chat)][-1]

    async def press(
        self,
        sent: dict[str, Any],
        user_id: int,
        *,
        index: int = 0,
        callback_id: str | None = None,
    ) -> str:
        callback_id = callback_id or str(next(_ids) + 10**15)
        button = sent["reply_markup"]["inline_keyboard"][0][index]
        response = await self.update(
            {
                "update_id": next(_ids),
                "callback_query": {
                    "id": callback_id,
                    "from": {"id": user_id, "is_bot": False, "first_name": "Someone"},
                    "chat_instance": "1",
                    "message": {
                        "message_id": sent["message_id"],
                        "date": 0,
                        "chat": {"id": int(sent["chat_id"]), "type": "private"},
                    },
                    "data": button["callback_data"],
                },
            }
        )
        assert response.status_code == 200
        answers = [
            a for a in self.bot.of("answerCallbackQuery") if a["callback_query_id"] == callback_id
        ]
        return str(answers[-1]["text"])

    async def notification(self, notification_id: uuid.UUID) -> Notification:
        async with self.h.sessions() as session:
            found = await session.get(Notification, notification_id)
            assert found is not None
            return found

    async def address(self, person: Person) -> ChannelAddress | None:
        async with self.h.sessions() as session:
            return await session.get(
                ChannelAddress, (person.tenant_id, person.iam_principal_id, "telegram")
            )


@pytest.fixture
async def tg(
    harness_factory: Callable[..., Any],
    signing_key: SigningKey,
    tenant_id: uuid.UUID,
    token: Callable[..., str],
) -> AsyncIterator[Telegram]:
    bot = FakeBotApi()
    iam = FakeIam(tenant_id)
    cp = FakeControlPlane(iam)
    # The service account's IAM-audience token: the tenant of the IAM paths.
    service_token = signing_key.issue(
        ttl_seconds=600,
        issuer=ISSUER,
        audience="iam",
        tenant_id=tenant_id,
        subject=uuid.uuid4(),
        scopes=["iam:channel-links"],
        principal_type="service_account",
    )
    links = IamChannelLinks(
        "https://iam.test",
        StaticTokens(service_token),  # type: ignore[arg-type]
        TokenVerifier(
            StaticKeySet(signing_key.public_pem, key_id=signing_key.key_id),
            VerifierConfig(issuer=ISSUER, audience="iam"),
        ),
        transport=iam.transport,
    )
    service_client = ControlPlaneClient("https://cp.test", "service-key", transport=cp.transport)
    approvals = ControlPlaneApprovals("https://cp.test", service_client, transport=cp.transport)
    launcher = FakeLauncher()
    async with harness_factory(
        settings=telegram_settings(),
        telegram_transport=bot.transport,
        channel_links=links,
        approvals=approvals,
        harness=launcher,
    ) as harness:
        yield Telegram(harness, bot, iam, cp, token, launcher)
    await service_client.aclose()
    await links.aclose()


def to(person: Person) -> dict[str, str]:
    return {"kind": "principal", "id": str(person.principal_id)}


# --- Linking ---------------------------------------------------------------------


async def test_code_links_the_account_that_sent_it(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)

    address = await tg.address(person)
    assert address is not None and address.address == "1001" and address.disabled_at is None
    assert tg.iam.requests[0][1] == {
        "channel": "telegram",
        "code": tg.iam.requests[0][1]["code"],
        "externalSubject": "1001",
    }

    # The code is spent: sending it again links nothing.
    code = tg.iam.requests[0][1]["code"]
    await tg.update(private(2002, f"/start {code}"))
    assert "Код не подходит" in tg.bot.of("sendMessage")[-1]["text"]


async def test_unknown_code_and_iam_outage_are_explained(tg: Telegram) -> None:
    await tg.update(private(1001, "/start nope"))
    assert "Код не подходит" in tg.bot.of("sendMessage")[-1]["text"]
    tg.iam.down = True
    await tg.update(private(1001, "/start whatever"))
    assert "недоступен" in tg.bot.of("sendMessage")[-1]["text"]
    await tg.update(private(1001, "/start"))
    assert "/start <код>" in tg.bot.of("sendMessage")[-1]["text"]


# --- Messages and decisions ----------------------------------------------------------


async def test_decision_from_a_button_end_to_end(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval({person.iam_principal_id})

    notification_id = await tg.send(to(person), approval)
    sent = tg.sent_to(1001)
    buttons = sent["reply_markup"]["inline_keyboard"][0]
    assert [b["text"] for b in buttons] == ["Одобрить", "Отклонить"]
    assert "<b>Нужно решение: TASK-1 &lt;Release&gt;</b>" in sent["text"]
    assert '<a href="https://example.test/tasks/1">Открыть</a>' in sent["text"]

    answer = await tg.press(sent, 1001, callback_id="cb-1")

    assert answer == "Решение принято: одобрено."
    # IAM exchanged the press for this approval only; the core decided once,
    # keyed by the callback.
    assert tg.iam.exchanges() == [
        {"channel": "telegram", "externalSubject": "1001", "purposeRef": f"approval:{approval.id}"}
    ]
    assert tg.cp.decisions == [{"approval": approval.id, "verb": "approve", "key": "cb-1"}]
    assert approval.status == "approved" and approval.decision_by == person.principal_id

    notification = await tg.notification(notification_id)
    assert notification.actions_closed_at is not None
    assert notification.actions_outcome == {
        "status": "approved",
        "by": str(person.principal_id),
        "channel": "telegram",
        "at": notification.actions_outcome["at"],  # type: ignore[index]
    }
    # The message now shows the outcome and no buttons.
    (edited,) = tg.bot.of("editMessageText")
    assert edited["message_id"] == sent["message_id"] and "reply_markup" not in edited
    assert "✅ Одобрено, через Telegram" in edited["text"]


async def test_repeated_press_and_redelivered_callback_decide_nothing_more(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval({person.iam_principal_id})
    await tg.send(to(person), approval)
    sent = tg.sent_to(1001)
    await tg.press(sent, 1001, callback_id="cb-1")

    # Telegram redelivers the very same update: the recorded answer, no new call.
    again = await tg.press(sent, 1001, callback_id="cb-1")
    assert again == "Решение принято: одобрено."
    # A second press (a new callback) — explained, not decided.
    other = await tg.press(sent, 1001, index=1, callback_id="cb-2")
    assert other.startswith("Уже решено: ✅ Одобрено")

    assert len(tg.iam.exchanges()) == 1
    assert len(tg.cp.decisions) == 1 and approval.status == "approved"


async def test_decided_in_the_web_before_the_press(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person, other = make_person(), make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval({person.iam_principal_id})
    notification_id = await tg.send(to(person), approval)
    # Someone decided in the web; the core's event has not reached the service yet.
    approval.status, approval.decision_by = "rejected", other.principal_id

    answer = await tg.press(tg.sent_to(1001), 1001)

    assert answer.startswith("Уже решено: ❌ Отклонено")
    assert tg.iam.exchanges() == [] and tg.cp.decisions == []
    notification = await tg.notification(notification_id)
    assert notification.actions_outcome == {
        "status": "rejected",
        "by": str(other.principal_id),
        "at": notification.actions_outcome["at"],  # type: ignore[index]
    }
    assert "❌ Отклонено" in tg.bot.of("editMessageText")[-1]["text"]


async def test_closure_from_the_core_updates_every_message(
    tg: Telegram, make_person: Callable[..., Person], directory: FakeDirectory
) -> None:
    first, second = make_person(), make_person()
    await tg.link(first, 1001)
    await tg.link(second, 1002)
    role, workspace = uuid.uuid4(), uuid.uuid4()
    directory.roles[(role, workspace)] = [first.principal_id, second.principal_id]
    approval = tg.cp.approval({first.iam_principal_id, second.iam_principal_id})
    notification_id = await tg.send(
        {"kind": "role", "id": str(role), "workspaceId": str(workspace)}, approval
    )
    notification = await tg.notification(notification_id)

    # What the event consumer does on ``approval.cancelled``.
    await tg.h.app.state.sender.close_actions(
        notification.tenant_id,
        notification.sender_id,
        notification.dedup_key,
        {"status": "cancelled"},
    )

    edited = {e["chat_id"] for e in tg.bot.of("editMessageText")}
    assert edited == {"1001", "1002"}
    assert all("Отменено" in e["text"] for e in tg.bot.of("editMessageText"))
    answer = await tg.press(tg.sent_to(1002), 1002)
    assert answer.startswith("Уже решено: Отменено")
    assert tg.iam.exchanges() == []


async def test_no_right_to_decide_is_refused(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval(eligible=set())
    notification_id = await tg.send(to(person), approval)

    answer = await tg.press(tg.sent_to(1001), 1001, callback_id="cb-1")

    assert answer.startswith("У вас нет права")
    assert approval.status == "pending"
    assert (await tg.notification(notification_id)).actions_closed_at is None
    async with tg.h.sessions() as session:
        press = await session.get(ChannelCallback, ("telegram", "cb-1"))
    assert press is not None and press.result == "not_eligible"


async def test_unlinked_account_cannot_decide(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval({person.iam_principal_id})
    await tg.send(to(person), approval)
    sent = tg.sent_to(1001)

    await tg.update(private(1001, "/unlink"))
    assert "отвязан" in tg.bot.of("sendMessage")[-1]["text"]
    address = await tg.address(person)
    assert address is not None and address.disabled_reason == "unlinked_by_user"

    answer = await tg.press(sent, 1001)
    assert "не привязан" in answer
    assert tg.iam.exchanges() == [] and approval.status == "pending"

    # Nothing is delivered to the chat any more; ``/start`` does not undo it.
    await tg.update(private(1001, "/start"))
    before = len(tg.bot.messages)
    await tg.send(to(person), tg.cp.approval({person.iam_principal_id}))
    assert len(tg.bot.messages) == before


async def test_link_revoked_in_iam_is_refused_and_stops_the_address(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval({person.iam_principal_id})
    await tg.send(to(person), approval)
    del tg.iam.links["1001"]  # revoked by the person in the web

    answer = await tg.press(tg.sent_to(1001), 1001)

    assert "отозвана" in answer and approval.status == "pending"
    address = await tg.address(person)
    assert address is not None and address.disabled_reason == "iam_link_revoked"


async def test_outage_is_not_recorded_so_the_same_press_can_succeed(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval({person.iam_principal_id})
    await tg.send(to(person), approval)
    sent = tg.sent_to(1001)

    tg.cp.down = True
    assert "недоступен" in await tg.press(sent, 1001, callback_id="cb-1")
    tg.cp.down = False
    assert await tg.press(sent, 1001, callback_id="cb-1") == "Решение принято: одобрено."
    assert approval.status == "approved"


async def test_foreign_or_forged_buttons_are_refused(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    approval = tg.cp.approval({person.iam_principal_id})
    await tg.send(to(person), approval)
    sent = tg.sent_to(1001)

    # The same button data, but on a message the bot did not send for it.
    forged = {**sent, "message_id": sent["message_id"] + 50}
    assert "не действует" in await tg.press(forged, 1001)
    garbage = {
        **sent,
        "reply_markup": {"inline_keyboard": [[{"text": "x", "callback_data": "a:zz:0"}]]},
    }
    assert "не действует" in await tg.press(garbage, 1001)
    assert tg.iam.exchanges() == []


# --- Groups ------------------------------------------------------------------------


async def bind_group(tg: Telegram, workspace: uuid.UUID, role: uuid.UUID | None, chat: int) -> str:
    response = await tg.h.client.post(
        f"/api/v1/workspaces/{workspace}/channel-groups",
        json={"channel": "telegram", "roleId": str(role) if role else None},
        headers=auth(tg.token(scopes=(SCOPE_ADMIN,))),
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["command"] == f"/start@{BOT} {body['code']}"
    assert body["deepLink"] == f"https://t.me/{BOT}?startgroup={body['code']}"
    await tg.update(in_group(chat, 1, body["command"]))
    return str(body["code"])


async def test_group_bound_by_code_gets_role_notifications_and_decides_by_the_holder(
    tg: Telegram, make_person: Callable[..., Person], directory: FakeDirectory
) -> None:
    holder, outsider = make_person(), make_person()
    await tg.link(holder, 1001)
    await tg.link(outsider, 1003)
    role, workspace = uuid.uuid4(), uuid.uuid4()
    directory.roles[(role, workspace)] = [holder.principal_id]
    code = await bind_group(tg, workspace, role, -500)
    assert "Группа привязана" in tg.bot.of("sendMessage")[-1]["text"]

    groups = await tg.h.client.get(
        f"/api/v1/workspaces/{workspace}/channel-groups",
        headers=auth(tg.token(scopes=(SCOPE_ADMIN,))),
    )
    (group,) = groups.json()
    assert group["externalChatId"] == "-500" and group["roleId"] == str(role)
    assert group["title"] == "Team" and group["disabledAt"] is None

    # The code is spent.
    await tg.update(in_group(-600, 1, f"/start {code}"))
    assert "Код не подходит" in tg.bot.of("sendMessage")[-1]["text"]

    approval = tg.cp.approval({holder.iam_principal_id})
    await tg.send({"kind": "role", "id": str(role), "workspaceId": str(workspace)}, approval)
    in_chat = tg.sent_to(-500)
    assert in_chat["reply_markup"]["inline_keyboard"][0][0]["text"] == "Одобрить"

    # Someone in the group without a linked account, then a linked non-holder.
    assert "не привязан" in await tg.press(in_chat, 7777)
    assert (await tg.press(in_chat, 1003)).startswith("У вас нет права")
    assert approval.status == "pending"
    # The holder decides from the group; both their messages show the outcome.
    assert await tg.press(in_chat, 1001, index=1) == "Решение принято: отклонено."
    assert approval.status == "rejected" and approval.decision_by == holder.principal_id
    assert {e["chat_id"] for e in tg.bot.of("editMessageText")} == {"1001", "-500"}


async def test_expired_group_code_is_refused(tg: Telegram) -> None:
    workspace = uuid.uuid4()
    response = await tg.h.client.post(
        f"/api/v1/workspaces/{workspace}/channel-groups",
        json={},
        headers=auth(tg.token(scopes=(SCOPE_ADMIN,))),
    )
    async with tg.h.sessions() as session, session.begin():
        await session.execute(
            update(ChannelGroupIntent).values(expires_at=utcnow() - timedelta(seconds=1))
        )
    await tg.update(in_group(-500, 1, response.json()["command"]))
    assert "Код не подходит" in tg.bot.of("sendMessage")[-1]["text"]
    async with tg.h.sessions() as session:
        assert (await session.scalars(select(ChannelGroup))).all() == []


async def test_group_binding_needs_an_admin(tg: Telegram) -> None:
    response = await tg.h.client.post(
        f"/api/v1/workspaces/{uuid.uuid4()}/channel-groups",
        json={},
        headers=auth(tg.token(scopes=(SCOPE_SEND, SCOPE_READ))),
    )
    assert response.status_code == 403


async def test_bot_removed_from_group_or_unbound_stops_delivery(tg: Telegram) -> None:
    workspace = uuid.uuid4()
    await bind_group(tg, workspace, None, -500)
    await tg.update(
        {
            "update_id": next(_ids),
            "my_chat_member": {
                "chat": {"id": -500, "type": "supergroup", "title": "Team"},
                "from": {"id": 1, "is_bot": False, "first_name": "Admin"},
                "date": 0,
                "old_chat_member": {"status": "member", "user": {"id": 9, "is_bot": True}},
                "new_chat_member": {"status": "kicked", "user": {"id": 9, "is_bot": True}},
            },
        }
    )
    async with tg.h.sessions() as session:
        group = (await session.scalars(select(ChannelGroup))).one()
    assert group.disabled_reason == "bot_removed"

    await bind_group(tg, workspace, None, -700)
    async with tg.h.sessions() as session:
        active = (
            await session.scalars(select(ChannelGroup).where(ChannelGroup.disabled_at.is_(None)))
        ).one()
    response = await tg.h.client.delete(
        f"/api/v1/workspaces/{workspace}/channel-groups/{active.id}",
        headers=auth(tg.token(scopes=(SCOPE_ADMIN,))),
    )
    assert response.status_code == 204
    async with tg.h.sessions() as session:
        assert (await session.get(ChannelGroup, active.id)).disabled_reason == "unlinked_by_admin"  # type: ignore[union-attr]


# --- Delivery failures -------------------------------------------------------------


async def test_blocked_bot_disables_the_address_until_the_person_returns(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 1001)
    tg.bot.failing["1001"] = (403, "Forbidden: bot was blocked by the user")
    await tg.send(to(person), tg.cp.approval({person.iam_principal_id}))

    async with tg.h.sessions() as session:
        (delivery,) = (
            await session.scalars(select(Delivery).where(Delivery.channel == "telegram"))
        ).all()
    assert delivery.status == "failed"
    assert delivery.last_error == "telegram_403: Forbidden: bot was blocked by the user"
    address = await tg.address(person)
    assert address is not None and address.disabled_at is not None

    # Unblocking sends ``/start``: the address is back.
    del tg.bot.failing["1001"]
    await tg.update(private(1001, "/start"))
    address = await tg.address(person)
    assert address is not None and address.disabled_at is None


async def test_rate_limit_is_retried(tg: Telegram, make_person: Callable[..., Person]) -> None:
    person = make_person()
    await tg.link(person, 1001)
    tg.bot.failing["1001"] = (429, "Too Many Requests: retry after 1")
    await tg.send(to(person), tg.cp.approval({person.iam_principal_id}))
    async with tg.h.sessions() as session:
        delivery = (
            await session.scalars(select(Delivery).where(Delivery.channel == "telegram"))
        ).one()
    # Retried up to the limit (no backoff in tests), not failed at once.
    assert delivery.attempts == 8
    assert (
        delivery.last_error == "retries_exhausted: telegram_429: Too Many Requests: retry after 1"
    )
    address = await tg.address(person)
    assert address is not None and address.disabled_at is None


# --- Webhook -----------------------------------------------------------------------


async def test_webhook_requires_the_secret(tg: Telegram) -> None:
    assert (await tg.update(private(1, "/help"), secret=None)).status_code == 401
    assert (await tg.update(private(1, "/help"), secret="wrong")).status_code == 401
    assert tg.bot.calls == []
    assert (await tg.update(private(1, "/help"))).status_code == 200


async def test_webhook_is_absent_without_a_bot(harness: Harness) -> None:
    response = await harness.client.post(
        "/channels/telegram/webhook", json={}, headers={SECRET_HEADER: SECRET}
    )
    assert response.status_code == 404


# --- Rendering ---------------------------------------------------------------------


def test_long_text_is_cut_keeping_links() -> None:
    text = render_text(
        "Title",
        "x" * 10_000,
        [{"label": "Open", "url": "https://example.test/a"}],
        closing="Одобрено",
    )
    assert len(text) <= MAX_TEXT
    assert text.endswith('<a href="https://example.test/a">Open</a>\n<i>Одобрено</i>')
    assert "…" in text


def test_callback_data_fits_and_round_trips() -> None:
    notification_id = uuid.uuid4()
    data = callback_data(notification_id, 4)
    assert len(data.encode()) <= 64
    assert parse_callback_data(data) == (notification_id, 4)
    assert parse_callback_data("a:not-a-uuid:1") is None
    assert parse_callback_data("x:" + notification_id.hex + ":1") is None


def test_commands_addressed_to_the_bot() -> None:
    assert parse_command("/start@example_bot abc") == ("start", "abc")
    assert parse_command("/unlink") == ("unlink", "")
    assert parse_command("hello") is None


# --- The assistant conversation (TAI-ADR-0051 §7) ---------------------------------


def harness_actions(request_id: str) -> list[dict[str, Any]]:
    return [
        {
            "id": verb,
            "label": label,
            "data": {"kind": "harness_approval", "requestId": request_id, "decision": verb},
        }
        for verb, label in (("approve", "Разрешить"), ("reject", "Отклонить"))
    ]


async def send_confirmation(tg: Telegram, person: Person, request_id: str) -> None:
    response = await tg.h.client.post(
        "/api/v1/notifications",
        json={
            "recipient": to(person),
            "type": "harness.confirmation",
            "title": "Ассистент просит подтверждения",
            "body": "Создать задачу «Сверить акты»?",
            "actions": harness_actions(request_id),
        },
        headers={**auth(tg.token()), "Idempotency-Key": uuid.uuid4().hex},
    )
    assert response.status_code == 201, response.text
    await tg.h.drain()


async def test_free_text_of_a_linked_person_goes_to_their_conversation(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 7001)
    before = len(tg.bot.of("sendMessage"))
    assert (await tg.update(private(7001, "Что сегодня важного?"))).status_code == 200
    assert len(tg.launcher.received) == 1
    principal, body = tg.launcher.received[0]
    assert principal == person.iam_principal_id
    assert body["channel"] == "telegram" and body["text"] == "Что сегодня важного?"
    assert body["messageId"].startswith("7001:")
    # The answer comes from the assistant later, not from the bot now.
    assert len(tg.bot.of("sendMessage")) == before


async def test_text_of_an_unlinked_account_goes_no_further(tg: Telegram) -> None:
    await tg.update(private(7002, "Привет"))
    assert tg.launcher.received == []
    assert "не привязан" in tg.bot.of("sendMessage")[-1]["text"]


async def test_group_text_and_commands_are_not_conversation(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 7003)
    await tg.update(in_group(-100500, 7003, "Всем привет"))
    await tg.update(private(7003, "/help"))
    assert tg.launcher.received == []


async def test_assistant_unavailable_or_absent_is_explained(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 7004)
    tg.launcher.answer = "unavailable"
    await tg.update(private(7004, "Ты тут?"))
    assert "недоступен" in tg.bot.of("sendMessage")[-1]["text"]
    tg.launcher.answer = "no_harness"
    await tg.update(private(7004, "Ты тут?"))
    assert "нет рабочего места" in tg.bot.of("sendMessage")[-1]["text"]


async def test_harness_confirmation_press_reaches_the_harness_once(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 7005)
    await send_confirmation(tg, person, "req-1")
    sent = tg.sent_to(7005)
    answer = await tg.press(sent, 7005, index=0, callback_id="cb-harness-1")
    assert "разрешено" in answer
    assert tg.launcher.received == [
        (
            person.iam_principal_id,
            {
                "channel": "telegram",
                "messageId": "cb-harness-1",
                "approval": {"id": "req-1", "decision": "approve"},
            },
        )
    ]
    # Telegram redelivers the same callback: one press.
    await tg.press(sent, 7005, index=0, callback_id="cb-harness-1")
    assert len(tg.launcher.received) == 1
    # Another press after the answer: the buttons are closed.
    again = await tg.press(sent, 7005, index=1)
    assert "Уже решено" in again
    assert len(tg.launcher.received) == 1


async def test_harness_confirmation_is_retried_after_an_outage(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 7006)
    await send_confirmation(tg, person, "req-2")
    sent = tg.sent_to(7006)
    tg.launcher.answer = "unavailable"
    assert "недоступен" in await tg.press(sent, 7006, callback_id="cb-harness-2")
    tg.launcher.answer = "accepted"
    assert "разрешено" in await tg.press(sent, 7006, callback_id="cb-harness-2")
    assert len(tg.launcher.received) == 1


async def test_harness_confirmation_of_an_unlinked_account_is_refused(
    tg: Telegram, make_person: Callable[..., Person]
) -> None:
    person = make_person()
    await tg.link(person, 7007)
    await send_confirmation(tg, person, "req-3")
    sent = tg.sent_to(7007)
    await tg.update(private(7007, "/unlink"))
    answer = await tg.press(sent, 7007)
    assert "не привязан" in answer
    assert tg.launcher.received == []
