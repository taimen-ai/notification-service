"""Sending: deduplication (SC-003), addressing and access."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from typing import Any

from sqlalchemy import func, select

from notification_service.auth import SCOPE_ADMIN, SCOPE_READ
from notification_service.channels import OutboundMessage, SendResult
from notification_service.models import ChannelGroup, Delivery, InboxItem, Notification

from conftest import FakeDirectory, Harness, Person, auth, notification, to_principal


async def count(harness: Harness, model: Any) -> int:
    async with harness.sessions() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def send(harness: Harness, token: str, body: dict[str, Any], key: str | None = None) -> Any:
    headers = auth(token)
    if key is not None:
        headers["Idempotency-Key"] = key
    return await harness.client.post("/api/v1/notifications", json=body, headers=headers)


async def test_notification_reaches_the_principals_inbox(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    response = await send(harness, token(), notification(to_principal(person)), "k1")

    assert response.status_code == 201, response.text
    sent = response.json()
    assert [(d["channel"], d["status"]) for d in sent["deliveries"]] == [("web", "pending")]

    await harness.drain()
    inbox = await harness.client.get(
        "/api/v1/me/notifications", headers=auth(token(person.iam_principal_id))
    )
    items = inbox.json()["items"]
    assert [item["notificationId"] for item in items] == [sent["id"]]
    assert items[0]["title"] == "Review requested"
    assert inbox.json()["unreadCount"] == 1


async def test_same_key_does_not_duplicate(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    sender = uuid.uuid4()
    body = notification(to_principal(person))

    first = await send(harness, token(sender), body, "same-key")
    await harness.drain()
    second = await send(harness, token(sender), body, "same-key")
    await harness.drain()

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["deliveries"][0]["status"] == "delivered"
    assert await count(harness, Notification) == 1
    assert await count(harness, Delivery) == 1
    assert await count(harness, InboxItem) == 1


async def test_concurrent_retries_create_one_notification(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    sender_token = token()
    body = notification(to_principal(person))

    responses = await asyncio.gather(*(send(harness, sender_token, body, "race") for _ in range(5)))

    assert {r.status_code for r in responses} <= {200, 201}
    assert len({r.json()["id"] for r in responses}) == 1
    assert await count(harness, Notification) == 1


async def test_same_key_with_other_content_is_a_conflict(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    sender_token = token()
    await send(harness, sender_token, notification(to_principal(person)), "k")

    response = await send(
        harness, sender_token, notification(to_principal(person), title="Changed"), "k"
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "idempotency_conflict"


async def test_keys_of_different_senders_do_not_collide(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    body = notification(to_principal(person))

    first = await send(harness, token(), body, "shared")
    second = await send(harness, token(), body, "shared")

    assert first.status_code == second.status_code == 201
    assert first.json()["id"] != second.json()["id"]


async def test_idempotency_key_is_required(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    response = await send(harness, token(), notification(to_principal(make_person())))
    assert response.status_code == 422


async def test_sending_requires_the_send_scope(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    body = notification(to_principal(make_person()))
    denied = await send(harness, token(scopes=(SCOPE_READ,)), body, "k")
    anonymous = await harness.client.post(
        "/api/v1/notifications", json=body, headers={"Idempotency-Key": "k"}
    )
    foreign = await send(harness, token(audience="control-plane"), body, "k")

    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "insufficient_scope"
    assert anonymous.status_code == 401
    assert foreign.status_code == 401
    assert foreign.json()["error"]["code"] == "invalid_token"


async def test_content_is_validated(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    bad = [
        notification(to_principal(person), type="Not a type"),
        notification(to_principal(person), title="two\nlines"),
        notification(to_principal(person), links=[{"label": "x", "url": "javascript:alert(1)"}]),
        notification({"kind": "role", "id": str(uuid.uuid4())}),
    ]
    for index, body in enumerate(bad):
        response = await send(harness, token(), body, f"bad-{index}")
        assert response.status_code == 422, body


async def test_unknown_principal_is_rejected(harness: Harness, token: Callable[..., str]) -> None:
    body = notification({"kind": "principal", "id": str(uuid.uuid4())})
    response = await send(harness, token(), body, "k")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "unknown_recipient"


async def test_directory_outage_is_retriable(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    directory: FakeDirectory,
) -> None:
    person = make_person()
    directory.unavailable = True
    response = await send(harness, token(), notification(to_principal(person)), "k")
    assert response.status_code == 503
    assert await count(harness, Notification) == 0

    directory.unavailable = False
    retried = await send(harness, token(), notification(to_principal(person)), "k")
    assert retried.status_code == 201


async def test_principal_without_identity_is_journaled_as_failed(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person(bound=False)
    response = await send(harness, token(), notification(to_principal(person)), "k")

    deliveries = response.json()["deliveries"]
    assert [(d["status"], d["lastError"]) for d in deliveries] == [
        ("failed", "recipient_has_no_identity")
    ]


class ChatChannel:
    """A group-capable channel standing in for a messenger adapter."""

    name = "chat"
    push = True
    needs_address = True

    def __init__(self) -> None:
        self.sent: list[OutboundMessage] = []

    async def send(self, message: OutboundMessage) -> SendResult:
        self.sent.append(message)
        return SendResult(external_id=f"msg-{len(self.sent)}")


async def add_group(harness: Harness, tenant_id: uuid.UUID, **fields: Any) -> ChannelGroup:
    group = ChannelGroup(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        channel=fields.pop("channel", "chat"),
        external_chat_id=fields.pop("external_chat_id", "-100200"),
        linked_by=uuid.uuid4(),
        **fields,
    )
    async with harness.sessions() as session, session.begin():
        session.add(group)
    return group


async def test_role_reaches_its_holders_and_bound_groups(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
    directory: FakeDirectory,
    tenant_id: uuid.UUID,
) -> None:
    chat = ChatChannel()
    role_id, workspace_id = uuid.uuid4(), uuid.uuid4()
    alice, bob = make_person(), make_person()
    directory.roles[(role_id, workspace_id)] = [alice.principal_id, bob.principal_id]

    async with harness_factory(channels=[chat]) as harness:
        group = await add_group(harness, tenant_id, workspace_id=workspace_id, role_id=role_id)
        # Bound to the same role elsewhere, or to the workspace only: not addressed.
        await add_group(
            harness, tenant_id, workspace_id=uuid.uuid4(), role_id=role_id, external_chat_id="-1"
        )
        await add_group(harness, tenant_id, workspace_id=workspace_id, external_chat_id="-2")

        body = notification({"kind": "role", "id": str(role_id), "workspaceId": str(workspace_id)})
        response = await send(harness, token(), body, "role")
        await harness.drain()

    assert response.status_code == 201, response.text
    deliveries = {
        (d["recipientKind"], d["recipientId"], d["channel"]) for d in response.json()["deliveries"]
    }
    assert ("principal", str(alice.principal_id), "web") in deliveries
    assert ("principal", str(bob.principal_id), "web") in deliveries
    assert ("group", str(group.id), "chat") in deliveries
    assert [m.address for m in chat.sent] == ["-100200"]


async def test_group_is_addressed_directly(
    harness_factory: Callable[..., Any], token: Callable[..., str], tenant_id: uuid.UUID
) -> None:
    chat = ChatChannel()
    async with harness_factory(channels=[chat]) as harness:
        group = await add_group(harness, tenant_id, workspace_id=uuid.uuid4())
        response = await send(
            harness, token(), notification({"kind": "group", "id": str(group.id)}), "g"
        )
        await harness.drain()
        journal = await harness.client.get(
            f"/api/v1/notifications/{response.json()['id']}", headers=auth(token())
        )

    assert response.status_code == 201
    assert len(chat.sent) == 1
    assert journal.status_code == 404  # another sender does not see the journal


async def test_group_of_another_tenant_or_disabled_is_unknown(
    harness: Harness, token: Callable[..., str], tenant_id: uuid.UUID
) -> None:
    foreign = await add_group(harness, uuid.uuid4(), workspace_id=uuid.uuid4())
    disabled = await add_group(
        harness,
        tenant_id,
        workspace_id=uuid.uuid4(),
        external_chat_id="-9",
        disabled_at=func.now(),
    )
    for group in (foreign, disabled):
        response = await send(
            harness, token(), notification({"kind": "group", "id": str(group.id)}), str(group.id)
        )
        assert response.status_code == 422


async def test_group_on_an_unconfigured_channel_fails_visibly(
    harness: Harness, token: Callable[..., str], tenant_id: uuid.UUID
) -> None:
    group = await add_group(harness, tenant_id, workspace_id=uuid.uuid4(), channel="telegram")
    response = await send(
        harness, token(), notification({"kind": "group", "id": str(group.id)}), "g"
    )
    assert [(d["status"], d["lastError"]) for d in response.json()["deliveries"]] == [
        ("failed", "channel_not_configured")
    ]


async def test_sender_reads_the_delivery_journal(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    sender = uuid.uuid4()
    person = make_person()
    sent = (await send(harness, token(sender), notification(to_principal(person)), "k")).json()
    await harness.drain()

    own = await harness.client.get(
        f"/api/v1/notifications/{sent['id']}", headers=auth(token(sender))
    )
    admin = await harness.client.get(
        f"/api/v1/notifications/{sent['id']}",
        headers=auth(token(scopes=(SCOPE_ADMIN,))),
    )
    other_tenant = await harness.client.get(
        f"/api/v1/notifications/{sent['id']}",
        headers=auth(token(sender, tenant=uuid.uuid4())),
    )

    assert own.status_code == 200
    assert own.json()["deliveries"][0]["status"] == "delivered"
    assert own.json()["deliveries"][0]["attempts"] == 1
    assert admin.status_code == 200
    assert other_tenant.status_code == 404
