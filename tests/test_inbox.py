"""The web inbox: listing, read marks, isolation between recipients."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from typing import Any

from sqlalchemy import select

from notification_service.auth import SCOPE_SEND
from notification_service.channels.web import append_to_inbox
from notification_service.models import InboxItem, Notification

from conftest import Harness, Person, auth, notification, to_principal


async def deliver(harness: Harness, token: Callable[..., str], person: Person, n: int) -> None:
    for index in range(n):
        response = await harness.client.post(
            "/api/v1/notifications",
            json=notification(to_principal(person), title=f"N{index + 1}"),
            headers={**auth(token()), "Idempotency-Key": uuid.uuid4().hex},
        )
        assert response.status_code == 201
    await harness.drain()


async def inbox(harness: Harness, token: str, **params: Any) -> dict[str, Any]:
    response = await harness.client.get(
        "/api/v1/me/notifications", params=params, headers=auth(token)
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_inbox_lists_newest_first_with_paging(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    me = token(person.iam_principal_id)
    await deliver(harness, token, person, 5)

    first = await inbox(harness, me, limit=2)
    second = await inbox(harness, me, limit=2, cursor=first["nextCursor"])
    third = await inbox(harness, me, limit=2, cursor=second["nextCursor"])

    assert [i["title"] for i in first["items"]] == ["N5", "N4"]
    assert [i["title"] for i in second["items"]] == ["N3", "N2"]
    assert [i["title"] for i in third["items"]] == ["N1"]
    assert third["nextCursor"] is None
    assert first["unreadCount"] == 5


async def test_read_marks(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    me = token(person.iam_principal_id)
    await deliver(harness, token, person, 3)
    items = (await inbox(harness, me))["items"]

    read = await harness.client.post(
        f"/api/v1/me/notifications/{items[0]['id']}:read", headers=auth(me)
    )
    assert read.status_code == 200
    assert read.json()["readAt"] is not None
    unread = await inbox(harness, me, unreadOnly="true")
    assert [i["title"] for i in unread["items"]] == ["N2", "N1"]
    assert unread["unreadCount"] == 2

    all_read = await harness.client.post("/api/v1/me/notifications:read-all", headers=auth(me))
    assert all_read.json() == {"marked": 2}
    assert (await inbox(harness, me))["unreadCount"] == 0


async def test_one_cannot_see_or_mark_anothers_inbox(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    owner, stranger = make_person(), make_person()
    await deliver(harness, token, owner, 1)
    item = (await inbox(harness, token(owner.iam_principal_id)))["items"][0]

    assert (await inbox(harness, token(stranger.iam_principal_id)))["items"] == []
    # Same identity in another tenant is another inbox.
    assert (await inbox(harness, token(owner.iam_principal_id, tenant=uuid.uuid4())))["items"] == []
    marked = await harness.client.post(
        f"/api/v1/me/notifications/{item['id']}:read",
        headers=auth(token(stranger.iam_principal_id)),
    )
    assert marked.status_code == 404


async def test_inbox_requires_the_read_scope(harness: Harness, token: Callable[..., str]) -> None:
    response = await harness.client.get(
        "/api/v1/me/notifications", headers=auth(token(scopes=(SCOPE_SEND,)))
    )
    assert response.status_code == 403


async def test_concurrent_appends_get_contiguous_sequence(
    harness: Harness, tenant_id: uuid.UUID
) -> None:
    principal = uuid.uuid4()
    ids = [uuid.uuid4() for _ in range(20)]
    async with harness.sessions() as session, session.begin():
        for notification_id in ids:
            session.add(
                Notification(
                    id=notification_id,
                    tenant_id=tenant_id,
                    sender_id=uuid.uuid4(),
                    sender_type="service_account",
                    dedup_key=notification_id.hex,
                    request_hash="x",
                    recipient_kind="principal",
                    recipient_id=uuid.uuid4(),
                    type="t",
                    title="t",
                    body="",
                    links=[],
                    actions=[],
                )
            )

    async def append(notification_id: uuid.UUID) -> int:
        async with harness.sessions() as session, session.begin():
            return await append_to_inbox(session, tenant_id, principal, notification_id)

    seqs = await asyncio.gather(*(append(i) for i in ids))
    repeated = await append(ids[0])

    assert sorted(seqs) == list(range(1, 21))
    assert repeated == seqs[0]
    async with harness.sessions() as session:
        stored = list(await session.scalars(select(InboxItem.seq).order_by(InboxItem.seq)))
    assert stored == list(range(1, 21))
