"""The delivery worker: retries, permanent failures, isolation of channels, leases."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from sqlalchemy import select, update

from notification_service.channels import (
    OutboundMessage,
    PermanentDeliveryError,
    RecipientUnreachable,
    SendResult,
    TransientDeliveryError,
)
from notification_service.db import utcnow
from notification_service.models import ChannelAddress, Delivery
from notification_service.worker import DeliveryWorker, backoff

from conftest import Harness, Person, auth, notification, settings_for, to_principal


class ScriptedChannel:
    """Fails according to a script, then succeeds."""

    push = True
    needs_address = True

    def __init__(self, name: str, script: list[Exception | None]) -> None:
        self.name = name
        self.script = list(script)
        self.calls: list[OutboundMessage] = []

    async def send(self, message: OutboundMessage) -> SendResult:
        self.calls.append(message)
        outcome = self.script.pop(0) if self.script else None
        if outcome is not None:
            raise outcome
        return SendResult(external_id=f"{self.name}-{len(self.calls)}")


async def prepare(
    harness: Harness, token: Callable[..., str], person: Person, channel: str
) -> None:
    async with harness.sessions() as session, session.begin():
        session.add(
            ChannelAddress(
                tenant_id=person.tenant_id,
                principal_id=person.iam_principal_id,
                channel=channel,
                address=f"{channel}-address",
            )
        )


async def send(harness: Harness, token: str, person: Person, key: str = "k") -> dict[str, Any]:
    response = await harness.client.post(
        "/api/v1/notifications",
        json=notification(to_principal(person)),
        headers={**auth(token), "Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    return response.json()


async def journal(harness: Harness, notification_id: str) -> dict[str, Delivery]:
    async with harness.sessions() as session:
        rows = await session.scalars(
            select(Delivery).where(Delivery.notification_id == uuid.UUID(notification_id))
        )
        return {row.channel: row for row in rows}


async def test_transient_failures_are_retried_until_delivered(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    flaky = ScriptedChannel(
        "chat", [TransientDeliveryError("timeout"), TransientDeliveryError("429")]
    )
    person = make_person()
    async with harness_factory(channels=[flaky]) as harness:
        await prepare(harness, token, person, "chat")
        sent = await send(harness, token(), person)
        await harness.drain()
        rows = await journal(harness, sent["id"])

    assert rows["chat"].status == "delivered"
    assert rows["chat"].attempts == 3
    assert rows["chat"].last_error is None
    assert rows["chat"].external_id == "chat-3"
    assert [m.address for m in flaky.calls] == ["chat-address"] * 3


async def test_retries_are_bounded(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    dead = ScriptedChannel("chat", [TransientDeliveryError("timeout")] * 10)
    person = make_person()
    async with harness_factory(
        settings=settings_for(delivery_max_attempts=4), channels=[dead]
    ) as harness:
        await prepare(harness, token, person, "chat")
        sent = await send(harness, token(), person)
        await harness.drain()
        rows = await journal(harness, sent["id"])

    assert rows["chat"].status == "failed"
    assert rows["chat"].attempts == 4
    assert rows["chat"].last_error == "retries_exhausted: timeout"
    assert len(dead.calls) == 4


async def test_permanent_failure_is_not_retried_and_spares_other_channels(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    broken = ScriptedChannel("chat", [PermanentDeliveryError("message_too_long")])
    person = make_person()
    async with harness_factory(channels=[broken]) as harness:
        await prepare(harness, token, person, "chat")
        sent = await send(harness, token(), person)
        await harness.drain()
        rows = await journal(harness, sent["id"])

    assert (rows["chat"].status, rows["chat"].attempts) == ("failed", 1)
    assert rows["chat"].last_error == "message_too_long"
    assert rows["web"].status == "delivered"


async def test_crashing_adapter_counts_as_transient(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    buggy = ScriptedChannel("chat", [RuntimeError("bug")])
    person = make_person()
    async with harness_factory(channels=[buggy]) as harness:
        await prepare(harness, token, person, "chat")
        sent = await send(harness, token(), person)
        await harness.drain()
        rows = await journal(harness, sent["id"])

    assert (rows["chat"].status, rows["chat"].attempts) == ("delivered", 2)


async def test_unreachable_address_is_disabled_and_no_longer_selected(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    blocked = ScriptedChannel("chat", [RecipientUnreachable("blocked_by_user")])
    person = make_person()
    async with harness_factory(channels=[blocked]) as harness:
        await prepare(harness, token, person, "chat")
        first = await send(harness, token(), person, "one")
        await harness.drain()
        second = await send(harness, token(), person, "two")
        prefs = await harness.client.get(
            "/api/v1/me/notification-preferences", headers=auth(token(person.iam_principal_id))
        )

    assert first["deliveries"][0]["channel"] in {"web", "chat"}
    assert {d["channel"] for d in second["deliveries"]} == {"web"}
    [address] = prefs.json()["addresses"]
    assert address["disabledReason"] == "blocked_by_user"


async def test_expired_lease_is_taken_over_and_late_outcome_is_dropped(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    person = make_person()
    sent = await send(harness, token(), person)
    delivery_id = uuid.UUID(sent["deliveries"][0]["id"])
    # A worker claimed the delivery (attempt 1) and died.
    async with harness.sessions() as session, session.begin():
        await session.execute(
            update(Delivery)
            .where(Delivery.id == delivery_id)
            .values(status="sending", attempts=1, locked_until=utcnow() - timedelta(seconds=1))
        )

    settings = settings_for()
    rescuer = DeliveryWorker(harness.sessions, harness.app.state.channels, settings)
    assert await rescuer.run_once() == 1

    async with harness.sessions() as session:
        row = await session.get(Delivery, delivery_id)
    assert row is not None
    assert (row.status, row.attempts) == ("delivered", 2)

    # The first worker wakes up and reports its attempt: fenced out.
    late = DeliveryWorker(harness.sessions, harness.app.state.channels, settings)
    await late._failed(row, 1, PermanentDeliveryError("late"), None)
    async with harness.sessions() as session:
        again = await session.get(Delivery, delivery_id)
    assert again is not None and again.status == "delivered"


async def test_postponed_delivery_waits(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    sent = await send(harness, token(), person)
    async with harness.sessions() as session, session.begin():
        await session.execute(
            update(Delivery)
            .where(Delivery.notification_id == uuid.UUID(sent["id"]))
            .values(next_attempt_at=utcnow() + timedelta(hours=1))
        )
    assert await harness.worker.run_once() == 0


def test_backoff_is_exponential_and_capped() -> None:
    assert [backoff(n, base=5, cap=60).total_seconds() for n in (1, 2, 3, 4, 5)] == [
        5,
        10,
        20,
        40,
        60,
    ]
