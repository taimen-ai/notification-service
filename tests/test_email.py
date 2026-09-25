"""The email channel against an SMTP server (aiosmtpd) speaking RFC 5321."""

from __future__ import annotations

import email
import email.policy
import socket
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from aiosmtpd.controller import Controller
from sqlalchemy import select

from notification_service.channels.base import OutboundMessage
from notification_service.channels.email import render
from notification_service.models import Delivery

from conftest import Harness, Person, auth, notification, settings_for, to_principal


class Mailbox:
    def __init__(self) -> None:
        self.messages: list[email.message.EmailMessage] = []
        self.refuse: set[str] = set()
        self.defer: set[str] = set()

    async def handle_RCPT(
        self, server: Any, session: Any, envelope: Any, address: str, rcpt_options: Any
    ) -> str:
        if address in self.refuse:
            return "550 5.1.1 mailbox unavailable"
        if address in self.defer:
            return "450 4.2.1 try later"
        envelope.rcpt_tos.append(address)
        return "250 OK"

    async def handle_DATA(self, server: Any, session: Any, envelope: Any) -> str:
        self.messages.append(
            email.message_from_bytes(envelope.content, policy=email.policy.default)  # type: ignore[arg-type]
        )
        return "250 Message accepted"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def smtp() -> Iterator[tuple[Mailbox, int]]:
    mailbox = Mailbox()
    port = free_port()
    controller = Controller(mailbox, hostname="127.0.0.1", port=port)
    controller.start()
    try:
        yield mailbox, port
    finally:
        controller.stop()


def smtp_settings(port: int, **changes: Any) -> Any:
    return settings_for(
        email_mode="smtp",
        smtp_host="127.0.0.1",
        smtp_port=port,
        smtp_starttls=False,
        email_from="notify@example.com",
        **changes,
    )


async def set_email(harness: Harness, token: str, address: str) -> None:
    response = await harness.client.patch(
        "/api/v1/me/notification-preferences", json={"email": address}, headers=auth(token)
    )
    assert response.status_code == 200, response.text


async def send(harness: Harness, token: str, person: Person, key: str) -> dict[str, Any]:
    response = await harness.client.post(
        "/api/v1/notifications",
        json=notification(to_principal(person)),
        headers={**auth(token), "Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    return response.json()


async def email_delivery(harness: Harness, notification_id: str) -> Delivery:
    async with harness.sessions() as session:
        row = await session.scalar(
            select(Delivery).where(
                Delivery.notification_id == uuid.UUID(notification_id), Delivery.channel == "email"
            )
        )
    assert row is not None
    return row


async def test_email_is_sent_over_smtp(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
    smtp: tuple[Mailbox, int],
) -> None:
    mailbox, port = smtp
    person = make_person()
    async with harness_factory(settings=smtp_settings(port)) as harness:
        await set_email(harness, token(person.iam_principal_id), "person@example.com")
        sent = await send(harness, token(), person, "k")
        await harness.drain()
        row = await email_delivery(harness, sent["id"])

    assert row.status == "delivered"
    [message] = mailbox.messages
    assert message["To"] == "person@example.com"
    assert message["From"] == "notify@example.com"
    assert message["Subject"] == "Review requested"
    content = message.get_content()
    assert "A change waits for your review." in content
    assert "Open: https://example.test/items/1" in content


async def test_refused_mailbox_fails_and_disables_the_address(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
    smtp: tuple[Mailbox, int],
) -> None:
    mailbox, port = smtp
    mailbox.refuse.add("gone@example.com")
    person = make_person()
    me = token(person.iam_principal_id)
    async with harness_factory(settings=smtp_settings(port)) as harness:
        await set_email(harness, me, "gone@example.com")
        sent = await send(harness, token(), person, "k")
        await harness.drain()
        row = await email_delivery(harness, sent["id"])
        after = await send(harness, token(), person, "k2")

        # Setting the address again re-enables the channel.
        await set_email(harness, me, "gone@example.com")
        again = await send(harness, token(), person, "k3")

    assert (row.status, row.attempts) == ("failed", 1)
    assert row.last_error == "smtp_recipient_refused: 550"
    assert {d["channel"] for d in after["deliveries"]} == {"web"}
    assert {d["channel"] for d in again["deliveries"]} == {"web", "email"}


async def test_deferral_and_outage_are_retried(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
    smtp: tuple[Mailbox, int],
) -> None:
    mailbox, port = smtp
    mailbox.defer.add("busy@example.com")
    person = make_person()
    async with harness_factory(settings=smtp_settings(port)) as harness:
        await set_email(harness, token(person.iam_principal_id), "busy@example.com")
        sent = await send(harness, token(), person, "k")
        await harness.worker.run_once()
        deferred = await email_delivery(harness, sent["id"])

        mailbox.defer.clear()
        await harness.drain()
        delivered = await email_delivery(harness, sent["id"])

    assert (deferred.status, deferred.last_error) == ("pending", "smtp_recipient_deferred: [450]")
    assert (delivered.status, delivered.attempts) == ("delivered", 2)


async def test_unreachable_server_is_transient(
    harness_factory: Callable[..., Any],
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    person = make_person()
    async with harness_factory(settings=smtp_settings(free_port())) as harness:
        await set_email(harness, token(person.iam_principal_id), "person@example.com")
        sent = await send(harness, token(), person, "k")
        await harness.worker.run_once()
        row = await email_delivery(harness, sent["id"])

    assert row.status == "pending"
    assert row.last_error == "smtp_unavailable: ConnectionRefusedError"


async def test_log_mode_sends_nothing_and_counts_as_delivered(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    caplog: pytest.LogCaptureFixture,
) -> None:
    person = make_person()
    await set_email(harness, token(person.iam_principal_id), "person@example.com")
    caplog.set_level("INFO", logger="notification_service.email")
    sent = await send(harness, token(), person, "k")
    await harness.drain()

    assert (await email_delivery(harness, sent["id"])).status == "delivered"
    assert "email not sent (log mode)" in caplog.text


def test_rendered_subject_is_one_line() -> None:
    message = OutboundMessage(
        delivery_id=uuid.uuid4(),
        notification_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        channel="email",
        recipient_kind="principal",
        recipient_id=uuid.uuid4(),
        iam_principal_id=uuid.uuid4(),
        address="person@example.com",
        type="t",
        title="Title\r\nBcc: victim@example.com",
        body="",
    )
    mail = render(message, sender="notify@example.com")
    assert mail["Bcc"] is None
    assert mail["Subject"] == "Title Bcc: victim@example.com"
