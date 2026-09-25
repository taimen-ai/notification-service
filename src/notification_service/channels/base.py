"""The channel contract: every adapter turns one delivery into one send.

Adding a channel adds an adapter and never changes the sending contract
(FR-003): a sender names recipients and content, never channels.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class OutboundMessage:
    delivery_id: uuid.UUID
    notification_id: uuid.UUID
    tenant_id: uuid.UUID
    channel: str
    recipient_kind: str
    recipient_id: uuid.UUID
    iam_principal_id: uuid.UUID | None
    # Channel address of the recipient (email address, group chat id); empty
    # for channels that address by identity (the inbox).
    address: str
    type: str
    title: str
    body: str
    links: list[dict[str, Any]] = field(default_factory=list)
    actions: list[dict[str, Any]] = field(default_factory=list)
    created_at: datetime | None = None


@dataclass(frozen=True)
class SendResult:
    external_id: str | None = None


class DeliveryError(Exception):
    """A send that did not happen. ``reason`` goes to the delivery journal."""

    permanent = False

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason[:500]


class TransientDeliveryError(DeliveryError):
    """Worth retrying: timeouts, 4xx of SMTP, rate limits."""


class PermanentDeliveryError(DeliveryError):
    """Retrying will not help: the message itself was refused."""

    permanent = True


class RecipientUnreachable(PermanentDeliveryError):
    """The address itself is dead (mailbox refused, bot blocked).

    Besides failing the delivery this disables the address, so the channel is
    no longer selected for the recipient until they set it again.
    """


class Channel(Protocol):
    name: str
    # A push channel interrupts the recipient: quiet hours apply to it.
    push: bool
    # The recipient is reachable only with an address stored for this channel.
    needs_address: bool

    async def send(self, message: OutboundMessage) -> SendResult: ...
