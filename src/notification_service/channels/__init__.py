"""Channel adapters and the registry of those configured on this stand."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from notification_service.channels.base import (
    Channel,
    DeliveryError,
    OutboundMessage,
    PermanentDeliveryError,
    RecipientUnreachable,
    SendResult,
    TransientDeliveryError,
)


class ChannelRegistry:
    """The channels this stand can deliver over, in a stable order."""

    def __init__(self, channels: Iterable[Channel]) -> None:
        self._channels = {channel.name: channel for channel in channels}

    def get(self, name: str) -> Channel | None:
        return self._channels.get(name)

    def names(self) -> list[str]:
        return list(self._channels)

    def __iter__(self) -> Iterator[Channel]:
        return iter(self._channels.values())


__all__ = [
    "Channel",
    "ChannelRegistry",
    "DeliveryError",
    "OutboundMessage",
    "PermanentDeliveryError",
    "RecipientUnreachable",
    "SendResult",
    "TransientDeliveryError",
]
