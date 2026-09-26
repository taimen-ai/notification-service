"""The channel as an entry into the person's assistant conversation (TAI-ADR-0051 §7).

Free text a linked person writes to the bot in the private chat, and presses of
the harness's own confirmation buttons (``data.kind = "harness_approval"``),
go to the launcher of personal harnesses:

    POST {launcher}/_launcher/internal/principals/{iamPrincipalId}/inbound
    {"channel": "telegram", "messageId": "...", "text": "..."}
    {"channel": "telegram", "messageId": "...", "approval": {"id": "...", "decision": "approve"}}

with the service's own token: client credentials exchanged at IAM for audience
``human-harness``, scope ``harness:inbound``. The launcher wakes the person's
harness and appends the message to the single conversation; the answer comes
back through ``notify.send`` — nothing is answered here synchronously.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Literal, Protocol

import httpx
from platform_auth.service_identity import ServiceTokenProvider

logger = logging.getLogger("notification_service.harness")

Delivery = Literal["accepted", "no_harness", "unavailable", "unconfigured"]


class HarnessInbound(Protocol):
    async def deliver(self, principal_id: uuid.UUID, body: dict[str, Any]) -> Delivery: ...

    async def aclose(self) -> None: ...


class UnconfiguredHarness:
    """No launcher configured: the channel stays notifications-only."""

    async def deliver(self, principal_id: uuid.UUID, body: dict[str, Any]) -> Delivery:
        return "unconfigured"

    async def aclose(self) -> None:
        return None


class LauncherInbound:
    def __init__(
        self,
        launcher_url: str,
        tokens: ServiceTokenProvider,
        *,
        timeout: float = 40.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        # The launcher may start a sleeping harness first (cold start budget 30 s).
        self._tokens = tokens
        self._http = httpx.AsyncClient(
            base_url=launcher_url.rstrip("/"), timeout=timeout, transport=transport
        )

    async def aclose(self) -> None:
        await self._http.aclose()
        await self._tokens.aclose()

    async def deliver(self, principal_id: uuid.UUID, body: dict[str, Any]) -> Delivery:
        path = f"/_launcher/internal/principals/{principal_id}/inbound"
        try:
            response = await self._http.post(
                path, json=body, headers={"Authorization": f"Bearer {await self._tokens()}"}
            )
        except httpx.HTTPError as exc:
            logger.warning("harness inbound %s: launcher unreachable (%s)", principal_id, exc)
            return "unavailable"
        except Exception as exc:  # the token exchange at IAM
            logger.warning("harness inbound %s: no service token (%s)", principal_id, exc)
            return "unavailable"
        if response.status_code == 401:
            # The service's own token went stale or was revoked: the next call exchanges anew.
            self._tokens.forget()
            return "unavailable"
        if response.status_code == 404:
            return "no_harness"
        if response.status_code >= 300:
            logger.warning(
                "harness inbound %s: launcher answered %s", principal_id, response.status_code
            )
            return "unavailable"
        return "accepted"
