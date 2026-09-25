"""A decision from a channel: the person's link in IAM and their decision in the core.

Two parties, each with its own contract (TAI-ADR-0050, CP-ADR-0070):

- **IAM** (``iam-service``, ``channels/routes.py``) owns the link between a
  person and their messenger account. The service, as a channel adapter
  (service account with ``iam:channel-links`` in audience ``iam``), confirms a
  link with the code the person sent to the bot, and exchanges the assertion
  "this account pressed the button just now" for a one-decision token of that
  person: ``purposeRef = approval:<uuid>``, scope ``control-plane:decide``,
  one minute.
- **The Control Plane** decides: ``POST /approvals/{id}:approve|:reject`` with
  that token and ``Idempotency-Key`` = the channel's callback id, so a
  redelivered press replays the first decision instead of making another.
  Reading whether the approval is still open goes under the service's own
  identity.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
from control_plane_client import ControlPlaneClient, ControlPlaneError
from platform_auth import ServiceTokenProvider, TokenVerifier, TrustedAuthContext

logger = logging.getLogger("notification_service.decisions")


def purpose_ref(approval_id: uuid.UUID) -> str:
    """The subject of a one-decision token, in the form the core parses (CP-ADR-0070)."""
    return f"approval:{approval_id}"


# --- IAM: channel links --------------------------------------------------------


@dataclass(frozen=True)
class Linked:
    """A confirmed link: whose account it is, in which IAM tenant."""

    tenant_id: uuid.UUID
    iam_principal_id: uuid.UUID
    link_id: uuid.UUID


class LinkRefused(Exception):
    """IAM refused (``detail`` of its answer is the stable code)."""

    def __init__(self, status: int, code: str) -> None:
        super().__init__(f"{status}: {code}")
        self.status = status
        self.code = code


class LinksUnavailable(Exception):
    """IAM could not answer, or the service is not configured for it."""


class ChannelLinks(Protocol):
    async def confirm(self, channel: str, code: str, external_subject: str) -> Linked: ...

    async def exchange(
        self, tenant_id: uuid.UUID, channel: str, external_subject: str, purpose: str
    ) -> str:
        """A one-decision token of the person linked to ``external_subject``."""
        ...


class UnconfiguredChannelLinks:
    async def confirm(self, channel: str, code: str, external_subject: str) -> Linked:
        raise LinksUnavailable("iam_not_configured")

    async def exchange(
        self, tenant_id: uuid.UUID, channel: str, external_subject: str, purpose: str
    ) -> str:
        raise LinksUnavailable("iam_not_configured")


class IamChannelLinks:
    """``POST /api/v1/tenants/{t}/channel-links:confirm`` and ``…/channel-assertions:exchange``.

    The tenant of the path is the service account's own (IAM checks it against
    the token): read from its token, verified by platform-auth-sdk.
    """

    def __init__(
        self,
        iam_url: str,
        tokens: ServiceTokenProvider,
        verifier: TokenVerifier,
        *,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._tokens = tokens
        self._verifier = verifier
        self._identity: TrustedAuthContext | None = None
        self._http = httpx.AsyncClient(
            base_url=iam_url.rstrip("/"), timeout=timeout, transport=transport
        )

    async def aclose(self) -> None:
        await self._http.aclose()
        await self._tokens.aclose()

    async def _tenant(self) -> uuid.UUID:
        if self._identity is None:
            self._identity = await self._verifier.verify(await self._tokens())
        return self._identity.tenant_id

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self._http.post(
                path, json=body, headers={"Authorization": f"Bearer {await self._tokens()}"}
            )
        except httpx.HTTPError as exc:
            raise LinksUnavailable(type(exc).__name__) from exc
        if response.status_code >= 500:
            raise LinksUnavailable(f"iam_{response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise LinksUnavailable("unexpected_response") from exc
        if response.status_code >= 400:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            code = detail if isinstance(detail, str) else "refused"
            if response.status_code == 401:
                # The service's own token went stale or was revoked.
                self._tokens.forget()
                raise LinksUnavailable(f"iam_401_{code}")
            raise LinkRefused(response.status_code, code)
        return payload  # type: ignore[no-any-return]

    async def confirm(self, channel: str, code: str, external_subject: str) -> Linked:
        tenant_id = await self._tenant()
        body = await self._post(
            f"/api/v1/tenants/{tenant_id}/channel-links:confirm",
            {"channel": channel, "code": code, "externalSubject": external_subject},
        )
        return Linked(
            tenant_id=tenant_id,
            iam_principal_id=uuid.UUID(str(body["principalId"])),
            link_id=uuid.UUID(str(body["linkId"])),
        )

    async def exchange(
        self, tenant_id: uuid.UUID, channel: str, external_subject: str, purpose: str
    ) -> str:
        body = await self._post(
            f"/api/v1/tenants/{tenant_id}/channel-assertions:exchange",
            {"channel": channel, "externalSubject": external_subject, "purposeRef": purpose},
        )
        return str(body["accessToken"])


# --- Control Plane: approvals --------------------------------------------------


class Approvals(Protocol):
    async def get(self, approval_id: uuid.UUID) -> dict[str, Any] | None:
        """``ApprovalOut`` read by the service; ``None`` when it cannot be read."""
        ...

    async def decide(
        self, approval_id: uuid.UUID, *, approve: bool, token: str, idempotency_key: str
    ) -> dict[str, Any]:
        """``ApprovalOut`` after the decision; raises ``ControlPlaneError`` on refusal."""
        ...


class UnconfiguredApprovals:
    """Stand without a Control Plane: nothing can be decided from a channel."""

    async def get(self, approval_id: uuid.UUID) -> dict[str, Any] | None:
        return None

    async def decide(
        self, approval_id: uuid.UUID, *, approve: bool, token: str, idempotency_key: str
    ) -> dict[str, Any]:
        raise ControlPlaneError("control_plane_not_configured", "Control Plane is not configured")


class ControlPlaneApprovals:
    def __init__(
        self,
        server_url: str,
        service_client: ControlPlaneClient | None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._server_url = server_url
        self._service = service_client
        self._transport = transport

    async def get(self, approval_id: uuid.UUID) -> dict[str, Any] | None:
        if self._service is None:
            return None
        # ``GET /approvals/{id}`` (``ApprovalOut``); the client has no method for
        # it. Only a hint: the decision itself is checked by the core anyway.
        try:
            return await self._service._request("GET", f"/approvals/{approval_id}")
        except ControlPlaneError as exc:
            logger.info("approval %s not read before the decision: %s", approval_id, exc.code)
            return None

    async def decide(
        self, approval_id: uuid.UUID, *, approve: bool, token: str, idempotency_key: str
    ) -> dict[str, Any]:
        verb = "approve" if approve else "reject"
        # The person's token, for this one request: the client is theirs too.
        async with ControlPlaneClient(
            self._server_url,
            token,
            transport=self._transport,
            user_agent="notification-service/0.1",
        ) as client:
            return await client._request(
                "POST",
                f"/approvals/{approval_id}:{verb}",
                json_body={},
                headers={"Idempotency-Key": idempotency_key},
                idempotent=True,
            )
