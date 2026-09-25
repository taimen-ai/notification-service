"""Who a notification reaches: principals and role holders, from the Control Plane.

The Control Plane owns principals, roles and the bindings of its principals to
IAM identities; this service only reads them with its own service identity.
A person is addressed by their Control Plane principal and reached by the IAM
identity bound to it in the sender's IAM tenant — the identity that reads the
inbox. A principal without such a binding is known but unreachable: a binding
in another tenant never leaks a notification across tenants.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from control_plane_client import ControlPlaneClient, ControlPlaneError, NotFoundError
from platform_auth import ServiceTokenProvider


@dataclass(frozen=True)
class Addressee:
    principal_id: uuid.UUID
    iam_principal_id: uuid.UUID | None


class UnknownRecipient(Exception):
    """The addressed principal or role does not exist for this service."""


class DirectoryUnavailable(Exception):
    """The Control Plane could not answer; the caller may retry."""


class Directory(Protocol):
    async def principal(self, tenant_id: uuid.UUID, principal_id: uuid.UUID) -> Addressee: ...

    async def role_holders(
        self, tenant_id: uuid.UUID, role_id: uuid.UUID, workspace_id: uuid.UUID
    ) -> list[Addressee]: ...


class UnconfiguredDirectory:
    """Stand without a Control Plane connection: addressing people is impossible."""

    async def principal(self, tenant_id: uuid.UUID, principal_id: uuid.UUID) -> Addressee:
        raise DirectoryUnavailable("control_plane_not_configured")

    async def role_holders(
        self, tenant_id: uuid.UUID, role_id: uuid.UUID, workspace_id: uuid.UUID
    ) -> list[Addressee]:
        raise DirectoryUnavailable("control_plane_not_configured")


class ServiceCredential:
    """``control_plane_client.CredentialProvider`` over the SDK's client-credentials token."""

    def __init__(self, provider: ServiceTokenProvider) -> None:
        self._provider = provider

    async def token(self) -> str:
        return await self._provider()

    async def refresh(self) -> str:
        self._provider.forget()
        return await self._provider()

    @property
    def refreshable(self) -> bool:
        return True


def pick_identity(
    bindings: list[dict[str, Any]], *, iam_tenant_id: uuid.UUID, issuer: str
) -> uuid.UUID | None:
    """The active IAM identity of a principal in one IAM tenant (``IamBindingOut`` items)."""
    for binding in bindings:
        if binding.get("status") != "active":
            continue
        if str(binding.get("iamTenantId")) != str(iam_tenant_id):
            continue
        if issuer and binding.get("issuer") != issuer:
            continue
        return uuid.UUID(str(binding["iamPrincipalId"]))
    return None


class ControlPlaneDirectory:
    def __init__(self, client: ControlPlaneClient, *, iam_issuer: str) -> None:
        self._client = client
        self._issuer = iam_issuer

    async def principal(self, tenant_id: uuid.UUID, principal_id: uuid.UUID) -> Addressee:
        try:
            body = await self._client.list_iam_bindings(str(principal_id))
        except NotFoundError as exc:
            raise UnknownRecipient(f"principal {principal_id}") from exc
        except ControlPlaneError as exc:
            raise DirectoryUnavailable(exc.code) from exc
        identity = pick_identity(
            list(body.get("items", [])), iam_tenant_id=tenant_id, issuer=self._issuer
        )
        return Addressee(principal_id=principal_id, iam_principal_id=identity)

    async def role_holders(
        self, tenant_id: uuid.UUID, role_id: uuid.UUID, workspace_id: uuid.UUID
    ) -> list[Addressee]:
        holders: list[Addressee] = []
        for item in await self._role_principals(role_id, workspace_id):
            if item.get("status") != "active":
                continue
            holders.append(await self.principal(tenant_id, uuid.UUID(str(item["id"]))))
        return holders

    async def _role_principals(
        self, role_id: uuid.UUID, workspace_id: uuid.UUID
    ) -> list[dict[str, Any]]:
        # ``GET /roles/{id}/principals?workspaceId=`` (plan of the notifications
        # feature, task N002): a page of ``PrincipalOut``. The client has no
        # method for it yet, hence the transport call.
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"workspaceId": str(workspace_id)}
            if cursor:
                params["cursor"] = cursor
            try:
                page = await self._client._request(
                    "GET", f"/roles/{role_id}/principals", params=params
                )
            except NotFoundError as exc:
                raise UnknownRecipient(f"role {role_id}") from exc
            except ControlPlaneError as exc:
                raise DirectoryUnavailable(exc.code) from exc
            items.extend(page.get("items", []))
            cursor = page.get("nextCursor")
            if not cursor:
                return items
