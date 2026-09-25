"""Contract: the Control Plane directory adapter against the Control Plane's own API models.

Responses are built from ``control_plane.api.v1.schemas`` exactly as the
Control Plane serializes them (camelCase, ``{"items": ...}``), so a change of
those models breaks this test rather than production.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from control_plane.api.v1.schemas import IamBindingOut, PageOut, PrincipalOut
from control_plane_client import ControlPlaneClient

from notification_service.directory import (
    ControlPlaneDirectory,
    DirectoryUnavailable,
    UnknownRecipient,
)

ISSUER = "https://iam.test"
NOW = datetime(2026, 9, 25, tzinfo=UTC)


def binding(principal_id: uuid.UUID, iam_tenant: uuid.UUID, **fields: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": uuid.uuid4(),
        "principal_id": principal_id,
        "issuer": ISSUER,
        "iam_tenant_id": iam_tenant,
        "iam_principal_id": uuid.uuid4(),
        "permissions": ["tasks.read"],
        "status": "active",
        "revoked_at": None,
        "last_used_at": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(fields)
    return IamBindingOut(**values).model_dump(mode="json", by_alias=True)


def principal(principal_id: uuid.UUID, status: str = "active") -> dict[str, Any]:
    return PrincipalOut(
        id=principal_id,
        tenant_id=uuid.uuid4(),
        kind="human",
        display_name="Someone",
        status=status,
        metadata_json={},
        created_at=NOW,
        updated_at=NOW,
    ).model_dump(mode="json", by_alias=True)


def page(items: list[dict[str, Any]], next_cursor: str | None = None) -> dict[str, Any]:
    return PageOut(items=items, next_cursor=next_cursor).model_dump(mode="json", by_alias=True)


def error(code: str) -> dict[str, Any]:
    return {"error": {"code": code, "message": code, "details": {}}}


Handler = Callable[[httpx.Request], httpx.Response]


def directory(handler: Handler) -> ControlPlaneDirectory:
    client = ControlPlaneClient(
        "http://cp.test", "cp_test_key", transport=httpx.MockTransport(handler)
    )
    return ControlPlaneDirectory(client, iam_issuer=ISSUER)


async def test_principal_resolves_to_the_active_identity_of_the_tenant() -> None:
    target, tenant = uuid.uuid4(), uuid.uuid4()
    wanted = binding(target, tenant)
    bindings = [
        binding(target, uuid.uuid4()),  # another IAM tenant
        binding(target, tenant, status="revoked"),
        binding(target, tenant, issuer="https://other-iam.test"),
        wanted,
    ]
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"items": bindings})

    addressee = await directory(handler).principal(tenant, target)

    assert addressee.iam_principal_id == uuid.UUID(wanted["iamPrincipalId"])
    assert seen[0].url.path == f"/api/v1/principals/{target}/iam-bindings"
    assert seen[0].headers["authorization"] == "Bearer cp_test_key"


async def test_principal_without_binding_in_tenant_is_unreachable() -> None:
    target = uuid.uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [binding(target, uuid.uuid4())]})

    addressee = await directory(handler).principal(uuid.uuid4(), target)
    assert addressee.iam_principal_id is None


@pytest.mark.parametrize(
    ("status", "code", "raised"),
    [
        (404, "not_found", UnknownRecipient),
        (403, "permission_denied", DirectoryUnavailable),
        (503, "dependency_unavailable", DirectoryUnavailable),
    ],
)
async def test_errors_are_classified(status: int, code: str, raised: type[Exception]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=error(code))

    with pytest.raises(raised):
        await directory(handler).principal(uuid.uuid4(), uuid.uuid4())


async def test_role_holders_follow_pages_and_skip_inactive() -> None:
    tenant, role, workspace = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    alice, bob, gone = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    pages = {
        None: page([principal(alice), principal(gone, status="disabled")], "c2"),
        "c2": page([principal(bob)]),
    }
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/iam-bindings"):
            owner = uuid.UUID(request.url.path.split("/")[-2])
            return httpx.Response(200, json={"items": [binding(owner, tenant)]})
        return httpx.Response(200, json=pages[request.url.params.get("cursor")])

    holders = await directory(handler).role_holders(tenant, role, workspace)

    assert [h.principal_id for h in holders] == [alice, bob]
    assert all(h.iam_principal_id is not None for h in holders)
    role_calls = [r for r in requests if r.url.path == f"/api/v1/roles/{role}/principals"]
    assert [r.url.params.get("workspaceId") for r in role_calls] == [str(workspace)] * 2


async def test_unknown_role_is_unknown_recipient() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=error("not_found"))

    with pytest.raises(UnknownRecipient):
        await directory(handler).role_holders(uuid.uuid4(), uuid.uuid4(), uuid.uuid4())
