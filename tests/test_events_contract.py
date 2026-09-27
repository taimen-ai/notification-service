"""Contract: what the event consumer reads from the Control Plane, against the core's own code.

The fields the rules take from tasks and principals are checked against the
API models, and the reads of ``ControlPlaneCore`` against responses serialized
by those models. What rules may read from event payloads is checked against
the catalog snapshot (``test_notification_rules_contract``). A change on the
core's side breaks this test rather than notifications in production.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest
from control_plane.api.v1.org import router as org_router
from control_plane.api.v1.schemas import PrincipalOut, RoleHolderOut, TaskOut
from control_plane_client import ControlPlaneClient, ControlPlaneError

from notification_service.events import ControlPlaneCore

NOW = datetime(2026, 9, 25, tzinfo=UTC)


def _wire_fields(model: type) -> set[str]:
    return set(model.model_json_schema(by_alias=True)["properties"])  # type: ignore[attr-defined]


def test_task_and_principal_fields_read_are_in_the_api_models() -> None:
    # Task fields the recipients ``taskOwner`` / ``taskAssignee`` read.
    assert {"ownerId", "assigneeId"} <= _wire_fields(TaskOut)
    assert {"displayName"} <= _wire_fields(PrincipalOut)
    assert {"id", "status"} <= _wire_fields(RoleHolderOut)


def test_role_holders_route_exists_with_a_workspace_filter() -> None:
    [route] = [r for r in org_router.routes if r.path == "/roles/{role_id}/principals"]
    assert route.methods == {"GET"}  # type: ignore[attr-defined]
    params = {p.alias for p in route.dependant.query_params}  # type: ignore[attr-defined]
    assert {"workspaceId", "cursor"} <= params


Handler = Callable[[httpx.Request], httpx.Response]


def core(handler: Handler) -> ControlPlaneCore:
    return ControlPlaneCore(
        ControlPlaneClient("http://cp.test", "cp_test_key", transport=httpx.MockTransport(handler))
    )


def error(status: int, code: str) -> httpx.Response:
    return httpx.Response(status, json={"error": {"code": code, "message": code, "details": {}}})


async def test_principal_name_from_principal_out() -> None:
    target = uuid.uuid4()
    body = PrincipalOut(
        id=target,
        tenant_id=uuid.uuid4(),
        kind="human",
        display_name="Finance Director",
        status="active",
        metadata_json={},
        created_at=NOW,
        updated_at=NOW,
    ).model_dump(mode="json", by_alias=True)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=body)

    assert await core(handler).principal_name(str(target)) == "Finance Director"
    assert seen[0].url.path == f"/api/v1/principals/{target}"


async def test_principal_name_is_optional() -> None:
    assert await core(lambda _: error(403, "permission_denied")).principal_name("x") is None


@pytest.mark.parametrize(("status", "code"), [(404, "not_found"), (403, "permission_denied")])
async def test_task_that_cannot_be_read_is_none(status: int, code: str) -> None:
    assert await core(lambda _: error(status, code)).task(str(uuid.uuid4())) is None


async def test_task_read_failure_propagates_for_a_retry() -> None:
    with pytest.raises(ControlPlaneError):
        await core(lambda _: error(503, "dependency_unavailable")).task(str(uuid.uuid4()))
