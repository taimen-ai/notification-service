"""Core events as the service reads them, and the seams of the rule-driven consumer.

Events are built from the Control Plane's own model (``EventOut``) and their
payloads are checked against the core's event catalog, so the rules are tested
on what the core actually writes. The journal is served over the core client's
transport; the cursor store is the SDK's SQL store on the service's database.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import jsonschema
from control_plane.api.v1.schemas import EventOut
from control_plane.domain.event_catalog import current_version, schema_for
from platform_auth import TrustedAuthContext
from sqlalchemy import func, select

from notification_service.auth import SCOPE_ADMIN
from notification_service.channels import OutboundMessage, SendResult
from notification_service.events import RuleEventHandler, stored_rules
from notification_service.models import Notification
from notification_service.sending import NotificationSender

from conftest import Harness, Person, auth

NOW = datetime(2026, 9, 25, 12, tzinfo=UTC)
CP_TENANT = uuid.uuid4()
TASK_CREATED = {
    "publicId": "TASK-000001",
    "title": "Unrelated",
    "status": "todo",
    "systemStatusCategory": "open",
    "typeKey": "task",
    "typeVersion": 1,
    "priority": "normal",
    "workspaceId": None,
    "startDate": None,
    "dueDate": None,
    "customFields": False,
    "goalId": None,
    "origin": None,
    "acceptanceChecks": 0,
}


class CaptureChannel:
    """A channel that renders actions (what Telegram is), recording what it got."""

    name = "chat"
    push = False
    needs_address = False

    def __init__(self) -> None:
        self.sent: list[OutboundMessage] = []

    async def send(self, message: OutboundMessage) -> SendResult:
        self.sent.append(message)
        return SendResult()


class FakeCore:
    """Tasks and principal names of the core, counting the task reads."""

    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.names: dict[str, str] = {}
        self.task_reads = 0

    async def task(self, task_id: str) -> dict[str, Any] | None:
        self.task_reads += 1
        return self.tasks.get(task_id)

    async def principal_name(self, principal_id: str) -> str | None:
        return self.names.get(principal_id)


def event(
    event_type: str,
    entity_id: uuid.UUID,
    payload: dict[str, Any],
    *,
    version: int | None = None,
    workspace_id: uuid.UUID | None = None,
    actor_id: uuid.UUID | None = None,
    sequence: int = 1,
) -> dict[str, Any]:
    """A journal event exactly as ``GET /events`` serializes it."""
    version = version or current_version(event_type)
    schema = schema_for(event_type, version)
    assert schema is not None
    jsonschema.validate(payload, schema)
    body = EventOut(
        sequence=sequence,
        id=uuid.uuid4(),
        tenant_id=CP_TENANT,
        event_type=event_type,
        entity_type=event_type.split(".")[0],
        entity_id=entity_id,
        actor_id=actor_id,
        session_id=None,
        correlation_id="corr",
        causation_id=None,
        request_id="req",
        trace_run_id=None,
        workspace_id=workspace_id,
        schema_version=version,
        payload=payload,
        occurred_at=NOW + timedelta(seconds=sequence),
    ).model_dump(mode="json", by_alias=True)
    body["cursor"] = f"c{sequence}"
    return body


def requested(
    approval_id: uuid.UUID,
    *,
    task_id: uuid.UUID | None = None,
    assignee: uuid.UUID | None = None,
    role: uuid.UUID | None = None,
    workspace: uuid.UUID | None = None,
    requester: uuid.UUID | None = None,
    comment: str = "Please decide today",
    sequence: int = 1,
) -> dict[str, Any]:
    return event(
        "approval.requested",
        approval_id,
        {
            "taskId": str(task_id or uuid.uuid4()),
            "artifactId": None,
            "requiredRoleId": str(role) if role else None,
            "assignedPrincipalId": str(assignee) if assignee else None,
            "gate": True,
            "workspaceId": str(workspace) if workspace else None,
            "taskPublicId": "TASK-000123",
            "taskTitle": "Pay supplier invoice",
            "requestedBy": str(requester or uuid.uuid4()),
            "comment": comment,
            "excludedPrincipals": [],
        },
        workspace_id=workspace,
        sequence=sequence,
    )


def decided(
    approval_id: uuid.UUID, kind: str, by: uuid.UUID, *, sequence: int = 2
) -> dict[str, Any]:
    """A decision as the core records it: the decider is also the event's actor."""
    if kind == "approval.cancelled":
        payload: dict[str, Any] = {"taskId": None, "cancelledBy": str(by)}
    else:
        payload = {
            "taskId": None,
            "artifactId": None,
            "outcomeStatus": None,
            "decisionBy": str(by),
            "comment": None,
            "channel": "telegram",
        }
    return event(kind, approval_id, payload, actor_id=by, sequence=sequence)


def verification_failed(task_id: uuid.UUID, *, blocked: bool, sequence: int = 1) -> dict[str, Any]:
    return event(
        "task.verification_failed",
        task_id,
        {
            "publicId": "TASK-000321",
            "taskId": str(task_id),
            "verificationId": str(uuid.uuid4()),
            "attempt": 3,
            "trigger": "complete",
            "checks": 2,
            "results": [],
            "failedCheck": "tests-pass",
            "reason": "expectation_not_met",
            "consecutiveFailures": 3 if blocked else 1,
            "blocked": blocked,
            "fromStatus": "verifying",
            "status": "blocked" if blocked else "in_progress",
            "systemStatusCategory": "blocked" if blocked else "active",
        },
        sequence=sequence,
    )


def service_context(tenant_id: uuid.UUID) -> TrustedAuthContext:
    """The service's own identity: the sender of what the rules create."""
    return TrustedAuthContext(
        tenant_id=tenant_id,
        principal_id=uuid.uuid4(),
        principal_type="service_account",
        credential_id="ns",
        audience="control-plane",
        issuer="https://iam.test",
        token_id="t",
        scopes=frozenset(),
        expires_at=NOW + timedelta(days=1),
    )


def rule_handler(harness: Harness, core: FakeCore, ctx: TrustedAuthContext) -> RuleEventHandler:
    """The handler over the rules stored in the service's database."""

    async def identity() -> TrustedAuthContext:
        return ctx

    sender: NotificationSender = harness.app.state.sender
    return RuleEventHandler(sender, core, identity, stored_rules(harness.sessions))


async def apply_rule(
    harness: Harness, token: Callable[..., str], key: str, spec: dict[str, Any]
) -> dict[str, Any]:
    response = await harness.client.post(
        "/api/v1/notification-rules",
        json={"key": key, "spec": spec},
        headers=auth(token(scopes=(SCOPE_ADMIN,))),
    )
    assert response.status_code in (200, 201), response.text
    return dict(response.json())


async def retire_rule(harness: Harness, token: Callable[..., str], key: str) -> None:
    response = await harness.client.post(
        f"/api/v1/notification-rules/{key}:retire",
        headers=auth(token(scopes=(SCOPE_ADMIN,))),
    )
    assert response.status_code == 200, response.text


async def inbox(harness: Harness, token: Callable[..., str], person: Person) -> list[dict]:
    response = await harness.client.get(
        "/api/v1/me/notifications",
        headers=auth(token(person.iam_principal_id, tenant=person.tenant_id)),
    )
    assert response.status_code == 200, response.text
    return list(response.json()["items"])


async def notifications(harness: Harness) -> list[Notification]:
    async with harness.sessions() as session:
        return list(await session.scalars(select(Notification).order_by(Notification.created_at)))


async def count_notifications(harness: Harness) -> int:
    async with harness.sessions() as session:
        return int(await session.scalar(select(func.count()).select_from(Notification)) or 0)


class Journal:
    """``GET /api/v1/events`` of the core over a list of events; cursors ``c<n>``."""

    def __init__(self, events: list[dict[str, Any]]) -> None:
        self.events = events
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/events"
        self.requests.append(request)
        prefixes = request.url.params["types"].split(",")
        cursor = request.url.params.get("cursor")
        after = int(cursor[1:]) if cursor else 0
        limit = int(request.url.params.get("limit", 200))
        matching = [
            e
            for e in self.events
            if int(e["cursor"][1:]) > after and e["type"].startswith(tuple(prefixes))
        ]
        page = matching[:limit]
        next_cursor = page[-1]["cursor"] if page else (cursor or "c0")
        return httpx.Response(
            200,
            json={"items": page, "nextCursor": next_cursor, "hasMore": len(matching) > limit},
        )

    def filters(self) -> set[str]:
        return {request.url.params["types"] for request in self.requests}
