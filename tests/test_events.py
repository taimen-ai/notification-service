"""The consumer of Control Plane events: decisions and failed checks become notifications.

Events are built from the Control Plane's own models (``EventOut``) and their
payloads are checked against the core's event catalog, so the handler is tested
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
import pytest
from control_plane.api.v1.schemas import EventOut
from control_plane.domain.event_catalog import current_version, schema_for
from control_plane_client import ControlPlaneClient
from control_plane_client.events import EventConsumer, HandlerError
from control_plane_client.events.sqlalchemy import SqlAlchemyCursorStore
from platform_auth import TrustedAuthContext
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from notification_service.channels import OutboundMessage, SendResult
from notification_service.events import (
    CONSUMER_NAME,
    DECIDE,
    EVENT_TYPES,
    CoreEventHandler,
    approval_key,
)
from notification_service.models import HANDLED_EVENTS, Notification
from notification_service.sending import NotificationSender

from conftest import FakeDirectory, Harness, Person, auth

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
    """A channel that renders actions (what Telegram will be), recording what it got."""

    name = "chat"
    push = False
    needs_address = False

    def __init__(self) -> None:
        self.sent: list[OutboundMessage] = []

    async def send(self, message: OutboundMessage) -> SendResult:
        self.sent.append(message)
        return SendResult()


class FakeCore:
    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.names: dict[str, str] = {}

    async def task(self, task_id: str) -> dict[str, Any] | None:
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
    assignee: uuid.UUID | None = None,
    role: uuid.UUID | None = None,
    workspace: uuid.UUID | None = None,
    requester: uuid.UUID | None = None,
    sequence: int = 1,
) -> dict[str, Any]:
    return event(
        "approval.requested",
        approval_id,
        {
            "taskId": str(uuid.uuid4()),
            "artifactId": None,
            "requiredRoleId": str(role) if role else None,
            "assignedPrincipalId": str(assignee) if assignee else None,
            "gate": True,
            "workspaceId": str(workspace) if workspace else None,
            "taskPublicId": "TASK-000123",
            "taskTitle": "Pay supplier invoice",
            "requestedBy": str(requester or uuid.uuid4()),
            "comment": "Please decide today",
        },
        workspace_id=workspace,
        sequence=sequence,
    )


def decided(
    approval_id: uuid.UUID, kind: str, by: uuid.UUID, *, sequence: int = 2
) -> dict[str, Any]:
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
    return event(kind, approval_id, payload, sequence=sequence)


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


@pytest.fixture
def service_ctx(tenant_id: uuid.UUID) -> TrustedAuthContext:
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


@pytest.fixture
def core() -> FakeCore:
    return FakeCore()


@pytest.fixture
def chat() -> CaptureChannel:
    return CaptureChannel()


@pytest.fixture
async def hx(harness_factory: Callable[..., Any], chat: CaptureChannel) -> Any:
    async with harness_factory(channels=[chat]) as built:
        yield built


def handler_for(
    harness: Harness, core: FakeCore, ctx: TrustedAuthContext, **options: Any
) -> CoreEventHandler:
    async def identity() -> TrustedAuthContext:
        return ctx

    sender: NotificationSender = harness.app.state.sender
    return CoreEventHandler(sender, core, identity, **options)


async def inbox(harness: Harness, token: Callable[..., str], person: Person) -> list[dict]:
    response = await harness.client.get(
        "/api/v1/me/notifications",
        headers=auth(token(person.iam_principal_id, tenant=person.tenant_id)),
    )
    assert response.status_code == 200, response.text
    return list(response.json()["items"])


async def count_notifications(harness: Harness) -> int:
    async with harness.sessions() as session:
        return int(await session.scalar(select(func.count()).select_from(Notification)) or 0)


# --- approvals -------------------------------------------------------------------


async def test_requested_approval_reaches_the_assignee_with_decision_actions(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
    chat: CaptureChannel,
) -> None:
    person, requester = make_person(), uuid.uuid4()
    core.names[str(requester)] = "Finance bot"
    approval = uuid.uuid4()
    handler = handler_for(
        hx, core, service_ctx, task_url_template="https://console.test/tasks/{taskPublicId}"
    )

    await handler(requested(approval, assignee=person.principal_id, requester=requester))
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["type"] == "approval.requested"
    assert item["title"] == "Нужно решение: TASK-000123 Pay supplier invoice"
    assert "Запрашивает: Finance bot" in item["body"]
    assert "Комментарий: Please decide today" in item["body"]
    assert item["links"] == [
        {"label": "Открыть задачу", "url": "https://console.test/tasks/TASK-000123"}
    ]
    assert [a["id"] for a in item["actions"]] == ["approve", "reject"]
    assert item["actions"][0]["data"] == {
        "kind": DECIDE,
        "approvalId": str(approval),
        "decision": "approve",
    }
    assert item["actionsClosedAt"] is None
    assert item["senderId"] == str(service_ctx.principal_id)
    # A channel that renders buttons gets them.
    [message] = chat.sent
    assert [a["id"] for a in message.actions] == ["approve", "reject"]


async def test_role_approval_reaches_every_holder_in_the_workspace(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    directory: FakeDirectory,
    make_person: Callable[..., Person],
    token: Callable[..., str],
) -> None:
    alice, bob, outsider = make_person(), make_person(), make_person()
    role, workspace = uuid.uuid4(), uuid.uuid4()
    directory.roles[(role, workspace)] = [alice.principal_id, bob.principal_id]

    handler = handler_for(hx, core, service_ctx)
    await handler(requested(uuid.uuid4(), role=role, workspace=workspace))
    await hx.drain()

    assert len(await inbox(hx, token, alice)) == 1
    assert len(await inbox(hx, token, bob)) == 1
    assert await inbox(hx, token, outsider) == []


@pytest.mark.parametrize(
    ("kind", "status"),
    [
        ("approval.approved", "approved"),
        ("approval.rejected", "rejected"),
        ("approval.cancelled", "cancelled"),
    ],
)
async def test_decision_closes_the_actions_of_the_request(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
    kind: str,
    status: str,
) -> None:
    person, decider = make_person(), uuid.uuid4()
    approval = uuid.uuid4()
    handler = handler_for(hx, core, service_ctx)
    await handler(requested(approval, assignee=person.principal_id))

    await handler(decided(approval, kind, decider))
    # A later closing event does not overwrite the first outcome.
    await handler(decided(approval, "approval.cancelled", uuid.uuid4(), sequence=3))
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["actionsClosedAt"] is not None
    assert item["actionsOutcome"]["status"] == status
    assert item["actionsOutcome"]["by"] == str(decider)
    if kind != "approval.cancelled":
        assert item["actionsOutcome"]["channel"] == "telegram"


async def test_actions_closed_before_delivery_are_not_sent(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    chat: CaptureChannel,
) -> None:
    person, approval = make_person(), uuid.uuid4()
    handler = handler_for(hx, core, service_ctx)
    await handler(requested(approval, assignee=person.principal_id))
    await handler(decided(approval, "approval.approved", uuid.uuid4()))

    await hx.drain()

    [message] = chat.sent
    assert message.actions == []


async def test_decision_without_a_request_is_a_no_op(
    hx: Harness, core: FakeCore, service_ctx: TrustedAuthContext
) -> None:
    await handler_for(hx, core, service_ctx)(
        decided(uuid.uuid4(), "approval.approved", uuid.uuid4())
    )
    assert await count_notifications(hx) == 0


async def test_version_1_request_reads_the_task_from_the_core(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
) -> None:
    person, task_id = make_person(), uuid.uuid4()
    core.tasks[str(task_id)] = {"id": str(task_id), "publicId": "TASK-000007", "title": "Old"}
    payload = {
        "taskId": str(task_id),
        "artifactId": None,
        "requiredRoleId": None,
        "assignedPrincipalId": str(person.principal_id),
        "gate": False,
    }

    await handler_for(hx, core, service_ctx)(
        event("approval.requested", uuid.uuid4(), payload, version=1)
    )
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["title"] == "Нужно решение: TASK-000007 Old"


async def test_request_without_a_decider_or_to_an_unknown_one_is_skipped(
    hx: Harness, core: FakeCore, service_ctx: TrustedAuthContext
) -> None:
    handler = handler_for(hx, core, service_ctx)
    # A role without a workspace cannot be resolved; an unknown principal is not
    # in the directory. Neither is retried, neither sends anything.
    await handler(requested(uuid.uuid4(), role=uuid.uuid4()))
    await handler(requested(uuid.uuid4(), assignee=uuid.uuid4()))
    assert await count_notifications(hx) == 0


async def test_malformed_event_is_skipped(
    hx: Harness, core: FakeCore, service_ctx: TrustedAuthContext
) -> None:
    broken = requested(uuid.uuid4(), assignee=uuid.uuid4())
    broken["payload"]["assignedPrincipalId"] = "not-a-uuid"
    await handler_for(hx, core, service_ctx)(broken)
    assert await count_notifications(hx) == 0


async def test_directory_outage_raises_so_the_event_is_retried(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    directory: FakeDirectory,
    make_person: Callable[..., Person],
) -> None:
    person = make_person()
    directory.unavailable = True
    handler = handler_for(hx, core, service_ctx)
    with pytest.raises(Exception, match="unavailable"):
        await handler(requested(uuid.uuid4(), assignee=person.principal_id))
    assert await count_notifications(hx) == 0


# --- verification ----------------------------------------------------------------


@pytest.mark.parametrize("blocked", [False, True])
async def test_failed_verification_reaches_the_task_owner(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
    blocked: bool,
) -> None:
    owner, executor, task_id = make_person(), make_person(), uuid.uuid4()
    core.tasks[str(task_id)] = {
        "id": str(task_id),
        "publicId": "TASK-000321",
        "title": "Ship the report",
        "ownerId": str(owner.principal_id),
        "assigneeId": str(executor.principal_id),
    }

    await handler_for(hx, core, service_ctx)(verification_failed(task_id, blocked=blocked))
    await hx.drain()

    [item] = await inbox(hx, token, owner)
    assert item["type"] == "task.verification_failed"
    assert item["title"] == "Проверка не пройдена: TASK-000321 Ship the report"
    assert "«tests-pass» (expectation_not_met)" in item["body"]
    assert ("ждёт человека" in item["body"]) is blocked
    assert item["actions"] == []
    assert await inbox(hx, token, executor) == []


async def test_failed_verification_without_owner_reaches_the_assignee(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
) -> None:
    executor, task_id = make_person(), uuid.uuid4()
    core.tasks[str(task_id)] = {
        "id": str(task_id),
        "publicId": "TASK-000321",
        "title": "Ship the report",
        "ownerId": None,
        "assigneeId": str(executor.principal_id),
    }
    await handler_for(hx, core, service_ctx)(verification_failed(task_id, blocked=False))
    await hx.drain()
    assert len(await inbox(hx, token, executor)) == 1


# --- the consumer: exactly once, across restarts -----------------------------------


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


def consumer(
    engine: AsyncEngine, journal: Journal, handler: Any, *, page_size: int = 2
) -> EventConsumer:
    client = ControlPlaneClient(
        "http://cp.test", "cp_test_key", transport=httpx.MockTransport(journal)
    )
    # A fresh store object per consumer: what a restarted process has.
    store = SqlAlchemyCursorStore(engine)
    return EventConsumer(
        client,
        EVENT_TYPES,
        None,
        store,
        handler,
        name=CONSUMER_NAME,
        start="earliest",
        page_size=page_size,
        websocket=False,
    )


async def test_every_event_notifies_exactly_once_across_a_crash_and_a_restart(
    hx: Harness,
    engine: AsyncEngine,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
) -> None:
    person = make_person()
    approvals = [uuid.uuid4() for _ in range(3)]
    events = [
        requested(approvals[0], assignee=person.principal_id, sequence=1),
        event("task.created", uuid.uuid4(), TASK_CREATED, sequence=2),
        requested(approvals[1], assignee=person.principal_id, sequence=3),
        decided(approvals[0], "approval.approved", uuid.uuid4(), sequence=4),
        requested(approvals[2], assignee=person.principal_id, sequence=5),
    ]
    journal = Journal(events)
    handler = handler_for(hx, core, service_ctx)
    crashed = False

    async def crashing(e: dict[str, Any]) -> None:
        # The notification is committed, then the process dies before the SDK
        # records the event: after the restart it is handled again.
        nonlocal crashed
        await handler(e)
        if e["entityId"] == str(approvals[1]) and not crashed:
            crashed = True
            raise RuntimeError("crash")

    with pytest.raises(HandlerError):
        await consumer(engine, journal, crashing).drain()
    await consumer(engine, journal, crashing).drain()
    # And a plain restart with nothing new handles nothing.
    assert await consumer(engine, journal, handler).drain() == 0
    await hx.drain()

    assert await count_notifications(hx) == 3
    items = await inbox(hx, token, person)
    assert len(items) == 3
    closed = [i for i in items if i["actionsClosedAt"] is not None]
    assert len(closed) == 1
    async with hx.sessions() as session:
        handled = await session.scalar(select(func.count()).select_from(HANDLED_EVENTS))
    assert handled == 4  # task.created is filtered out by the core, never seen
    assert {r.url.params["types"] for r in journal.requests} == {
        "approval.,task.verification_failed"
    }


async def test_repeated_notification_under_the_same_key_is_one(
    hx: Harness,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
) -> None:
    # The dedup record of the SDK is gone (pruned, restored backup): the
    # notification's own key still keeps the event to one notification.
    person, approval = make_person(), uuid.uuid4()
    handler = handler_for(hx, core, service_ctx)
    first = requested(approval, assignee=person.principal_id)
    await handler(first)
    await handler(first)
    assert await count_notifications(hx) == 1
    async with hx.sessions() as session:
        row = await session.scalar(select(Notification))
    assert row is not None and row.dedup_key == approval_key(str(approval))
