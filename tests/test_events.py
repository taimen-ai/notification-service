"""The consumer of Control Plane events, driven by notification rules (ADR-0005).

What the rule engine does with an event — condition, addressee, templates,
buttons, closing, one rule's failure — and how the consumer follows the rules:
no enabled rule, no reads; a new filter resumes from the same cursor; every
event notifies once across a crash and a restart.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from control_plane_client import ControlPlaneClient
from control_plane_client.events import EventConsumer, HandlerError
from control_plane_client.events.sqlalchemy import SqlAlchemyCursorStore
from platform_auth import TrustedAuthContext
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from notification_service.events import (
    CONSUMER_NAME,
    RuleConsumer,
    RuleEventHandler,
    stored_rules,
)
from notification_service.models import HANDLED_EVENTS
from notification_service.rules import subscription

from conftest import FakeDirectory, Harness, Person, adr_rules
from core_events import (
    TASK_CREATED,
    CaptureChannel,
    FakeCore,
    Journal,
    apply_rule,
    count_notifications,
    decided,
    event,
    inbox,
    notifications,
    requested,
    retire_rule,
    rule_handler,
    service_context,
    verification_failed,
)


@pytest.fixture
def service_ctx(tenant_id: uuid.UUID) -> TrustedAuthContext:
    return service_context(tenant_id)


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


@pytest.fixture
def handler(hx: Harness, core: FakeCore, service_ctx: TrustedAuthContext) -> RuleEventHandler:
    return rule_handler(hx, core, service_ctx)


def spec(**changes: Any) -> dict[str, Any]:
    """A rule on approval requests to the assigned decider; ``changes`` replace top keys."""
    body: dict[str, Any] = {
        "on": {"type": "approval.requested"},
        "recipient": {"kind": "assigned"},
        "notification": {"type": "work.decision_needed", "title": "Decide {{payload.taskTitle}}"},
    }
    body.update(changes)
    return body


# --- opening ----------------------------------------------------------------------


async def test_no_rule_no_notification(
    hx: Harness, handler: RuleEventHandler, make_person: Callable[..., Person]
) -> None:
    await handler(requested(uuid.uuid4(), assignee=make_person().principal_id))
    assert await count_notifications(hx) == 0


async def test_a_rule_turns_its_event_into_a_notification_under_the_default_key(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    service_ctx: TrustedAuthContext,
) -> None:
    person = make_person()
    await apply_rule(hx, token, "decide", spec())
    request = requested(uuid.uuid4(), assignee=person.principal_id)

    await handler(request)
    await handler(request)  # the same event again: the key keeps it to one
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["type"] == "work.decision_needed"
    assert item["title"] == "Decide Pay supplier invoice"
    assert item["body"] == ""
    assert item["actions"] == []
    assert item["senderId"] == str(service_ctx.principal_id)
    [row] = await notifications(hx)
    assert row.dedup_key == f"rule:decide:event:{request['id']}"


async def test_prefix_type_and_condition_select_the_events(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    person = make_person().principal_id
    await apply_rule(
        hx,
        token,
        "approvals",
        spec(
            on={"type": "approval.*", "when": {"exists": "payload.taskId"}},
            recipient={"kind": "principal", "ref": str(person)},
            notification={"type": "work.approval", "title": "{{event.type}}"},
        ),
    )
    # Not under ``approval.*``; under it, but without a task: the condition is false.
    await handler(event("task.created", uuid.uuid4(), TASK_CREATED))
    await handler(decided(uuid.uuid4(), "approval.cancelled", uuid.uuid4()))
    await handler(requested(uuid.uuid4(), sequence=3))

    [row] = await notifications(hx)
    assert row.title == "approval.requested"
    assert row.recipient_id == person


async def test_every_matching_rule_notifies_and_a_broken_one_does_not_stop_the_others(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    person = make_person()
    # Orders a string against a number: an evaluation error on every event.
    broken = spec(
        on={"type": "approval.requested", "when": {"lt": [{"var": "payload.taskTitle"}, 1]}}
    )
    await apply_rule(hx, token, "a-broken", broken)
    await apply_rule(hx, token, "b-first", spec())
    await apply_rule(hx, token, "c-second", spec(notification={"type": "work.other", "title": "x"}))

    await handler(requested(uuid.uuid4(), assignee=person.principal_id))

    assert sorted(row.type for row in await notifications(hx)) == [
        "work.decision_needed",
        "work.other",
    ]


async def test_disabled_and_retired_rules_do_not_run(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    person = make_person()
    await apply_rule(hx, token, "disabled", spec(status="disabled"))
    await apply_rule(hx, token, "retired", spec())
    await retire_rule(hx, token, "retired")

    await handler(requested(uuid.uuid4(), assignee=person.principal_id))
    assert await count_notifications(hx) == 0


async def test_a_new_version_applies_to_later_events_and_leaves_sent_ones(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    person = make_person()
    await apply_rule(hx, token, "decide", spec())
    await handler(requested(uuid.uuid4(), assignee=person.principal_id))
    await apply_rule(hx, token, "decide", spec(notification={"type": "work.v2", "title": "v2"}))
    await handler(requested(uuid.uuid4(), assignee=person.principal_id, sequence=2))

    assert [(row.type, row.title) for row in await notifications(hx)] == [
        ("work.decision_needed", "Decide Pay supplier invoice"),
        ("work.v2", "v2"),
    ]


async def test_role_recipient_by_paths_and_the_task_fallback(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    directory: FakeDirectory,
    make_person: Callable[..., Person],
    core: FakeCore,
) -> None:
    holder, owner = make_person(), make_person()
    role, workspace, task_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    directory.roles[(role, workspace)] = [holder.principal_id]
    core.tasks[str(task_id)] = {"id": str(task_id), "ownerId": str(owner.principal_id)}
    await apply_rule(
        hx,
        token,
        "by-role",
        spec(
            recipient={
                "kind": "role",
                "ref": "payload.requiredRoleId",
                "workspace": "payload.workspaceId",
                "fallback": "taskOwner",
            }
        ),
    )

    await handler(requested(uuid.uuid4(), role=role, workspace=workspace))
    # No role in this one: the task's owner gets it.
    await handler(requested(uuid.uuid4(), task_id=task_id, sequence=2))
    await hx.drain()

    assert len(await inbox(hx, token, holder)) == 1
    assert len(await inbox(hx, token, owner)) == 1


async def test_nobody_to_notify_an_unknown_one_or_a_malformed_id_skip_the_event(
    hx: Harness, handler: RuleEventHandler, token: Callable[..., str]
) -> None:
    await apply_rule(hx, token, "decide", spec())
    # A role without a workspace, a principal the directory does not know, an
    # id that is not one: nothing is retried, nothing is sent.
    await handler(requested(uuid.uuid4(), role=uuid.uuid4()))
    await handler(requested(uuid.uuid4(), assignee=uuid.uuid4()))
    broken = requested(uuid.uuid4(), assignee=uuid.uuid4())
    broken["payload"]["assignedPrincipalId"] = "not-a-uuid"
    await handler(broken)
    assert await count_notifications(hx) == 0


async def test_directory_outage_raises_so_the_event_is_retried(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    directory: FakeDirectory,
    make_person: Callable[..., Person],
) -> None:
    person = make_person()
    await apply_rule(hx, token, "decide", spec())
    directory.unavailable = True
    with pytest.raises(Exception, match="unavailable"):
        await handler(requested(uuid.uuid4(), assignee=person.principal_id))
    assert await count_notifications(hx) == 0


async def test_templates_leave_out_empty_lines_and_links_and_fall_back_to_the_type(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    core: FakeCore,
) -> None:
    person, requester = make_person(), uuid.uuid4()
    core.names[str(requester)] = "Finance bot"
    await apply_rule(
        hx,
        token,
        "decide",
        spec(
            notification={
                "type": "work.decision_needed",
                "title": "{{payload.comment}}",
                "body": "By: {{payload.requestedBy.displayName}}\n"
                "Comment: {{payload.comment}}\nfixed line",
                "links": [
                    {"label": "Task", "url": "https://console.test/{{payload.taskPublicId}}"},
                    {"label": "Nothing", "url": "https://console.test/{{payload.comment}}"},
                    {"label": "Not a URL", "url": "{{payload.taskPublicId}}"},
                ],
            }
        ),
    )

    await handler(
        requested(uuid.uuid4(), assignee=person.principal_id, requester=requester, comment="")
    )
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["title"] == "work.decision_needed"
    assert item["body"] == "By: Finance bot\nfixed line"
    assert item["links"] == [{"label": "Task", "url": "https://console.test/TASK-000123"}]


async def test_the_task_is_read_only_for_rules_that_read_it(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    core: FakeCore,
) -> None:
    person = make_person()
    await apply_rule(hx, token, "decide", spec())
    await handler(requested(uuid.uuid4(), assignee=person.principal_id))
    assert core.task_reads == 0

    await apply_rule(
        hx, token, "decide", spec(notification={"type": "a.b", "title": "{{task.title}}"})
    )
    await handler(requested(uuid.uuid4(), assignee=person.principal_id, sequence=2))
    assert core.task_reads == 1


# --- closing ------------------------------------------------------------------------


async def test_close_by_the_rule_key_with_a_rendered_outcome(
    hx: Harness,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    chat: CaptureChannel,
) -> None:
    person, decider, approval = make_person(), uuid.uuid4(), uuid.uuid4()
    await apply_rule(
        hx,
        token,
        "decide",
        spec(
            notification={
                "type": "work.decision_needed",
                "title": "t",
                "actions": ["approvalDecide"],
            },
            dedupKeyTemplate="decision:{{event.entityId}}",
            close={"on": ["approval.approved"], "outcome": "{{event.type}}!"},
        ),
    )
    await handler(requested(approval, assignee=person.principal_id))
    await handler(decided(approval, "approval.approved", decider))
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["actionsOutcome"] == {
        "status": "approval.approved!",
        "by": str(decider),
        "channel": "telegram",
        "at": item["actionsOutcome"]["at"],
    }
    # Closed before delivery: no buttons went out.
    [message] = chat.sent
    assert message.actions == []


async def test_closing_without_a_notification_is_a_no_op(
    hx: Harness, handler: RuleEventHandler, token: Callable[..., str]
) -> None:
    await apply_rule(
        hx,
        token,
        "decide",
        spec(dedupKeyTemplate="d:{{event.entityId}}", close={"on": ["approval.approved"]}),
    )
    await handler(decided(uuid.uuid4(), "approval.approved", uuid.uuid4()))
    assert await count_notifications(hx) == 0


# --- the consumer ---------------------------------------------------------------------


def test_the_filter_is_the_union_of_on_and_close_of_enabled_rules() -> None:
    assert subscription(
        [
            spec(on={"type": "approval.*"}, close={"on": ["task.cancelled"]}),
            spec(on={"type": "task.verification_failed"}),
            spec(on={"type": "goal.created"}, status="disabled"),
        ]
    ) == ("approval.", "task.cancelled", "task.verification_failed")
    assert subscription([]) == ()


def rule_consumer(
    engine: AsyncEngine,
    journal: Journal,
    hx: Harness,
    handler: Any,
    ctx: TrustedAuthContext,
    *,
    page_size: int = 2,
) -> RuleConsumer:
    client = ControlPlaneClient(
        "http://cp.test", "cp_test_key", transport=httpx.MockTransport(journal)
    )

    async def identity() -> TrustedAuthContext:
        return ctx

    def build(types: tuple[str, ...]) -> EventConsumer:
        # A fresh store object per consumer: what a restarted one has.
        return EventConsumer(
            client,
            types,
            None,
            SqlAlchemyCursorStore(engine),
            handler,
            name=CONSUMER_NAME,
            start="earliest",
            page_size=page_size,
            poll_interval=0.05,
            websocket=False,
        )

    return RuleConsumer(build, stored_rules(hx.sessions), identity, poll_seconds=0.05)


async def eventually(check: Callable[[], Any]) -> None:
    async with asyncio.timeout(5.0):
        while not await check():  # noqa: ASYNC110 - polls the database, nothing to wait on
            await asyncio.sleep(0.02)


async def test_without_rules_no_event_is_read_and_the_filter_follows_the_rules(
    hx: Harness,
    engine: AsyncEngine,
    handler: RuleEventHandler,
    token: Callable[..., str],
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
) -> None:
    person, task_id = make_person(), uuid.uuid4()
    journal = Journal([requested(uuid.uuid4(), assignee=person.principal_id, sequence=1)])
    supervisor = rule_consumer(engine, journal, hx, handler, service_ctx)
    running = asyncio.create_task(supervisor.run())
    try:
        await asyncio.sleep(0.2)
        assert journal.requests == []
        assert supervisor.consumer is None

        await apply_rule(hx, token, "decide", spec(close={"on": ["approval.approved"]}))
        supervisor.wake()

        async def notified() -> bool:
            return await count_notifications(hx) == 1

        await eventually(notified)
        assert journal.filters() == {"approval.approved,approval.requested"}

        # Another filter: the consumer restarts on it from the same cursor, and
        # what the old filter already passed is not read again.
        journal.events.append(verification_failed(task_id, blocked=False, sequence=2))
        await apply_rule(
            hx,
            token,
            "failures",
            spec(
                on={"type": "task.verification_failed"},
                recipient={"kind": "principal", "ref": str(person.principal_id)},
                notification={"type": "work.failed", "title": "{{payload.publicId}}"},
            ),
        )
        supervisor.wake()

        async def both() -> bool:
            return await count_notifications(hx) == 2

        await eventually(both)
        assert supervisor.types == (
            "approval.approved",
            "approval.requested",
            "task.verification_failed",
        )

        await retire_rule(hx, token, "decide")
        await retire_rule(hx, token, "failures")
        supervisor.wake()

        async def stopped() -> bool:
            return supervisor.consumer is None

        await eventually(stopped)
        seen = len(journal.requests)
        await asyncio.sleep(0.2)
        assert len(journal.requests) == seen
    finally:
        supervisor.stop()
        await asyncio.wait_for(running, 5)
    async with hx.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HANDLED_EVENTS)) == 2


async def test_every_event_notifies_exactly_once_across_a_crash_and_a_restart(
    hx: Harness,
    engine: AsyncEngine,
    core: FakeCore,
    handler: RuleEventHandler,
    token: Callable[..., str],
    make_person: Callable[..., Person],
) -> None:
    for rule in adr_rules():
        await apply_rule(hx, token, rule["key"], rule["spec"])
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
    crashed = False

    async def crashing(e: dict[str, Any]) -> None:
        # The notification is committed, then the process dies before the SDK
        # records the event: after the restart it is handled again.
        nonlocal crashed
        await handler(e)
        if e["entityId"] == str(approvals[1]) and not crashed:
            crashed = True
            raise RuntimeError("crash")

    types = subscription(rule["spec"] for rule in adr_rules())

    def consumer(on_event: Any) -> EventConsumer:
        client = ControlPlaneClient(
            "http://cp.test", "cp_test_key", transport=httpx.MockTransport(journal)
        )
        return EventConsumer(
            client,
            types,
            None,
            SqlAlchemyCursorStore(engine),
            on_event,
            name=CONSUMER_NAME,
            start="earliest",
            page_size=2,
            websocket=False,
        )

    with pytest.raises(HandlerError):
        await consumer(crashing).drain()
    await consumer(crashing).drain()
    # And a plain restart with nothing new handles nothing.
    assert await consumer(handler).drain() == 0
    await hx.drain()

    assert await count_notifications(hx) == 3
    items = await inbox(hx, token, person)
    assert len(items) == 3
    assert len([i for i in items if i["actionsClosedAt"] is not None]) == 1
    async with hx.sessions() as session:
        handled = await session.scalar(select(func.count()).select_from(HANDLED_EVENTS))
    assert handled == 4  # task.created is filtered out by the core, never seen
