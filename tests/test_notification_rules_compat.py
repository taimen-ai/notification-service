"""Compatibility: the three rules of ADR-0005 §8 notify as the built-in table of ADR-0002 did.

The table (``EVENT_TYPES``, ``CLOSING`` and the handlers of
``events.CoreEventHandler``) is gone; ``BUILT_IN`` below is what it produced on
these very events — addressee, type, title, text, buttons, link, dedup key and
the closing outcome. The rules are applied through the API exactly as the
``notify`` package installer does (``${TASK_URL_BASE}`` substituted), and the
same events go through the rule-driven handler.

Texts are compared by content (ADR-0005 §8 п. 4): the words of the old text in
the same order; the rule's text may only add the label of a line it split off
(«Причина:»).
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from platform_auth import TrustedAuthContext

from notification_service.events import DECIDE, RuleEventHandler
from notification_service.sending import NotificationSender

from conftest import FakeDirectory, Harness, Person, adr_rules
from core_events import (
    CaptureChannel,
    FakeCore,
    apply_rule,
    decided,
    inbox,
    notifications,
    requested,
    rule_handler,
    service_context,
    verification_failed,
)

TASK_URL_BASE = "https://console.test/tasks"
# What the rules may add to a text of the table: a label of a line of their own.
ADDED_WORDS = {"Причина"}


def built_in_decision_actions(approval_id: str) -> list[dict[str, Any]]:
    return [
        {
            "id": "approve",
            "label": "Одобрить",
            "data": {"kind": DECIDE, "approvalId": approval_id, "decision": "approve"},
        },
        {
            "id": "reject",
            "label": "Отклонить",
            "data": {"kind": DECIDE, "approvalId": approval_id, "decision": "reject"},
        },
    ]


# What ``CoreEventHandler`` sent for the events of the tests below
# (NS_TASK_URL_TEMPLATE = https://console.test/tasks/{taskPublicId}).
BUILT_IN = {
    "approval.requested": {
        "type": "approval.requested",
        "title": "Нужно решение: TASK-000123 Pay supplier invoice",
        "body": "Работа: TASK-000123 Pay supplier invoice\n"
        "Запрашивает: Finance bot\n"
        "Комментарий: Please decide today",
        "links": [{"label": "Открыть задачу", "url": f"{TASK_URL_BASE}/TASK-000123"}],
    },
    "approval.requested without a comment": {
        "body": "Работа: TASK-000123 Pay supplier invoice\nЗапрашивает: Finance bot",
    },
    "task.verification_failed": {
        "type": "task.verification_failed",
        "title": "Проверка не пройдена: TASK-000321 Ship the report",
        "body": "Попытка 3: не пройдена проверка «tests-pass» (expectation_not_met).\n"
        "Задача вернулась в работу (статус in_progress).",
        "links": [{"label": "Открыть задачу", "url": f"{TASK_URL_BASE}/TASK-000321"}],
        "actions": [],
    },
    "task.verification_failed blocked": {
        "type": "task.verification_failed",
        "title": "Проверка не пройдена: TASK-000321 Ship the report",
        "body": "Попытка 3: не пройдена проверка «tests-pass» (expectation_not_met).\n"
        "3 неудачных попыток подряд — задача ждёт человека (статус blocked).",
        "links": [{"label": "Открыть задачу", "url": f"{TASK_URL_BASE}/TASK-000321"}],
        "actions": [],
    },
}


def words(text: str) -> list[str]:
    return re.findall(r"[\w-]+", text)


def same_content(old: str, new: str) -> bool:
    return words(old) == [word for word in words(new) if word not in ADDED_WORDS]


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
async def hx(
    harness_factory: Callable[..., Any], chat: CaptureChannel, token: Callable[..., str]
) -> Any:
    async with harness_factory(channels=[chat]) as built:
        for rule in adr_rules(TASK_URL_BASE):
            await apply_rule(built, token, rule["key"], rule["spec"])
        yield built


@pytest.fixture
def handler(hx: Harness, core: FakeCore, service_ctx: TrustedAuthContext) -> RuleEventHandler:
    return rule_handler(hx, core, service_ctx)


def approval_task(core: FakeCore) -> uuid.UUID:
    """The task of ``requested()``: what the core answers for its ``taskId``."""
    task_id = uuid.uuid4()
    core.tasks[str(task_id)] = {
        "id": str(task_id),
        "publicId": "TASK-000123",
        "title": "Pay supplier invoice",
    }
    return task_id


# --- approval.requested ------------------------------------------------------------------


@pytest.mark.parametrize("comment", ["Please decide today", ""])
async def test_approval_request_to_the_assignee(
    hx: Harness,
    handler: RuleEventHandler,
    core: FakeCore,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
    chat: CaptureChannel,
    comment: str,
) -> None:
    person, requester, approval = make_person(), uuid.uuid4(), uuid.uuid4()
    core.names[str(requester)] = "Finance bot"
    task_id = approval_task(core)

    await handler(
        requested(
            approval,
            task_id=task_id,
            assignee=person.principal_id,
            requester=requester,
            comment=comment,
        )
    )
    await hx.drain()

    expected = BUILT_IN["approval.requested"]
    [item] = await inbox(hx, token, person)
    assert item["type"] == expected["type"]
    assert item["title"] == expected["title"]
    body = BUILT_IN["approval.requested" if comment else "approval.requested without a comment"]
    assert same_content(body["body"], item["body"]), item["body"]
    assert item["links"] == expected["links"]
    assert item["actions"] == built_in_decision_actions(str(approval))
    assert item["senderId"] == str(service_ctx.principal_id)
    [row] = await notifications(hx)
    assert (row.recipient_kind, row.recipient_id) == ("principal", person.principal_id)
    assert row.dedup_key == f"control-plane:approval:{approval}"
    [message] = chat.sent
    assert [a["id"] for a in message.actions] == ["approve", "reject"]


async def test_approval_request_to_the_holders_of_the_role(
    hx: Harness,
    handler: RuleEventHandler,
    core: FakeCore,
    directory: FakeDirectory,
    make_person: Callable[..., Person],
    token: Callable[..., str],
) -> None:
    alice, bob, outsider = make_person(), make_person(), make_person()
    role, workspace = uuid.uuid4(), uuid.uuid4()
    directory.roles[(role, workspace)] = [alice.principal_id, bob.principal_id]

    await handler(
        requested(uuid.uuid4(), task_id=approval_task(core), role=role, workspace=workspace)
    )
    await hx.drain()

    [row] = await notifications(hx)
    assert (row.recipient_kind, row.recipient_id, row.workspace_id) == ("role", role, workspace)
    assert len(await inbox(hx, token, alice)) == 1
    assert len(await inbox(hx, token, bob)) == 1
    assert await inbox(hx, token, outsider) == []


async def test_approval_request_without_a_decider_is_skipped(
    hx: Harness, handler: RuleEventHandler, core: FakeCore
) -> None:
    # A role without a workspace cannot be resolved; neither could it before.
    await handler(requested(uuid.uuid4(), task_id=approval_task(core), role=uuid.uuid4()))
    assert await notifications(hx) == []


# --- decisions close the buttons ----------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "status", "channel"),
    [
        ("approval.approved", "approved", "telegram"),
        ("approval.rejected", "rejected", "telegram"),
        ("approval.cancelled", "cancelled", None),
    ],
)
async def test_decision_closes_the_buttons_with_the_built_in_outcome(
    hx: Harness,
    handler: RuleEventHandler,
    core: FakeCore,
    make_person: Callable[..., Person],
    token: Callable[..., str],
    kind: str,
    status: str,
    channel: str | None,
) -> None:
    person, decider, approval = make_person(), uuid.uuid4(), uuid.uuid4()
    await handler(requested(approval, task_id=approval_task(core), assignee=person.principal_id))

    decision = decided(approval, kind, decider)
    await handler(decision)
    # A later closing event does not overwrite the first outcome.
    await handler(decided(approval, "approval.cancelled", uuid.uuid4(), sequence=3))
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["actionsClosedAt"] is not None
    expected = {"status": status, "by": str(decider), "at": decision["occurredAt"]}
    if channel:
        expected["channel"] = channel
    assert item["actionsOutcome"] == expected


async def test_buttons_of_a_notification_sent_before_the_rules_close_by_the_rule(
    hx: Harness,
    handler: RuleEventHandler,
    service_ctx: TrustedAuthContext,
    make_person: Callable[..., Person],
    token: Callable[..., str],
) -> None:
    # Sent by the built-in table before the rollout, under its key (ADR-0005 §8 п. 2).
    from notification_service.schemas import Action, NotificationCreate, Recipient

    person, approval = make_person(), uuid.uuid4()
    sender: NotificationSender = hx.app.state.sender
    await sender.accept(
        service_ctx,
        NotificationCreate(
            recipient=Recipient(kind="principal", id=person.principal_id),
            type="approval.requested",
            title="Нужно решение: TASK-000123 Pay supplier invoice",
            actions=[Action.model_validate(a) for a in built_in_decision_actions(str(approval))],
        ),
        f"control-plane:approval:{approval}",
    )

    await handler(decided(approval, "approval.rejected", uuid.uuid4()))
    await hx.drain()

    [item] = await inbox(hx, token, person)
    assert item["actionsOutcome"]["status"] == "rejected"


# --- task.verification_failed ------------------------------------------------------------


@pytest.mark.parametrize("blocked", [False, True])
async def test_failed_verification_to_the_task_owner(
    hx: Harness,
    handler: RuleEventHandler,
    core: FakeCore,
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

    failure = verification_failed(task_id, blocked=blocked)
    await handler(failure)
    await hx.drain()

    expected = BUILT_IN[
        "task.verification_failed blocked" if blocked else "task.verification_failed"
    ]
    [item] = await inbox(hx, token, owner)
    assert item["type"] == expected["type"]
    assert item["title"] == expected["title"]
    assert same_content(expected["body"], item["body"]), item["body"]
    assert item["links"] == expected["links"]
    assert item["actions"] == expected["actions"]
    assert await inbox(hx, token, executor) == []
    # Exactly one of the two rules fired, under the table's key.
    [row] = await notifications(hx)
    assert row.dedup_key == f"control-plane:event:{failure['id']}"


async def test_failed_verification_without_owner_reaches_the_assignee(
    hx: Harness,
    handler: RuleEventHandler,
    core: FakeCore,
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
    await handler(verification_failed(task_id, blocked=False))
    await hx.drain()
    assert len(await inbox(hx, token, executor)) == 1


async def test_failed_verification_of_a_task_that_cannot_be_read_is_skipped(
    hx: Harness, handler: RuleEventHandler
) -> None:
    await handler(verification_failed(uuid.uuid4(), blocked=True))
    assert await notifications(hx) == []
