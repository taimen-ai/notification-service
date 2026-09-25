"""Consumer of Control Plane events: the core's decisions and failed checks reach people.

The Control Plane announces what it waits for in its journal; this module reads
the journal with the consumer SDK of the core (``control_plane_client.events``)
and turns it into ordinary notifications, sent under the service's own identity:

- ``approval.requested`` — a notification to the assigned principal or to the
  holders of the required role in the approval's workspace, with the decision
  actions (``approve``, ``reject``);
- ``approval.approved|rejected|cancelled`` — the actions of that notification
  close with the outcome; channels render them inactive;
- ``task.verification_failed`` — a notification to the task's owner (else its
  assignee) that an acceptance check failed.

Exactly one notification per event, across restarts: the SDK records every
handled event id together with the cursor, and a notification is sent with a
dedup key derived from the event (``approval:<id>``, ``event:<id>``), so an
event handled again after a crash between the two replays the notification
instead of creating a second one.
"""

from __future__ import annotations

import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from control_plane_client import (
    ControlPlaneClient,
    ControlPlaneError,
    NotFoundError,
    PermissionDeniedError,
)
from control_plane_client.events import Event, EventConsumer
from control_plane_client.events.sqlalchemy import SqlAlchemyCursorStore
from platform_auth import ServiceTokenProvider, TokenVerifier, TrustedAuthContext
from sqlalchemy.ext.asyncio import AsyncEngine

from notification_service.config import Settings
from notification_service.errors import Conflict, Unprocessable
from notification_service.schemas import (
    MAX_BODY,
    MAX_TITLE,
    Action,
    Link,
    NotificationCreate,
    Recipient,
)
from notification_service.sending import NotificationSender

logger = logging.getLogger(__name__)

CONSUMER_NAME = "notification-service"
EVENT_TYPES = ("approval.", "task.verification_failed")
REQUESTED = "approval.requested"
VERIFICATION_FAILED = "task.verification_failed"
# Decision events -> the outcome the actions of the request close with.
CLOSING = {
    "approval.approved": "approved",
    "approval.rejected": "rejected",
    "approval.cancelled": "cancelled",
}
# ``data.kind`` of a decision action: what a channel that executes actions
# (Telegram, N007) does with it.
DECIDE = "approval.decide"

_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def approval_key(approval_id: str) -> str:
    """Dedup key of the notification about an approval: decisions find it by it."""
    return f"control-plane:approval:{approval_id}"


def event_key(event_id: str) -> str:
    return f"control-plane:event:{event_id}"


def _line(value: str, limit: int) -> str:
    """One line of plain text within ``limit``: what a title must be."""
    text = " ".join(_CONTROL.sub(" ", value).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _text(value: str, limit: int) -> str:
    text = _CONTROL.sub(" ", value).replace("\r", "").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


class Core(Protocol):
    """What the handler reads from the Control Plane besides the event itself."""

    async def task(self, task_id: str) -> dict[str, Any] | None:
        """``TaskOut`` or ``None`` when the task is gone or hidden from the service."""
        ...

    async def principal_name(self, principal_id: str) -> str | None:
        """Display name of a principal; ``None`` when it cannot be read."""
        ...


class ControlPlaneCore:
    def __init__(self, client: ControlPlaneClient) -> None:
        self._client = client

    async def task(self, task_id: str) -> dict[str, Any] | None:
        try:
            return await self._client.get_task(task_id)
        except (NotFoundError, PermissionDeniedError):
            return None
        # Anything else (the core is down) propagates: the event is retried.

    async def principal_name(self, principal_id: str) -> str | None:
        # ``GET /principals/{id}`` (``PrincipalOut``); the client has no method
        # for it. The name only decorates the text, so a failure omits it.
        try:
            body = await self._client._request("GET", f"/principals/{principal_id}")
        except ControlPlaneError:
            return None
        name = body.get("displayName")
        return str(name) if name else None


SenderIdentity = Callable[[], Awaitable[TrustedAuthContext]]


class ServiceIdentity:
    """The service's own IAM identity: the sender of the notifications built from events.

    Read from the token the service presents to the Control Plane, verified by
    platform-auth-sdk like any other token: the IAM tenant and ``sub`` of the
    service account. Only those two are used, so the first verified context is
    kept for the life of the process.
    """

    def __init__(self, tokens: ServiceTokenProvider, verifier: TokenVerifier) -> None:
        self._tokens = tokens
        self._verifier = verifier
        self._context: TrustedAuthContext | None = None

    async def __call__(self) -> TrustedAuthContext:
        if self._context is None:
            self._context = await self._verifier.verify(await self._tokens())
        return self._context


class CoreEventHandler:
    """``control_plane_client.events.Handler``: one journal event, handled once."""

    def __init__(
        self,
        sender: NotificationSender,
        core: Core,
        identity: SenderIdentity,
        *,
        task_url_template: str = "",
    ) -> None:
        self._sender = sender
        self._core = core
        self._identity = identity
        self._task_url = task_url_template

    async def __call__(self, event: Event) -> None:
        kind = event.get("type")
        try:
            if kind == REQUESTED:
                await self._requested(event)
            elif kind in CLOSING:
                await self._closed(event, CLOSING[kind])
            elif kind == VERIFICATION_FAILED:
                await self._verification_failed(event)
            # Other ``approval.*`` (outcome execution) tell people nothing here.
        except (KeyError, ValueError):
            # A payload outside its catalog schema: retrying cannot fix it, and
            # raising would hold every later event behind it.
            logger.exception("event %s (%s) is malformed; skipped", event.get("id"), kind)

    # -- approvals ------------------------------------------------------------

    async def _requested(self, event: Event) -> None:
        payload: dict[str, Any] = event.get("payload") or {}
        approval_id = str(event["entityId"])
        recipient = self._decider(event, payload)
        if recipient is None:
            logger.warning(
                "approval %s: neither an assigned principal nor a role in a workspace; "
                "nobody to notify",
                approval_id,
            )
            return

        # Version 2 carries the task's id and title; for older events they are
        # read from the core (FR-008: carried or readable).
        task_id = payload.get("taskId")
        public_id = payload.get("taskPublicId")
        title = payload.get("taskTitle")
        if task_id and public_id is None and title is None:
            task = await self._core.task(str(task_id))
            if task is not None:
                public_id, title = task.get("publicId"), task.get("title")
        requester = payload.get("requestedBy") or event.get("actorId")
        requester_name = await self._core.principal_name(str(requester)) if requester else None

        work = " ".join(str(part) for part in (public_id, title) if part)
        lines = []
        if work:
            lines.append(f"Работа: {work}")
        if requester_name:
            lines.append(f"Запрашивает: {requester_name}")
        comment = payload.get("comment")
        if comment:
            lines.append(f"Комментарий: {comment}")
        await self._send(
            NotificationCreate(
                recipient=recipient,
                type=REQUESTED,
                title=_line(f"Нужно решение: {work}" if work else "Нужно решение", MAX_TITLE),
                body=_text("\n".join(lines), MAX_BODY),
                links=self._task_links(task_id, public_id),
                actions=[
                    Action(
                        id="approve",
                        label="Одобрить",
                        data={"kind": DECIDE, "approvalId": approval_id, "decision": "approve"},
                    ),
                    Action(
                        id="reject",
                        label="Отклонить",
                        data={"kind": DECIDE, "approvalId": approval_id, "decision": "reject"},
                    ),
                ],
            ),
            approval_key(approval_id),
            event,
        )

    @staticmethod
    def _decider(event: Event, payload: dict[str, Any]) -> Recipient | None:
        assigned = payload.get("assignedPrincipalId")
        if assigned:
            return Recipient(kind="principal", id=uuid.UUID(str(assigned)))
        role = payload.get("requiredRoleId")
        workspace = payload.get("workspaceId") or event.get("workspaceId")
        if role and workspace:
            return Recipient(
                kind="role", id=uuid.UUID(str(role)), workspace_id=uuid.UUID(str(workspace))
            )
        return None

    async def _closed(self, event: Event, status: str) -> None:
        payload: dict[str, Any] = event.get("payload") or {}
        by = payload.get("decisionBy") or payload.get("cancelledBy") or event.get("actorId")
        outcome = {
            "status": status,
            "by": str(by) if by else None,
            "channel": payload.get("channel"),
            "at": event.get("occurredAt"),
        }
        identity = await self._identity()
        closed = await self._sender.close_actions(
            identity.tenant_id,
            identity.principal_id,
            approval_key(str(event["entityId"])),
            {key: value for key, value in outcome.items() if value is not None},
        )
        if closed is None:
            logger.info("approval %s %s: no notification to close", event["entityId"], status)

    # -- verification ---------------------------------------------------------

    async def _verification_failed(self, event: Event) -> None:
        payload: dict[str, Any] = event.get("payload") or {}
        task_id = str(payload.get("taskId") or event["entityId"])
        task = await self._core.task(task_id)
        if task is None:
            logger.info("verification of task %s failed, but the task cannot be read", task_id)
            return
        person = task.get("ownerId") or task.get("assigneeId")
        if not person:
            logger.info("verification of task %s failed; the task has nobody to tell", task_id)
            return
        public_id = payload.get("publicId") or task.get("publicId")
        work = " ".join(str(part) for part in (public_id, task.get("title")) if part)
        check = payload.get("failedCheck")
        reason = payload.get("reason")
        lines = [
            f"Попытка {payload.get('attempt')}: не пройдена проверка «{check}»"
            + (f" ({reason})" if reason else "")
            + "."
        ]
        status = payload.get("status")
        if payload.get("blocked"):
            lines.append(
                f"{payload.get('consecutiveFailures')} неудачных попыток подряд — "
                f"задача ждёт человека (статус {status})."
            )
        else:
            lines.append(f"Задача вернулась в работу (статус {status}).")
        await self._send(
            NotificationCreate(
                recipient=Recipient(kind="principal", id=uuid.UUID(str(person))),
                type=VERIFICATION_FAILED,
                title=_line(f"Проверка не пройдена: {work}", MAX_TITLE),
                body=_text("\n".join(lines), MAX_BODY),
                links=self._task_links(task_id, public_id),
            ),
            event_key(str(event["id"])),
            event,
        )

    # -- shared ---------------------------------------------------------------

    def _task_links(self, task_id: object, public_id: object) -> list[Link]:
        if not (self._task_url and task_id):
            return []
        try:
            url = self._task_url.format(taskId=task_id, taskPublicId=public_id or task_id)
            return [Link(label="Открыть задачу", url=url)]  # type: ignore[arg-type]
        except (KeyError, IndexError, ValueError):
            logger.warning("NS_TASK_URL_TEMPLATE does not give a URL; the link is left out")
            return []

    async def _send(self, payload: NotificationCreate, key: str, event: Event) -> None:
        identity = await self._identity()
        try:
            await self._sender.accept(identity, payload, key)
        except Unprocessable as exc:
            # The addressee does not exist for the directory (a removed
            # principal or role): no retry will change that.
            logger.warning("event %s (%s): %s", event.get("id"), event.get("type"), exc.message)
        except Conflict:
            # The key was already used with another text: the notification
            # exists (the event is handled again, and something it is built
            # from — a display name — changed meanwhile).
            logger.info("event %s: already notified under %s", event.get("id"), key)
        # Unavailable (the directory is down) propagates: the event is retried.


def build_consumer(
    settings: Settings,
    client: ControlPlaneClient,
    engine: AsyncEngine,
    handler: CoreEventHandler,
) -> EventConsumer:
    """The SDK consumer over the service's database (tables of migration ``0002``)."""
    return EventConsumer(
        client,
        EVENT_TYPES,
        settings.events_workspace_id or None,
        SqlAlchemyCursorStore(engine),
        handler,
        name=CONSUMER_NAME,
        start=settings.events_start,
        poll_interval=settings.events_poll_seconds,
    )
