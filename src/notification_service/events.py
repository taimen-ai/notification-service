"""Consumer of Control Plane events, driven by notification rules (ADR-0005).

Which events become notifications, for whom, with what text and buttons, and
which events close those buttons is data: the ``NotificationRule`` versions
applied to the service (``rule_store``). This module reads the journal with the
consumer SDK of the core (``control_plane_client.events``) on the filter the
enabled rules describe and executes them under the service's own identity:

- an event of a rule's ``on.type`` whose ``on.when`` holds becomes one
  notification of that rule, with the rule's dedup key;
- an event of a rule's ``close.on`` closes the actions of the notification
  with the dedup key the rule renders over it.

No enabled rule — no consumer: the service reads no events and its cursor
stays where it was (FR-012). The filter follows the rules within one poll.

Exactly one notification per event and rule, across restarts: the SDK records
every handled event id together with the cursor, and a notification is sent
with the rule's dedup key, so an event handled again after a crash between the
two replays the notification instead of creating a second one.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
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
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from notification_service import rule_store
from notification_service.config import Settings
from notification_service.errors import Conflict, Unprocessable
from notification_service.rule_store import ActiveRule
from notification_service.rules import (
    APPROVAL_DECIDE,
    DEFAULT_ASSIGNED_REF,
    DISPLAY_NAME,
    ROOT_EVENT,
    ROOT_PAYLOAD,
    ROOT_TASK,
    ConditionError,
    closes_on,
    condition_paths,
    dedup_template,
    evaluate,
    fill,
    on_matches,
    parse_path,
    placeholders,
    subscription,
    walk,
)
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
CONSUMER_STOP_SECONDS = 10.0
# ``data.kind`` of a decision action: what a channel that executes actions
# (Telegram, ADR-0003) does with it.
DECIDE = "approval.decide"
MAX_DEDUP_KEY = 200
MAX_OUTCOME = 200
MAX_LINK_LABEL = 100

_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_ABSOLUTE_URL = re.compile(r"^https?://[^\s/]+", re.IGNORECASE)


def _line(value: str, limit: int) -> str:
    """One line of plain text within ``limit``: what a title must be."""
    text = " ".join(_CONTROL.sub(" ", value).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _text(value: str, limit: int) -> str:
    text = _CONTROL.sub(" ", value).replace("\r", "").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


class Core(Protocol):
    """What rules read from the Control Plane besides the event itself."""

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
    kept for the life of the process. The tenant is also whose rules run.
    """

    def __init__(self, tokens: ServiceTokenProvider, verifier: TokenVerifier) -> None:
        self._tokens = tokens
        self._verifier = verifier
        self._context: TrustedAuthContext | None = None

    async def __call__(self) -> TrustedAuthContext:
        if self._context is None:
            self._context = await self._verifier.verify(await self._tokens())
        return self._context


RuleSource = Callable[[uuid.UUID], Awaitable[list[ActiveRule]]]


def stored_rules(sessions: async_sessionmaker[AsyncSession]) -> RuleSource:
    async def load(tenant_id: uuid.UUID) -> list[ActiveRule]:
        return await rule_store.executable(sessions, tenant_id)

    return load


class EventFacts:
    """The data roots of one event (ADR-0005 §4): ``payload``, ``event``, ``task``.

    The task is read from the core only when a rule reads the root ``task``,
    once per event; principal names once per principal.
    """

    _UNREAD = object()

    def __init__(self, event: Event, core: Core) -> None:
        self.event = event
        self.payload: Mapping[str, Any] = event.get("payload") or {}
        self._core = core
        self._task: Any = self._UNREAD
        self._names: dict[str, str | None] = {}

    async def task(self) -> Mapping[str, Any] | None:
        if self._task is self._UNREAD:
            if self.event.get("entityType") == "task":
                task_id = self.event.get("entityId")
            else:
                task_id = self.payload.get("taskId")
            self._task = await self._core.task(str(task_id)) if task_id else None
        return self._task  # type: ignore[no-any-return]

    async def get(self, path: str) -> Any:
        parsed = parse_path(path)
        if parsed is None:
            return None
        root, segments = parsed
        if segments and segments[-1] == DISPLAY_NAME:
            principal = await self._walk(root, segments[:-1])
            if isinstance(principal, str) and principal:
                if principal not in self._names:
                    self._names[principal] = await self._core.principal_name(principal)
                return self._names[principal]
        return await self._walk(root, segments)

    async def _walk(self, root: str, segments: tuple[str, ...]) -> Any:
        if root == ROOT_PAYLOAD:
            return walk(self.payload, segments)
        if root == ROOT_EVENT:
            return walk({k: v for k, v in self.event.items() if k != "payload"}, segments)
        assert root == ROOT_TASK
        return walk(await self.task(), segments)

    async def values(self, paths: list[str]) -> dict[str, Any]:
        return {path: await self.get(path) for path in paths}

    async def fill(self, template: str) -> tuple[str, bool]:
        return fill(template, await self.values(placeholders(template)))


class RuleSkipped(Exception):
    """This rule does nothing with this event; the reason goes to the log."""


class RuleEventHandler:
    """``control_plane_client.events.Handler``: one journal event, the rules of the moment."""

    def __init__(
        self,
        sender: NotificationSender,
        core: Core,
        identity: SenderIdentity,
        rules: RuleSource,
    ) -> None:
        self._sender = sender
        self._core = core
        self._identity = identity
        self._rules = rules

    async def __call__(self, event: Event) -> None:
        identity = await self._identity()
        kind = str(event.get("type"))
        facts = EventFacts(event, self._core)
        # The versions in force now: an event read under an older filter that
        # no rule describes any more is simply passed over.
        for rule in await self._rules(identity.tenant_id):
            try:
                if on_matches(rule.spec, kind):
                    await self._open(rule, facts, identity)
                if closes_on(rule.spec, kind):
                    await self._close(rule, facts, identity)
            except RuleSkipped as skipped:
                logger.info("rule %s, event %s (%s): %s", rule.key, event.get("id"), kind, skipped)
            except (ConditionError, KeyError, ValueError, ValidationError):
                # A broken rule or a payload outside its catalog schema:
                # retrying cannot fix it, and raising would hold every later
                # event behind it. The other rules still run.
                logger.exception("rule %s failed on event %s (%s)", rule.key, event.get("id"), kind)

    # -- a notification ---------------------------------------------------------------

    async def _open(
        self, rule: ActiveRule, facts: EventFacts, identity: TrustedAuthContext
    ) -> None:
        spec = rule.spec
        when = spec["on"].get("when", True)
        values = await facts.values([path for path, _ in condition_paths(when)])
        if not evaluate(when, values.__getitem__):
            return
        recipient = await self._recipient(spec["recipient"], facts)
        if recipient is None:
            raise RuleSkipped("nobody to notify")
        key = await self._dedup_key(rule, facts)
        notification: Mapping[str, Any] = spec["notification"]
        title, _ = await facts.fill(notification["title"])
        payload = NotificationCreate(
            recipient=recipient,
            type=notification["type"],
            title=_line(title, MAX_TITLE) or notification["type"],
            body=_text(await self._body(notification.get("body", ""), facts), MAX_BODY),
            links=await self._links(notification.get("links", []), facts),
            actions=self._actions(notification.get("actions", []), facts),
        )
        try:
            await self._sender.accept(identity, payload, key)
        except Unprocessable as exc:
            # The addressee does not exist for the directory (a removed
            # principal or role): no retry will change that.
            raise RuleSkipped(exc.message) from exc
        except Conflict as exc:
            # The key was already used with another text: the notification
            # exists (the event is handled again, and something it is built
            # from — a display name — changed meanwhile).
            raise RuleSkipped(f"already notified under {key}") from exc
        # Unavailable (the directory is down) propagates: the event is retried.

    async def _recipient(self, recipient: Mapping[str, Any], facts: EventFacts) -> Recipient | None:
        kind = recipient["kind"]
        found: Recipient | None = None
        if kind == "principal":
            found = _principal(recipient["ref"])
        elif kind == "assigned":
            found = _principal(await facts.get(recipient.get("ref") or DEFAULT_ASSIGNED_REF))
            if found is None:
                workspace = (
                    await facts.get(recipient["workspace"])
                    if recipient.get("workspace")
                    else (
                        await facts.get("payload.workspaceId")
                        or await facts.get("event.workspaceId")
                    )
                )
                found = _role(await facts.get("payload.requiredRoleId"), workspace)
        elif kind == "role":
            found = _role(
                await facts.get(recipient["ref"]),
                await facts.get(recipient.get("workspace") or "event.workspaceId"),
            )
        else:
            found = await _task_person(kind, facts)
        if found is None:
            found = await _task_person(recipient.get("fallback", "none"), facts)
        return found

    @staticmethod
    async def _body(template: str, facts: EventFacts) -> str:
        lines = []
        for line in template.split("\n"):
            text, empty = await facts.fill(line)
            # A line whose every placeholder is empty is left out ("Комментарий: …").
            if not empty:
                lines.append(text)
        return "\n".join(lines)

    @staticmethod
    async def _links(links: list[Mapping[str, str]], facts: EventFacts) -> list[Link]:
        built = []
        for link in links:
            url, _ = await facts.fill(link["url"])
            url_values = await facts.values(placeholders(link["url"]))
            if any(not text for text in map(_scalar_text, url_values.values())):
                continue
            label, _ = await facts.fill(link["label"])
            if not _ABSOLUTE_URL.match(url):
                continue
            try:
                built.append(Link(label=_line(label, MAX_LINK_LABEL), url=url))  # type: ignore[arg-type]
            except ValidationError:
                continue
        return built

    @staticmethod
    def _actions(actions: list[str], facts: EventFacts) -> list[Action]:
        if APPROVAL_DECIDE not in actions:
            return []
        # Events ``approval.*`` are about the approval: its id is the entity's.
        approval_id = str(facts.event["entityId"])
        return [
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
        ]

    @staticmethod
    async def _dedup_key(rule: ActiveRule, facts: EventFacts) -> str:
        template = dedup_template(rule.key, rule.spec)
        values = await facts.values(placeholders(template))
        if any(not _scalar_text(value) for value in values.values()):
            # A key missing a part would merge notifications of unrelated events.
            raise RuleSkipped("the dedup key has an empty part")
        key, _ = fill(template, values)
        if len(key) > MAX_DEDUP_KEY:
            raise RuleSkipped(f"the dedup key is longer than {MAX_DEDUP_KEY}")
        return key

    # -- closing its actions ---------------------------------------------------------

    async def _close(
        self, rule: ActiveRule, facts: EventFacts, identity: TrustedAuthContext
    ) -> None:
        key = await self._dedup_key(rule, facts)
        event = facts.event
        status = ""
        template = (rule.spec.get("close") or {}).get("outcome")
        if template:
            status = _line((await facts.fill(template))[0], MAX_OUTCOME)
        outcome = {
            # By default the last segment of the type: approved, rejected, cancelled.
            "status": status or str(event["type"]).rsplit(".", 1)[-1],
            "by": event.get("actorId"),
            "channel": facts.payload.get("channel"),
            "at": event.get("occurredAt"),
        }
        closed = await self._sender.close_actions(
            identity.tenant_id,
            identity.principal_id,
            key,
            {name: value for name, value in outcome.items() if value is not None},
        )
        if closed is None:
            logger.info(
                "rule %s, event %s: no notification %s to close", rule.key, event.get("id"), key
            )


def _scalar_text(value: Any) -> str:
    if value is None or isinstance(value, (dict, list)):
        return ""
    return str(value)


def _principal(value: Any) -> Recipient | None:
    if not value:
        return None
    return Recipient(kind="principal", id=uuid.UUID(str(value)))


def _role(role: Any, workspace: Any) -> Recipient | None:
    if not (role and workspace):
        return None
    return Recipient(kind="role", id=uuid.UUID(str(role)), workspace_id=uuid.UUID(str(workspace)))


async def _task_person(kind: str, facts: EventFacts) -> Recipient | None:
    if kind == "taskOwner":
        return _principal(await facts.get("task.ownerId"))
    if kind == "taskAssignee":
        return _principal(await facts.get("task.assigneeId"))
    return None


# --- the consumer on the rules' filter --------------------------------------------------------


class RuleConsumer:
    """Runs the SDK consumer on the filter of the enabled rules; none of them — no consumer.

    Every ``poll_seconds`` (or on :meth:`wake`) the rules of the service's
    tenant are read again; a different filter stops the running consumer and
    starts one on the new filter under the same name, so it resumes from the
    same stored cursor.
    """

    def __init__(
        self,
        build: Callable[[tuple[str, ...]], EventConsumer],
        rules: RuleSource,
        identity: SenderIdentity,
        *,
        poll_seconds: float,
    ) -> None:
        self.build = build
        self._rules = rules
        self._identity = identity
        self._poll_seconds = poll_seconds
        self.types: tuple[str, ...] = ()
        self.consumer: EventConsumer | None = None
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()
        self._wake.set()

    def wake(self) -> None:
        """Read the rules now: one was applied or retired."""
        self._wake.set()

    async def run(self) -> None:
        try:
            while not self._stopping.is_set():
                self._wake.clear()
                try:
                    identity = await self._identity()
                    types = subscription(r.spec for r in await self._rules(identity.tenant_id))
                except Exception:
                    # IAM or the database is down: the running consumer keeps
                    # its filter until the rules can be read again.
                    logger.exception("notification rules cannot be read; the filter stays")
                else:
                    if types != self.types:
                        await self._switch(types)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), self._poll_seconds)
        finally:
            await self._switch(())

    async def _switch(self, types: tuple[str, ...]) -> None:
        if self.consumer is not None and self._task is not None:
            self.consumer.stop()
            # The event in hand is finished; a read hanging on the core is
            # cancelled — its event is simply read again on the new filter.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._task, CONSUMER_STOP_SECONDS)
        self.consumer, self._task = None, None
        if types:
            logger.info("reading core events of %s", ", ".join(types))
        elif self.types:
            logger.info("no enabled notification rule: core events are not read")
        self.types = types
        if types:
            self.consumer = self.build(types)
            self._task = asyncio.create_task(_run(self.consumer))


async def _run(consumer: EventConsumer) -> None:
    try:
        await consumer.run()
    except asyncio.CancelledError:
        raise
    except Exception:
        # The core refuses the subscription itself (no events.read, a
        # credential it rejects): rereading changes nothing until an operator
        # fixes the grant or the rules change. The API and delivery keep working.
        logger.exception("event consumer stopped: the Control Plane refused the subscription")


def build_consumer(
    settings: Settings,
    client: ControlPlaneClient,
    engine: AsyncEngine,
    handler: RuleEventHandler,
    types: tuple[str, ...],
) -> EventConsumer:
    """The SDK consumer over the service's database (tables of migration ``0002``)."""
    return EventConsumer(
        client,
        types,
        settings.events_workspace_id or None,
        SqlAlchemyCursorStore(engine),
        handler,
        name=CONSUMER_NAME,
        start=settings.events_start,
        poll_interval=settings.events_poll_seconds,
    )
