"""Notification rules as data (ADR-0005): the spec, its check, conditions and templates.

A rule says which core event becomes a notification, for whom, with what text
and buttons, and which events close those buttons. Its shape is the platform's
``notificationRuleSpec`` (``contracts/notification-rule.schema.json``); what it
may reference is the snapshot of the core's event catalog
(``contracts/catalog.json``) and the core's task projection (``TASK_FIELDS``).

Conditions are the core's rule grammar (CP-ADR-0063 §2) over the roots
``payload``, ``event`` and ``task``, evaluated here by the service's own
evaluator; templates are ``{{ path }}`` placeholders without any logic.

Pure functions, no database and no I/O.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

CONTRACTS = Path(__file__).resolve().parent / "contracts"

RULE_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")

ROOT_PAYLOAD = "payload"
ROOT_EVENT = "event"
ROOT_TASK = "task"
ROOTS = frozenset({ROOT_PAYLOAD, ROOT_EVENT, ROOT_TASK})

# The path grammar of CP-ADR-0063 §2.
SEGMENT_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_PATH_SEGMENTS = 16
PLACEHOLDER_RE = re.compile(r"\{\{\s*([^{}]*?)\s*\}\}")
MAX_CONDITION_BYTES = 16 * 1024
MAX_EXPRESSION_DEPTH = 16
MAX_EXPRESSION_NODES = 256
MAX_OPERATOR_ARGS = 50
_LOGICAL = ("and", "or")
_COMPARISONS = ("eq", "ne", "lt", "le", "gt", "ge")
OPERATORS = frozenset({*_LOGICAL, "not", *_COMPARISONS, "in", "exists"})

# A path ending with this segment after a principal id reads the principal's name.
DISPLAY_NAME = "displayName"

# Fields of the core's task projection (``TaskOut`` of the pinned core revision)
# and the JSON type of each; the contract test keeps them equal to the model.
TASK_FIELDS: dict[str, str] = {
    "id": "string",
    "tenantId": "string",
    "publicId": "string",
    "workspaceId": "string",
    "projectId": "string",
    "typeId": "string",
    "typeKey": "string",
    "typeVersion": "integer",
    "title": "string",
    "description": "string",
    "status": "string",
    "systemStatusCategory": "string",
    "priority": "string",
    "ownerId": "string",
    "assigneeId": "string",
    "customFields": "object",
    "startDate": "string",
    "dueDate": "string",
    "goalId": "string",
    "origin": "object",
    "acceptance": "array",
    "evidence": "array",
    "version": "integer",
    "claimEpoch": "integer",
    "activeClaimId": "string",
    "createdBy": "string",
    "createdAt": "string",
    "updatedAt": "string",
    "completedAt": "string",
    "verification": "object",
}
# Principal ids outside the payload whose ``displayName`` a template may read.
PRINCIPAL_FIELDS = {
    ROOT_EVENT: frozenset({"actorId"}),
    ROOT_TASK: frozenset({"ownerId", "assigneeId", "createdBy"}),
}

DEFAULT_ASSIGNED_REF = "payload.assignedPrincipalId"
APPROVAL_DECIDE = "approvalDecide"

# Error codes of ``details.errors`` (ADR-0005 §6).
INVALID_SPEC = "invalid_spec"
UNKNOWN_EVENT_TYPE = "unknown_event_type"
UNKNOWN_FIELD = "unknown_field"
INVALID_CONDITION = "invalid_condition"
INVALID_RULE = "invalid_rule"


# --- the catalog snapshot ------------------------------------------------------------


class Catalog:
    """The core's event catalog (CP-ADR-0068) as rules see it."""

    def __init__(self, document: Mapping[str, Any]) -> None:
        self.envelope = frozenset(document["envelope"]) - {"payload"}
        self._types: Mapping[str, Any] = document["types"]

    def __contains__(self, event_type: object) -> bool:
        return event_type in self._types

    def matching(self, on_type: str) -> list[str]:
        """Types ``on.type`` names: itself, or every type under a ``prefix.*``."""
        if on_type.endswith(".*"):
            prefix = on_type[:-1]
            return sorted(t for t in self._types if t.startswith(prefix))
        return [on_type] if on_type in self._types else []

    def entity(self, event_type: str) -> str:
        return str(self._types[event_type]["entityType"])

    def payload_schema(self, event_type: str) -> Mapping[str, Any]:
        entry = self._types[event_type]
        return entry["versions"][str(entry["currentVersion"])]["schema"]  # type: ignore[no-any-return]

    def carries_task(self, event_type: str) -> bool:
        """The event names a task: it is about one, or its payload has ``taskId``."""
        return self.entity(event_type) == "task" or "taskId" in self.payload_schema(event_type).get(
            "properties", {}
        )


@cache
def catalog() -> Catalog:
    return Catalog(json.loads((CONTRACTS / "catalog.json").read_text(encoding="utf-8")))


@cache
def _spec_validator() -> Draft202012Validator:
    schema = json.loads((CONTRACTS / "notification-rule.schema.json").read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


# --- hashing ---------------------------------------------------------------------------


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def spec_hash(spec: Mapping[str, Any]) -> str:
    """sha256 of the canonical JSON of a spec: equal specs are one version."""
    return hashlib.sha256(canonical(spec)).hexdigest()


# --- paths -----------------------------------------------------------------------------


def parse_path(value: Any) -> tuple[str, tuple[str, ...]] | None:
    """``root(.segment)*`` with a known root; ``None`` when it is not one."""
    if not isinstance(value, str) or not value:
        return None
    root, *segments = value.split(".")
    if root not in ROOTS or len(segments) > MAX_PATH_SEGMENTS:
        return None
    if not all(SEGMENT_RE.match(segment) for segment in segments):
        return None
    return root, tuple(segments)


def placeholders(template: str) -> list[str]:
    return [match.group(1) for match in PLACEHOLDER_RE.finditer(template)]


def walk(document: Any, segments: Iterable[str]) -> Any:
    """Follow segments through objects (by key) and lists (by index), as the core does."""
    current = document
    for segment in segments:
        if isinstance(current, Mapping):
            current = current.get(segment)
        elif isinstance(current, list) and segment.isdigit():
            index = int(segment)
            current = current[index] if index < len(current) else None
        else:
            return None
        if current is None:
            return None
    return current


# --- conditions (CP-ADR-0063 §2) ----------------------------------------------------------


class ConditionInvalid(Exception):
    """An expression outside the grammar; ``pointer`` is where in the spec."""

    def __init__(self, message: str, pointer: str) -> None:
        super().__init__(message)
        self.message = message
        self.pointer = pointer


class ConditionError(Exception):
    """An expression could not be evaluated on these facts (a broken rule)."""


def condition_paths(expression: Any, pointer: str = "/on/when") -> list[tuple[str, str]]:
    """Refuse anything outside the grammar; every ``(path, pointer)`` the condition reads."""
    paths: list[tuple[str, str]] = []
    nodes = 0

    def count(at: str, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > MAX_EXPRESSION_NODES:
            raise ConditionInvalid(f"the condition exceeds {MAX_EXPRESSION_NODES} nodes", at)
        if depth > MAX_EXPRESSION_DEPTH:
            raise ConditionInvalid(
                f"the condition is nested deeper than {MAX_EXPRESSION_DEPTH}", at
            )

    def path(value: Any, at: str) -> None:
        if parse_path(value) is None:
            raise ConditionInvalid(f"{value!r} is not a path with a root of {sorted(ROOTS)}", at)
        paths.append((str(value), at))

    def operand(value: Any, at: str, depth: int) -> None:
        count(at, depth)
        if value is None or isinstance(value, (bool, int, float, str)):
            return
        if isinstance(value, list):
            if len(value) > MAX_OPERATOR_ARGS:
                raise ConditionInvalid(f"more than {MAX_OPERATOR_ARGS} items", at)
            for index, item in enumerate(value):
                operand(item, f"{at}/{index}", depth + 1)
            return
        if isinstance(value, dict) and set(value) == {"var"}:
            path(value["var"], f"{at}/var")
            return
        if isinstance(value, dict) and set(value) == {"const"}:
            return
        # An operator used as an operand is still an expression (a boolean).
        node(value, at, depth)

    def node(value: Any, at: str, depth: int) -> None:
        count(at, depth)
        if isinstance(value, bool):
            return
        if not isinstance(value, dict) or len(value) != 1:
            raise ConditionInvalid("an expression is true, false or a single-operator object", at)
        ((operator, args),) = value.items()
        if operator not in OPERATORS:
            raise ConditionInvalid(f"unknown operator {operator!r}", at)
        here = f"{at}/{operator}"
        if operator in _LOGICAL:
            if not isinstance(args, list) or not 1 <= len(args) <= MAX_OPERATOR_ARGS:
                raise ConditionInvalid(
                    f"{operator} takes a list of 1..{MAX_OPERATOR_ARGS} expressions", here
                )
            for index, item in enumerate(args):
                node(item, f"{here}/{index}", depth + 1)
        elif operator == "not":
            node(args, here, depth + 1)
        elif operator == "exists":
            path(args, here)
        else:
            if not isinstance(args, list) or len(args) != 2:
                raise ConditionInvalid(f"{operator} takes exactly two operands", here)
            operand(args[0], f"{here}/0", depth + 1)
            operand(args[1], f"{here}/1", depth + 1)

    node(expression, pointer, 0)
    return paths


Resolver = Callable[[str], Any]


def _value(operand: Any, resolve: Resolver) -> Any:
    if isinstance(operand, dict):
        if set(operand) == {"var"}:
            return resolve(operand["var"])
        if set(operand) == {"const"}:
            return operand["const"]
        return evaluate(operand, resolve)
    if isinstance(operand, list):
        return [_value(item, resolve) for item in operand]
    return operand


def _strict_equal(left: Any, right: Any) -> bool:
    """JSON equality without Python's ``True == 1``."""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _strict_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    if isinstance(left, dict):
        return set(left) == set(right) and all(_strict_equal(left[k], right[k]) for k in left)
    return bool(left == right)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _ordered(operator: str, left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    if not (
        (_is_number(left) and _is_number(right))
        or (isinstance(left, str) and isinstance(right, str))
    ):
        raise ConditionError(
            f"{operator} compares {type(left).__name__} with {type(right).__name__}"
        )
    if operator == "lt":
        return bool(left < right)
    if operator == "le":
        return bool(left <= right)
    if operator == "gt":
        return bool(left > right)
    return bool(left >= right)


def evaluate(expression: Any, resolve: Resolver) -> bool:
    """Evaluate a checked expression; :class:`ConditionError` on a type clash."""
    if isinstance(expression, bool):
        return expression
    ((operator, args),) = expression.items()
    if operator == "and":
        return all(evaluate(item, resolve) for item in args)
    if operator == "or":
        return any(evaluate(item, resolve) for item in args)
    if operator == "not":
        return not evaluate(args, resolve)
    if operator == "exists":
        return resolve(args) is not None
    left = _value(args[0], resolve)
    right = _value(args[1], resolve)
    if operator == "eq":
        return _strict_equal(left, right)
    if operator == "ne":
        return not _strict_equal(left, right)
    if operator == "in":
        if right is None:
            return False
        if not isinstance(right, list):
            raise ConditionError(f"in expects a list on the right, got {type(right).__name__}")
        return any(_strict_equal(left, item) for item in right)
    return _ordered(operator, left, right)


# --- templates ---------------------------------------------------------------------------


def text_of(value: Any) -> str:
    """What a placeholder becomes: a scalar's text; nothing for null, objects and lists."""
    if value is None or isinstance(value, (dict, list)):
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value)


def fill(template: str, values: Mapping[str, Any]) -> tuple[str, bool]:
    """The template with its placeholders filled, and whether every one of them was empty.

    A template without placeholders is never "empty".
    """
    found = False
    filled = False

    def one(match: re.Match[str]) -> str:
        nonlocal found, filled
        found = True
        text = text_of(values.get(match.group(1)))
        filled = filled or bool(text)
        return text

    result = PLACEHOLDER_RE.sub(one, template)
    return result, found and not filled


# --- the spec ------------------------------------------------------------------------------


@dataclass(frozen=True)
class RuleError:
    """One finding of the check: where in the spec (JSON Pointer), which code, why."""

    path: str
    code: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"path": self.path, "code": self.code, "message": self.message}


def _pointer(parts: Iterable[Any]) -> str:
    return "".join("/" + str(part).replace("~", "~0").replace("/", "~1") for part in parts) or ""


def _types_of(schema: Mapping[str, Any]) -> set[str]:
    declared = schema.get("type")
    if declared is None:
        return {"any"}
    kinds = {declared} if isinstance(declared, str) else set(declared)
    return (kinds - {"null"}) or {"any"}


def _payload_kind(schema: Mapping[str, Any], segments: tuple[str, ...]) -> tuple[str | None, str]:
    """Kind of the value at ``payload.<segments>`` by the schema, or ``None`` and why not."""
    node: Mapping[str, Any] = schema
    for index, segment in enumerate(segments):
        if (
            segment == DISPLAY_NAME
            and index == len(segments) - 1
            and index > 0
            and node.get("format") == "uuid"
        ):
            return "string", ""
        kinds = _types_of(node)
        if "array" in kinds and segment.isdigit():
            node = node.get("items", {})
            continue
        properties = node.get("properties")
        if properties is None:
            if kinds & {"object", "any"}:
                # An open object: nothing to check deeper.
                return "any", ""
            return None, f"{'.'.join(segments[:index])} has no fields"
        if segment not in properties:
            return None, f"no field {segment!r}"
        node = properties[segment]
    kinds = _types_of(node)
    if kinds <= {"object", "array"}:
        return "object" if "object" in kinds else "array", ""
    return "scalar", ""


def _path_errors(
    path: str,
    pointer: str,
    event_types: list[str],
    known: Catalog,
    *,
    substituted: bool,
) -> list[RuleError]:
    """Is ``path`` readable on every event the rule reads it from (ADR-0005 §6 п. 3)?"""
    parsed = parse_path(path)
    if parsed is None:
        return [
            RuleError(
                pointer,
                UNKNOWN_FIELD,
                f"{path!r} is not a path with a root of {sorted(ROOTS)}",
            )
        ]
    root, segments = parsed
    if not segments:
        return [RuleError(pointer, UNKNOWN_FIELD, f"{path!r} names a root, not a field")]
    errors: list[RuleError] = []
    kinds: set[str] = set()
    field, rest = segments[0], segments[1:]
    if root == ROOT_PAYLOAD:
        for event_type in event_types:
            kind, why = _payload_kind(known.payload_schema(event_type), segments)
            if kind is None:
                errors.append(RuleError(pointer, UNKNOWN_FIELD, f"{path}: {why} in {event_type}"))
            else:
                kinds.add(kind)
    else:
        if root == ROOT_EVENT:
            declared = "string" if field in known.envelope else None
        else:
            declared = TASK_FIELDS.get(field)
            for event_type in event_types:
                if not known.carries_task(event_type):
                    errors.append(
                        RuleError(
                            pointer,
                            UNKNOWN_FIELD,
                            f"{path}: {event_type} names no task (entity or payload.taskId)",
                        )
                    )
        if declared is None:
            errors.append(RuleError(pointer, UNKNOWN_FIELD, f"{path}: no field {field!r}"))
        elif not rest:
            kinds.add("scalar" if declared not in ("object", "array") else declared)
        elif rest == (DISPLAY_NAME,) and field in PRINCIPAL_FIELDS[root]:
            kinds.add("scalar")
        elif declared == "object" or (declared == "array" and rest[0].isdigit()):
            kinds.add("any")
        else:
            errors.append(RuleError(pointer, UNKNOWN_FIELD, f"{path}: {field} has no fields"))
    if substituted and not errors and kinds & {"object", "array"}:
        errors.append(
            RuleError(pointer, INVALID_RULE, f"{path} is an object or a list, not a text")
        )
    return errors


def _template_errors(
    template: Any, pointer: str, event_types: list[str], known: Catalog
) -> list[RuleError]:
    if not isinstance(template, str):
        return []
    return [
        error
        for path in placeholders(template)
        for error in _path_errors(path, pointer, event_types, known, substituted=True)
    ]


def check_spec(spec: Any, known: Catalog | None = None) -> list[RuleError]:
    """Every finding against a rule spec, in the order of ADR-0005 §6; empty — valid."""
    known = known or catalog()
    shape = sorted(_spec_validator().iter_errors(spec), key=lambda error: list(error.absolute_path))
    if shape:
        # Nothing below can be trusted on a spec of the wrong shape.
        return [
            RuleError(_pointer(error.absolute_path), INVALID_SPEC, error.message) for error in shape
        ]
    errors: list[RuleError] = []

    on_type: str = spec["on"]["type"]
    on_types = known.matching(on_type)
    if not on_types:
        errors.append(
            RuleError("/on/type", UNKNOWN_EVENT_TYPE, f"no event type {on_type!r} in the catalog")
        )
    close: Mapping[str, Any] = spec.get("close") or {}
    close_types: list[str] = []
    for index, event_type in enumerate(close.get("on", [])):
        if event_type in known:
            close_types.append(event_type)
        else:
            errors.append(
                RuleError(
                    f"/close/on/{index}",
                    UNKNOWN_EVENT_TYPE,
                    f"no event type {event_type!r} in the catalog",
                )
            )

    when = spec["on"].get("when", True)
    try:
        if len(canonical(when)) > MAX_CONDITION_BYTES:
            raise ConditionInvalid(f"the condition exceeds {MAX_CONDITION_BYTES} bytes", "/on/when")
        read = condition_paths(when)
    except ConditionInvalid as exc:
        errors.append(RuleError(exc.pointer, INVALID_CONDITION, exc.message))
        read = []
    if on_types:
        for path, pointer in read:
            errors += _path_errors(path, pointer, on_types, known, substituted=False)

    notification: Mapping[str, Any] = spec["notification"]
    if on_types:
        errors += _template_errors(notification["title"], "/notification/title", on_types, known)
        errors += _template_errors(notification.get("body"), "/notification/body", on_types, known)
        for index, link in enumerate(notification.get("links", [])):
            for part in ("label", "url"):
                errors += _template_errors(
                    link[part], f"/notification/links/{index}/{part}", on_types, known
                )
    if on_types or close_types:
        errors += _template_errors(
            spec.get("dedupKeyTemplate"), "/dedupKeyTemplate", on_types + close_types, known
        )
    if close_types:
        errors += _template_errors(close.get("outcome"), "/close/outcome", close_types, known)

    errors += _recipient_errors(spec["recipient"], on_types, known)

    if APPROVAL_DECIDE in notification.get("actions", []):
        others = sorted({known.entity(t) for t in on_types} - {"approval"})
        if others:
            errors.append(
                RuleError(
                    "/notification/actions",
                    INVALID_RULE,
                    f"{APPROVAL_DECIDE} needs events about an approval, not {others}",
                )
            )
    return errors


def _recipient_errors(
    recipient: Mapping[str, Any], on_types: list[str], known: Catalog
) -> list[RuleError]:
    errors: list[RuleError] = []
    kind = recipient["kind"]
    ref = recipient.get("ref")
    if kind == "principal":
        if not ref or not _is_uuid(ref):
            errors.append(
                RuleError(
                    "/recipient/ref", INVALID_RULE, "a principal recipient needs its id as ref"
                )
            )
        return errors
    if kind == "role" and not ref:
        errors.append(
            RuleError("/recipient/ref", INVALID_RULE, "a role recipient needs the path to its id")
        )
    if on_types and kind in ("assigned", "role"):
        for name in ("ref", "workspace"):
            if recipient.get(name):
                errors += _path_errors(
                    recipient[name], f"/recipient/{name}", on_types, known, substituted=True
                )
    for field, value in (("kind", kind), ("fallback", recipient.get("fallback"))):
        if value in ("taskOwner", "taskAssignee"):
            lacking = [t for t in on_types if not known.carries_task(t)]
            if lacking:
                errors.append(
                    RuleError(
                        f"/recipient/{field}",
                        INVALID_RULE,
                        f"{value} needs events that name a task, not {lacking}",
                    )
                )
    return errors


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


# --- reading a rule --------------------------------------------------------------------------


def on_matches(spec: Mapping[str, Any], event_type: str) -> bool:
    on_type: str = spec["on"]["type"]
    if on_type.endswith(".*"):
        return event_type.startswith(on_type[:-1])
    return on_type == event_type


def closes_on(spec: Mapping[str, Any], event_type: str) -> bool:
    return event_type in (spec.get("close") or {}).get("on", [])


def enabled(spec: Mapping[str, Any]) -> bool:
    return spec.get("status", "enabled") == "enabled"


def subscription(specs: Iterable[Mapping[str, Any]]) -> tuple[str, ...]:
    """The consumer's filter: ``on.type`` (``x.*`` as the prefix ``x.``) and ``close.on``."""
    types: set[str] = set()
    for spec in specs:
        if not enabled(spec):
            continue
        on_type: str = spec["on"]["type"]
        types.add(on_type[:-1] if on_type.endswith(".*") else on_type)
        types.update((spec.get("close") or {}).get("on", []))
    return tuple(sorted(types))


def dedup_template(key: str, spec: Mapping[str, Any]) -> str:
    return str(spec.get("dedupKeyTemplate") or f"rule:{key}:event:{{{{event.id}}}}")
