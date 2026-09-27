"""Contract: the notification rule data of ADR-0005 against the core and the platform schema.

- ``contracts/catalog.json`` is a snapshot of the core's event catalog: rule
  specs are checked against it (event types, payload fields), so it must equal
  the catalog of the pinned core revision.
- ``contracts/notification-rule.schema.json`` is a verbatim copy of
  ``$defs.notificationRuleSpec`` of the superproject's
  ``packages/schema/v1/object.schema.json`` (C001); the ADR records its hash.
- The rules that carry the former built-in behaviour (the YAML blocks of
  ADR-0005) are valid by that schema and reference only what the catalog and
  the task projection have.

To refresh the snapshot after the core moved: copy ``docs/events/catalog.json``
of ``../control-plane`` over ``src/notification_service/contracts/catalog.json``.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any

import pytest
import yaml
from control_plane.api.v1.schemas import TaskOut
from control_plane.domain.event_catalog import catalog_document
from jsonschema import Draft202012Validator

from notification_service import rules

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "src" / "notification_service" / "contracts"
ADR = ROOT / "docs" / "adr" / "0005-notification-rules-as-data.md"

# sha256 of the canonical JSON of ``$defs.notificationRuleSpec`` (C001, b8d6d11),
# also written in ADR-0005 §1.
SPEC_SCHEMA_SHA256 = "76563093d8bf5e2e5b2e37f2d75f14dc001c606e16030c84bca30b88ba062bf4"

_PLACEHOLDER = re.compile(r"\{\{\s*([^}\s]+)\s*\}\}")


def _load(name: str) -> Any:
    return json.loads((CONTRACTS / name).read_text(encoding="utf-8"))


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _adr_rules() -> list[dict[str, Any]]:
    text = ADR.read_text(encoding="utf-8")
    blocks = re.findall(r"```yaml\n(.*?)```", text, flags=re.DOTALL)
    return [yaml.safe_load(block) for block in blocks]


def _matching_types(catalog: dict[str, Any], on_type: str) -> list[str]:
    if on_type.endswith(".*"):
        prefix = on_type[:-1]
        return [name for name in catalog["types"] if name.startswith(prefix)]
    return [on_type] if on_type in catalog["types"] else []


def _payload_properties(catalog: dict[str, Any], event_type: str) -> set[str]:
    entry = catalog["types"][event_type]
    schema = entry["versions"][str(entry["currentVersion"])]["schema"]
    return set(schema.get("properties", {}))


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for item in value.values() for s in _strings(item)]
    if isinstance(value, list):
        return [s for item in value for s in _strings(item)]
    return []


def _condition_paths(condition: Any) -> list[str]:
    if isinstance(condition, dict):
        if set(condition) == {"var"}:
            return [str(condition["var"])]
        return [p for item in condition.values() for p in _condition_paths(item)]
    if isinstance(condition, list):
        return [p for item in condition for p in _condition_paths(item)]
    return []


def test_catalog_snapshot_matches_the_core() -> None:
    assert _load("catalog.json") == json.loads(json.dumps(catalog_document())), (
        "contracts/catalog.json is stale: copy docs/events/catalog.json of the core"
    )


def test_rule_schema_is_the_platform_copy() -> None:
    schema = _load("notification-rule.schema.json")
    Draft202012Validator.check_schema(schema)
    body = {key: value for key, value in schema.items() if key != "$schema"}
    assert hashlib.sha256(_canonical(body)).hexdigest() == SPEC_SCHEMA_SHA256
    assert SPEC_SCHEMA_SHA256 in ADR.read_text(encoding="utf-8")


def test_the_adr_carries_the_three_rules_of_the_former_behaviour() -> None:
    rules = _adr_rules()
    assert [rule["key"] for rule in rules] == [
        "approval-requested",
        "verification-failed",
        "verification-blocked",
    ]
    handled = {rule["spec"]["on"]["type"] for rule in rules}
    closing = {t for rule in rules for t in rule["spec"].get("close", {}).get("on", [])}
    # What the built-in table of ADR-0002 handles.
    assert handled | closing == {
        "approval.requested",
        "approval.approved",
        "approval.rejected",
        "approval.cancelled",
        "task.verification_failed",
    }


@pytest.mark.parametrize("rule", _adr_rules(), ids=lambda rule: rule["key"])
def test_adr_rule_is_valid_against_schema_and_catalog(rule: dict[str, Any]) -> None:
    assert rule["apiVersion"] == "taimen.ai/v1"
    assert rule["kind"] == "NotificationRule"
    assert re.fullmatch(r"[a-z0-9][a-z0-9._-]*", rule["key"])
    spec = rule["spec"]
    errors = list(Draft202012Validator(_load("notification-rule.schema.json")).iter_errors(spec))
    assert not errors, [error.message for error in errors]

    catalog = _load("catalog.json")
    on_types = _matching_types(catalog, spec["on"]["type"])
    assert on_types, spec["on"]["type"]
    close_types = spec.get("close", {}).get("on", [])
    for event_type in close_types:
        assert event_type in catalog["types"], event_type

    envelope = set(catalog["envelope"])
    task_fields = set(TaskOut.model_json_schema(by_alias=True)["properties"])
    principal_fields = {"actorId", "ownerId", "assigneeId", "createdBy"}

    def check(path: str, event_types: list[str]) -> None:
        root, *rest = path.split(".")
        assert rest, path
        field = rest[0]
        if root == "payload":
            for event_type in event_types:
                assert field in _payload_properties(catalog, event_type), (path, event_type)
        elif root == "event":
            assert field in envelope, path
        elif root == "task":
            assert field in task_fields, path
            for event_type in event_types:
                entry = catalog["types"][event_type]
                assert entry["entityType"] == "task" or "taskId" in _payload_properties(
                    catalog, event_type
                ), (path, event_type)
        else:
            pytest.fail(f"unknown root in {path}")
        if rest[-1] == "displayName":
            assert len(rest) == 2, path
            if root == "payload":
                for event_type in event_types:
                    entry = catalog["types"][event_type]
                    schema = entry["versions"][str(entry["currentVersion"])]["schema"]
                    assert schema["properties"][field].get("format") == "uuid", path
            else:
                assert field in principal_fields, path

    notification = spec["notification"]
    for text in _strings(notification):
        for path in _PLACEHOLDER.findall(text):
            check(path, on_types)
    for path in _condition_paths(spec["on"].get("when")):
        check(path, on_types)
    for path in _PLACEHOLDER.findall(spec.get("dedupKeyTemplate", "")):
        check(path, on_types + close_types)
    if "approvalDecide" in notification.get("actions", []):
        assert {catalog["types"][t]["entityType"] for t in on_types} == {"approval"}


# --- the service's own check and evaluator against the core ---------------------------


def _json_type(schema: dict[str, Any]) -> str:
    if "$ref" in schema:
        return "object"
    if "anyOf" in schema:
        [kind] = [_json_type(s) for s in schema["anyOf"] if s.get("type") != "null"]
        return kind
    return str(schema["type"])


def test_task_fields_are_the_task_projection_of_the_core() -> None:
    properties = TaskOut.model_json_schema(by_alias=True)["properties"]
    assert {name: _json_type(s) for name, s in properties.items()} == rules.TASK_FIELDS


def test_the_event_the_core_serializes_has_only_envelope_fields() -> None:
    from core_events import requested

    body = requested(uuid.uuid4())
    assert set(body) - {"payload"} <= rules.catalog().envelope


@pytest.mark.parametrize("rule", _adr_rules(), ids=lambda rule: rule["key"])
def test_the_service_check_accepts_the_adr_rules(rule: dict[str, Any]) -> None:
    assert rules.check_spec(rule["spec"]) == []


FACTS: dict[str, Any] = {
    "payload": {"n": 3, "s": "abc", "flag": True, "items": ["a", "b"], "none": None},
    "event": {"type": "approval.requested"},
    "task": {"title": "T"},
}
CONDITIONS: list[Any] = [
    True,
    False,
    {"eq": [{"var": "payload.n"}, 3]},
    {"eq": [{"var": "payload.flag"}, 1]},
    {"ne": [{"var": "payload.flag"}, True]},
    {"lt": [{"var": "payload.n"}, 5]},
    {"ge": [{"var": "payload.s"}, "abd"]},
    {"gt": [{"var": "payload.none"}, 1]},
    {"lt": [{"var": "payload.s"}, 1]},
    {"in": [{"var": "payload.s"}, {"const": ["abc", "x"]}]},
    {"in": ["a", {"var": "payload.items"}]},
    {"in": ["a", {"var": "payload.s"}]},
    {"in": ["a", {"var": "payload.none"}]},
    {"exists": "payload.none"},
    {"exists": "task.title"},
    {"and": [{"exists": "payload.n"}, {"not": {"eq": [{"var": "event.type"}, "x"]}}]},
    {"or": [False, {"eq": [{"var": "payload.items.1"}, "b"]}]},
    {"eq": [{"eq": [{"var": "payload.n"}, 3]}, True]},
    # Outside the grammar.
    {"bogus": []},
    {"and": []},
    {"eq": [1]},
    {"eq": [1, 2], "ne": [1, 2]},
    {"exists": "goal.id"},
    {"eq": [{"var": "payload..x"}, 1]},
    "payload.n",
    {
        "not": {
            "not": {
                "not": {
                    "not": {
                        "not": {
                            "not": {
                                "not": {
                                    "not": {
                                        "not": {
                                            "not": {
                                                "not": {
                                                    "not": {
                                                        "not": {
                                                            "not": {"not": {"not": {"not": True}}}
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    },
]


def _core_answer(expression: Any) -> Any:
    from control_plane.domain import work_rules
    from control_plane.domain.errors import ValidationError

    # The core's roots for event rules (``trigger`` and ``goal`` unused here);
    # ``event`` is the service's root: the core reads the envelope as ``trigger``.
    roots = frozenset({"payload", "event", "task"})
    try:
        work_rules.validate_expression(expression, roots=roots)
    except ValidationError:
        return "invalid"
    try:
        return work_rules.evaluate(
            expression,
            lambda path: work_rules.walk(FACTS[path.root], path.segments),
            roots=roots,
        )
    except work_rules.ConditionError:
        return "error"


def _service_answer(expression: Any) -> Any:
    try:
        rules.condition_paths(expression)
    except rules.ConditionInvalid:
        return "invalid"

    def resolve(path: str) -> Any:
        root, *segments = path.split(".")
        return rules.walk(FACTS[root], segments)

    try:
        return rules.evaluate(expression, resolve)
    except rules.ConditionError:
        return "error"


@pytest.mark.parametrize("expression", CONDITIONS, ids=lambda e: json.dumps(e)[:60])
def test_conditions_answer_as_the_core_evaluator(expression: Any) -> None:
    assert _service_answer(expression) == _core_answer(expression)
