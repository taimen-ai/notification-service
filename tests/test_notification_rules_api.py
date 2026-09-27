"""The notification rules API (ADR-0005 §5–§7): apply, validate, retire, list."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import pytest

from notification_service.auth import SCOPE_ADMIN, SCOPE_SEND
from notification_service.rules import spec_hash

from conftest import Harness, adr_rules, auth

RULES = "/api/v1/notification-rules"


def spec(title: str = "Decide {{payload.taskTitle}}", **changes: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "on": {"type": "approval.requested"},
        "recipient": {"kind": "assigned"},
        "notification": {"type": "work.decision_needed", "title": title},
    }
    body.update(changes)
    return body


@pytest.fixture
def admin(token: Callable[..., str]) -> dict[str, str]:
    return auth(token(uuid.uuid4(), scopes=(SCOPE_ADMIN,)))


async def test_apply_is_idempotent_and_a_change_is_a_new_version(
    harness: Harness, admin: dict[str, str]
) -> None:
    first = await harness.client.post(RULES, json={"key": "decide", "spec": spec()}, headers=admin)
    assert first.status_code == 201, first.text
    body = first.json()
    assert body["version"] == 1
    assert body["state"] == "active"
    assert body["spec"] == spec()
    assert body["specHash"] == spec_hash(spec())

    again = await harness.client.post(RULES, json={"key": "decide", "spec": spec()}, headers=admin)
    assert again.status_code == 200
    assert again.json() == body

    changed = await harness.client.post(
        RULES, json={"key": "decide", "spec": spec("Other")}, headers=admin
    )
    assert changed.status_code == 201
    assert changed.json()["version"] == 2

    listed = (await harness.client.get(RULES, headers=admin)).json()
    assert [(i["key"], i["version"], i["state"]) for i in listed["items"]] == [
        ("decide", 2, "active")
    ]
    assert listed["nextCursor"] is None


async def test_validate_tells_whether_applying_would_change_anything(
    harness: Harness, admin: dict[str, str]
) -> None:
    check = await harness.client.post(
        f"{RULES}:validate", json={"key": "decide", "spec": spec()}, headers=admin
    )
    assert check.status_code == 200, check.text
    assert check.json() == {"valid": True, "specHash": spec_hash(spec()), "changed": True}
    assert (await harness.client.get(RULES, headers=admin)).json()["items"] == []

    await harness.client.post(RULES, json={"key": "decide", "spec": spec()}, headers=admin)
    check = await harness.client.post(
        f"{RULES}:validate", json={"key": "decide", "spec": spec()}, headers=admin
    )
    assert check.json()["changed"] is False


async def test_an_invalid_spec_is_refused_with_every_finding(
    harness: Harness, admin: dict[str, str]
) -> None:
    broken = spec(
        "{{payload.nope}} {{task.title}}",
        on={"type": "approval.requested", "when": {"bogus": []}},
        close={"on": ["approval.nope"]},
    )
    for path in (RULES, f"{RULES}:validate"):
        response = await harness.client.post(
            path, json={"key": "decide", "spec": broken}, headers=admin
        )
        assert response.status_code == 422
        error = response.json()["error"]
        assert error["code"] == "invalid_notification_rule"
        assert {(e["path"], e["code"]) for e in error["details"]["errors"]} == {
            ("/close/on/0", "unknown_event_type"),
            ("/on/when", "invalid_condition"),
            ("/notification/title", "unknown_field"),
        }
    shape = await harness.client.post(
        RULES, json={"key": "decide", "spec": {**spec(), "extra": 1}}, headers=admin
    )
    assert shape.status_code == 422
    assert shape.json()["error"]["details"]["errors"][0]["code"] == "invalid_spec"
    assert (await harness.client.get(RULES, headers=admin)).json()["items"] == []


async def test_a_bad_key_is_refused(harness: Harness, admin: dict[str, str]) -> None:
    response = await harness.client.post(
        RULES, json={"key": "Not A Key", "spec": spec()}, headers=admin
    )
    assert response.status_code == 422


async def test_retire_and_apply_again(harness: Harness, admin: dict[str, str]) -> None:
    missing = await harness.client.post(f"{RULES}/decide:retire", headers=admin)
    assert missing.status_code == 404

    await harness.client.post(RULES, json={"key": "decide", "spec": spec()}, headers=admin)
    retired = await harness.client.post(f"{RULES}/decide:retire", headers=admin)
    assert retired.status_code == 200
    assert (retired.json()["version"], retired.json()["state"]) == (1, "retired")
    repeat = await harness.client.post(f"{RULES}/decide:retire", headers=admin)
    assert repeat.json() == retired.json()

    assert (await harness.client.get(RULES, headers=admin)).json()["items"] == []
    with_retired = await harness.client.get(RULES, params={"includeRetired": "true"}, headers=admin)
    assert [i["state"] for i in with_retired.json()["items"]] == ["retired"]

    # The same spec into a retired key: the next version, active again.
    back = await harness.client.post(RULES, json={"key": "decide", "spec": spec()}, headers=admin)
    assert back.status_code == 201
    assert (back.json()["version"], back.json()["state"]) == (2, "active")


async def test_listing_pages_by_key_and_filters_one(
    harness: Harness, admin: dict[str, str]
) -> None:
    for key in ("c", "a", "b"):
        await harness.client.post(RULES, json={"key": key, "spec": spec()}, headers=admin)

    first = (await harness.client.get(RULES, params={"limit": 2}, headers=admin)).json()
    assert [i["key"] for i in first["items"]] == ["a", "b"]
    assert first["nextCursor"] == "b"
    rest = await harness.client.get(
        RULES, params={"limit": 2, "cursor": first["nextCursor"]}, headers=admin
    )
    assert [i["key"] for i in rest.json()["items"]] == ["c"]
    assert rest.json()["nextCursor"] is None

    one = await harness.client.get(RULES, params={"key": "b"}, headers=admin)
    assert [i["key"] for i in one.json()["items"]] == ["b"]
    bad = await harness.client.get(RULES, params={"cursor": "../x"}, headers=admin)
    assert bad.status_code == 422


async def test_rules_are_the_admins_and_their_tenants_only(
    harness: Harness, admin: dict[str, str], token: Callable[..., str]
) -> None:
    sender = auth(token(scopes=(SCOPE_SEND,)))
    assert (await harness.client.get(RULES, headers=sender)).status_code == 403
    denied = await harness.client.post(
        RULES, json={"key": "decide", "spec": spec()}, headers=sender
    )
    assert denied.status_code == 403

    await harness.client.post(RULES, json={"key": "decide", "spec": spec()}, headers=admin)
    other = auth(token(scopes=(SCOPE_ADMIN,), tenant=uuid.uuid4()))
    assert (await harness.client.get(RULES, headers=other)).json()["items"] == []
    assert (await harness.client.post(f"{RULES}/decide:retire", headers=other)).status_code == 404


async def test_the_rules_of_the_former_behaviour_apply(
    harness: Harness, admin: dict[str, str]
) -> None:
    for rule in adr_rules():
        response = await harness.client.post(
            RULES, json={"key": rule["key"], "spec": rule["spec"]}, headers=admin
        )
        assert response.status_code == 201, response.text
    listed = (await harness.client.get(RULES, headers=admin)).json()["items"]
    # What the installer exports is what it applied (FR-016).
    assert {i["key"]: i["spec"] for i in listed} == {r["key"]: r["spec"] for r in adr_rules()}
