"""The skill notify.send@1: the executor's envelope, deduplication, skill-sdk errors.

The calls go through the Control Plane's own executor (``HttpProtocol`` of
``control_plane_agent.skills``, the neighbouring checkout) into this app, so
the envelope it posts, the token audience it asks for and the way it reads
outputs and errors are its code, not a copy of it. The published contract is
checked by the core's own publication rules.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import jsonschema
import pytest
from control_plane.domain.skill_contract import (
    normalize_contract,
    require_safe_retries,
    validate_policy_columns,
)
from control_plane_agent.skills import EndpointPolicy, HttpProtocol, SkillCall, SkillFailure
from sqlalchemy import func, select

from notification_service.auth import SCOPE_READ, SCOPE_SEND
from notification_service.models import Notification
from notification_service.skill import (
    ENDPOINT_PLACEHOLDER,
    SKILL_PATH,
    dedup_key,
    publication,
)

from conftest import AUDIENCE, FakeDirectory, Harness, Person, auth, notification, to_principal

ORIGIN = "https://ns.test"
ENDPOINT = ORIGIN + SKILL_PATH
CONTRACT = publication(ENDPOINT, AUDIENCE)
DOCUMENTED = Path(__file__).parent.parent / "docs" / "skills" / "notify.send@1.json"


def inputs(person: Person, **fields: Any) -> dict[str, Any]:
    return {**notification(to_principal(person)), **fields}


class Executor:
    """The core's HTTP skill protocol, pointed at the app under test."""

    def __init__(self, harness: Harness, token: str | None) -> None:
        async def resolve(host: str, port: int) -> list[str]:
            return ["10.0.0.10"]

        async def token_source(audience: str) -> str | None:
            assert audience == AUDIENCE
            return token

        policy = EndpointPolicy(
            origins=frozenset({ORIGIN}),
            audiences=frozenset({AUDIENCE}),
            private_hosts=frozenset({"ns.test"}),
            resolve=resolve,
        )
        transport = httpx.ASGITransport(app=harness.app)
        self.protocol = HttpProtocol(token_source, policy=policy, transport=transport)

    async def call(self, inputs: dict[str, Any], key: str | None) -> dict[str, Any]:
        outcome = await self.protocol.call(
            SkillCall(
                invocation_id=str(uuid.uuid4()),
                idempotency_key=key,
                inputs=inputs,
                skill="notify.send@1",
                implementation=CONTRACT["contract"]["implementation"],
                timeout_seconds=CONTRACT["contract"]["timeoutSeconds"],
            )
        )
        return outcome.output  # type: ignore[no-any-return]

    async def fail(self, inputs: dict[str, Any], key: str | None) -> SkillFailure:
        with pytest.raises(SkillFailure) as caught:
            await self.call(inputs, key)
        return caught.value


async def notifications(harness: Harness) -> int:
    async with harness.sessions() as session:
        return int(await session.scalar(select(func.count()).select_from(Notification)) or 0)


async def test_invocation_sends_one_notification(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    unique_key: Callable[[], str],
) -> None:
    person = make_person()
    key = unique_key()

    outputs = await Executor(harness, token()).call(inputs(person), key)

    jsonschema.validate(outputs, CONTRACT["contract"]["outputs"])
    assert outputs["deliveries"] == [{"channel": "web", "status": "pending"}]
    async with harness.sessions() as session:
        stored = await session.get(Notification, uuid.UUID(outputs["notificationId"]))
    assert stored is not None
    assert stored.dedup_key == f"skill:{key}"
    assert (stored.type, stored.title, stored.actions) == (
        "work.review_requested",
        "Review requested",
        [],
    )

    await harness.drain()
    inbox = await harness.client.get(
        "/api/v1/me/notifications", headers=auth(token(person.iam_principal_id))
    )
    assert [i["notificationId"] for i in inbox.json()["items"]] == [outputs["notificationId"]]


async def test_repeated_idempotency_key_returns_the_same_notification(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    unique_key: Callable[[], str],
) -> None:
    person = make_person()
    executor = Executor(harness, token(uuid.uuid4()))
    key = unique_key()

    first = await executor.call(inputs(person), key)
    await harness.drain()
    # Another attempt of the same invocation: a new invocation id, the same key.
    second = await executor.call(inputs(person), key)

    assert second["notificationId"] == first["notificationId"]
    assert second["deliveries"] == [{"channel": "web", "status": "delivered"}]
    assert await notifications(harness) == 1


async def test_the_skill_and_the_api_do_not_share_keys(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    unique_key: Callable[[], str],
) -> None:
    person = make_person()
    sender = token(uuid.uuid4())
    key = unique_key()

    direct = await harness.client.post(
        "/api/v1/notifications",
        json=inputs(person),
        headers={**auth(sender), "Idempotency-Key": key},
    )
    via_skill = await Executor(harness, sender).call(inputs(person), key)

    assert direct.status_code == 201
    assert via_skill["notificationId"] != direct.json()["id"]


async def test_same_key_with_other_inputs_is_a_conflict(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    unique_key: Callable[[], str],
) -> None:
    person = make_person()
    executor = Executor(harness, token(uuid.uuid4()))
    key = unique_key()
    await executor.call(inputs(person), key)

    failure = await executor.fail(inputs(person, title="Something else"), key)

    assert failure.details is not None
    assert failure.details["status"] == 409
    assert (failure.code, failure.retryable) == ("idempotency_conflict", False)


@pytest.mark.parametrize(
    ("change", "loc"),
    [
        ({"title": ""}, ["inputs", "title"]),
        ({"type": "Not A Type"}, ["inputs", "type"]),
        ({"recipient": {"kind": "role", "id": str(uuid.uuid4())}}, ["inputs", "recipient"]),
        # Decision actions come from the core only.
        ({"actions": [{"id": "approve", "label": "Approve"}]}, ["inputs", "actions"]),
    ],
)
async def test_invalid_inputs_are_a_non_retryable_skill_error(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    unique_key: Callable[[], str],
    change: dict[str, Any],
    loc: list[str],
) -> None:
    failure = await Executor(harness, token()).fail(inputs(make_person(), **change), unique_key())

    assert failure.code == "invalid_inputs"
    assert failure.retryable is False
    assert failure.details is not None and failure.details["status"] == 422
    body = json.loads(failure.details["body"])
    assert [e["loc"][: len(loc)] for e in body["error"]["details"]["errors"]] == [loc]
    assert await notifications(harness) == 0


async def test_invocation_without_idempotency_key_is_rejected(
    harness: Harness, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    failure = await Executor(harness, token()).fail(inputs(make_person()), None)

    assert (failure.code, failure.retryable) == ("invalid_inputs", False)
    assert await notifications(harness) == 0


async def test_unknown_recipient_is_not_retried(
    harness: Harness, token: Callable[..., str], unique_key: Callable[[], str]
) -> None:
    stranger = {"kind": "principal", "id": str(uuid.uuid4())}

    failure = await Executor(harness, token()).fail(notification(stranger), unique_key())

    assert (failure.code, failure.retryable) == ("unknown_recipient", False)


async def test_unavailable_directory_is_retryable(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    directory: FakeDirectory,
    unique_key: Callable[[], str],
) -> None:
    person = make_person()
    directory.unavailable = True
    try:
        failure = await Executor(harness, token()).fail(inputs(person), unique_key())
    finally:
        directory.unavailable = False

    assert (failure.code, failure.retryable) == ("dependency_unavailable", True)


async def test_without_the_send_scope_the_call_is_forbidden(
    harness: Harness,
    token: Callable[..., str],
    make_person: Callable[..., Person],
    unique_key: Callable[[], str],
) -> None:
    failure = await Executor(harness, token(scopes=(SCOPE_READ,))).fail(
        inputs(make_person()), unique_key()
    )

    assert failure.details is not None and failure.details["status"] == 403
    assert (failure.code, failure.retryable) == ("insufficient_scope", False)
    assert await notifications(harness) == 0


async def test_without_a_token_the_call_is_unauthorized(
    harness: Harness, make_person: Callable[..., Person], unique_key: Callable[[], str]
) -> None:
    response = await harness.client.post(
        SKILL_PATH,
        json={
            "invocationId": str(uuid.uuid4()),
            "idempotencyKey": unique_key(),
            "inputs": inputs(make_person()),
        },
    )

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert response.json()["error"]["retryable"] is False


async def test_a_body_that_is_not_json_is_rejected(
    harness: Harness, token: Callable[..., str]
) -> None:
    response = await harness.client.post(
        SKILL_PATH, content=b"{", headers={**auth(token()), "Content-Type": "application/json"}
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_json"


def test_a_long_idempotency_key_fits_the_column() -> None:
    assert dedup_key("k") == "skill:k"
    long = "k" * 200
    assert len(dedup_key(long)) <= 200
    assert dedup_key(long) == dedup_key(long) != dedup_key("j" * 200)


def test_the_contract_passes_the_cores_publication_rules() -> None:
    side_effects, _ = validate_policy_columns(CONTRACT["sideEffects"], CONTRACT["riskLevel"])
    contract = normalize_contract(CONTRACT["contract"])
    require_safe_retries(contract, side_effects)

    assert side_effects == "external_write"
    assert contract["idempotency"] == "required"
    assert contract["implementation"]["auth"] == {"audience": AUDIENCE, "scopes": [SCOPE_SEND]}


def test_the_documented_contract_is_the_published_one() -> None:
    documented = json.loads(DOCUMENTED.read_text())

    assert documented == publication(ENDPOINT_PLACEHOLDER), (
        "regenerate: uv run python -m notification_service.skill > 'docs/skills/notify.send@1.json'"
    )


def test_the_inputs_schema_agrees_with_the_endpoint() -> None:
    schema = CONTRACT["contract"]["inputs"]
    good = notification({"kind": "principal", "id": str(uuid.uuid4())})
    jsonschema.validate(good, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**good, "actions": []}, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**good, "title": ""}, schema)
