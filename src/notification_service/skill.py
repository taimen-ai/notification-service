"""The skill ``notify.send@1``: sending a notification from a package (TAI-ADR-0048 §6).

The Control Plane's skill executor calls an ``http`` skill with
``POST endpoint`` and the envelope ``{invocationId, idempotencyKey, inputs}``
under a Bearer token of this service's audience. The 2xx body is the skill's
outputs; a failure is ``{"error": {code, message, retryable, details}}``
(skill-sdk, TAI-ADR-0045), whose ``retryable`` the executor takes as is — so
every error of this route carries it, including the ones of authentication.

The inputs are a notification without actions: decision actions come from
the core only (ADR 0003). The idempotency key of the invocation, the same on
every attempt, is the deduplication key of the notification.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any, Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from platform_auth import EnforcementError, TrustedAuthContext
from pydantic import Field, ValidationError

from notification_service.auth import SCOPE_SEND, authenticate
from notification_service.errors import ApiError
from notification_service.schemas import ApiModel, NotificationContent, NotificationCreate
from notification_service.sending import NotificationSender

SKILL_NAME = "notify.send"
SKILL_VERSION = "1"
SKILL_PATH = "/api/v1/skills/notify.send"
DEDUP_PREFIX = "skill:"
# Notification.dedup_key is 200 characters; the core allows keys of 200.
MAX_DEDUP_KEY = 200
# The published contract names the deployment's address; the documented one
# leaves it to the package that publishes it.
ENDPOINT_PLACEHOLDER = "${NOTIFICATION_SERVICE_URL}" + SKILL_PATH

router = APIRouter()


class SkillInvocation(ApiModel):
    """What the executor posts (control_plane_agent.skills.HttpProtocol)."""

    invocation_id: str = Field(min_length=1, max_length=200)
    # The contract declares ``idempotency: required``: the core does not
    # accept an invocation without a key, so one always comes.
    idempotency_key: str = Field(min_length=1, max_length=200)
    inputs: NotificationContent


class DeliveryState(ApiModel):
    channel: str
    status: Literal["pending", "sending", "delivered", "failed"]


class NotifySendOutputs(ApiModel):
    notification_id: uuid.UUID
    deliveries: list[DeliveryState]


def dedup_key(idempotency_key: str) -> str:
    """``skill:<idempotencyKey>``; a key too long for the column is hashed."""
    key = DEDUP_PREFIX + idempotency_key
    if len(key) <= MAX_DEDUP_KEY:
        return key
    return DEDUP_PREFIX + "sha256:" + hashlib.sha256(idempotency_key.encode()).hexdigest()


def failure(
    status: int,
    code: str,
    message: str,
    *,
    retryable: bool | None = None,
    details: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """The skill-sdk error; unless told otherwise, a 5xx is retryable, a 4xx is not."""
    error = {
        "code": code,
        "message": message,
        "retryable": status >= 500 if retryable is None else retryable,
        "details": details or {},
    }
    return JSONResponse({"error": error}, status_code=status, headers=headers)


def _validation_details(exc: ValidationError) -> dict[str, Any]:
    return {
        "errors": [
            {"loc": list(err.get("loc", ())), "msg": str(err.get("msg", ""))}
            for err in exc.errors()
        ]
    }


async def _authorize(request: Request) -> TrustedAuthContext:
    ctx = await authenticate(request)
    ctx.require_scope(SCOPE_SEND)
    return ctx


@router.post(
    SKILL_PATH,
    response_model=NotifySendOutputs,
    status_code=201,
    summary="Skill notify.send@1: send a notification for a skill invocation",
)
async def notify_send(request: Request) -> JSONResponse:
    try:
        ctx = await _authorize(request)
    except EnforcementError as exc:
        # The SDK's deny contract: a stable code only, the reason stays in audit.
        headers = {"WWW-Authenticate": "Bearer"} if exc.http_status == 401 else None
        return failure(exc.http_status, exc.code, exc.code, headers=headers)

    try:
        raw = await request.json()
    except ValueError:
        return failure(400, "invalid_json", "the body is not JSON")
    try:
        invocation = SkillInvocation.model_validate(raw)
    except ValidationError as exc:
        details = _validation_details(exc)
        return failure(422, "invalid_inputs", "the invocation is invalid", details=details)

    payload = NotificationCreate.model_validate(invocation.inputs.model_dump(mode="json"))
    sender: NotificationSender = request.app.state.sender
    try:
        accepted = await sender.accept(ctx, payload, dedup_key(invocation.idempotency_key))
    except ApiError as exc:
        return failure(exc.status, exc.code, exc.message, details=exc.details)

    outputs = NotifySendOutputs(
        notification_id=accepted.notification.id,
        deliveries=[
            DeliveryState(channel=d.channel, status=d.status)  # type: ignore[arg-type]
            for d in accepted.deliveries
        ],
    )
    return JSONResponse(
        outputs.model_dump(mode="json", by_alias=True),
        status_code=201 if accepted.created else 200,
    )


def publication(endpoint: str, audience: str = "notification-service") -> dict[str, Any]:
    """The version to publish in the Control Plane (``POST /api/v1/skills``).

    The package ``notify`` of the superproject owns the published YAML; this
    is the source of its schemas. ``endpoint`` is where this service is
    reachable from the skill executor, ending with ``SKILL_PATH``; ``audience``
    is this service's IAM audience (``NS_AUDIENCE``).
    """
    return {
        "name": SKILL_NAME,
        "version": SKILL_VERSION,
        "description": "Send a notification to a principal, a role in a workspace or a group.",
        "sideEffects": "external_write",
        "riskLevel": "low",
        "contract": {
            "inputs": _schema(NotificationContent),
            "outputs": _schema(NotifySendOutputs),
            "timeoutSeconds": 30,
            "retryPolicy": {"maxAttempts": 3, "backoffSeconds": 10},
            "idempotency": "required",
            "implementation": {
                "protocol": "http",
                "endpoint": endpoint,
                # The executor asks IAM for exactly these scopes; the ceiling of
                # its PAT still bounds them (CP-ADR-0056 amendment M2.2).
                "auth": {"audience": audience, "scopes": [SCOPE_SEND]},
            },
        },
    }


def _schema(model: type[ApiModel]) -> dict[str, Any]:
    schema = model.model_json_schema(by_alias=True, mode="validation")
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", **schema}


if __name__ == "__main__":  # pragma: no cover - regenerates docs/skills/notify.send@1.json
    import json

    print(json.dumps(publication(ENDPOINT_PLACEHOLDER), indent=2, ensure_ascii=False))
