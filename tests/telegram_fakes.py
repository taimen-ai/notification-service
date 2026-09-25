"""Fakes of the three parties of a Telegram decision, each by its documented contract.

- ``FakeBotApi`` — the Telegram Bot API (``https://core.telegram.org/bots/api``):
  ``POST /bot<token>/<method>``, answers ``{"ok": true, "result": ...}`` or the
  error object ``{"ok": false, "error_code", "description", "parameters"}``.
- ``FakeIam`` — IAM's channel routes (``iam-service``, ``channels/routes.py``):
  requests are parsed with IAM's own request models and answered with its
  response models when the neighbouring checkout is present.
- ``FakeControlPlane`` — ``GET /approvals/{id}`` and ``:approve|:reject``
  answered with the core's ``ApprovalOut``; a decision token is checked with
  the core's own ``decision_purpose``, the Idempotency-Key replays.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import httpx
from control_plane.api.v1.schemas import ApprovalOut
from control_plane.domain.errors import AuthorizationError
from control_plane.infrastructure.auth.iam import decision_purpose

ROOT = Path(__file__).resolve().parents[1]
IAM_SCHEMAS = ROOT.parent / "iam-service" / "src" / "iam_service" / "channels" / "schemas.py"
BOT_TOKEN = "123456:TEST-TOKEN"
NOW = datetime(2026, 9, 25, 12, tzinfo=UTC)


def iam_schemas() -> ModuleType | None:
    """IAM's channel models, loaded from the neighbouring checkout (pydantic only)."""
    if not IAM_SCHEMAS.exists():
        return None
    name = "_iam_channel_schemas"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, IAM_SCHEMAS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# --- Telegram Bot API ------------------------------------------------------------


@dataclass
class BotCall:
    method: str
    params: dict[str, Any]


@dataclass
class FakeBotApi:
    calls: list[BotCall] = field(default_factory=list)
    # chat id -> (status, description) returned by sendMessage for it.
    failing: dict[str, tuple[int, str]] = field(default_factory=dict)
    next_message_id: int = 100
    # Messages sent successfully: chat_id, message_id, text, reply_markup.
    messages: list[dict[str, Any]] = field(default_factory=list)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def of(self, method: str) -> list[dict[str, Any]]:
        return [call.params for call in self.calls if call.method == method]

    def _handle(self, request: httpx.Request) -> httpx.Response:
        prefix = f"/bot{BOT_TOKEN}/"
        if not request.url.path.startswith(prefix):
            return self._error(404, "Not Found", status=404)
        method = request.url.path[len(prefix) :]
        params = json.loads(request.content or b"{}")
        self.calls.append(BotCall(method, params))
        if method == "sendMessage":
            chat = str(params["chat_id"])
            assert params["text"] and len(params["text"]) <= 4096
            for row in (params.get("reply_markup") or {}).get("inline_keyboard", []):
                for button in row:
                    assert len(button.get("callback_data", "").encode()) <= 64
            if chat in self.failing:
                status, description = self.failing[chat]
                return self._error(status, description)
            self.next_message_id += 1
            self.messages.append(
                {
                    "chat_id": chat,
                    "message_id": self.next_message_id,
                    "text": params["text"],
                    "reply_markup": params.get("reply_markup"),
                }
            )
            return self._ok(
                {
                    "message_id": self.next_message_id,
                    "date": 0,
                    "chat": {"id": int(chat), "type": "private"},
                    "text": params["text"],
                }
            )
        if method == "editMessageText":
            assert params["message_id"] and params["text"]
            return self._ok(True)
        if method == "answerCallbackQuery":
            assert len(params.get("text", "")) <= 200
            return self._ok(True)
        return self._error(404, "Not Found: method not found")

    @staticmethod
    def _ok(result: Any) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "result": result})

    @staticmethod
    def _error(code: int, description: str, *, status: int | None = None) -> httpx.Response:
        return httpx.Response(
            status or code, json={"ok": False, "error_code": code, "description": description}
        )


# --- IAM ---------------------------------------------------------------------------


@dataclass
class Issued:
    iam_principal_id: uuid.UUID
    purpose_ref: str


@dataclass
class FakeIam:
    tenant_id: uuid.UUID
    # link code -> IAM principal it was issued to
    codes: dict[str, uuid.UUID] = field(default_factory=dict)
    # Telegram user id -> IAM principal (active links)
    links: dict[str, uuid.UUID] = field(default_factory=dict)
    provider_enabled: bool = True
    down: bool = False
    tokens: dict[str, Issued] = field(default_factory=dict)
    requests: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def exchanges(self) -> list[dict[str, Any]]:
        return [body for path, body in self.requests if path.endswith(":exchange")]

    def _handle(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            return httpx.Response(503, json={"detail": "unavailable"})
        body = json.loads(request.content or b"{}")
        path = request.url.path
        self.requests.append((path, body))
        assert request.headers["authorization"].startswith("Bearer ")
        assert path.startswith(f"/api/v1/tenants/{self.tenant_id}/")
        schemas = iam_schemas()
        if path.endswith("/channel-links:confirm"):
            if schemas is not None:
                schemas.ChannelLinkConfirm.model_validate(body)
            return self._confirm(body, schemas)
        if path.endswith("/channel-assertions:exchange"):
            if schemas is not None:
                schemas.ChannelAssertionExchange.model_validate(body)
            return self._exchange(body, schemas)
        return httpx.Response(404, json={"detail": "Not Found"})

    def _confirm(self, body: dict[str, Any], schemas: ModuleType | None) -> httpx.Response:
        if not self.provider_enabled:
            return httpx.Response(403, json={"detail": "channel_provider_disabled"})
        principal = self.codes.pop(body["code"], None)
        if principal is None:
            return httpx.Response(400, json={"detail": "invalid_link_code"})
        self.links[body["externalSubject"]] = principal
        view = {
            "linkId": str(uuid.uuid4()),
            "principalId": str(principal),
            "channel": body["channel"],
            "status": "active",
            "linkedAt": NOW.isoformat(),
            "lastUsedAt": None,
        }
        if schemas is not None:
            view = schemas.ChannelLinkView.model_validate(view).model_dump(
                mode="json", by_alias=True
            )
        return httpx.Response(200, json=view)

    def _exchange(self, body: dict[str, Any], schemas: ModuleType | None) -> httpx.Response:
        if not self.provider_enabled:
            return httpx.Response(403, json={"detail": "channel_provider_disabled"})
        principal = self.links.get(body["externalSubject"])
        if principal is None:
            return httpx.Response(404, json={"detail": "channel_account_not_linked"})
        token = f"channel-token-{uuid.uuid4().hex}"
        self.tokens[token] = Issued(principal, body["purposeRef"])
        answer = {
            "accessToken": token,
            "tokenType": "Bearer",
            "expiresIn": 60,
            "audience": "control-plane",
            "scope": ["control-plane:decide"],
            "sessionId": str(uuid.uuid4()),
            "principalId": str(principal),
            "purposeRef": body["purposeRef"],
        }
        if schemas is not None:
            answer = schemas.ChannelAssertionToken.model_validate(answer).model_dump(
                mode="json", by_alias=True
            )
        return httpx.Response(200, json=answer)


class StaticTokens:
    """``ServiceTokenProvider`` as the adapter uses it: a token, ``forget``, ``aclose``."""

    def __init__(self, token: str) -> None:
        self._token = token

    async def __call__(self) -> str:
        return self._token

    def forget(self) -> None:
        pass

    async def aclose(self) -> None:
        pass


# --- Control Plane -----------------------------------------------------------------


@dataclass
class FakeApproval:
    id: uuid.UUID
    # IAM principals allowed to decide (the core's eligibility, in the fake)
    eligible: set[uuid.UUID]
    status: str = "pending"
    decision_by: uuid.UUID | None = None
    decision_at: datetime | None = None


@dataclass
class FakeControlPlane:
    iam: FakeIam
    # IAM principal -> Control Plane principal (the core's bindings)
    principals: dict[uuid.UUID, uuid.UUID] = field(default_factory=dict)
    approvals: dict[uuid.UUID, FakeApproval] = field(default_factory=dict)
    replays: dict[str, dict[str, Any]] = field(default_factory=dict)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    down: bool = False

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def approval(self, eligible: set[uuid.UUID]) -> FakeApproval:
        approval = FakeApproval(uuid.uuid4(), eligible)
        self.approvals[approval.id] = approval
        return approval

    def out(self, approval: FakeApproval) -> dict[str, Any]:
        return ApprovalOut(
            id=approval.id,
            tenant_id=uuid.uuid4(),
            workspace_id=None,
            task_id=None,
            artifact_id=None,
            requested_by_principal_id=uuid.uuid4(),
            status=approval.status,
            gate=True,
            required_role_id=None,
            assigned_principal_id=None,
            decision_by_principal_id=approval.decision_by,
            decision_at=approval.decision_at,
            comment="",
            version=1,
            outcome_status=None,
            created_at=NOW,
            updated_at=NOW,
        ).model_dump(mode="json", by_alias=True)

    @staticmethod
    def _error(status: int, code: str) -> httpx.Response:
        return httpx.Response(
            status, json={"error": {"code": code, "message": code, "details": {}}}
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            return self._error(503, "dependency_unavailable")
        path = request.url.path
        if not path.startswith("/api/v1/approvals/"):
            return self._error(404, "not_found")
        token = request.headers["authorization"].removeprefix("Bearer ")
        if request.method == "GET":
            if token in self.iam.tokens:
                return self._error(403, "outside_purpose")
            approval = self.approvals.get(uuid.UUID(path.rsplit("/", 1)[1]))
            if approval is None:
                return self._error(404, "not_found")
            return httpx.Response(200, json=self.out(approval))
        return self._decide(request, path, token)

    def _decide(self, request: httpx.Request, path: str, token: str) -> httpx.Response:
        issued = self.iam.tokens.get(token)
        if issued is None:
            return self._error(401, "invalid_credentials")
        try:
            # The core's own check of a decision token against the request.
            decision_purpose({"purpose_ref": issued.purpose_ref}, f"POST {path}")
        except AuthorizationError as exc:
            return self._error(403, exc.code)
        key = request.headers.get("idempotency-key")
        if key is None:
            return self._error(422, "idempotency_key_required")
        replay_key = f"{issued.iam_principal_id}:{key}"
        if replay_key in self.replays:
            return httpx.Response(
                200, json=self.replays[replay_key], headers={"Idempotency-Replayed": "true"}
            )
        raw_id, _, verb = path.rsplit("/", 1)[1].partition(":")
        approval = self.approvals.get(uuid.UUID(raw_id))
        if approval is None:
            return self._error(404, "not_found")
        if approval.status != "pending":
            return self._error(409, "approval_already_decided")
        if issued.iam_principal_id not in approval.eligible:
            return self._error(403, "not_eligible")
        approval.status = "approved" if verb == "approve" else "rejected"
        approval.decision_by = self.principals.get(issued.iam_principal_id)
        approval.decision_at = NOW
        body = self.out(approval)
        self.replays[replay_key] = body
        self.decisions.append({"approval": approval.id, "verb": verb, "key": key})
        return httpx.Response(200, json=body)
